/*
Copyright (c) 2007-2011 VMware, Inc. All Rights Reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted (subject to the limitations in the
disclaimer below) provided that the following conditions are met:

* Redistributions of source code must retain the above copyright
   notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the
   distribution.

* Neither the name of VMware nor the names of its
   contributors may be used to endorse or promote products derived
   from this software without specific prior written permission.

NO EXPRESS OR IMPLIED LICENSES TO ANY PARTY'S PATENT RIGHTS ARE
GRANTED BY THIS LICENSE.  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT
HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/* **********************************************************
 * **********************************************************/

/*
 * logfs.c --
 *
 * This is the vmkernel component of CloudFS. This file mostly interface with
 * the vmkernel. Virtual disk emulation is in vDisk.c.
 *
 */

#include "vm_types.h"
#include "vm_libc.h"
#include "vmkernel.h"
#include "splock.h"
#include "world.h"
#include "fss_int.h"
#include "logfs_int.h"
#include "fs_common.h"
#include "fsSwitch.h"
#include "scattergather.h"
#include "scsi_ext.h"
#include "scsi_vmware.h"
#include "timer.h"
#include "helper_ext.h"
#include "semaphore_ext.h"
#include "util.h"
#include "config.h"
#include "libc.h"
#include "volumeCache.h"
#include "objectCache.h"
#include "vcfs.h"
#include "stopwatch.h"
#include "kseg_dist.h"
#include "srm_ext.h"
#include "fsvcb_public.h"
#include "devfs.h"
#include "deviceLib.h"
#include "fsdisk.h"

#include "heapsort.h"

#include "system.h"
#include "logfsConstants.h"
#include "logfsHash.h"
#include "globals.h"
#include "metaLog.h"
#include "vDisk.h"
#include "vDiskMap.h"
#include "logfsDiskLayout.h"
#include "pagedTree.h"
#include "logfsHttpd.h"
#include "logfsHttpClient.h"
#include "common.h"

#define LOGFS_FILE_SIZE (LogFS_VDiskGetCapacity(NULL))   // XXX
#define LOGFS_PROC_READ_LENGTH 4096

static FDS_DriverID logfsDriverID;
static struct DevLib_Object *logfsDevLibObj = NULL;

static LogFS_MetaLog *globalMetaLog;

static World_ID compactorWorld;
static void LogFS_GC(void *data);
static Bool gcExit = FALSE;


typedef struct {
   World_ID worldID;
   LogFS_MetaLog *log;
   char *buffer;
   size_t headleft;
   size_t bodyleft;
   int buf_offs;
   int state;
   size_t totalBytes;
   List_Links appendDataList;
} LogFS_AppendData;

static List_Links appendDataList;

typedef enum {
   LogFS_FileModeRawDisk = 1,
   LogFS_FileModeCurrentId,
   LogFS_FileModeMetaData,
   LogFS_FileModeSecret,
} LogFS_FileMode;

typedef struct {
   LogFS_FileMode mode;
   char name[64];
   void *vd;
   List_Links vDiskInfoList;
} VDiskInfo;                    //XXX 

List_Links vDiskInfoList;


static VMK_ReturnStatus
LogFSGetIdCapCB(void *data,
                uint32 partitionNum, uint64 *numBlocks, uint32 *blockSize)
{
   *blockSize = 40;
   *numBlocks = 1;
   return VMK_OK;
}


#if 0
static VMK_ReturnStatus
LogFSGetCmdCapCB(void *data,
                 uint32 partitionNum, uint64 *numBlocks, uint32 *blockSize)
{
   *blockSize = *numBlocks = sizeof(LogFS_FetchCommand);
   return VMK_OK;
}
#endif

static VMK_ReturnStatus
LogFSGetCtlCapCB2(void *data,
                  uint32 partitionNum, uint64 *numBlocks, uint32 *blockSize)
{
// VDiskInfo* info = data;

   *blockSize = SHA1_HEXED_SIZE - 1;
   *numBlocks = 1;

#if 0
   if (VDISK_INTERFACE(info->vd)->type == VDI_VDISK) {
      LogFS_VDisk *vd = (LogFS_VDisk *)info->vd;
      if (LogFS_HashIsValid(vd->secret_parent))
         *numBlocks = 1;
   }
#endif
   return VMK_OK;
}

static VMK_ReturnStatus
LogFSGetVDiskCapCB(void *data,
                   uint32 partitionNum, uint64 *numBlocks, uint32 *blockSize)
{
   *blockSize = BLKSIZE;
   *numBlocks = LOGFS_FILE_SIZE / BLKSIZE;
   return VMK_OK;
}

int time_before_gc = 10;

void postpone_gc(void)
{
   time_before_gc = 10;
}

void schedule_gc(void)
{
   while (time_before_gc > 0) {
      CpuSched_Sleep(50);
      --time_before_gc;
   }
}



typedef struct {
   void *tmp;
   void *sg;
} LogFS_FreeBufferContext;

void LogFS_FreeSimpleBuffer(Async_Token * token, void *data)
{
   void **pBuf = data;
   aligned_free(*pBuf);

   Async_TokenCallback(token);
}

void LogFS_UnmapBuffer(Async_Token * token, void *data)
{
   void **pBuf = data;
   Kseg_ReleaseVA(*pBuf);
   Async_TokenCallback(token);
}

void LogFS_FreeSimpleBufferAndToken(Async_Token * token, void *data)
{
   void **pBuf = data;
   aligned_free(*pBuf);

   Async_TokenCallback(token);
   Async_ReleaseToken(token);
}

void LogFS_ReleaseBuffer(Async_Token * token, void *data)
{
   void **pRb = data;
   LogFS_RefCountedBuffer *rb = *pRb;

   LogFS_RefCountedBufferRelease(rb);
   Async_TokenCallback(token);
}

/* Copy read data from buffer to sg, and free the sg meta-data and the buffer
 * after use. XXX we should be passing sg's around instead */

void LogFS_FreeBuffer(Async_Token * token, void *data)
{
   LogFS_FreeBufferContext *c = data;

   int i;
   char *b = c->tmp;
   SG_Array *sgArr = (SG_Array *) c->sg;

   uint64 baseOffset = sgArr->sg[0].offset;

   for (i = 0; i < sgArr->length; i++) {
      uint64 length = sgArr->sg[i].length;
      uint64 off = sgArr->sg[i].offset - baseOffset;

      if (sgArr->addrType == SG_MACH_ADDR) {
         char *dst = Kseg_MapMA(sgArr->sg[i].addr, length);
         ASSERT(dst);
         memcpy(dst, b + off, length);
         Kseg_ReleaseVA(dst);
      } else {
         char *dst = (char *)sgArr->sg[i].addr;
         memcpy(dst, b + off, length);
      }
   }

   aligned_free(c->tmp);
   free(c->sg);

   Async_TokenCallback(token);
}

void LogFS_RemoveDiskDevices(void)
{
   VMK_ReturnStatus status;
   void *driverData;
   List_Links *curr, *next;

   LIST_FORALL_SAFE(&vDiskInfoList, curr, next) {
      VDiskInfo *info = List_Entry(curr, VDiskInfo, vDiskInfoList);
      LogFS_VDisk *vd = info->vd;

      zprintf("remove dev %s\n", info->name);
      status = DevLib_DestroyDevice(logfsDevLibObj, info->name, &driverData);

      if (info->mode == LogFS_FileModeRawDisk) {
         LogFS_VDiskCleanup((LogFS_VDisk *)vd);
         free(vd);
      }

      List_Remove(curr);
      free(info);
   }

}

#if 1
VMK_ReturnStatus LogFS_MakeDiskDevice(LogFS_VDisk *vd)
{
   VMK_ReturnStatus status;
   VDiskInfo *info;

   LogFS_Hash disk = LogFS_VDiskGetBaseId(vd);

   int wr = (LogFS_VDiskIsWritable(vd));

   info = malloc(sizeof(VDiskInfo));
   LogFS_HashPrint(info->name, &disk);
   List_Insert(&info->vDiskInfoList, LIST_ATREAR(&vDiskInfoList));
   info->vd = vd;
   info->mode = LogFS_FileModeRawDisk;
   status = DevLib_CreateDevice(logfsDevLibObj, logfsDriverID, info->name, LogFSGetVDiskCapCB, 0600, wr, info);   /* Only announce when writable */

   if (status != VMK_OK)
      return status;

   info = malloc(sizeof(VDiskInfo));
   LogFS_HashPrint(info->name, &disk);
   strcat(info->name, ".info");
   List_Insert(&info->vDiskInfoList, LIST_ATREAR(&vDiskInfoList));
   info->vd = vd;
   info->mode = LogFS_FileModeMetaData;
   status = DevLib_CreateDevice(logfsDevLibObj, logfsDriverID,
                                info->name, LogFSGetCtlCapCB2,
                                0600, FALSE, info);

   if (status != VMK_OK)
      return status;

   info = malloc(sizeof(VDiskInfo));
   LogFS_HashPrint(info->name, &disk);
   strcat(info->name, ".id");
   List_Insert(&info->vDiskInfoList, LIST_ATREAR(&vDiskInfoList));
   info->vd = vd;
   info->mode = LogFS_FileModeCurrentId;
   status = DevLib_CreateDevice(logfsDevLibObj, logfsDriverID,
                                info->name, LogFSGetIdCapCB, 0600, FALSE, info);

   if (status != VMK_OK)
      return status;

   info = malloc(sizeof(VDiskInfo));
   LogFS_HashPrint(info->name, &disk);
   strcat(info->name, ".secret");
   List_Insert(&info->vDiskInfoList, LIST_ATREAR(&vDiskInfoList));
   info->vd = vd;
   info->mode = LogFS_FileModeSecret;
   status = DevLib_CreateDevice(logfsDevLibObj, logfsDriverID,
                                info->name, LogFSGetCtlCapCB2,
                                0600, FALSE, info);

   return status;
}
#endif

VMK_ReturnStatus
LogFS_HandleViewChange(struct log_head * head,
                       LogFS_VDisk **retVd, LogFS_MetaLog *ml)
{
   zprintf("%s\n", __FUNCTION__);
   VMK_ReturnStatus status = VMK_OK;

   Hash id, parent;
   LogFS_HashSetRaw(&id, head->id);
   LogFS_HashSetRaw(&parent, head->parent);

   LogFS_VDisk *vd = NULL;

   /* An empty disk is a branch with no parent */
   if (LogFS_HashIsNull(parent)) {
      zprintf("parent was null\n");
      if (LogFS_DiskMapLookupDisk(id)) {
         zprintf("%s exists already (A)\n", LogFS_HashShow(&id));
         status = VMK_EXISTS;
         goto out;
      }

      vd = malloc(sizeof(LogFS_VDisk));
      LogFS_VDiskInit(vd, NULL, id, ml);

      LogFS_DiskMapInsert(vd);

#if 1
      status = LogFS_MakeDiskDevice(vd);

      if (status != VMK_OK) {
         zprintf("can't make device\n");
         goto out;
      }
#endif
   }

   /* is this a branch off an existing vd that we have? */
   else if ((vd = LogFS_DiskMapLookupDiskForVersion(parent))) {
      zprintf("is branch with parent, id is %s\n", LogFS_HashShow(&id));
      LogFS_VDisk *subDisk;
      LogFS_VDisk *snapshot = NULL;

      /* Make sure we don't have this branch mirrored
       * already */
      if (LogFS_DiskMapLookupDisk(id)) {
         status = VMK_EXISTS;
         zprintf("%s exists already (B)\n", LogFS_HashShow(&id));
         goto out;
      }

      status = LogFS_VDiskSnapshot(vd, &snapshot);
      if (status != VMK_OK)
         goto out;

      subDisk = malloc(sizeof(LogFS_VDisk));
      LogFS_VDiskInit(subDisk, snapshot, id, ml);

      LogFS_DiskMapInsert(subDisk);
#if 0
      status = LogFS_MakeDiskDevice(subDisk);
      if (status != VMK_OK) {
         zprintf("device create failed\n");
         goto out;
      }
#endif
      vd = subDisk;

      status = VMK_OK;
   } else {
      /* It's probably a view-change then */
      vd = LogFS_DiskMapLookupDiskForVersion(LogFS_HashApply(parent));
      if (vd)
         zprintf("was view change\n");

   }
 out:

   *retVd = vd;
   return status;
}

VMK_ReturnStatus
LogFS_AppendEntry(LogFS_RefCountedBuffer *rb,
                  Async_Token * token, LogFS_MetaLog *ml)
{
   VMK_ReturnStatus status;
   Bool needsCallback = TRUE;

   struct log_head *head = rb->buffer;

   unsigned char checksum[SHA1_DIGEST_SIZE];
   log_entry_checksum(checksum, head, ((char *)head) + BLKSIZE,
                      log_body_size(head));

   if (memcmp(checksum, head->update.checksum, SHA1_DIGEST_SIZE) != 0) {
      zprintf("checksum mismatch %" FMT64 "d+%d!\n",
              head->update.blkno, head->update.num_blocks);
      status = VMK_WRITE_ERROR;
      goto out;
   }

   LogFS_Hash diskId;
   LogFS_HashSetRaw(&diskId, head->disk);
   if (LogFS_HashIsNull(diskId)) {
      status = VMK_INVALID_HANDLE;
      goto out;
   }

   LogFS_Hash parent;
   LogFS_HashSetRaw(&parent, head->parent);
   LogFS_Hash id;
   LogFS_HashSetRaw(&id, head->id);

   LogFS_VDisk *vd = NULL;
#if 0
   LogFS_VDisk *orphan = NULL;

   if ((orphan = LogFS_DiskMapLookupOrphan(id))) {
      zprintf("Found orphan!\n");
      orphan->parentDisk = vd;
   }
#endif

   if (head->tag == log_entry_type && head->update.blkno == METADATA_BLOCK) {
      status = LogFS_HandleViewChange(head, &vd, ml);

      if (status == VMK_EXISTS) {
         zprintf("ignoring double branch creation\n");
         status = VMK_OK;
         goto out;
      }

      if (status != VMK_OK) {
         zprintf("leave!\n");
         goto out;
      }
   }

   else if (head->tag == log_entry_type) {
      vd = LogFS_DiskMapLookupDiskForVersion(LogFS_HashApply(parent));
      if (vd != NULL)
         status = VMK_OK;
   }

   if (vd == NULL) {
      zprintf("vd not found, tag %u!\n", head->tag);
      status = VMK_INVALID_HANDLE;
      goto out;

   }

   status = LogFS_VDiskAppend(vd, token, rb);
   needsCallback = FALSE;

   if (status != VMK_OK) {
      zprintf("append failed!\n");
      goto out;
   }

 out:
   if (needsCallback) {
      token->transientStatus = status;
      Async_TokenCallback(token);
   }
   return status;
}

VMK_ReturnStatus
LogFS_AppendRawBytes(Async_Token * token,
                     LogFS_AppendData * d, const SG_Array * sgArr)
{
   VMK_ReturnStatus status = VMK_OK;

   Async_IOHandle *ioh;
   Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);

   int i;
   for (i = 0; i < sgArr->length; i++) {
      //zprintf("sg %2d: offset %"FMT64"x len %x\n",i,sgArr->sg[i].offset,sgArr->sg[i].length);

      void *buf = (void *)(VA) sgArr->sg[i].addr;
      size_t length = sgArr->sg[i].length;

      ASSERT(length > 0);

      if (sgArr->addrType == SG_MACH_ADDR) {
         //zprintf("mapping some %lu bytes of virtual memory\n",length);
         buf = Kseg_MapMA(sgArr->sg[i].addr, length);
         if (!buf) {
            zprintf("mapping failure\n");
            status = VMK_FAILURE;
            goto out;
         }
      }

      char *in = (char *)buf;
      size_t inputleft = length;

      if (i == 0) {
         size_t offset = sgArr->sg[i].offset;
         if (offset < d->totalBytes) {
            size_t skip = d->totalBytes - offset;
            //zprintf("repeated write, skip %lu\n",skip);

            if (skip > inputleft)
               continue;

            in += skip;
            inputleft -= skip;
         }
      }

      status = VMK_OK;

      while (inputleft > 0) {
         if (d->state == 1)     // head
         {
            size_t sz;

            /* Handle common case: seeing the complete log entry in one piece,
             * without buffering */

#if 0
            if (d->headleft == LOG_HEAD_SIZE
                && (sz = log_entry_size((struct log_head *)in))
                && (sz <= inputleft)) {
               zprintf("direct %ld left %ld\n", sz, inputleft);
               Async_Token *childToken = Async_PrepareOneIO(ioh, NULL);
               status =
                   LogFS_AppendEntry((struct log_head *)in, sz, childToken,
                                     d->log);
               d->totalBytes += sz;

               if (status != VMK_OK)
                  break;

               inputleft -= sz;
               in += sz;
            }

            else
#endif
            {
               size_t take = inputleft < d->headleft ? inputleft : d->headleft;

               if (d->headleft == LOG_HEAD_SIZE) {
                  sz = log_entry_size((struct log_head *)in);
                  d->buffer = aligned_malloc(sz);
               }

               memcpy(d->buffer + d->buf_offs, in, take);
               d->buf_offs += take;
               in += take;

               d->headleft -= take;
               inputleft -= take;

               if (d->headleft == 0) {
                  struct log_head *head = (struct log_head *)d->buffer;

                  printf("%" FMT64 "d + %d\n", head->blkno, head->num_blocks);
                  if (head->tag == log_entry_type
                      && (head->update.num_blocks > LOG_HEAD_MAX_BLOCKS
                          || head->update.num_blocks == 0)) {
                     zprintf("too many blocks %d\n", head->update.num_blocks);
                     status = VMK_WRITE_ERROR;
                     goto out;
                  }

                  d->bodyleft = log_body_size(head);

                  if (d->bodyleft > LOG_HEAD_MAX_BLOCKS * BLKSIZE) {
                     zprintf("too large %" FMT64 "u\n", d->bodyleft);
                     status = VMK_WRITE_ERROR;
                     goto out;
                  }

                  d->state = 2;
               }
            }
         }

         if (d->state == 2)     // body
         {
            size_t take = inputleft < d->bodyleft ? inputleft : d->bodyleft;

            memcpy(d->buffer + d->buf_offs, in, take);
            d->buf_offs += take;
            in += take;

            d->bodyleft -= take;
            inputleft -= take;

            if (d->bodyleft == 0) {
               //zprintf("buffered\n"); 

               /* Reset the state machine to by ready for next entry. 
                * Better to do it here, because we want to have a clean
                * slate regardless of any error conditions below */

               size_t sz = d->buf_offs;   /* Need this value later */

               d->buf_offs = 0;
               d->state = 1;
               d->headleft = LOG_HEAD_SIZE;

               struct log_head *head = (struct log_head *)d->buffer;

               Async_Token *childToken = Async_PrepareOneIO(ioh, NULL);

               LogFS_RefCountedBuffer *rb =
                   malloc(sizeof(LogFS_RefCountedBuffer));
               Atomic_Write(&rb->refCount, 1);
               rb->buffer = head;
               rb->count = sz;

               *((void **)Async_PushCallbackFrame(childToken,
                                                  LogFS_ReleaseBuffer,
                                                  sizeof(void *))) = rb;

               status = LogFS_AppendEntry(rb, childToken, d->log);
               d->totalBytes += sz;

               if (status != VMK_OK)
                  break;
            }
         }
      }

      if (sgArr->addrType == SG_MACH_ADDR) {
         Kseg_ReleaseVA(buf);
      }

      if (status != VMK_OK)
         break;
   }

 out:
   if (status != VMK_OK)
      zprintf("bad status! \n");
   Async_EndSplitIO(ioh, VMK_OK, FALSE);

   return status;
}

VMK_ReturnStatus LogFS_CopyToSg(const SG_Array * sgArr, void *src,
                                uint32 *bytesTransferred)
{
   VMK_ReturnStatus status;

   unsigned offset = 0;
   unsigned mapOffset = 0;
   unsigned mapLength = 0;
   unsigned length = LOGFS_PROC_READ_LENGTH;
   void *va;
   int i;

   /* code lifted from procfs */
   if (*bytesTransferred <= sgArr->sg[0].offset) {
      *bytesTransferred = 0;
      status = VMK_OK;
      zprintf("already done,  sg off %ld\n", sgArr->sg[0].offset);
      goto done;
   }

   *bytesTransferred -= sgArr->sg[0].offset;

   for (i = 0; i < sgArr->length; i++) {
      length = MIN(*bytesTransferred - offset, sgArr->sg[i].length);
      if (!length) {
         break;
      }
      if (sgArr->addrType == SG_MACH_ADDR) {
         mapOffset = 0;
         while (mapOffset < length) {
            mapLength = MIN(length - mapOffset, PAGE_SIZE);
            va = Kseg_MapMA(sgArr->sg[i].addr + mapOffset, mapLength);
            if (!va) {
               status = VMK_NO_MEMORY;
               zprintf("map failed\n");
               goto done;
            }
            memcpy(va, src + offset + sgArr->sg[0].offset, mapLength);
            Kseg_ReleaseVA(va);
            mapOffset += mapLength;
            offset += mapLength;
         }
      } else if (length) {
         va = (void *)(VA) sgArr->sg[i].addr;
         memcpy(va, src + offset + sgArr->sg[0].offset, length);
         offset += length;
      }
   }

   *bytesTransferred = offset;
   status = VMK_OK;

 done:
   return status;
}

VMK_ReturnStatus
LogFS_CopyFromSg(void *dst, const SG_Array * sgArr, uint32 *bytesTransferred)
{
   VMK_ReturnStatus status;
   //zprintf("%s %u\n",__FUNCTION__,*bytesTransferred);

   unsigned offset = 0;
   unsigned mapOffset = 0;
   unsigned mapLength = 0;
   unsigned length = LOGFS_PROC_READ_LENGTH;
   void *va;
   int i;

   if (*bytesTransferred <= sgArr->sg[0].offset) {
      *bytesTransferred = 0;
      status = VMK_OK;
      goto done;
   }

   *bytesTransferred -= sgArr->sg[0].offset;

   for (i = 0; i < sgArr->length; i++) {
      //zprintf("bytesTransferred %u %u\n",*bytesTransferred,offset);
      length = MIN(*bytesTransferred - offset, sgArr->sg[i].length);
      if (!length) {
         break;
      }
      if (sgArr->addrType == SG_MACH_ADDR) {
         mapOffset = 0;
         while (mapOffset < length) {
            mapLength = MIN(length - mapOffset, PAGE_SIZE);
            va = Kseg_MapMA(sgArr->sg[i].addr + mapOffset, mapLength);
            if (!va) {
               status = VMK_NO_MEMORY;
               goto done;
            }
            //zprintf("cpy %p <- %p, %d\n",dst + offset + sgArr->sg[0].offset,va,length);
            memcpy(dst + offset + sgArr->sg[0].offset, va, mapLength);
            Kseg_ReleaseVA(va);
            mapOffset += mapLength;
            offset += mapLength;
         }
      } else if (length) {
         va = (void *)(VA) sgArr->sg[i].addr;
         //zprintf("cpy %p <- %p, %d\n",dst + offset + sgArr->sg[0].offset,va,length);
         memcpy(dst + offset + sgArr->sg[0].offset, va, length);
         offset += length;
      }
   }

   *bytesTransferred = offset;
   status = VMK_OK;

 done:
   return status;
}

/**** from deltadisk! **********************/

typedef void *LogFS_HandleInfo;

static char logfsStrLog[] = "log";
static char logfsStrFastLog[] = "fastlog";

VMK_ReturnStatus LogFS_CreateLogDeviceState(void **result, Bool isFast)
{
   LogFS_MetaLog *ml = globalMetaLog;

   if (ml == NULL)
      return VMK_INVALID_HANDLE;

   LogFS_AppendData *ad = malloc(sizeof(LogFS_AppendData));
   memset(ad, 0, sizeof(ad));
   ASSERT(ad);

   ad->buffer = NULL;
   ad->log = ml;
   ad->headleft = LOG_HEAD_SIZE;
   ad->bodyleft = 0;
   ad->buf_offs = 0;
   ad->totalBytes = 0;
   ad->state = 1;

   *result = ad;
   return VMK_OK;
}

static VMK_ReturnStatus
LogFS_OpenDevice(World_ID worldID,
                 const FSS_ObjectID * dirOID,
                 const char *deviceName, int flags, FDS_HandleID * handleID)
{
   VMK_ReturnStatus status;
   FDS_HandleID hid;

   if (flags & FILEDRIVER_OPEN_LVMFILE) {
      zprintf("tried LVMFILE!\n");
      return VMK_BAD_PARAM;
   }

   /* update control HIDs */

   if (strcmp(deviceName, logfsStrLog) == 0
       || strcmp(deviceName, logfsStrFastLog) == 0) {

      status = DevLib_OpenDevice(logfsDevLibObj, deviceName, flags, &hid);
      if (status != VMK_OK) {
         return status;
      }

      LogFS_AppendData *ad;
      status =
          LogFS_CreateLogDeviceState((void **)&ad,
                                     (strcmp(deviceName, logfsStrFastLog) ==
                                      0));

      List_Insert(&ad->appendDataList, LIST_ATREAR(&appendDataList));

      status = VMK_OK;
   }

   else {
      LogFS_Hash disk;

      if (LogFS_HashSetString(&disk, deviceName)) {
         LogFS_VDisk *vd = LogFS_DiskMapLookupDisk(disk);
         if (vd == NULL) {
            status = VMK_INVALID_HANDLE;
            return status;
         }
      }

      status = DevLib_OpenDevice(logfsDevLibObj, deviceName, flags, &hid);
      if (status != VMK_OK) {
         return status;
      }
   }

   if (status == VMK_OK) {
      *handleID = hid;
      vmk_ModuleIncUseCount(logfsModuleID);
   }

   return status;
}

static VMK_ReturnStatus
LogFS_CloseDevice(World_ID worldID, FDS_HandleID fdsHandleID)
{
   VMK_ReturnStatus status;
   LogFS_HandleInfo *handleInfo;

   handleInfo = DevLib_HandleIDToDriverData(logfsDevLibObj, fdsHandleID);
   char *handleStr = (char *)handleInfo;

   status = DevLib_CloseDevice(logfsDevLibObj, fdsHandleID);

   if (handleStr == logfsStrLog) {
      World_ID leader;
      leader = World_GetGroupLeaderID(MY_RUNNING_WORLD);

      List_Links *curr, *next;
      LIST_FORALL_SAFE(&appendDataList, curr, next) {
         LogFS_AppendData *ad =
             List_Entry(curr, LogFS_AppendData, appendDataList);
         if (ad->worldID == leader) {
            List_Remove(curr);
            free(ad);
            break;
         }
      }
   }

   vmk_ModuleDecUseCount(logfsModuleID);
   return status;
}

char *logfsControlNodeStrings[] = {
   logfsStrLog,
   logfsStrFastLog,
   NULL,
};

static VMK_ReturnStatus
LogFS_AsyncIO(FDS_HandleID fdsHandleID,
              const SG_Array * sgArr, IO_Flags ioFlags, Async_Token * token)
{
   VMK_ReturnStatus status = VMK_OK;
   int i;

   LogFS_HandleInfo *handleInfo;

   /* For some reason VMFS3 heartbeats don't set the FS_CANTBLOCK block flag,
    * and this can cause some problems. Make sure we don't try to block when
    * called from a BH or interrupt context */
   if (PRDA_BHInProgress() || PRDA_InInterruptHandler())
      ioFlags |= FS_CANTBLOCK;

   handleInfo = DevLib_HandleIDToDriverData(logfsDevLibObj, fdsHandleID);
   ASSERT(handleInfo);
   if (!handleInfo) {
      return VMK_INVALID_HANDLE;
   }

   char *handleStr = (char *)handleInfo;
   if (handleStr == logfsStrLog || handleStr == logfsStrFastLog) {
      if (ioFlags & FS_WRITE_OP) {
         ASSERT((ioFlags & FS_CANTBLOCK) == 0);

         status = VMK_NOT_FOUND;

         World_ID leader;
         leader = World_GetGroupLeaderID(MY_RUNNING_WORLD);

         LogFS_AppendData *ad = NULL;
         List_Links *curr;
         LIST_FORALL(&appendDataList, curr) {
            ad = List_Entry(curr, LogFS_AppendData, appendDataList);
            if (ad->worldID == leader) {
               status = LogFS_AppendRawBytes(token, ad, sgArr);
               break;
            }
         }
         if (status == VMK_NOT_FOUND) {
            /* Wing it */
            if (ad) {
               status = LogFS_AppendRawBytes(token, ad, sgArr);
            } else
               zprintf("not found %x\n", leader);
         }

         return status;
      }
      return VMK_NO_PERMISSION;

   }

   /* if not a special file, this is vDisk IO */
   else {
      VDiskInfo *info = (VDiskInfo *) handleInfo;
      LogFS_VDisk *vd = info->vd;

      if (info->mode == LogFS_FileModeMetaData) {
         if (ioFlags & FS_READ_OP) {
            if (sgArr->sg[0].offset == 0) {
               uint32 bytesTransferred = SHA1_HEXED_SIZE - 1;
               char sEntropy[SHA1_HEXED_SIZE];
               LogFS_HashPrint(sEntropy, &vd->entropy);
               status = LogFS_CopyToSg(sgArr, sEntropy, &bytesTransferred);

               if (token) {
                  token->transientStatus = status;
                  Async_TokenCallback(token);
               }

            } else
               status = VMK_READ_ERROR;
            return status;
         }
      }

      if (info->mode == LogFS_FileModeSecret) {
         if (ioFlags & FS_READ_OP) {
            if (sgArr->sg[0].offset == 0) {
               uint32 bytesTransferred = SHA1_HEXED_SIZE - 1;
               LogFS_Hash id;
               status = LogFS_VDiskGetSecret(vd, &id, FALSE);

               if (status == VMK_OK) {
                  char sSecret[SHA1_HEXED_SIZE];
                  LogFS_HashPrint(sSecret, &id);
                  status = LogFS_CopyToSg(sgArr, sSecret, &bytesTransferred);
               } else {
                  char sZero[SHA1_HEXED_SIZE];
                  memset(sZero, 0, sizeof(sZero));
                  status = LogFS_CopyToSg(sgArr, sZero, &bytesTransferred);
               }

               if (token) {
                  token->transientStatus = status;
                  Async_TokenCallback(token);
               }

            } else
               status = VMK_READ_ERROR;
            return status;
         }
         if (ioFlags & FS_WRITE_OP) {
            if (sgArr->sg[0].offset == 0) {
               char *sSecret = malloc(BLKSIZE);

               uint32 bytesTransferred = SHA1_HEXED_SIZE_UNTERMINATED * 2;
               status = LogFS_CopyFromSg(sSecret, sgArr, &bytesTransferred);

               if (status == VMK_OK) {
                  if (strlen(sSecret) >= SHA1_HEXED_SIZE_UNTERMINATED * 2) {
                     Hash secret;
                     Hash secretView;

                     if (LogFS_HashSetString(&secret, sSecret) &&
                         LogFS_HashSetString(&secretView,
                                             sSecret +
                                             SHA1_HEXED_SIZE_UNTERMINATED)) {
                        status = LogFS_VDiskSetSecret(vd, secret, secretView);
                     } else {
                        zprintf("could not decode input string\n");
                        status = VMK_BAD_PARAM;
                     }
                  }
               }

               if (token) {
                  token->transientStatus = status;
                  Async_TokenCallback(token);
               }

               free(sSecret);

            } else
               status = VMK_WRITE_ERROR;
            return status;
         }
      }

      if (info->mode == LogFS_FileModeCurrentId && (ioFlags & FS_READ_OP)) {
         if (sgArr->sg[0].offset == 0) {
            LogFS_Hash id = LogFS_VDiskGetCurrentId(vd);

            if (status == VMK_OK) {
               char sId[SHA1_HEXED_SIZE];
               LogFS_HashPrint(sId, &id);
               uint32 bytesTransferred = SHA1_HEXED_SIZE_UNTERMINATED;
               status = LogFS_CopyToSg(sgArr, sId, &bytesTransferred);
            }

            if (token) {
               token->transientStatus = status;
               Async_TokenCallback(token);
            }

         }
         return status;
      }

      ASSERT(token);

      postpone_gc();

      uint32 bytes = SG_TotalLength(sgArr);

      if (ioFlags & FS_WRITE_OP) {
         printf("writing %u bytes\n", bytes);
         char *buffer = aligned_malloc(bytes);
         ASSERT(buffer);

         void **pva =
             Async_PushCallbackFrame(token, LogFS_FreeSimpleBuffer,
                                     sizeof(void *));
         *pva = buffer;

         uint64 baseOffset = sgArr->sg[0].offset;

         for (i = 0; i < sgArr->length; i++) {
            uint32 length = sgArr->sg[i].length;
            uint64 off = sgArr->sg[i].offset - baseOffset;

            ASSERT(length + off <= bytes);

            if (sgArr->addrType == SG_MACH_ADDR) {
               char *buf = Kseg_MapMA(sgArr->sg[i].addr, length);
               ASSERT(buf);
               memcpy(buffer + off, buf, length);
               Kseg_ReleaseVA(buf);
            } else {
               memcpy(buffer + off, (void *)sgArr->sg[i].addr, length);
            }
         }

         status =
             LogFS_VDiskWrite(vd, token, buffer, sgArr->sg[0].offset / BLKSIZE,
                              bytes / BLKSIZE, ioFlags);
      }

      else if (ioFlags & FS_READ_OP) {
         char *buffer = aligned_malloc(bytes);
         ASSERT(buffer);

         LogFS_FreeBufferContext *c =
             Async_PushCallbackFrame(token, LogFS_FreeBuffer,
                                     sizeof(LogFS_FreeBufferContext));

         size_t arraySize = sizeof(SG_Array) + sizeof(SG_Elem) * sgArr->length;
         c->tmp = buffer;
         c->sg = malloc(arraySize);
         ASSERT(c->sg);
         memcpy(c->sg, sgArr, arraySize); //XXX

         status = LogFS_VDiskRead(vd, token, buffer,
                                  sgArr->sg[0].offset / BLKSIZE,
                                  bytes / BLKSIZE, ioFlags);
      }
   }
   return status;
}

VMK_ReturnStatus LogFS_AddPhysicalDevice(const char *deviceName)
{
   VMK_ReturnStatus status;
   FDS_Handle *logfsFDSHandle = NULL;
   uint64 generation;
   log_id_t logEnd;
   disk_block_t superTreeRoot;

   /* Increase module refcount to prevent a parallel unload of the module
    * before we are completely done here */
   vmk_ModuleIncUseCount(logfsModuleID);

   status = FDS_OpenDevice(Host_GetWorldID(), FDS_ALL_DRIVERS,
                           deviceName, SCSI_OPEN_VMFS, &logfsFDSHandle);

   if (status != VMK_OK) {
      zprintf("bad status: %s\n", VMK_ReturnStatusToString(status));
      return status;
   }

   /************* global init ***************/

   LogFS_MetaLog *ml = (LogFS_MetaLog *)malloc(sizeof(LogFS_MetaLog));

   FDSI_GetCapacity result;
   status = FDS_Ioctl(logfsFDSHandle, FDS_IOCTL_GET_CAPACITY, &result);
   if (status != VMK_OK) {
      return status;
   }
   zprintf("capacity %" FMT64 "d\n",
           result.diskBlockSize * result.numDiskBlocks);

   LogFS_Device *device = malloc(sizeof(LogFS_Device));
   LogFS_DeviceInit(device, logfsFDSHandle);
   LogFS_DiskLayoutInit(&device->diskLayout,
                        result.diskBlockSize * result.numDiskBlocks);
   char *buf = aligned_malloc(BLKSIZE);
   memset(buf, 0, BLKSIZE);
   memcpy(buf, &device->diskLayout, sizeof(LogFS_DiskLayout));

   status = LogFS_DeviceWriteSimple(device, NULL, buf, BLKSIZE, 0,
                              LogFS_DiskHeaderSection);

   aligned_free(buf);

   if (status != VMK_OK) {
      zprintf("write header bad status %s\n", VMK_ReturnStatusToString(status));
      return status;
   }

   LogFS_MetaLogInit(ml, device);

   status = LogFS_InitHttpd(ml);

   if (status != VMK_OK) {
      return status;
   }

   Bool recover = ((LogFS_RecoverCheckPoint(ml, &generation, &logEnd, &superTreeRoot) == VMK_OK));

   /* Init B-tree AFTER recovering the checkpoint, as otherwise the checkpointer
   * thread (which runs from the B-tree right now) may race ahead of recovery! */

   LogFS_BTreeRangeMapPreInit(ml);

   if (recover) {
      LogFS_PagedTreeDiskReopen(ml,superTreeRoot);
      if (!is_invalid_version(logEnd)) {
         LogFS_ReplayFromCheckPoint(ml, logEnd);
      }
   } else {
      zprintf("formatting disk\n");
      generation = 0;
      LogFS_PagedTreeDiskReopen(ml,tree_null_block);
   }

   LogFS_BTreeRangeStartFlusher(ml, generation);

#if 1
   status = World_NewSystemWorld("logCompactor", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &compactorWorld);
   ASSERT(status == VMK_OK);
   Sched_Add(World_Find(compactorWorld), LogFS_GC, (void *)ml);
#endif

   globalMetaLog = ml;

   /********* end global init ***************/

   zprintf("device added!\n");

   FSS_Probe("logfs:cloudfs", FALSE);

   vmk_ModuleDecUseCount(logfsModuleID);
   return status;
}

VMK_ReturnStatus LogFS_RemovePhysicalDevice(LogFS_Device *device)
{
   VMK_ReturnStatus status;
   char **strPtr;
   for (strPtr = logfsControlNodeStrings; *strPtr; ++strPtr) {
      void *driverData;
      status = DevLib_DestroyDevice(logfsDevLibObj, *strPtr, &driverData);
      if (status != VMK_OK)
         break;
   }

   status = FDS_CloseDevice(device->fd);
   free(device);
   return status;
}

static VMK_ReturnStatus
LogFS_Ioctl(FDS_HandleID fdsHandleID, FDS_IoctlCmdType cmd, void *dataInOut)
{
   VMK_ReturnStatus status;
   LogFS_HandleInfo *handleInfo;
   handleInfo = DevLib_HandleIDToDriverData(logfsDevLibObj, fdsHandleID);

   if (handleInfo == NULL) {
      return VMK_NOT_FOUND;
   }

   switch (cmd) {
      case FDS_IOCTL_SET_RESV_MODE:
         {
            // Only supports SCSI-2 currently
            Bool *useSCSI3 = (Bool *)dataInOut;
            if (*useSCSI3) {
               /* XXX - Implemention required from VMFS4 with SCSI3 */
               LOG(0,"xhost support for SCSI3 VMFS4 using file backend is broken");
               return VMK_NOT_SUPPORTED;
            }
            return VMK_OK;
         }
      case FDS_IOCTL_CLEANUP_RESV_MODE:
         return VMK_OK;

#if 0
   case FDS_IOCTL_GET_VOLUMEINFO:
      {
         FDS_VolInfo *volInfo = (FDS_VolInfo *) dataInOut;

         VDiskInfo *info = (VDiskInfo *) handleInfo;
         Hash diskId = VDISK_INTERFACE(info->vd)->GetBaseId(info->vd);

         memset(volInfo->id, 0, sizeof(volInfo->id));
         LogFS_HashCopy(volInfo->id, diskId);

         return VMK_OK;
      }
#endif

#if 0
   case FDS_IOCTL_GET_PHYSICAL_DEVUUID:
      {
         zprintf("FDS_IOCTL_GET_PHYSICAL_DEVUUID\n");
         FDSI_GetPhysDevUUID *data = (FDSI_GetPhysDevUUID *) dataInOut;
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         Hash diskId = VDISK_INTERFACE(info->vd)->GetBaseId(info->vd);

         memcpy(data->devID, diskId.raw, sizeof(data->devID));

         break;
      }
#endif
   case FDS_IOCTL_GET_CANONICAL_NAME:
      {
         //zprintf("FDS_IOCTL_GET_CANONICAL_NAME\n");
         FDSI_GetCanonicalName *args = (FDSI_GetCanonicalName *) dataInOut;
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         Hash diskId = LogFS_VDiskGetBaseId(info->vd);

         // SCSI_GetDeviceName(handle, args->cname, args->maxLength);
         LogFS_HashPrint(args->cname, &diskId);

      }
      return VMK_OK;

   case FDS_IOCTL_GET_PHYSICAL_LAYOUT:
      {
         //zprintf("FDS_IOCTL_GET_PHYSICAL_LAYOUT\n");
         Log_Backtrace(MY_RUNNING_WORLD->worldID);

         FDSI_GetPhysLayout *data = (FDSI_GetPhysLayout *) dataInOut;
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         Hash diskId = LogFS_VDiskGetBaseId(info->vd);

         //       LogFS_HashPrint(data->devNames[0].peName,&diskId);
#if 0
         strncpy(data->devNames[0].peName, globalDeviceName,
                 sizeof(data->devNames[0].peName));
#endif

         data->devNames[0].peName[sizeof(data->devNames[0].peName) - 1] = '\0';
         data->devsReturned = 1;
         data->somethingOffline = FALSE;
         LogFS_HashPrint(data->logicalDevice, &diskId);

         return VMK_OK;
      }
   case FDS_IOCTL_GET_PHYSICAL_LAYOUT_EXT:
      {
         return VMK_INVALID_IOCTL;
      }

   case FDS_IOCTL_PERSISTENT_RESERVE:
   case FDS_IOCTL_PERSISTENT_RELEASE:
      return VMK_NOT_SUPPORTED;

   case FDS_IOCTL_LOCK_DEVICE: // XXX: Only SCSI-2 reserve for now
   case FDS_IOCTL_RESERVE_DEVICE:
      {
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         return LogFS_VDiskReserve(info->vd);
      }
   case FDS_IOCTL_UNLOCK_DEVICE: // XXX: Only SCSI-2 release for now
   case FDS_IOCTL_RELEASE_DEVICE:
      {
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         LogFS_VDiskRelease(info->vd);
         return VMK_OK;
      }

   case FDS_IOCTL_RESET_DEVICE:
   case FDS_IOCTL_REREAD_PARTITIONS:
      return VMK_OK;

   case FDS_IOCTL_PERSISTENT_REGISTER:
   case FDS_IOCTL_PERSISTENT_CLEARKEYS:
      return VMK_NOT_SUPPORTED;

   case FDS_IOCTL_GET_CAPACITY:
      {
         FDSI_GetCapacity *result = (FDSI_GetCapacity *) dataInOut;

         result->diskBlockSize = BLKSIZE;
         result->numDiskBlocks = LOGFS_FILE_SIZE / BLKSIZE;

         return VMK_OK;
      }

#if 0
   case FDS_IOCTL_GET_DISKID:
      {
         VDiskInfo *info = (VDiskInfo *) handleInfo;
         FDSI_GetDiskIdResult *args = (FDSI_GetDiskIdResult *) dataInOut;

         memset(&args->diskId, 0, sizeof(args->diskId));
         args->diskId.type = VMWARE_SCSI_ID_SCSISTK;
         LogFS_Hash id = VDISK_INTERFACE(info->vd)->GetBaseId(info->vd);
         LogFS_HashPrint((char *)&args->diskId.data.uid, &id);
         return VMK_OK;
      }

   case FDS_IOCTL_CMP_DISKID:
      {
         FDSI_CmpDiskIdArgs *args = (FDSI_CmpDiskIdArgs *) dataInOut;
         Hash h;
         LogFS_HashSetRaw(&h, (char *)&args->diskId->data.uid);

         VDiskInfo *info = (VDiskInfo *) handleInfo;
         LogFS_Hash id = VDISK_INTERFACE(info->vd)->GetBaseId(info->vd);
         char sId[SHA1_HEXED_SIZE];
         LogFS_HashPrint(sId, &id);

         /* Field sizes are checked at compile time. */
         if (args->diskId->type == VMWARE_SCSI_ID_SCSISTK &&
             memcmp(&args->diskId->data.uid, sId, SHA1_HEXED_SIZE - 1) == 0) {
            args->isSameDevice = TRUE;
         } else {
            args->isSameDevice = FALSE;
         }

         return VMK_OK;
      }
#else
   case FDS_IOCTL_GET_DISKID:
      {
         FDSI_GetDiskIdResult *getDiskIdResult =
             (FDSI_GetDiskIdResult *) dataInOut;
         getDiskIdResult->diskId.type = VMWARE_SCSI_ID_UNIQUE;
         return VMK_OK;
      }
   case FDS_IOCTL_CMP_DISKID:
      {
         FDSI_CmpDiskIdArgs *cmpDiskIdArgs = (FDSI_CmpDiskIdArgs *) dataInOut;
         cmpDiskIdArgs->isSameDevice =
             (cmpDiskIdArgs->diskId->type == VMWARE_SCSI_ID_UNIQUE);
         return VMK_OK;
      }
#endif

   case FDS_IOCTL_ABORT_COMMAND:
   case FDS_IOCTL_RESET_COMMAND:
      {
         status = VMK_BAD_PARAM;

         LogFS_MetaLog *ml = globalMetaLog;
         if (ml) {
            LogFS_Device *dev = ml->device;
            status = FDS_Ioctl(dev->fd, cmd, dataInOut);
            if (status != VMK_OK)
               break;
         }

         zprintf("%s command %s\n",
                 cmd == FDS_IOCTL_ABORT_COMMAND ? "abort" : "reset",
                 VMK_ReturnStatusToString(status));
         return status;
      }

   default:
      //zprintf( "Unsupported ioctl %u", cmd);
      //LOG(1, "Unsupported ioctl %u", cmd);
#ifdef VMX86_DEBUG
      if (LOGLEVEL() >= 3) {
         Log_Backtrace(MY_RUNNING_WORLD->worldID);
      }
#endif
      return VMK_INVALID_IOCTL;
   }

   NOT_REACHED();
}

static VMK_ReturnStatus
LogFS_RescanDevices(void *driverData, void *fdsDeviceData)
{
   // XXX: What about lvmFile devices?
   VMK_ReturnStatus status = VMK_OK;

   int i;

   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];

      //XXX if(LogFS_VDiskIsWritable(vd))
      {
         char name[SHA1_HEXED_SIZE];
         LogFS_Hash disk = LogFS_VDiskGetBaseId(vd);
         LogFS_HashPrint(name, &disk);

         status =
             FDS_AnnounceDevice(logfsDriverID, name, fdsDeviceData, TRUE, NULL);
         if (status != VMK_OK)
            break;
      }
   }

   return status;
}

static VMK_ReturnStatus LogFS_RemoveDev(const char *deviceName)
{
   VMK_ReturnStatus status;

   void *driverData;

   status =
       DevLib_DestroyDevice(logfsDevLibObj, deviceName, (void **)&driverData);
   if (status == VMK_OK) {
      zprintf("Destroyed file device %s", deviceName);
   }
   return status;
}

// FDS entry points for the logfs driver.
static FDS_DeviceOps logfsDriverOps = {
   .FDS_OpenDeviceByDirOID = LogFS_OpenDevice,
   .FDS_CloseDevice = LogFS_CloseDevice,
   .FDS_AsyncIO = LogFS_AsyncIO,
   .FDS_Ioctl = LogFS_Ioctl,
   .FDS_RescanDevices = LogFS_RescanDevices,
   .FDS_MakeDev = (FDS_MakeDevOp) FDS_NotSupported,
   .FDS_Probe = (FDS_ProbeOp) FDS_NotSupported,
   .FDS_Poll = (FDS_PollOp) FDS_NotSupported,
   .FDS_RemoveDev = LogFS_RemoveDev,
};

static int logfsInit = 0;

#define GC_INTERVAL 4           /* times ten ms */

extern void LogFS_MetaLogGC(LogFS_MetaLog *ml);

#if 0
static void LogFS_GC(void *data)
{
   while (!gcExit) {
      schedule_gc();

      LogFS_MetaLog *ml = data;
      LogFS_MetaLogGC(ml);
      time_before_gc = GC_INTERVAL;
   }

   gcExit = FALSE;
   World_Exit(VMK_OK);
}
#endif
void LogFS_Compressor(LogFS_MetaLog *ml, Bool*);
static void LogFS_GC(void *data)
{
   LogFS_MetaLog *ml = data;
   LogFS_Compressor(ml, &gcExit);

   gcExit = FALSE;
   World_Exit(VMK_OK);
}

VMK_ReturnStatus LogFS_RegisterPosixInterface(void);
void LogFS_DatagramServerInit(void);

VMK_ReturnStatus LogFS_Init(void)
{

   VMK_ReturnStatus status;

   if (logfsInit) {
      Log("VMLOGFS driver already initialized\n");
      return VMK_BUSY;
   }

   status = LogFS_CommonInit();

   if (status!=VMK_OK) {
      return status;
   }

   if (!VMK_CheckVMKernelID()) {
      printf("HERE Invalid vmkernel ID %#x. Can't load LogFS driver",
             VMK_GetVMKernelID());
      return VMK_FAILURE;

   }

   LogFS_DiskMapInit();

   List_Init(&vDiskInfoList);
   List_Init(&appendDataList);

   status =
       DevLib_InitObject("cloudfs", logfsHeap, 1024 /*LogFS_NUM_FILE_HANDLES */ ,
                         &logfsDevLibObj);
   if (status != VMK_OK) {
      Warning("Initialization of devlib object for the logfs driver failed: %s",
              VMK_ReturnStatusToString(status));
      return -1;
   }

   status = FDS_RegisterDriverWithAttributes("cloudfs", &logfsDriverOps,
                                             logfsModuleID, FALSE,
                                             &logfsDriverID,
                                             MY_RUNNING_WORLD->ident.euid,
                                             MY_RUNNING_WORLD->ident.egid,
                                             DEVLIB_FILTER_DRIVER_MODE);

   /* The POSIX interface calls the HTTP client, so initialize that first */
   status = LogFS_InitHttpClient();
   ASSERT(status == VMK_OK);

   status = LogFS_RegisterPosixInterface();

   if(status != VMK_OK) {
      NOT_REACHED();
      return status;
   }

#if 0
   char **strPtr;
   for (strPtr = logfsControlNodeStrings; *strPtr; ++strPtr) {
      status = DevLib_CreateDevice(logfsDevLibObj, logfsDriverID,
                                   *strPtr, LogFSGetLogCapCB,
                                   0600, FALSE, *strPtr);
      if (status != VMK_OK)
         break;
   }
#endif

   logfsInit = TRUE;
   return VMK_OK;
}

void LogFS_CleanupHttpClient(void);
VMK_ReturnStatus LogFS_UnregisterPosixInterface(void);

void LogFS_Cleanup(void)
{
   LogFS_MetaLog *ml = globalMetaLog;
   if (ml) {
      LogFS_BTreeRangeMapCleanupGlobalState(ml);
      LogFS_RemovePhysicalDevice(ml->device);
      LogFS_MetaLogCleanup(ml);
      free(ml);
   }

   LogFS_UnregisterPosixInterface();
   FDS_UnregisterDriver(&logfsDriverOps);

   if (ml) {
      LogFS_CleanupHttp();
   }

   LogFS_CleanupHttpClient();

#if 0
   gcExit = TRUE;
   while(gcExit) {
      CpuSched_Sleep(1);
   }
#endif

   LogFS_RemoveDiskDevices();
   DevLib_CleanupObject(logfsDevLibObj);
   FDS_UnregisterDriver(&logfsDriverOps);

#ifdef TRACE_ALLOCS
   for (i = 0; i < na; i++) {
      if (allocs[i].a)
         zprintf("missing: %s\n", allocs[i].place);
   }
   na = 0;
#endif

   LogFS_CommonCleanup();
   logfsInit = FALSE;
}
