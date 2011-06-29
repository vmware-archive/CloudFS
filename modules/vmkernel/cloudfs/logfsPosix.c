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

#include "system.h"
#include "aligned.h"
#include "logfsHash.h"
#include "globals.h"
#include "metaLog.h"
#include "vDisk.h"
#include "vDiskMap.h"
#include "logfsHttpClient.h"

/*****************************************************************/

#if 0
static inline void *AM(size_t n, int line)
{
   zprintf("alloc %ld line %d\n", n, line);
   return malloc(n);
}

#undef aligned_malloc
#define aligned_malloc(N) AM(N,__LINE__)
#undef malloc
#define malloc(N) AM(N,__LINE__)
#endif

static FSS_FileOps logfsDirOps;
static FSS_FileOps logfsFileOps;
//static FSS_FileOps logfsSymlinkOps;
FSS_FSOps logfsFSOps;

extern Atomic_uint32 numDisks;

typedef struct {
   Hash diskId;
   List_Links next;
} LogFS_PosixCachedRootEntry;

static List_Links cachedRootEntries;
static SP_SpinLock posixLock;

typedef struct {
   size_t fileSize;
} LogFS_FileData;

typedef struct {
   enum {
      LOGFS_VOLUME = 1,
      LOGFS_ROOT,
      LOGFS_DIR,
      LOGFS_LOG,
      LOGFS_FILE,
      LOGFS_SYMLINK,
   } type;
   Hash diskId;
   void *state;
   uint64 iNode;
} LogFS_OID;

//static INLINE void
void
LogFS_SetOIDFromID(FS_ObjectID * oid, unsigned type, Hash diskId, uint64 iNode,
                   void *state)
{
   LogFS_OID *lOid = (LogFS_OID *) oid->data;
   lOid->type = type;
   lOid->diskId = diskId;
   lOid->state = state;
   lOid->iNode = iNode;

   oid->length = sizeof(LogFS_OID);
}

static INLINE LogFS_OID LogFS_GetIDFromOID(const FS_ObjectID * oid)
{
   LogFS_OID *lOid = (LogFS_OID *) oid->data;
   return *lOid;
}

#if 0
static void LogFSMakeVolumeOID(FS_ObjectID * oid, Hash diskId, int type)
{
   LogFS_OID *id = (LogFS_OID *) (oid->data);
   oid->length = sizeof(LogFS_OID);
   id->id = diskId;
   id->type = type;
}
#endif

static VMK_ReturnStatus
LogFSOpGetVolumeOID(const FS_ObjectID * src, FS_ObjectID * dst)
{
   Hash noId;
   LogFS_HashClear(&noId);
   LogFS_SetOIDFromID(dst, LOGFS_VOLUME, noId, 0, NULL);
   return VMK_OK;
}

static VMK_ReturnStatus
LogFSOpOpen(const char *deviceName, FSSOpen_Flags flags,
            ObjDescriptorInt * fsObj)
{

   if (deviceName) {
      FSDescriptorInt *fs = FSDESC(fsObj);

      Hash noId;
      LogFS_HashClear(&noId);
      LogFS_SetOIDFromID(&fsObj->oid.oid, LOGFS_VOLUME, noId, 0, NULL);
      strcpy(fs->volumeName, "cloudfs");

      fs->fsOps = &logfsFSOps;
      fs->readOnly = FALSE;
      fsObj->oid.fsTypeNum = LOGFS_FSTYPENUM;
   }

   return VMK_OK;
}

static VMK_ReturnStatus
LogFSOpClose(ObjDescriptorInt *file, FSSOpen_Flags flags)
{
   LogFS_OID oid = LogFS_GetIDFromOID(&file->oid.oid);
   if(oid.state != NULL) {
      free(oid.state);
   }
   return VMK_OK;
}

typedef struct {
   char perms[16];
   unsigned blocks;
   char owner[32];
   char group[32];
   char month[12];
   unsigned day;
   char time[16];
   char name[256];
   char symlink[256];

   uint64 iNode;

} __attribute__ ((__packed__)) LogFS_PosixDirEntry;

#define LOGFS_POSIXDIRSIZE 0x10000
typedef struct {
   uint32 nextFreeStream;
   uint32 numFiles;
   LogFS_PosixDirEntry entries[0];
} __attribute__ ((__packed__)) LogFS_PosixDir;

typedef struct {
   uint64 fileSize;
} __attribute__ ((__packed__)) LogFS_PosixInode;

static inline
    VMK_ReturnStatus LogFS_PosixRead(Hash diskId,
                                     Async_Token * token,
                                     char *buf, log_block_t blkno,
                                     size_t num_blocks, int flags)
{
   VMK_ReturnStatus status;

   LogFS_VDisk *vd = LogFS_DiskMapLookupDisk(diskId);
   if (vd != NULL) {
      status =
          LogFS_VDiskRead(vd, token, buf, blkno, num_blocks,
                          flags | FS_READ_OP);
   } else {
      LogFS_Hash noId;
      LogFS_HashClear(&noId);
      status = LogFS_HttpClientRequest(diskId, noId,
                                       token, buf, blkno, num_blocks,
                                       flags | FS_READ_OP);

   }
   return status;
}

void LogFS_TimerCallback(Async_Token * token, void *data)
{
   static uint64 sum = 0;
   static int n = 0;

   uint64 *ts = data;
   sum += Timer_AbsTCToUS(Timer_GetCycles() - *ts);
   ++n;

   if ((n & 0x7ff) == 0)
      zprintf("%d: avg write latency %luus\n", n, sum / n);

   Async_TokenCallback(token);
}

//static inline
VMK_ReturnStatus LogFS_PosixWrite(Hash diskId,
                                  Async_Token * token,
                                  const char *buf, log_block_t blkno,
                                  size_t num_blocks, int flags)
{
   VMK_ReturnStatus status;

   LogFS_VDisk *vd = LogFS_DiskMapLookupDisk(diskId);
   if (vd != NULL) {

      uint64 *ts =
          Async_PushCallbackFrame(token, LogFS_TimerCallback, sizeof(uint64));
      *ts = Timer_GetCycles();

      status =
          LogFS_VDiskWrite(vd, token, buf, blkno, num_blocks,
                           flags | FS_WRITE_OP);
   } else {
      LogFS_Hash noId;
      LogFS_HashClear(&noId);
      status = LogFS_HttpClientRequest(diskId, noId,
                                       token, (char *)buf, blkno, num_blocks,
                                       flags | FS_WRITE_OP);

   }
   return status;
}

//static inline
VMK_ReturnStatus LogFS_PosixReadDirectory(LogFS_PosixDir * dir, Hash diskId)
{
   VMK_ReturnStatus status;
   Async_Token *token = Async_AllocToken(0);
   status =
       LogFS_PosixRead(diskId, token, (void *)dir, 0,
                       LOGFS_POSIXDIRSIZE / BLKSIZE, FS_READ_OP);

   if (status != VMK_OK) {
      goto out;
   }

   Async_WaitForIO(token);
   status = token->transientStatus;

   if (status == VMK_OK) {

      LogFS_PosixCachedRootEntry *re;
      List_Links *curr;

      SP_Lock(&posixLock);

      LIST_FORALL(&cachedRootEntries, curr) {
         re = List_Entry(curr, LogFS_PosixCachedRootEntry, next);
         int cmp = LogFS_HashCompare(&diskId, &re->diskId);

         if (cmp == 0)
            goto found;

         else if (cmp > 0) {
            break;
         }
      }

      re = malloc(sizeof(LogFS_PosixCachedRootEntry));
      re->diskId = diskId;
      List_Insert(&re->next, LIST_BEFORE(curr));

 found:
      SP_Unlock(&posixLock);
   }

 out:
   Async_ReleaseToken(token);
   return status;
}

//static inline
VMK_ReturnStatus LogFS_PosixWriteDirectory(LogFS_PosixDir * dir, Hash diskId)
{
   VMK_ReturnStatus status;

   Async_Token *token = Async_AllocToken(0);
   status =
       LogFS_PosixWrite(diskId, token, (void *)dir, 0,
                        LOGFS_POSIXDIRSIZE / BLKSIZE, FS_WRITE_OP);

   if (status != VMK_OK) {
      goto out;
   }

   Async_WaitForIO(token);
   status = token->transientStatus;

 out:
   Async_ReleaseToken(token);
   return status;
}

static VMK_ReturnStatus
LogFS_PosixLookupDirEntry(LogFS_PosixDirEntry * result,
                          Hash diskId, const char *name)
{
   VMK_ReturnStatus status;

   int i;
   LogFS_PosixDir *dir = aligned_malloc(LOGFS_POSIXDIRSIZE);

   status = LogFS_PosixReadDirectory(dir, diskId);

   if (status != VMK_OK)
      goto out;

   status = VMK_NOT_FOUND;

   for (i = 0; i < dir->numFiles; ++i) {
      LogFS_PosixDirEntry *e = &dir->entries[i];
      if (strcmp(e->name, name) == 0) {
         *result = *e;
         status = VMK_OK;
         break;
      }
   }

 out:
   aligned_free(dir);
   return status;
}

VMK_ReturnStatus
LogFS_CreateLogDeviceState(LogFS_FileData ** result, Bool isFast);

static VMK_ReturnStatus
LogFSOpLookup(ObjDescriptorInt * parent, const char *name,
              FSS_ObjectID * fssOID)
{
   VMK_ReturnStatus status;

   FS_ObjectID *oid = &fssOID->oid;
   fssOID->fsTypeNum = LOGFS_FSTYPENUM;

   LogFS_OID parentOID = LogFS_GetIDFromOID(&parent->oid.oid);

   Hash diskId;

   Hash noId;
   LogFS_HashClear(&noId);

   if (strcmp(name, ".") == 0) {
      FSS_CopyOID(oid, &parent->oid);
      status = VMK_OK;
      goto done;
   } else if (strcmp(name, "..") == 0) {
      if (parentOID.type == LOGFS_ROOT) {
         VCFS_GetVolumesMountPointOID(fssOID);
         status = VMK_OK;
         goto done;
      }
      if (parentOID.type == LOGFS_DIR) {
         LogFS_SetOIDFromID(oid, LOGFS_ROOT, noId, 0, NULL);
         status = VMK_OK;
         goto done;
      }

   }

   if (strcmp(name, "/") == 0) {
      LogFS_SetOIDFromID(oid, LOGFS_ROOT, noId, 0, NULL);
      status = VMK_OK;
   } else if (LogFS_HashSetString(&diskId, name)) {
      LogFS_SetOIDFromID(oid, LOGFS_DIR, diskId, 0, NULL);
      status = VMK_OK;
   } else if (strcmp(name, "log") == 0) {
      LogFS_FileData *state;
      LogFS_CreateLogDeviceState(&state, FALSE);
      LogFS_SetOIDFromID(oid, LOGFS_LOG, noId, 0, state);
      status = VMK_OK;
   } else {
      LogFS_PosixDirEntry *e = malloc(sizeof(LogFS_PosixDirEntry));
      status = LogFS_PosixLookupDirEntry(e, parentOID.diskId, name);

      if (status == VMK_OK) {
         LogFS_SetOIDFromID(oid, LOGFS_FILE, parentOID.diskId, e->iNode, NULL);
      }

      free(e);
   }

 done:
   return status;
}

static VMK_ReturnStatus
LogFSOpGetAttributes(ObjDescriptorInt * fsObj,
                     uint32 maxPartitions,
                     FS_GetAttrSpec getAttrSpec,
                     FS_PartitionListResult * result)
{
   VMK_ReturnStatus status;
   FSDescriptorInt *fs = FSDESC(fsObj);

   /* memset result to 0 */
   memset(result, 0, sizeof(*result));
   result->config = FS_CONFIG_PUBLIC;
   result->readOnly = fs->readOnly;
   strncpy(result->driverType, VC_DRIVERTYPE_NONE_STR,
           sizeof(result->driverType));
   result->versionNumber = 1;
   result->minorVersion = 0;
   strcpy(result->fsType, "LogFS");
   memset(result->logicalDevice, 0, sizeof(result->logicalDevice));
   result->numPhyExtents = 1;
   result->numPhyExtentsReturned = 1;

   memset(&result->uuid, 0, sizeof(UUID));
   result->diskBlockSize = BLKSIZE;
   result->fileBlockSize = BLKSIZE;

   result->numDiskBlocks = 0x800000000ULL;
   result->numFileBlocks = 0x800000000ULL;

   result->numFileBlocksUsed = 0;
   result->numFileBlocksAvailable = 0x800000000ULL;

   result->numFiles = 1;        //statResult.tfiles;
   result->numFilesFree = 1;    //statResult.ffiles;

   // The following are not available for LogFS
   result->numPtrBlocks = 0;
   result->numPtrBlocksFree = 0;

//XXX memcpy(&result->uuid, &mpe->uuid, sizeof (mpe->uuid));

   /* Make a unique volumeName for this LogFS volume. This'll show up as a
    * directory in VCFS_MOUNT_POINT.
    */
   strcpy(result->volumeName, "cloudfs");
   LogFSOpLookup(fsObj, "/", &result->rootDirOID);
   //snprintf(result->volumeName, sizeof(result->volumeName),
   //"%08x-%08x", 0x1234234,0x63b8f7);
   memcpy(&result->uuid, result->volumeName, sizeof(result->uuid) - 1);

   status = VMK_OK;
   return status;
}

static VMK_ReturnStatus LogFSNotImplemented(void)
{
   return VMK_NOT_IMPLEMENTED;
}

static VMK_ReturnStatus LogFSNotSupported(void)
{
   return VMK_NOT_SUPPORTED;
}

#if 0
static VMK_ReturnStatus
LogFSOpLookupAndOpen(ObjDescriptorInt * parent, const char *child,
                     uint32 openFlags, FS_FileHandleID * fileHandleID)
{
   zprintf("%s\n", __FUNCTION__);
   VMK_ReturnStatus status;
   FSS_ObjectID fileOID;
   //ObjDescriptorInt *file;

   /*
    * We don't handle these cases in the nfsclient, we rely on
    * FSS_Lookup to handle them for us. We return VMK_NOT_SUPPORTED,
    * which will force FSS_LookupAndOpen to use the standard lookup
    * and open path.
    */
   if (strcmp(".", child) == 0 || strcmp("", child) == 0) {
      return VMK_NOT_SUPPORTED;
   }

   fileOID.fsTypeNum = LOGFS_FSTYPENUM;

   status = FSS_OpenFile(&fileOID, openFlags, fileHandleID);
   if (status != VMK_OK) {
      return status;
   }

   return VMK_OK;
}
#endif

static void LOGFS_ObjEvictCB(struct ObjDescriptorInt *desc)
{
   //Log("%s\n",__FUNCTION__);
   FileDescriptorInt *fd;

   if (desc->objType != OBJ_VOLUME) {
      fd = FILEDESC(desc);
      OC_ReleaseVolume(desc->fs);
      desc->fs = NULL;
      fd->fileOps = NULL;
      if (fd->fileData != NULL) {
         free(fd->fileData);
         fd->fileData = NULL;
      }
   }
}

static VMK_ReturnStatus LOGFS_ObjLastRefCB(struct ObjDescriptorInt *desc)
{
   return VMK_OK;
}

VMK_ReturnStatus
LogFS_PosixReadInode(Hash diskId, LogFS_PosixInode * dst, uint64 iNode)
{
   VMK_ReturnStatus status;

   Async_Token *token = Async_AllocToken(0);
   status = LogFS_PosixRead(diskId, token, (void *)dst, iNode, 1, FS_READ_OP);

   if (status != VMK_OK) {
      goto out;
   }

   Async_WaitForIO(token);
 out:
   Async_ReleaseToken(token);
   return status;
}

static VMK_ReturnStatus
LogFSOpGetObject(FS_ObjectID * objId, ObjDescriptorInt * desc,
                 SemaRankMinor * descLockMinorRank)
{
   VMK_ReturnStatus status = VMK_OK;

   Hash noId;
   LogFS_HashClear(&noId);

   FileDescriptorInt *fd;
   LogFS_OID oid = LogFS_GetIDFromOID(objId);
   unsigned type = oid.type;
   //desc->oid.fsTypeNum = LOGFS_FSTYPENUM;

   Bool reserveVolume = FALSE;
   if (type == LOGFS_VOLUME) {
      FSDescriptorInt *fs = FSDESC(desc);
      fs->fsData = NULL;
      fs->fsOps = &logfsFSOps;
      fs->readOnly = FALSE;
      desc->evictCB = LOGFS_ObjEvictCB;
      desc->lastRefCB = LOGFS_ObjLastRefCB;
      //LogFSSetOIDFromID(&desc->oid.oid, volNode.fsObj.oid);
      desc->oid.fsTypeNum = LOGFS_FSTYPENUM;
      desc->objType = OBJ_VOLUME;
      desc->fs = NULL;
   }

   else if (type == LOGFS_ROOT || type == LOGFS_DIR) {
      reserveVolume = TRUE;
      fd = FILEDESC(desc);
      fd->fileData = NULL;
      desc->evictCB = LOGFS_ObjEvictCB;
      desc->lastRefCB = LOGFS_ObjLastRefCB;
      fd->fileOps = &logfsDirOps;
      desc->objType = OBJ_DIRECTORY;
   }

   else if (type == LOGFS_LOG) {
      reserveVolume = TRUE;
      fd = FILEDESC(desc);

      ASSERT(oid.state);
      fd->fileData = oid.state;

      desc->evictCB = LOGFS_ObjEvictCB;
      desc->lastRefCB = LOGFS_ObjLastRefCB;
      fd->fileOps = &logfsFileOps;
      desc->objType = OBJ_REGFILE;
   }

   else if (type == LOGFS_FILE) {
      reserveVolume = TRUE;
      fd = FILEDESC(desc);

      LogFS_FileData *fileData = malloc(sizeof(LogFS_FileData));

      LogFS_PosixInode *iNode = aligned_malloc(BLKSIZE);
      status = LogFS_PosixReadInode(oid.diskId, iNode, oid.iNode);
      fileData->fileSize = iNode->fileSize;
      aligned_free(iNode);
      fd->fileData = fileData;

      desc->evictCB = LOGFS_ObjEvictCB;
      desc->lastRefCB = LOGFS_ObjLastRefCB;
      fd->fileOps = &logfsFileOps;
      desc->objType = OBJ_REGFILE;
   }

   if (status == VMK_OK && reserveVolume) {
      FSS_ObjectID volOID;
      ObjDescriptorInt *fsObj;
      volOID.fsTypeNum = LOGFS_FSTYPENUM;
      LogFS_SetOIDFromID(&volOID.oid, LOGFS_VOLUME, noId, 0, NULL);
      status = OC_ReserveVolume(&volOID, &fsObj);
      desc->fs = fsObj;
   }

   return status;
}

FSS_FSOps logfsFSOps = {
   .FSS_Create = (FSS_CreateOp) LogFSNotImplemented,
   .FSS_Extend = (FSS_ExtendOp) LogFSNotImplemented,

   .FSS_Open = LogFSOpOpen,
   .FSS_Close = LogFSOpClose,

   .FSS_SetAttribute = (FSS_SetAttributeOp) LogFSNotImplemented,  //LogFSOpSetAttributes,
   .FSS_GetAttributes = LogFSOpGetAttributes,
   .FSS_Callback = (FSS_CallbackOp) LogFSNotSupported,

   .FSS_Lookup = LogFSOpLookup,
   .FSS_LookupAndOpen = (FSS_LookupAndOpenOp) LogFSNotSupported,  //LogFSOpLookupAndOpen,
   .FSS_GetObject = LogFSOpGetObject,

   .FSS_GetVolumeOID = LogFSOpGetVolumeOID,
   .FSS_GetRootOID = NULL,
   .FSS_IsRootOID = NULL,
   .FSS_MountInit = NULL,
   .FSS_UmountClean = NULL,
   .FSS_Ioctl = (FSS_IoctlOp) FDS_NotSupported,
   .FSS_Probe = (FSS_ProbeOp) FDS_NotSupported,
};

static VMK_ReturnStatus LogFSIsADirectory(void)
{
   return VMK_IS_A_DIRECTORY;
}

static VMK_ReturnStatus
LogFSOpReaddir(ObjDescriptorInt * dir, uint32 maxFiles, uint64 offset,
               uint64 verifier, FS_ReaddirResult * oresult)
{
   uint32 entries = 0;

   if (offset == 0) {
      FSS_FillDirent(&oresult->dirent[entries], ".",
                     FS_TYPE_DIRECTORY, (VA) NULL, 1);
      entries++;

      if (entries >= maxFiles) {
         goto done;
      }
   }

   if (offset <= 1) {
      FSS_FillDirent(&oresult->dirent[entries], "..",
                     FS_TYPE_DIRECTORY, (VA) NULL, 2);
      entries++;
      if (entries >= maxFiles) {
         goto done;
      }
   }

   LogFS_OID oid = LogFS_GetIDFromOID(&dir->oid.oid);

   if (oid.type == LOGFS_ROOT) {
      char sDiskId[SHA1_HEXED_SIZE];
      List_Links *curr;
      int i = 0;

      SP_Lock(&posixLock);

      LIST_FORALL(&cachedRootEntries, curr) {
         LogFS_PosixCachedRootEntry *re =
             List_Entry(curr, LogFS_PosixCachedRootEntry, next);
         if (offset <= 2 + i) {
            LogFS_HashPrint(sDiskId, &re->diskId);

            FSS_FillDirent(&oresult->dirent[entries], sDiskId,
                           FS_TYPE_DIRECTORY, (VA) LOGFS_DIR, 3 + i);
            entries++;

            if (entries >= maxFiles)
               break;
         } else
            break;
      }

      SP_Unlock(&posixLock);
   }

   else if (oid.type == LOGFS_DIR) {
      int bottom = 2;
      int i;

      LogFS_PosixDir *dir = aligned_malloc(LOGFS_POSIXDIRSIZE);
      LogFS_PosixReadDirectory(dir, oid.diskId);

      for (i = 0; i < dir->numFiles; ++i) {
         LogFS_PosixDirEntry *e = &dir->entries[i];
         if (offset <= bottom + i) {
            if (e->symlink[0]) {
               FSS_FillDirent(&oresult->dirent[entries], e->name,
                              FS_TYPE_SYMLINK, (VA) LOGFS_FILE, 1 + bottom + i);
            } else {
               FSS_FillDirent(&oresult->dirent[entries], e->name,
                              FS_TYPE_REGULAR_FILE,
                              (VA) LOGFS_FILE, 1 + bottom + i);
            }
            ++entries;
            if (entries >= maxFiles)
               break;
         }
#if 0
         else if (LogFS_PosixParseDirEntry(e, c) == VMK_IS_A_DIRECTORY) {
            if (offset <= bottom + i) {
               FSS_FillDirent(&oresult->dirent[entries], e->name + 1,
                              FS_TYPE_DIRECTORY,
                              (VA) LOGFS_DIR, 1 + bottom + i);
               ++entries;
            }
         }
#endif

         //else zprintf("unmatched\n");
      }

      aligned_free(dir);
   }

 done:
   oresult->eof = (entries < maxFiles) ? TRUE : FALSE;
   oresult->numDirEntriesReturned = entries;

   return VMK_OK;
}

//#define ff(name) static VMK_ReturnStatus N_##name(void) { zprintf("Called %s\n",__FUNCTION__); return VMK_NOT_SUPPORTED; }
#define ff(name) static VMK_ReturnStatus N_##name(void) { return VMK_NOT_SUPPORTED; }

ff(A);
ff(B);
ff(C);
ff(E);

static VMK_ReturnStatus
LogFSOpGetFileAttributes(ObjDescriptorInt * file, FS_FileAttributes * attrs)
{
   VMK_ReturnStatus status = VMK_OK;

   LogFS_OID oid = LogFS_GetIDFromOID(&file->oid.oid);
   memset(attrs, 0, sizeof(*attrs));

   attrs->fsBlockSize = BLKSIZE;
   attrs->diskBlockSize = BLKSIZE;
   attrs->length = LogFS_VDiskGetCapacity(NULL);
   attrs->numBlocks = attrs->length / attrs->fsBlockSize;

   if (oid.type == LOGFS_LOG) {
      attrs->type = FS_TYPE_REGULAR_FILE;
      attrs->length = 0;
      attrs->numBlocks = 0;

   } else if (oid.type == LOGFS_FILE) {

      attrs->type = FS_TYPE_REGULAR_FILE;

      LogFS_PosixInode *iNode = aligned_malloc(BLKSIZE);
      status = LogFS_PosixReadInode(oid.diskId, iNode, oid.iNode);

      attrs->length = iNode->fileSize;
      aligned_free(iNode);

      if (status != VMK_OK)
         goto out;

      attrs->numBlocks = CEIL(attrs->length, attrs->fsBlockSize);
      attrs->rawHandleID = SCSI_INVALID_HANDLE;

   } else if (oid.type == LOGFS_ROOT) {
      attrs->type = FS_TYPE_DIRECTORY;
   } else if (oid.type == LOGFS_DIR) {
      attrs->type = FS_TYPE_DIRECTORY;
   } else {
      attrs->type = FS_TYPE_SYMLINK;
   }
   //attrs->flags = /*FS_NOT_ESX_DISK_IMAGE | */ FS_NOT_CACHEABLE;
   //attrs->flags |= FS_USER_BUFFER_ON_WRITE;

   attrs->mtime = 0;            //NTPClock_GetTimeSec();
   attrs->ctime = attrs->mtime;
   attrs->atime = attrs->mtime;
   attrs->descNum = 0;

#if 0
   attrs->uid = obj->ident.uid;
   attrs->gid = obj->ident.gid;
#endif
   attrs->mode = 0666;          //obj->ident.mode;

 out:
   return status;
}

static VMK_ReturnStatus
LogFSOpOpenFile(ObjDescriptorInt * file, uint32 openFlags, void *dataIn)
{
   //zprintf("opening file"FS_OID_FMTSTR, FS_OID_VAARGS(&file->oid));
   return VMK_OK;
}

static VMK_ReturnStatus
LogFSOpCheckAccess(ObjDescriptorInt * file, uint32 openFlags)
{
   return VMK_OK;
}

static VMK_ReturnStatus LogFSOpCloseFile(ObjDescriptorInt * file)
{
   //FileDescriptorInt *fd = FILEDESC(file);
   //LogFS_FileData* fileData = fd->fileData;

   // if(fileData) free(fileData);

   return VMK_OK;
}

typedef struct {
   void *tmp;
   void *sg;
} LogFS_FreeBufferContext;

extern void LogFS_FreeBuffer(Async_Token * token, void *data);
extern VMK_ReturnStatus LogFS_AppendRawBytes(Async_Token * token, void *d,
                                             const SG_Array * sgArr);

typedef struct {
   Hash diskId;
   const char *buf;
   char *aligned_buf;
   log_offset_t offset;
   log_size_t length;
   int flags;
   Async_Token *token;
} LogFS_VDiskPatchReadContext;

void LogFS_VDiskPatchReadsCB(Async_Token * token, void *data)
{
   LogFS_VDiskPatchReadContext *ctx = data;
   log_size_t aligned_start = (ctx->offset / BLKSIZE) * BLKSIZE;
   log_size_t aligned_end =
       ((ctx->offset + ctx->length + BLKSIZE - 1) / BLKSIZE) * BLKSIZE;
   log_size_t aligned_length = aligned_end - aligned_start;

   memcpy(ctx->aligned_buf + (ctx->offset % BLKSIZE), ctx->buf, ctx->length);

   token->transientStatus =
       LogFS_PosixWrite(ctx->diskId, token, ctx->aligned_buf,
                        aligned_start / BLKSIZE, aligned_length / BLKSIZE,
                        ctx->flags);

   ASSERT(token->transientStatus == VMK_OK);

}

void LogFS_VDiskPatchReadsCB2(Async_Token * token, void *data)
{
   LogFS_VDiskPatchReadContext *ctx = data;
   printf("freeing aligned buf\n");
   aligned_free((void *)ctx->aligned_buf);

   ((SCSI_Result *) ctx->token->result)->status =
       SCSI_MAKE_STATUS(SCSI_HOST_OK, SDSTAT_GOOD);

   Async_TokenCallback(ctx->token);

   Async_ReleaseToken(ctx->token);
}

VMK_ReturnStatus LogFS_PosixWriteBytes(Hash diskId,
                                       Async_Token * token,
                                       const void *buf,
                                       log_offset_t offset,
                                       log_size_t length, int flags)
{
   VMK_ReturnStatus status = VMK_OK;

   log_size_t aligned_start = (offset / BLKSIZE) * BLKSIZE;
   log_size_t aligned_end =
       ((offset + length + BLKSIZE - 1) / BLKSIZE) * BLKSIZE;
   log_size_t aligned_length = aligned_end - aligned_start;

   if (length != aligned_length || offset != aligned_start) {
      char *aligned_buf = (char *)aligned_malloc(aligned_length);
      ASSERT(aligned_buf);

      if (length != aligned_length || offset != aligned_start) {
         Async_IOHandle *ioh;

         LogFS_VDiskPatchReadContext ct = {
            .diskId = diskId,
            .buf = buf,
            .aligned_buf = aligned_buf,
            .offset = offset,
            .length = length,
            .flags = flags,
            .token = token,
         };

         // XXX no corresponding release of patchToken
         Async_Token *patchToken = Async_AllocToken(0);

         Async_RefToken(token);

         *(LogFS_VDiskPatchReadContext *) Async_PushCallbackFrame(patchToken,
                                                                  LogFS_VDiskPatchReadsCB2,
                                                                  sizeof
                                                                  (LogFS_VDiskPatchReadContext))
             = ct;

         *(LogFS_VDiskPatchReadContext *) Async_PushCallbackFrame(patchToken,
                                                                  LogFS_VDiskPatchReadsCB,
                                                                  sizeof
                                                                  (LogFS_VDiskPatchReadContext))
             = ct;

         Async_StartSplitIO(patchToken, Async_DefaultChildDoneFn, 0, &ioh);

         Async_Token *t1 = Async_PrepareOneIO(ioh, NULL);
         status =
             LogFS_PosixRead(diskId, t1, aligned_buf,
                             aligned_start / BLKSIZE, 1, flags);
         ASSERT(status == VMK_OK);

         if (status == VMK_OK) {
            Async_Token *t2 = Async_PrepareOneIO(ioh, NULL);
            status =
                LogFS_PosixRead(diskId, t2,
                                aligned_buf + aligned_length - BLKSIZE,
                                aligned_end / BLKSIZE - 1, 1, flags);
         }
         ASSERT(status == VMK_OK);

         Async_EndSplitIO(ioh, status, FALSE);

      }

   } else {
      status =
          LogFS_PosixWrite(diskId, token, buf, offset / BLKSIZE,
                           length / BLKSIZE, flags);
   }

   return status;
}

VMK_ReturnStatus LogFS_PosixReadBytes(Hash diskId,
                                      Async_Token * token,
                                      void *buf,
                                      log_offset_t offset,
                                      log_size_t length, int flags)
{
   //printf("pread %p offset %lu length %lu\n",buf,offset,length);
   VMK_ReturnStatus status;

   log_size_t aligned_start = (offset / BLKSIZE) * BLKSIZE;
   log_size_t aligned_end =
       ((offset + length + BLKSIZE - 1) / BLKSIZE) * BLKSIZE;
   log_size_t aligned_length = aligned_end - aligned_start;

   if (!is_aligned(buf) || length != aligned_length || offset != aligned_start) {
      /* I/O is unaligned */

      printf("UR 0x%lx 0x%lx token %p\n", offset, length, token);

      ASSERT((flags & FS_CANTBLOCK) == 0);

      const log_size_t chunk_size = 0x1000;

      char *aligned_buf = (char *)aligned_malloc(chunk_size);
      ASSERT(aligned_buf);

      log_size_t off = offset % BLKSIZE;

      log_ssize_t left;         /* Can be less than zero, so must be signed */
      for (left = length; left > 0;) {
         log_size_t take = (left < chunk_size - off) ? left : chunk_size - off;
         //printf("take %"FMT64"u, off %"FMT64"u\n",take,off);

         Async_Token *t2 = Async_AllocToken(0);
         ASSERT(t2);

         /* XXX perhaps read less than chunk_size here */
         status =
             LogFS_PosixRead(diskId, t2, aligned_buf,
                             aligned_start / BLKSIZE, chunk_size / BLKSIZE,
                             flags);
         if (status != VMK_OK) {
            goto out;
         }

         Async_WaitForIO(t2);
         Async_ReleaseToken(t2);

         memcpy(buf, aligned_buf + off, take);

         off = 0;

         /* Advance input ptr to next aligned chunk */
         aligned_start += chunk_size;

         /* Advance output ptr with how much we took, and count down remaining */
         buf += take;
         left -= take;
      }

      aligned_free((void *)aligned_buf);  // XXX plays badly with async!

      ASSERT(token);
      ((SCSI_Result *) token->result)->status =
          SCSI_MAKE_STATUS(SCSI_HOST_OK, SDSTAT_GOOD);
      Async_TokenCallback(token);

   } else {
      /* Pass nicely aligned I/O down to lower layers without changes */
      printf("AR 0x%lx 0x%lx token %p\n", offset, length, token);
      status =
          LogFS_PosixRead(diskId, token, buf, offset / BLKSIZE,
                          length / BLKSIZE, flags);
   }

 out:
   return status;

}

static VMK_ReturnStatus
LogFSOpFileIO(Identity * openIdentity,
              ObjDescriptorInt * file,
              const SG_Array * sgArr,
              Async_Token * token, IO_Flags ioFlags, uint32 *bytesTransferred)
{
   VMK_ReturnStatus status;
   Async_Token *newToken = NULL;

   LogFS_FileData *fileData = FILEDESC(file)->fileData;

   if (!token) {
      token = newToken = Async_AllocToken(0);
      token->resID = Host_GetWorldID();
   }

   uint32 bytes = SG_TotalLength(sgArr);

   LogFS_OID oid = LogFS_GetIDFromOID(&file->oid.oid);

   if (oid.type == LOGFS_LOG) {
      if (ioFlags & FS_WRITE_OP) {
         status = LogFS_AppendRawBytes(token, oid.state, sgArr);
         if (status == VMK_OK)
            *bytesTransferred = bytes;
      }
   }

   else if (ioFlags & FS_WRITE_OP) {
      //Bool reserved = FALSE;
      uint64 startBlock = oid.iNode;
      int i;

      Async_IOHandle *ioh;
      Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);

      uint64 baseOffset = sgArr->sg[0].offset;

      ASSERT(fileData);
      if (baseOffset + bytes > fileData->fileSize) {
         //LogFS_VDiskReserve(vd); XXX reserve
         //reserved = TRUE;

         Async_Token *t1 = Async_PrepareOneIO(ioh, NULL);

         LogFS_PosixInode *iNode = aligned_malloc(BLKSIZE);
         memset(iNode, 0, BLKSIZE);

         fileData->fileSize = iNode->fileSize = baseOffset + bytes;

         void **pva = Async_PushCallbackFrame(t1, LogFS_FreeSimpleBuffer,
                                              sizeof(void *));
         *pva = iNode;

         status = LogFS_PosixWrite(oid.diskId, t1, 
            (void *)iNode, startBlock, 1, ioFlags);
   }

      int n;

      for (i=0, n=0 ; i < sgArr->length; i++) {

         baseOffset = sgArr->sg[i-n].offset;
         uint64 totalLen = sgArr->sg[i].offset - baseOffset + sgArr->sg[i].length;

         /* If not contiguous wrt to next element, or if last element, flush write */

         if ( i==sgArr->length-1 || 
               totalLen > LOG_HEAD_MAX_BLOCKS*BLKSIZE ||
               sgArr->sg[i].offset + sgArr->sg[i].length != sgArr->sg[i+1].offset) {

            void *buf = aligned_malloc(BLKSIZE_ALIGNUP(totalLen));
            ASSERT(buf);
            char* b = buf;

            int j;
            for(j=i-n; j<=i; ++j) {

               if (sgArr->addrType == SG_MACH_ADDR) {
                  void *tmp = Kseg_MapMA(sgArr->sg[j].addr, sgArr->sg[j].length);
                  ASSERT(tmp);
                  memcpy(b, tmp, sgArr->sg[j].length);
                  Kseg_ReleaseVA(tmp);
               } else {
                  memcpy(b, (void*) sgArr->sg[j].addr, sgArr->sg[j].length);
               }

               b += sgArr->sg[j].length;
            }


            Async_Token *t2 = Async_PrepareOneIO(ioh, NULL);

            *((void **)Async_PushCallbackFrame(t2, LogFS_FreeSimpleBuffer,
                     sizeof(void *))) = buf;

            status = LogFS_PosixWriteBytes(oid.diskId, t2, buf, baseOffset +
                  (startBlock + 1) * BLKSIZE, totalLen, ioFlags); 

            if (status != VMK_OK) {
               zprintf("write status %s\n",VMK_ReturnStatusToString(status));
            }

            n=0;
         }
         else ++n;
      }

      Async_EndSplitIO(ioh, VMK_OK, FALSE);
      *bytesTransferred = bytes;


      //if(reserved) LogFS_VDiskRelease(vd); XXX release!
   }

   else if (ioFlags & FS_READ_OP) {
      uint64 startBlock = oid.iNode;
      uint64 baseOffset = sgArr->sg[0].offset;

      if (baseOffset + bytes > fileData->fileSize) {
         if (baseOffset >= fileData->fileSize) {
            if (newToken) {
               Async_ReleaseToken(newToken);
            }
            return VMK_LIMIT_EXCEEDED;
         } else
            bytes = fileData->fileSize - baseOffset;
      }
      /* Round up bytes to nearest BLKSIZE multiple */
      char *buffer = aligned_malloc((bytes + (BLKSIZE - 1)) & (~(BLKSIZE - 1)));
      ASSERT(buffer);

      LogFS_FreeBufferContext *c =
          Async_PushCallbackFrame(token, LogFS_FreeBuffer,
                                  sizeof(LogFS_FreeBufferContext));

      size_t arraySize = sizeof(SG_Array) + sizeof(SG_Elem) * sgArr->length;
      c->tmp = buffer;
      c->sg = malloc(arraySize);
      ASSERT(c->sg);
      memcpy(c->sg, sgArr, arraySize); //XXX

      status =
          LogFS_PosixReadBytes(oid.diskId, token, buffer,
                               (startBlock + 1) * BLKSIZE + baseOffset,
                               bytes, ioFlags);
      *bytesTransferred = bytes;
   }

   if (newToken) {
      Async_WaitForIO(newToken);
      Async_ReleaseToken(newToken);
   }

   return VMK_OK;
}

static inline
    VMK_ReturnStatus
LogFS_PosixInsertEntry(LogFS_PosixDir * dir, LogFS_PosixDirEntry * newEntry)
{
   int i;
   LogFS_PosixDirEntry *e = dir->entries;
   int n = dir->numFiles;

   /* Loop over dir entries to see if the name is already taken, and, if not,
    * push down entries to make room for the new one */

   for (i = 0, e = dir->entries; i < n; ++i, ++e) {
      int cmp = strcmp(e->name, newEntry->name);
      if (cmp == 0)
         return VMK_EXISTS;

      else if (cmp > 0) {
         memmove(e + 1, e, (n + 1 - i) * sizeof(LogFS_PosixDirEntry));
         break;
      }
   }

   *e = *newEntry;
   ++(dir->numFiles);

   return VMK_OK;
}

static inline
    VMK_ReturnStatus
LogFS_PosixUpdateEntry(LogFS_PosixDir * dir,
                       const char *replaceName, LogFS_PosixDirEntry * newEntry)
{
   return VMK_NOT_FOUND;
}

static VMK_ReturnStatus
LogFSOpCreateFile(ObjDescriptorInt * parent, const char *name,
                  uint32 opFlags, FS_FileAttributes * attrs,
                  void *dataIn, FS_ObjectID * fileOID)
{
   VMK_ReturnStatus status;

   LogFS_OID parentOID = LogFS_GetIDFromOID(&parent->oid.oid);
   if (parentOID.type == LOGFS_DIR) {
      Hash diskId = parentOID.diskId;
      //LogFS_VDiskReserve(vd); XXX reserve!

      LogFS_PosixDir *dir = aligned_malloc(LOGFS_POSIXDIRSIZE);
      status = LogFS_PosixReadDirectory(dir, diskId);

      if (status != VMK_OK)
         goto out;

      LogFS_PosixDirEntry *e = malloc(sizeof(LogFS_PosixDirEntry));
      memset(e, 0, sizeof(LogFS_PosixDirEntry));
      strcpy(e->name, name);

      e->iNode = ++(dir->nextFreeStream) * 0x100000000;
      status = LogFS_PosixInsertEntry(dir, e);

      if (status == VMK_OK) {
         //LogFS_FileData *fileData = malloc(sizeof(LogFS_FileData));
         //fileData->fileSize = 0;

         LogFS_SetOIDFromID(fileOID, LOGFS_FILE, diskId, e->iNode, NULL);

         /* Write out the inode for the newly created file */
         LogFS_PosixInode *iNode = aligned_malloc(BLKSIZE);
         memset(iNode, 0, BLKSIZE);

         Async_Token *token = Async_AllocToken(0);
         status =
             LogFS_PosixWrite(diskId, token, (void *)iNode, e->iNode, 1,
                              FS_WRITE_OP);
         ASSERT(status == VMK_OK);
         Async_WaitForIO(token);
         Async_ReleaseToken(token);
         aligned_free(iNode);

         status = LogFS_PosixWriteDirectory(dir, diskId);
         free(dir);
      }

      free(e);

      //LogFS_VDiskRelease(vd); XXX release
   } else
      status = VMK_NOT_SUPPORTED;

 out:
   return status;
}

static VMK_ReturnStatus
LogFSOpSetFileAttributes(ObjDescriptorInt * file,
                         uint16 opFlags, const FS_FileAttributes * attrs)
{
   VMK_ReturnStatus status;

   if (opFlags & FILEATTR_SET_LENGTH) {
      LogFS_OID oid = LogFS_GetIDFromOID(&file->oid.oid);
      uint64 startBlock = oid.iNode;

      Async_Token *token = Async_AllocToken(0);

      LogFS_PosixInode *iNode = aligned_malloc(BLKSIZE);
      memset(iNode, 0, BLKSIZE);

      LogFS_FileData *fileData = FILEDESC(file)->fileData;
      fileData->fileSize = iNode->fileSize = attrs->length;

      status =
          LogFS_PosixWrite(oid.diskId, token, (void *)iNode, startBlock, 1,
                           FS_WRITE_OP);

      Async_WaitForIO(token);
      aligned_free(iNode);
      Async_RefToken(token);
   } else {
      status = VMK_OK;
   }

   return status;
}

#if 0
static VMK_ReturnStatus
LogFSOpReadlink(ObjDescriptorInt * symlink, char *buf, uint32 bufLen,
                uint32 *linkLength)
{
#if 0
   FileDescriptorInt *fd = FILEDESC(symlink);
   char *symName = fd->fileData;
   zprintf("symlink %p %s\n", symName, symName);
   ASSERT(symName);

   strcpy(buf, symName);
   *linkLength = strlen(symName);

   return VMK_OK;
#endif
   return VMK_NOT_IMPLEMENTED;
}
#endif

static VMK_ReturnStatus
LogFSOpRenameFile(ObjDescriptorInt * srcDir, const char *srcName,
                  ObjDescriptorInt * dstDir, const char *dstName)
{
   int i;
   VMK_ReturnStatus status;

   if (srcDir != dstDir)
      return VMK_NOT_SUPPORTED;

   LogFS_OID parentOID = LogFS_GetIDFromOID(&srcDir->oid.oid);
   if (parentOID.type != LOGFS_DIR)
      return VMK_NOT_SUPPORTED;

   Hash diskId = parentOID.diskId;

   LogFS_PosixDir *dir = aligned_malloc(LOGFS_POSIXDIRSIZE);
   status = LogFS_PosixReadDirectory(dir, diskId);

   if (status != VMK_OK)
      goto out;

   zprintf("rename %s -> %s\n", srcName, dstName);

   LogFS_PosixDirEntry *e;

   status = VMK_NOT_FOUND;

   for (i = 0, e = dir->entries; i < dir->numFiles; ++i, ++e) {
      if (strcmp(e->name, srcName) == 0) {
         strcpy(e->name, dstName);
         status = VMK_OK;
         break;
      }
   }

 out:
   status = LogFS_PosixWriteDirectory(dir, diskId);
   aligned_free(dir);

   return VMK_OK;
}

static VMK_ReturnStatus
LogFSOpUnlink(ObjDescriptorInt * parent, const char *childName)
{
   int i;
   VMK_ReturnStatus status;

   LogFS_OID parentOID = LogFS_GetIDFromOID(&parent->oid.oid);
   if (parentOID.type != LOGFS_DIR)
      return VMK_NOT_SUPPORTED;

   Hash diskId = parentOID.diskId;

   LogFS_PosixDir *dir = aligned_malloc(LOGFS_POSIXDIRSIZE);
   status = LogFS_PosixReadDirectory(dir, diskId);

   if (status != VMK_OK)
      goto out;

   LogFS_PosixDirEntry *e;

   status = VMK_NOT_FOUND;

   int n = dir->numFiles;

   for (i = 0, e = dir->entries; i < n; ++i, ++e) {
      if (strcmp(e->name, childName) == 0) {
         memmove(e, e + 1, ((n - i) - 1) * sizeof(LogFS_PosixDirEntry));
         status = VMK_OK;
         --(dir->numFiles);
         break;
      }
   }

 out:
   status = LogFS_PosixWriteDirectory(dir, diskId);
   aligned_free(dir);

   return status;
}

static VMK_ReturnStatus
LogFSIoctl(ObjDescriptorInt * file, IOCTLCmdVmfs cmd, void *dataIn,
           void *result)
{
   switch (cmd) {
   case IOCTLCMD_VMFS_SET_SCHED_POLICY:
      /*
       * There is nothing for us to do for IOCTLCMD_VMFS_SET_SCHED_POLICY 
       * right now, maybe a future item.
       */
      return VMK_OK;
   case IOCTLCMD_VMFS_VERIFY_LOCK:
      {
         /*
          * IOCTL to validate the lock if the lock held by fd is still valid
          * returns VMK_OK if fd has a valid lock, VMK_NO_CONNECT otherwise
          */
         //FileDescriptorInt *fd = FILEDESC(file);
         VMK_ReturnStatus status = VMK_NO_CONNECT;

         //if (!DISK_LOCK_INACCESSIBLE(fd) && DISK_LOCK_VALID(fd)) {
         status = VMK_OK;
         //}
         return status;
      }
   default:
      return VMK_INVALID_IOCTL;
   }
}

static FSS_FileOps logfsFileOps = {
   .FSS_OpenFile = LogFSOpOpenFile,
   .FSS_CheckAccess = LogFSOpCheckAccess,
   .FSS_CloseFile = LogFSOpCloseFile,
   .FSS_NotifyClose = (FSS_NotifyCloseOp) LogFSNotSupported,
   .FSS_FileIO = (FSS_FileIOOp) LogFSOpFileIO,
   .FSS_Readlink = (FSS_ReadlinkOp) LogFSNotSupported,

   .FSS_GetFileAttributes = LogFSOpGetFileAttributes,
   .FSS_SetFileAttributes = (FSS_SetFileAttributesOp) LogFSOpSetFileAttributes,

   .FSS_ReserveFile = (FSS_ReserveFileOp) LogFSNotSupported,
   .FSS_ReleaseFile = (FSS_ReleaseFileOp) LogFSNotSupported,
   .FSS_AbortCommand = (FSS_AbortCommandOp) N_A,   //LogFSOpAbortCommand,
   .FSS_ResetCommand = (FSS_ResetCommandOp) N_A,   //LogFSOpResetCommand,

   .FSS_Readdir = (FSS_ReaddirOp) LogFSNotSupported,
   .FSS_CreateFile = (FSS_CreateFileOp) LogFSNotSupported,
   .FSS_Symlink = (FSS_SymlinkOp) LogFSNotSupported,
   .FSS_Unlink = (FSS_UnlinkOp) LogFSNotSupported,
   .FSS_Rmdir = (FSS_RmdirOp) LogFSNotSupported,
   .FSS_RenameFile = (FSS_RenameFileOp) LogFSNotSupported,
   .FSS_Ioctl = LogFSIoctl,
   .FSS_Poll = (FSS_PollOp) LogFSNotSupported,
};

#if 0
static FSS_FileOps logfsSymlinkOps = {
   .FSS_OpenFile = (FSS_OpenFileOp) LogFSNotSupported,
   .FSS_CheckAccess = LogFSOpCheckAccess,
   .FSS_CloseFile = (FSS_CloseFileOp) LogFSNotSupported,
   .FSS_NotifyClose = (FSS_NotifyCloseOp) LogFSNotSupported,
   .FSS_FileIO = (FSS_FileIOOp) LogFSNotSupported,
   .FSS_Readlink = LogFSOpReadlink,

   .FSS_GetFileAttributes = LogFSOpGetFileAttributes,
   .FSS_SetFileAttributes = LogFSOpSetFileAttributes,

   .FSS_ReserveFile = (FSS_ReserveFileOp) LogFSNotSupported,
   .FSS_ReleaseFile = (FSS_ReleaseFileOp) LogFSNotSupported,
   .FSS_AbortCommand = (FSS_AbortCommandOp) LogFSNotSupported,
   .FSS_ResetCommand = (FSS_ResetCommandOp) LogFSNotSupported,

   .FSS_Readdir = (FSS_ReaddirOp) LogFSNotSupported,
   .FSS_CreateFile = (FSS_CreateFileOp) LogFSNotSupported,
   .FSS_Symlink = (FSS_SymlinkOp) LogFSNotSupported,
   .FSS_Unlink = (FSS_UnlinkOp) LogFSNotSupported,
   .FSS_Rmdir = (FSS_RmdirOp) LogFSNotSupported,
   .FSS_RenameFile = (FSS_RenameFileOp) LogFSNotSupported,
   .FSS_Ioctl = (FSS_IoctlOp) LogFSNotSupported,
   .FSS_Poll = (FSS_PollOp) LogFSNotSupported,
};
#endif

static FSS_FileOps logfsDirOps = {
   .FSS_OpenFile = (FSS_OpenFileOp) LogFSOpOpenFile,
   .FSS_CheckAccess = (FSS_CheckAccessOp) N_B,  //LogFSNotSupported,//LogFSOpCheckAccess,
   .FSS_CloseFile = (FSS_CloseFileOp) N_C,   //LogFSNotSupported,//LogFSOpCloseDirectory,
   .FSS_NotifyClose = (FSS_NotifyCloseOp) LogFSNotSupported,
   .FSS_FileIO = (FSS_FileIOOp) LogFSIsADirectory,
   .FSS_Readlink = (FSS_ReadlinkOp) LogFSNotSupported,

   .FSS_GetFileAttributes = (FSS_GetFileAttributesOp) LogFSOpGetFileAttributes,
   .FSS_SetFileAttributes = (FSS_SetFileAttributesOp) N_E,  //LogFSNotSupported, //LogFSOpSetFileAttributes,

   .FSS_ReserveFile = (FSS_ReserveFileOp) LogFSNotSupported,
   .FSS_ReleaseFile = (FSS_ReleaseFileOp) LogFSNotSupported,
   .FSS_AbortCommand = (FSS_AbortCommandOp) LogFSNotSupported,
   .FSS_ResetCommand = (FSS_ResetCommandOp) LogFSNotSupported,

   .FSS_Readdir = LogFSOpReaddir,
   .FSS_CreateFile = (FSS_CreateFileOp) LogFSOpCreateFile,
   .FSS_Symlink = (FSS_SymlinkOp) LogFSNotSupported,  //LogFSOpSymlink,
   .FSS_Unlink = LogFSOpUnlink,
   .FSS_Rmdir = (FSS_RmdirOp) LogFSNotSupported,   //LogFSOpRmdir,
   .FSS_RenameFile = (FSS_RenameFileOp) LogFSOpRenameFile,
   .FSS_Ioctl = (FSS_IoctlOp) LogFSNotSupported,
   .FSS_Poll = (FSS_PollOp) LogFSNotSupported,
};

VMK_ReturnStatus LogFS_RegisterPosixInterface(void)
{
   List_Init(&cachedRootEntries);
   SP_InitLock("posixLock", &posixLock, SP_RANK_POSIX);

   if (FSS_RegisterFSWithFlags("cloudfs", &logfsFSOps, logfsModuleID,
                               LOGFS_FSTYPENUM, FS_REGISTER_WITH_DM) == -1) {
      zprintf("register fs failed\n");
      return VMK_FAILURE;
   } else
      zprintf("cloudfs FSS POSIX plugin loaded\n");
   return VMK_OK;
}

VMK_ReturnStatus LogFS_UnregisterPosixInterface(void)
{
   List_Links *curr, *next;
   SP_Lock(&posixLock);
   LIST_FORALL_SAFE(&cachedRootEntries, curr, next) {
      LogFS_PosixCachedRootEntry *re = List_Entry(curr,
            LogFS_PosixCachedRootEntry, next);
      List_Remove(curr);
      free(re);
   }
   SP_Unlock(&posixLock);

   SP_CleanupLock(&posixLock);
   FSS_UnregisterFS(&logfsFSOps, logfsModuleID);
   return VMK_OK;
}
