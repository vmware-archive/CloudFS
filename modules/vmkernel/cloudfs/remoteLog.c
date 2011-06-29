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

#include "logfsNet.h"
#include "logfsLog.h"
#include "metaLog.h"
#include "vDisk.h"
#include "remoteLog.h"
#include "vDiskMap.h"
#include "globals.h"

typedef struct {
   List_Links next;
   Async_Token *token;
   SG_Array *sgArr;
   size_t sent;

} LogFS_RemoteLogWrite;

static SP_SpinLock remoteLogsLock;

static List_Links asyncLogs;

static List_Links waitQueue;

static void LogFS_RemoteLogThread(void *data)
{
   VMK_ReturnStatus status;
   List_Links tmpList;

   for (;;) {

      SP_Lock(&remoteLogsLock);

      if (List_IsEmpty(&asyncLogs)) {
         CpuSched_Wait(&waitQueue, CPUSCHED_WAIT_SCSI, &remoteLogsLock);
         continue;
      }

      List_Init(&tmpList);
      List_Append(&tmpList, &asyncLogs);
      SP_Unlock(&remoteLogsLock);

      /* If we get to here, tmpList is non-empty */

      List_Links *curr, *next;
      LIST_FORALL_SAFE(&tmpList, curr, next) {
         LogFS_RemoteLog *rl = List_Entry(curr, LogFS_RemoteLog, next);

         status = LogFS_RemoteLogSpoolFromRevision(rl);
         zprintf("spool done %s\n", VMK_ReturnStatusToString(status));

#if 0
         if (status == VMK_WOULD_BLOCK) {
            SP_Lock(&remoteLogsLock);
            List_Insert(&rl->next, LIST_ATREAR(&asyncLogs));
            SP_Unlock(&remoteLogsLock);
         }
#endif

         if (status != VMK_OK) {
            zprintf("spooler returns error!\n");

            /* Activate the worldlet servicing this connection. It will try
             * to pop() an update, which will fail with VMK_LIMIT_EXCEEDED.
             * This will cause the connection to be closed. */

            rl->shouldClose = TRUE;
            vmk_WorldletActivate(rl->worldlet);
            //LogFS_RemoteLogRelease(rl);
         }
      }
   }

}

VMK_ReturnStatus
LogFS_RemoteLogPopUpdate(LogFS_RemoteLog *rl,
                         SG_Array **sgArr, Async_Token ** token)
{
   VMK_ReturnStatus status;

   SP_Lock(&rl->lock);

   if (rl->shouldClose) {
      status = VMK_LIMIT_EXCEEDED;
   }

   else if (!List_IsEmpty(&rl->outstandingWrites)) {
      List_Links *elem;
      elem = List_First(&rl->outstandingWrites);
      LogFS_RemoteLogWrite *write =
          List_Entry(elem, LogFS_RemoteLogWrite, next);
      List_Remove(elem);
      *sgArr = write->sgArr;
      *token = write->token;

      rl->numBuffered -= SG_TotalLength(write->sgArr);

      status = VMK_OK;
   }
// else if(rl->shouldClose) status = VMK_LIMIT_EXCEEDED;
   else
      status = VMK_WOULD_BLOCK;

   SP_Unlock(&rl->lock);

   return status;
}

void LogFS_RemoteLogRef(LogFS_RemoteLog *rl)
{
   Atomic_Inc(&rl->refCount);
}

void LogFS_RemoteLogRelease(LogFS_RemoteLog *rl)
{
   if (Atomic_FetchAndDec(&rl->refCount) == 1) {
      free(rl);
   }
}

/* Add a new update to the transmission queue. 
 * Must be called with rl->lock held. */

static inline VMK_ReturnStatus
LogFS_RemoteLogPushUpdate(LogFS_RemoteLog *rl,
      SG_Array *sgArr, Async_Token * token)
{
   VMK_ReturnStatus status;
   ASSERT(sgArr);

   if (LogFS_RemoteLogBufferFull(rl)) {
      status = VMK_WOULD_BLOCK;
   } else {
      LogFS_RemoteLogWrite *write = malloc(sizeof(LogFS_RemoteLogWrite));
      write->sgArr = SG_Dup(logfsHeap,sgArr,sgArr->length);
      write->token = token;

      List_Insert(&write->next, LIST_ATREAR(&rl->outstandingWrites));
      rl->numBuffered += SG_TotalLength(sgArr);
      status = VMK_OK;
   }

   vmk_WorldletActivate(rl->worldlet);

   return status;
}

static inline VMK_ReturnStatus
LogFS_RemoteLogPushUpdateSimple(LogFS_RemoteLog *rl, 
      void *buf, size_t count,
      Async_Token *token)
{
   SG_Array sg;
   SG_SingletonSGArray(&sg, 0, (VA) buf, count, SG_VIRT_ADDR);
   return LogFS_RemoteLogPushUpdate(rl,&sg,token);
}

void LogFS_RemoteLogPreInit(void)
{
   VMK_ReturnStatus status;
   static World_ID serverWorld;

   List_Init(&asyncLogs);
   List_Init(&waitQueue);

   SP_InitLock("asyncLogs", &remoteLogsLock, SP_RANK_REMOTELOG);

   status = World_NewSystemWorld("remotelog", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &serverWorld);
   Sched_Add(World_Find(serverWorld), LogFS_RemoteLogThread, NULL);

}

void LogFS_RemoteLogInit(LogFS_RemoteLog *rl,
                         LogFS_MetaLog *ml,
                         vmk_Worldlet worldlet,
                         LogFS_VDisk *vd,
                         uint64 lsn)
{
   Atomic_Write(&rl->refCount, 1);
   rl->isSocketOpen = TRUE;
   rl->shouldClose = FALSE;
   rl->ml = ml;
   rl->worldlet = worldlet;
   rl->lsn = lsn;
   rl->vd = vd;
   SP_InitLock("remotelock", &rl->lock, SP_RANK_REMOTELOG);
   List_Init(&rl->outstandingWrites);
   rl->numBuffered = 0;

   SP_Lock(&remoteLogsLock);
   List_Insert(&rl->next, LIST_ATREAR(&asyncLogs));
   CpuSched_Wakeup(&waitQueue);
   SP_Unlock(&remoteLogsLock);

}

void LogFS_RemoteLogClose(LogFS_RemoteLog *rl)
{
   if (rl->shouldClose) {
      zprintf("Warning: recursive call to %s\n", __FUNCTION__);
      return;
   }

   rl->shouldClose = TRUE;
   SP_Lock(&rl->lock);

   while (!List_IsEmpty(&rl->outstandingWrites)) {
      zprintf("cancel write\n");
      List_Links *elem;
      LogFS_RemoteLogWrite *write;

      elem = List_First(&rl->outstandingWrites);
      write = List_Entry(elem, LogFS_RemoteLogWrite, next);

      Async_Token *token = write->token;
      token->transientStatus = VMK_WRITE_ERROR;
      Async_TokenCallback(token);

      List_Remove(elem);
      free(write);
   }

   SP_Unlock(&rl->lock);
   CpuSched_Wakeup(&waitQueue);
}

void LogFS_RemoteLogExit(LogFS_RemoteLog *rl)
{
   Bool closeSocket = FALSE;

   SP_Lock(&rl->lock);
   closeSocket = rl->isSocketOpen;
   SP_Unlock(&rl->lock);

   LogFS_RemoteLogRelease(rl);
}

VMK_ReturnStatus LogFS_RemoteLogAppend(LogFS_RemoteLog *rl, 
      SG_Array *sgArr,
      Async_Token * token)
{
   VMK_ReturnStatus status;

   SP_Lock(&rl->lock);

   if (!rl->isSocketOpen) {
      status = VMK_BAD_PARAM;
   } else
      status = LogFS_RemoteLogPushUpdate(rl, sgArr, token);

   //CpuSched_Wakeup(&waitQueue);
   SP_Unlock(&rl->lock);

   /* If buffer is full, move to async queue (may be there already) */
   if (status == VMK_WOULD_BLOCK) {
      zprintf("move to async\n");
      SP_Lock(&remoteLogsLock);
      List_Remove(&rl->next);
      List_Insert(&rl->next, LIST_ATREAR(&asyncLogs));
      SP_Unlock(&remoteLogsLock);
   } else if (status != VMK_OK)
      zprintf("ret bad status %s\n", VMK_ReturnStatusToString(status));

   return status;
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_RemoteLogSpoolFromRevision --
 *
 *      Asynchronously copy data from the local disk, to the socket connected
 *      to the RemoteLog
 *
 * Results:
 *      None
 *
 * Side effects:
 *      None
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus LogFS_RemoteLogSpoolFromRevision(LogFS_RemoteLog *rl)
{
   VMK_ReturnStatus status = VMK_OK;
   log_segment_id_t segment;

   LogFS_MetaLog *ml = rl->ml;

   /* Check for the special case where the remote party is completely up to date,
    * either because this is a disk with 0 revisions, or just because the 
    * replica has been completely synced already.  */

   LogFS_VDisk *vd = rl->vd;
   ASSERT(vd);

   if (LogFS_VDiskAddRemoteLogIfSameVersion(vd, rl)) {
      return VMK_OK;
   }

   status = LogFS_BTreeRangeMapLookupLsn( LogFS_VDiskGetVersionsMap(vd),
      rl->lsn, &segment);
      
   if (status != VMK_OK) {
      return status;
   }


   zprintf("segment %lu is candidate for revision\n", segment);
   struct log_head *head = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);

   log_offset_t e;
   LogFS_Log *log = LogFS_MetaLogGetLog(ml, segment);

   for (e = 0;;) {
      do {
         status = LogFS_LogReadBody(log, NULL, head, LOG_HEAD_SIZE, e);
      } while (status == VMK_BUSY || status == VMK_STORAGE_RETRY_OPERATION);
      int retries = 0;

      if (status == VMK_LIMIT_EXCEEDED) {
         zprintf("limit exceeded at e %ld.%ld\n", LogFS_LogGetSegment(log), e);

         /* There is a race where the Bloomfilter is updated before 
          * the write is stable. */
         CpuSched_Sleep(100);
         if (++retries < 10)
            continue;

         else
            zprintf("giving up LIMIT_EXCEEDED!\n");
         status = VMK_NOT_FOUND;
         goto out;
      }

      else if (status != VMK_OK) {
         zprintf("read error log %ld, e %ld\n", log->index, e);
         NOT_REACHED();
         goto out;
      }

      if (is_block_zero((char *)head)) {
         zprintf("version not found!!\n");
         log = LogFS_MetaLogPutLog(ml, log);
         break;
      }

      if (head->update.lsn == rl->lsn+1) {
         zprintf("child revision found in segment %lu offset %" FMT64 "d\n",
                 segment, e);
         break;
      }
      e += log_entry_size(head);
   }

   size_t take;

   for (take = 0;; e += take) {

      do { status = LogFS_LogReadBody(log, NULL, head, LOG_HEAD_SIZE, e);
      } while (status == VMK_BUSY || status == VMK_STORAGE_RETRY_OPERATION);

      if (status == VMK_LIMIT_EXCEEDED) {
         zprintf("limit exceeded\n");

         if (LogFS_VDiskAddRemoteLogIfSameVersion(vd, rl)) {
            return VMK_OK;
         }

         take = 0;

         //CpuSched_Wait(&ml->remoteWaiters, CPUSCHED_WAIT_SCSI, NULL);
         CpuSched_TimedWait(&ml->remoteWaiters, CPUSCHED_WAIT_SCSI, NULL, 500, NULL);
         continue;
      }

      else if (status != VMK_OK) {
         Panic("read error log %ld, e %ld, %s\n", log->index, e,
               VMK_ReturnStatusToString(status));
         goto out;
      }

      if (is_block_zero((char *)head)) {
         zprintf("at end, log %ld block %" FMT64 "d\n", log->index,
                 e / BLKSIZE);

         if (rl->shouldClose) {
            zprintf("closing\n");
            return VMK_OK;
         }

         if (LogFS_VDiskAddRemoteLogIfSameVersion(vd, rl)) {
            return VMK_OK;
         }

         take = 0;
         //CpuSched_Wait(&ml->remoteWaiters, CPUSCHED_WAIT_SCSI, NULL);
         CpuSched_TimedWait(&ml->remoteWaiters, CPUSCHED_WAIT_SCSI, NULL, 500, NULL);
         continue;
      }

      if (head->tag == log_pointer_type && head->direction == log_prev_ptr) {
         take = LOG_HEAD_SIZE;
      }

      else if (head->tag == log_pointer_type && head->direction == log_next_ptr) {
         log_id_t position;
         position.v.segment = log->index;
         position.v.blk_offset = e / BLKSIZE;

         if (ml->compactionInProgress) {
            zprintf("GC ongoing\n");
            NOT_REACHED();
#if 0
            log_id_t redirect = LogFS_MetaLogLookupIndirection(ml, position);

            if (!is_invalid_version(redirect)) {
               head->target = redirect;
            }
#endif
         }

         log_segment_id_t segment = head->target.v.segment;
         e = head->target.v.blk_offset * BLKSIZE;

         zprintf("next %" FMT64 "d.%" FMT64 "d\n", segment, e / BLKSIZE);

         log = LogFS_MetaLogPutLog(ml, log);
         log = LogFS_MetaLogGetLog(ml, segment);

         take = 0;              /* We just set e directly */
      }

      else if (head->tag == log_entry_type) {

         /* Verify that this log entry fits in the hash chain requested by the
          * remote client */

         size_t sz = 0;

         if ( LogFS_HashEquals( LogFS_VDiskGetBaseId(vd),
                  LogFS_HashFromRaw(head->disk)) &&  rl->lsn+1 == head->update.lsn )
         {
            /* Now read the entry body */

 andagain:
            sz = log_body_size(head);
            take = LOG_HEAD_SIZE + sz;
            char *headAndBody = aligned_malloc(LOG_HEAD_SIZE + sz);
            char *body = headAndBody + LOG_HEAD_SIZE;
            memcpy(headAndBody, head, LOG_HEAD_SIZE);

            if (sz > 0) {
               do {
                  status =
                      LogFS_LogReadBody(log, NULL, body, sz, e + LOG_HEAD_SIZE);
               } while (status == VMK_BUSY
                        || status == VMK_STORAGE_RETRY_OPERATION);

               if (status != VMK_OK) {
                  if (status != VMK_LIMIT_EXCEEDED)
                     Panic("read error log %s %ld, e %ld\n",
                           VMK_ReturnStatusToString(status), log->index, e);
                  goto out;
               }
            }

            /* verify checksum before sending */

            unsigned char checksum[SHA1_DIGEST_SIZE];
            log_entry_checksum(checksum, head, body, sz);

            if (memcmp(checksum, head->update.checksum, SHA1_DIGEST_SIZE) != 0) {
               zprintf("chk prob\n");

               LogFS_Hash stable = LogFS_VDiskGetCurrentId(vd);
               if (memcmp(head->id, stable.raw, SHA1_DIGEST_SIZE) == 0) {
                  //zprintf("this happened on the stable version!\n");
                  CpuSched_Sleep(50);
                  goto andagain;
               } else {
                  char a[SHA1_HEXED_SIZE];
                  char b[SHA1_HEXED_SIZE];
                  hex(a, checksum);
                  hex(b, head->update.checksum);
                  zprintf("warning: bad content checksum %s != %s!\n", a, b);
                  zprintf("blkno %" FMT64 "d + %d\n",
                          head->update.blkno, head->update.num_blocks);

                  status = VMK_READ_ERROR;
                  goto out;
               }
            }

            /* Set up a callback to free the buffer after send */
            Async_Token *token = Async_AllocToken(0);
            *((void **)Async_PushCallbackFrame(token,
                                               LogFS_FreeSimpleBuffer,
                                               sizeof(void *))) = headAndBody;

            /* Queue up the buffer for sending, throttle if not possible right now.
             * The ideal would be to try servicing some other remotelog if this one's
             * connection cannot keep up. */

            int blocks;

            for (blocks = 0;;) {
               SP_Lock(&rl->lock);
               status =
                   LogFS_RemoteLogPushUpdateSimple(rl, headAndBody,
                                             LOG_HEAD_SIZE + sz, token);
               SP_Unlock(&rl->lock);

               if (status == VMK_WOULD_BLOCK) {
                  //zprintf("blocking to wait for HTTPd\n");
                  CpuSched_Sleep(100);
                  if (++blocks > 20)
                     goto out;
               } else
                  break;
            }

            rl->lsn = head->update.lsn;

            /* If we caught up with all writes to this virtual disk, we must
             * switch to synchronous mode. We do not check for the stable current
             * version, because the fact that we just read it from disk without 
             * problems means that it must be stable.
             * We let VDisk decide, to get proper locking and prevent a race. */

#if 0
            if (LogFS_VDiskAddRemoteLogIfSameVersion(vd, rl)) {
               zprintf("synced with %u bytes buffered\n", rl->numBuffered);
               vmk_WorldletActivate(rl->worldlet);
               goto out;
            }
#endif
         } else {
            /* this one was not for us, just skip it */
            take = log_entry_size(head);
         }

      }

      else {
         zprintf("unknown log entry %u, log %ld, blk-offset %ld!\n", head->tag,
                 log->index, e / BLKSIZE);
         status = VMK_READ_ERROR;
         //NOT_REACHED();
         goto out;
      }

   }

 out:
   LogFS_MetaLogPutLog(ml, log);
   zprintf("spool status %s\n", VMK_ReturnStatusToString(status));

   return status;
}
