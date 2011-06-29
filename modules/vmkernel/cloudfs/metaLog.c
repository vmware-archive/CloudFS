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
/*
 *
 * The MetaLog gathers multiple fixed-sized log segments into a larger log that will 
 * span the entire physical volume. It keeps state about free segments, and allocates
 * new segments when old ones fill up. It manages the in-memory cache of Log descriptors
 * that are accessed via the GetLog/PutLog interface.
 *
 * It also keeps track of the Bloom filter segments used when looking up a
 * certain version number. Each Bloom filter index (basically a bit matrix with a column
 * for each log segment) currently indexes 32GB of disk space, and when all columns are
 * filled, it is flushed to disk and a new one created. Because the exact amount of segments
 * needed for Bloom filters for a physical volume can be computed at startup, they really
 * should all be allocated beforehand, so they are close together and easy to find on 
 * a reboot.
 *
 * XXX This class really needs to be split up into several pieces.
 *
 */

#include <system.h>

#include "globals.h"
#include "metaLog.h"
#include "vDisk.h"
// #include "remoteLog.h"
#include "logfsCheckPoint.h"
#include "logfsDiskLayout.h"
#include "vebTree.h"
#include "vDiskMap.h"
#include "hashDb.h"
#include "fingerPrint.h"
#include "ownerMap.h"
#include "graph.h"

void LogFS_MetaLogInit(LogFS_MetaLog *ml, LogFS_Device *device)
{
   ml->device = device;

   SP_InitLock("appendlock", &ml->append_lock, SP_RANK_METALOG);
   SP_InitLock("refcountslock", &ml->refcounts_lock, SP_RANK_REFCOUNTS);

   LogFS_SegmentListInit(&ml->segment_list);

   memset(ml->openLogs, 0, sizeof(ml->openLogs));

   ml->compactionInProgress = FALSE;

   ml->activeLog = NULL;
   ml->spaceLeft = 0;

   LogFS_ObsoletedSegmentsInit(&ml->obsoleted);
   LogFS_ObsoletedSegmentsInit(&ml->dupes);

   ml->vt = malloc(sizeof(LogFS_VebTree));
   LogFS_VebTreeInit(ml->vt,NULL,0x200,0x10000);
   LogFS_VebTreeClear(ml->vt);

   LogFS_HashDb *hd = malloc(sizeof(LogFS_HashDb));
   LogFS_HashDbInit(hd);
   ml->hd = hd;

   List_Init(&ml->remoteWaiters);

   //extern void LogFS_DedupeInit(LogFS_MetaLog *ml);
   //LogFS_DedupeInit(ml);
}

void LogFS_MetaLogCleanup(LogFS_MetaLog *ml)
{
   int i;

   LogFS_ObsoletedSegmentsCleanup(&ml->obsoleted);
   LogFS_ObsoletedSegmentsCleanup(&ml->dupes);
   free(ml->hd);

   for (i = 0; i < MAX_OPEN_LOGS; i++) {
      LogFS_Log *log;
      if ((log = ml->openLogs[i])) {
         LogFS_LogClose(log);
         free(log);
      }
   }

   for(i=0 ; i<sizeof(ml->fingerPrints)/sizeof(ml->fingerPrints[0]); ++i) {
      LogFS_FingerPrint *fp = ml->fingerPrints[i];
      if (fp) {
         LogFS_FingerPrintCleanup(fp);
         free(fp);
      }
   }

   LogFS_VebTreeCleanup(ml->vt);
   free(ml->vt);
}

void LogFS_MetaLogFreeLog(LogFS_MetaLog *ml, LogFS_Log *log)
{
   /* postpone freeing the log until this thread is the only one holding a 
    * reference to it */

   log_segment_id_t index = log->index;
   int waitCount = 0;
   while (Atomic_Read(&log->refCount) > 1) {
      CpuSched_Sleep(200);
      if (waitCount++ > 20) {
         zprintf("segment %" FMT64 "d leaked\n", index);
         //NOT_REACHED();
         return;                /* give up :-( this is a bug */
      }
   }
   LogFS_MetaLogPutLog(ml, log);
   LogFS_SegmentListFreeSegment(&ml->segment_list, index);
}

/* XXX the caching functionality in the class is quite broken. We should
 * use the algorithms from PagedTree instead.  */

LogFS_Log *LogFS_MetaLogGetLog(LogFS_MetaLog *ml, log_segment_id_t segment)
{
   LogFS_Log *log = NULL;

   SP_Lock(&ml->refcounts_lock);

   int i;
   int first_free_slot = -1;

   for (i = 0; i < MAX_OPEN_LOGS; i++) {
      LogFS_Log *slot = ml->openLogs[i];
      if (slot) {
         if (slot->index == segment) {
            log = slot;
            Atomic_Inc(&log->refCount);
            goto out;
         }
      } else if (first_free_slot < 0) {
         first_free_slot = i;
      }
   }

   if (first_free_slot >= 0) {
      log = malloc(sizeof(LogFS_Log));
      LogFS_LogInit(log, ml, segment);

      Atomic_Write(&log->refCount, 1);
      ml->openLogs[first_free_slot] = log;
   }

   else {
      printf("out of open_log space!!\n");
      NOT_REACHED();
   }

 out:
   SP_Unlock(&ml->refcounts_lock);
   ASSERT(log);
   return log;
}

LogFS_Log *LogFS_MetaLogPutLog(LogFS_MetaLog *ml, LogFS_Log *log)
{
   SP_Lock(&ml->refcounts_lock);

   if (Atomic_FetchAndDec(&log->refCount) == 1) {
      int i;

      for (i = 0; i < MAX_OPEN_LOGS; i++) {
         if (ml->openLogs[i] == log) {
            ml->openLogs[i] = NULL;
            LogFS_LogClose(log);
            free(log);
            break;
         }
      }
      ASSERT(i != MAX_OPEN_LOGS);

   }

   SP_Unlock(&ml->refcounts_lock);
   return NULL;
}


typedef struct {
   LogFS_MetaLog *ml;
   LogFS_Log *log;
   void *endHead, *beginHead;
} LogFS_MetaLogCloseContext;

void LogFS_MetaLogCloseDone(Async_Token * token, void *data)
{
   LogFS_MetaLogCloseContext *c = data;

   if (c->log)
      LogFS_MetaLogPutLog(c->ml, c->log);
   aligned_free(c->endHead);
   aligned_free(c->beginHead);
   Async_TokenCallback(token);
}

VMK_ReturnStatus LogFS_MetaLogReopen(LogFS_MetaLog *ml, log_id_t position)
{
   log_segment_id_t s = position.v.segment;
   log_offset_t end = position.v.blk_offset * BLKSIZE;

   ml->activeLog = LogFS_MetaLogGetLog(ml, s);
   ml->spaceLeft = LOG_MAX_SEGMENT_SIZE - end;

   zprintf("reopen end %lu\n",end);
   LogFS_AppendLogInit(ml->activeLog, ml, s, end);

   return VMK_OK;
}


#if 0
/* ml->writeLock is assumed held to protect ml->hd */

void LogFS_MetaLogDedupeSg(
      LogFS_MetaLog *ml,
      Async_Token *token,
      log_id_t pos,
      LogFS_HashDb *hd,
      SG_Array *sgArr,
      SG_Array **sgOut)
{
   /* Dedupe blocks in update within scope of metaLog's hashDb */

   struct log_head *head = (struct log_head*) sgArr->sg[0].addr;

   SG_Array *out = SG_Alloc(logfsHeap,1024);
   out->length = 1;
   out->addrType = SG_VIRT_ADDR;
   out->sg[0] = sgArr->sg[0];

   ASSERT(out->sg[0].offset==0);

   int numTrailers = (((SG_TotalLength(sgArr)-LOG_HEAD_SIZE) / BLKSIZE) + 63) / 64;
   //zprintf("len %u, trailers %d\n",SG_TotalLength(sgArr),numTrailers);
   ASSERT(numTrailers<=7);
   size_t sz = LOG_HEAD_SIZE + numTrailers * BLKSIZE;

   struct log_head *head2 = aligned_malloc(sz);
   memset(head2,0,sz);
   memcpy(head2,head,LOG_HEAD_SIZE);// + head->update.trailers * BLKSIZE);

   log_id_t *trailer = (log_id_t*) (((char*)head2) + LOG_HEAD_SIZE);
   head2->update.trailers = numTrailers;

   pos.v.blk_offset += (1+numTrailers); /* For head and trailer */

   out->sg[0].addr = (VA) head2;
   out->sg[0].length = sz;

   uint64 offset = sz;
   int i,j,k;

   int dups = 0;

   for(i=1,k=0; i<sgArr->length; ++i) {

      for(j=0;j<sgArr->sg[i].length/BLKSIZE;++j) {

         char *blk = (char*) (sgArr->sg[i].addr + BLKSIZE*j);
         Hash h = LogFS_HashChecksum(blk,BLKSIZE);

         //LogFS_FingerPrintAddHash(log->fp,h);

         log_id_t existing;

         /* Only attempt to reuse duplicates if not
          * done already. */

#if 0
         if (trailer[k].raw != 0) {

            existing = trailer[k];

         }  else {

            existing = LogFS_HashDbLookupHash(hd,h,pos);

         }
#else
         existing = LogFS_HashDbLookupHash(hd,h,pos);

#endif

         ASSERT(k<LOG_HEAD_MAX_BLOCKS);

         //zprintf("%d <- %016lx\n",k,existing.raw);

         /* Is this the first occurence of the block within this segment? */

         if(existing.raw == pos.raw) {

            /* Increase current sg element by BLKSIZE, or create a new one */

            if(out->length>1 && 
                  out->sg[out->length-1].addr == ((VA)blk)-BLKSIZE) {

               out->sg[out->length-1].length += BLKSIZE;

            } else {
               ASSERT(out->length<1024);
               out->sg[out->length].addr = (VA) blk;
               out->sg[out->length].offset = offset;
               out->sg[out->length].length = BLKSIZE;
               ++(out->length);
            }

            ++(pos.v.blk_offset);
            offset += BLKSIZE;

         //   trailer[k].raw = 0;

            LogFS_FingerPrintAddHash(ml->fp,h);

         } else { 
            

            ++dups;
         }

         trailer[k].raw = existing.raw;
         ++k;
      }
   }
   /* Free head2 when done */

   *((void **)Async_PushCallbackFrame(token, LogFS_FreeSimpleBuffer,
            sizeof(void *))) = head2;

   *sgOut = out;
}
#endif

VMK_ReturnStatus
LogFS_MetaLogAppend(LogFS_MetaLog *ml, Async_Token *token,
      SG_Array *sgArr,
      log_id_t *retVersion, int flags)
{
   VMK_ReturnStatus status = VMK_OK;
   Async_IOHandle *ioh = NULL;

   Hash nil;
   LogFS_HashZero(&nil);

   SP_Lock(&ml->append_lock);

   /* Is current log segment full? */

   if (ml->spaceLeft < SG_TotalLength(sgArr) + 3 * LOG_HEAD_SIZE) {

      log_segment_id_t s = LogFS_SegmentListAllocSegment(&ml->segment_list);

      LogFS_Log *nextLog = LogFS_MetaLogGetLog(ml, s);
      LogFS_AppendLogInit(nextLog, ml, s, 0);
 
      ml->fp = malloc(sizeof(LogFS_FingerPrint));
      LogFS_FingerPrintInit(ml->fp);
      ml->fingerPrints[s] = ml->fp;

      mk_invalid_version(prev);

      ASSERT(token);

      /* append a 'next' pointer to end of log segment before close */

      log_id_t next;
      next.v.segment = s;
      next.v.blk_offset = 0;

      void *beginHead = aligned_malloc(LOG_HEAD_SIZE);
      void *endHead = aligned_malloc(LOG_HEAD_SIZE);

      init_forward_pointer(endHead, next);

      LogFS_MetaLogCloseContext *c = Async_PushCallbackFrame(token,
                                                             LogFS_MetaLogCloseDone,
                                                             sizeof
                                                             (LogFS_MetaLogCloseContext));
      c->ml = ml;
      c->log = ml->activeLog;
      c->beginHead = beginHead;
      c->endHead = endHead;

      Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);

      /* Except if its the first log segment ever */

      if (ml->activeLog != NULL) {
         Async_Token *t1 = Async_PrepareOneIO(ioh, NULL);
         status = LogFS_AppendLogAppendSimple(ml->activeLog, t1, endHead, LOG_HEAD_SIZE,
                               &prev, flags);
         ASSERT(status==VMK_OK);
         LogFS_FingerPrintAddHash(ml->fp,nil,NULL,0);

         Async_Token *t2 = Async_PrepareOneIO(ioh, NULL);
         status = LogFS_AppendLogClose(ml->activeLog, t2, flags);
         ASSERT(status==VMK_OK);

         LogFS_FingerPrintFinish(ml->fp,ml->vt,LogFS_LogGetSegment(ml->activeLog));
         ++(ml->lurt);
         zprintf("lurt %d\n",ml->lurt);
      }


      /* start the new segment with a pointer to previous segment */

      Async_Token *t3 = Async_PrepareOneIO(ioh, NULL);
      init_backward_pointer(beginHead, prev);
      status = LogFS_AppendLogAppendSimple(nextLog, t3, beginHead, LOG_HEAD_SIZE, NULL,
                            flags);

      LogFS_FingerPrintAddHash(ml->fp,nil,NULL,0);

      ml->activeLog = nextLog;
      ml->spaceLeft = LOG_MAX_SEGMENT_SIZE;

      /* Because we have split the IO, we override the incoming token
       * with our own */
      if (token != NULL) {
         token = Async_PrepareOneIO(ioh, NULL);
      }

   }

   /* Now we know the log segment has enough space to hold our write. */

   //log_id_t nil;
   //nil.raw = 0;
   //log_id_t pos = ml->activeLog ? LogFS_AppendLogGetEnd(ml->activeLog) : nil;
   //LogFS_MetaLogDedupeSg(ml,token,pos,ml->hd,sgIn,&sgArr);
   //

   struct log_head *head = (struct log_head*) sgArr->sg[0].addr;

   int i,j;

   if (head->tag == log_entry_type) {

      LogFS_VDisk *vd = LogFS_DiskMapLookupDisk( LogFS_HashFromRaw(head->disk));

      /* Add head to fingerprint for completeness */
      LogFS_FingerPrintAddHash(ml->fp,nil,NULL,0);

      int k=0;
      for(i=1; i<sgArr->length; ++i) {

         while(k < head->update.num_blocks && !BitTest(head->update.refs,k)) ++k;

         size_t sz = sgArr->sg[i].length;
         for(j=0;j<sz/BLKSIZE;++j) {

            char *blk = (char*) (sgArr->sg[i].addr + BLKSIZE*j);
            Hash h = LogFS_HashChecksum(blk,BLKSIZE);
            ASSERT(BitTest(head->update.refs,k));
            LogFS_FingerPrintAddHash(ml->fp,h,vd, head->update.blkno + k);


            ++k;

         }
      }
   }

   status = LogFS_AppendLogAppend(ml->activeLog, token, sgArr,
         retVersion, flags);

   /* XXX how do we know the IO has completed? We don't, but the
    * remoteLog will loop around waiting for data. */
   CpuSched_Wakeup(&ml->remoteWaiters);

   ml->spaceLeft -= SG_TotalLength(sgArr);

   /* Did we split the IO above? If so, end it here */
   if (ioh != NULL) {
      Async_EndSplitIO(ioh, status, FALSE);
   }

   SP_Unlock(&ml->append_lock);

   //SG_Free(logfsHeap,&sgArr);

   return status;
}
