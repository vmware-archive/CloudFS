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
#include "globals.h"
#include "logfsLog.h"
#include "logfsIO.h"
#include "metaLog.h"

typedef struct LogFS_LogWriteContext {
   LogFS_Log *log;
   log_offset_t logEnd;
   struct LogFS_LogWriteContext *prev;
   List_Links tokenChain;

   /* Information needed if retrying the write */
   Async_Token *token;
   LogFS_Device *device;
   SG_Array *sgArr;
   int flags;
   int retries;

   struct LogFS_LogWriteContext *origContext;

} LogFS_LogWriteContext;

typedef struct {
   Async_Token *token;
   LogFS_Log *log;
   log_offset_t logEnd;

   List_Links tokenChain;
} LogFS_LogTokenChainElement;

void LogFS_LogInit(LogFS_Log *log,
                   struct LogFS_MetaLog *metaLog, log_segment_id_t index)
{
   log->alive = 1;
   log->index = index;
   log->metaLog = metaLog;
   SP_InitLock("logWriteLock", &log->writeLock, SP_RANK_APPENDLOG);
   Atomic_Write(&log->isAppendLog, 0);
   log->buffer = NULL;
   log->prevWriteContext = NULL;

   Atomic_Write(&log->end, LOG_MAX_SEGMENT_SIZE);
   log->stableEnd = LOG_MAX_SEGMENT_SIZE;
}

void LogFS_AppendLogInit(LogFS_Log *log,
                         struct LogFS_MetaLog *metaLog,
                         log_segment_id_t index, log_offset_t end)
{
   LogFS_LogInit(log, metaLog, index);
   Atomic_Write(&log->isAppendLog, 1);

   Atomic_Write(&log->end, end);
   log->stableEnd = end;
}

char *LogFS_LogEnableBuffering(LogFS_Log *log)
{
   log->buffer = aligned_malloc(LOG_MAX_SEGMENT_SIZE);
   ASSERT(log->buffer);
   memset(log->buffer, 0, LOG_MAX_SEGMENT_SIZE);
   return log->buffer;
}

log_segment_id_t LogFS_LogGetSegment(LogFS_Log *log)
{
   return log->index;
}

void LogFS_LogWriteDone(Async_Token * token, void *data)
{
   LogFS_LogWriteContext *c = data;
   Bool postponeCallback;
   //VMK_ReturnStatus transientStatus = token->transientStatus;

   /* If the write gets aborted we will have no other choice than to 
    * retry the write until it succeeds, XXX implement that */

   if (token->transientStatus == VMK_BUSY && (c->flags & FS_CANTBLOCK) == 0) {
      zprintf("got busy\n");
      if ((!PRDA_BHInProgress()) && (!PRDA_InInterruptHandler()))
         CpuSched_Sleep(1);
   }

   if (token->transientStatus == VMK_ABORTED
       || token->transientStatus == VMK_BUSY
       || token->transientStatus == VMK_STORAGE_RETRY_OPERATION) {
      zprintf("WARNING retry aborted write!\n");
      VMK_ReturnStatus status;
      Async_Token *retryToken = Async_AllocToken(0);

      LogFS_LogWriteContext *rc = Async_PushCallbackFrame(retryToken,
                                                          LogFS_LogWriteDone,
                                                          sizeof
                                                          (LogFS_LogWriteContext));

      *rc = *c;
      rc->origContext = c;
      rc->retries = c->retries + 1;

      if (c->retries > 50)
         Panic(">50 retries %d %s!\n", c->retries,
               VMK_ReturnStatusToString(token->transientStatus));

      do {
         status = LogFS_DeviceWrite(c->device, retryToken, c->sgArr,
                                    LogFS_LogSegmentsSection);
      } while (status == VMK_STORAGE_RETRY_OPERATION);

      ASSERT(status == VMK_OK);
      return;
   }

   /* The IO went well, now lets release the temp tokens used for retries, if any */

   while (c->origContext != NULL) {
      Async_Token *tmp = c->origContext->token;
      c = c->origContext;

      Async_ReleaseToken(token);
      token = tmp;
   }

   if (token->transientStatus != VMK_OK
       && token->transientStatus != VMK_ABORTED
       && token->transientStatus != VMK_STORAGE_RETRY_OPERATION)
      Panic("token not ok %s\n",
            VMK_ReturnStatusToString(token->transientStatus));

   /* If we are completing ahead of an IO that happened-before us, we set
    * it's logEnd counter to ours, and unlink ourself from the list */

   LogFS_Log *log = c->log;

   SP_Lock(&log->writeLock);

   if (c->prev) {
      /* In case we finished before our predecessor, We cannot acknowledge the write yet.
       * Instead we link it on the to-be-acknowledged list of our predecessor */
      LogFS_LogTokenChainElement *tc =
          malloc(sizeof(LogFS_LogTokenChainElement));
      tc->token = token;
      tc->log = c->log;
      tc->logEnd = c->logEnd;
      List_Insert(&tc->tokenChain, LIST_ATREAR(&c->prev->tokenChain));
      postponeCallback = TRUE;
   } else {
      log->stableEnd = MAX(c->logEnd, log->stableEnd);
      postponeCallback = FALSE;
   }

   /* Is this IO the tail of the list of pending writes? */
   if (c == log->prevWriteContext) {
      /* Make our predecessor the new tail */
      log->prevWriteContext = c->prev;
   }
   /* If not, then find the next down from us so we can unlink */
   else {
      LogFS_LogWriteContext *successor;

      for (successor = log->prevWriteContext; successor->prev != c; successor = successor->prev)   //;
      {
         if (successor->log != log) {
            Panic("%p != %p\n", successor->log, log);
         }
         ASSERT(successor->log == log);
      }

      ASSERT(successor->prev == c);

      successor->prev = c->prev;
      c->prev = NULL;
   }

   SP_Unlock(&log->writeLock);

   List_Links *curr, *next;
   LIST_FORALL_SAFE(&c->tokenChain, curr, next) {
      LogFS_LogTokenChainElement *tc =
          List_Entry(curr, LogFS_LogTokenChainElement, tokenChain);
      LogFS_Log *l = tc->log;

      SP_Lock(&l->writeLock);
      l->stableEnd = MAX(l->stableEnd, tc->logEnd);
      SP_Unlock(&l->writeLock);

      LogFS_MetaLogPutLog(l->metaLog, l);

      List_Remove(curr);
      Async_TokenCallback(tc->token);
      free(tc);
   }

   if (!postponeCallback) {
      LogFS_MetaLogPutLog(log->metaLog, log);
      Async_TokenCallback(token);
   }
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_LogWriteBody --
 *
 *      Random write to a log segment.
 *
 * Results:
 *      VMK_LIMIT_EXCEEDED if out of bounds, or IO result.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus
LogFS_LogWriteBody(LogFS_Log *log,
                   Async_Token *token,
                   SG_Array *sgArr,
                   int flags)
{
   int i;
   VMK_ReturnStatus status;

   LogFS_MetaLog *ml = log->metaLog;
   LogFS_Device *device = ml->device;

   if (!log->alive) {
      zprintf("log %ld is dead\n", log->index);
      zprintf("appendlog? %d\n", Atomic_Read(&log->isAppendLog));
      NOT_REACHED();
   }

   log_offset_t newEnd = sgArr->sg[0].offset + SG_TotalLength(sgArr);

   if (newEnd > LOG_MAX_SEGMENT_SIZE) {
      return VMK_LIMIT_EXCEEDED;
   }

   /* If the log is buffered we just copy from the incoming sgArr */

   if (log->buffer) {

      for(i=0;i<sgArr->length;++i) {

         memcpy(log->buffer + sgArr->sg[i].offset, (void*)sgArr->sg[i].addr,
               sgArr->sg[i].length);

      }

      if (token != NULL) {
         zprintf("buffer write callback\n");
         Async_TokenCallback(token);
      }

      SP_Lock(&log->writeLock);
      log->stableEnd = MAX(log->stableEnd, newEnd);
      SP_Unlock(&log->writeLock);

      status = VMK_OK;

   } else {

      /* IO is destined for disk. Adjust sgArr contents to point within the
       * current log segment. */

      for(i=0;i<sgArr->length;++i) {
         sgArr->sg[i].offset += _cursor(log,0);
      }

      /* For async IO, we do not know in which order the writes will complete.
       * Because of the way log recovery works, we cannot ack writes back to
       * the caller before all previous writes in the log have completed. We
       * maintain a reverse linked list so that we each write knows about any
       * incomplete predecessors and we can delay the ack if necessary. */

      if (token) {
         LogFS_LogWriteContext *c = Async_PushCallbackFrame(token,
                                                            LogFS_LogWriteDone,
                                                            sizeof
                                                            (LogFS_LogWriteContext));
         memset(c, 0, sizeof(LogFS_LogWriteContext));

         c->log = log;
         c->logEnd = newEnd;

         c->token = token;
         c->device = device;
         c->sgArr = sgArr;
         c->origContext = NULL;
         c->flags = flags;
         c->retries = 0;

         List_Init(&c->tokenChain);
         Atomic_Inc(&log->refCount);

         SP_Lock(&log->writeLock);

         LogFS_LogWriteContext *next = NULL;
         LogFS_LogWriteContext *e;

         for (e = log->prevWriteContext; e && e->logEnd > c->logEnd;
              next = e, e = e->prev) {
            /* we reached the last element */
            if (e->prev == NULL) {
               e->prev = c;
               c->prev = NULL;
               goto done;
            }
         }

         if (e == log->prevWriteContext) {
            log->prevWriteContext = c;
            c->prev = e;
         } else {
            c->prev = next->prev;
            next->prev = c;
         }

 done:

         SP_Unlock(&log->writeLock);
      }

      status = LogFS_DeviceWrite(device, token, sgArr,
                                 LogFS_LogSegmentsSection);

      /* If synchronous IO, update the stableEnd pointer for the log segments
       * right away. Otherwise that will happen in the completion callback. */

      if (status == VMK_OK && token == NULL) {
         SP_Lock(&log->writeLock);
         log->stableEnd = MAX(log->stableEnd, newEnd);
         SP_Unlock(&log->writeLock);
      }
   }

   if (status != VMK_OK)
      zprintf("write returns bad status: %s\n",
              VMK_ReturnStatusToString(status));
   return status;
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_LogReadBody --
 *
 *      Random read from a log segment.
 *
 * Results:
 *      VMK_LIMIT_EXCEEDED if out of bounds, or IO result.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus LogFS_LogReadBody(LogFS_Log *log, Async_Token * token,
                                   void *buf, log_size_t count,
                                   log_offset_t offset)
{
   VMK_ReturnStatus status = VMK_OK;

   LogFS_MetaLog *ml = log->metaLog;
   LogFS_Device *device = ml->device;

   if (!log->alive) {
      zprintf("log %ld is dead\n", log->index);
      zprintf("appendlog? %d\n", Atomic_Read(&log->isAppendLog));
   }
   if (log->buffer) {
      zprintf("read from buffered logs not supported!\n");
      return VMK_NOT_SUPPORTED;
   }
   ASSERT(log->alive);

   /* if the log segment is appendable, we need to be more careful about 
    * concurrent accesses and reads past the end of the segment */

   SP_Lock(&log->writeLock);

   if (offset > log->stableEnd) {

      zprintf("trying to read overlapping end %lu\n",
              offset + count - log->stableEnd);
      memset(buf, 0, count);
      SP_Unlock(&log->writeLock);
      return VMK_LIMIT_EXCEEDED;

   } else if (offset + count > log->stableEnd) {

      memset(buf, 0, count);
      count -= ( offset + count - log->stableEnd) ;

   }

   SP_Unlock(&log->writeLock);

   status = LogFS_DeviceRead(device, token, buf, count,
                             _cursor(log, offset), LogFS_LogSegmentsSection);

   if (status != VMK_OK)
      zprintf("read failed with status %s\n", VMK_ReturnStatusToString(status));
   return status;
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_LogReadBody --
 *
 *      Random read from a log segment, without bounds checking.
 *
 * Results:
 *      Result of disk IO.
 *
 * Side effects:
 *      None.
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus LogFS_LogForceReadBody(LogFS_Log *log, Async_Token * token,
                                        void *buf, log_size_t count,
                                        log_offset_t offset)
{
   LogFS_MetaLog *ml = log->metaLog;
   LogFS_Device *device = ml->device;

   return LogFS_DeviceRead(device, token, buf, count,
                           _cursor(log, offset), LogFS_LogSegmentsSection);
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_LogReadBody --
 *
 *      Append data at the end of an appendable log segment.
 *
 * Results:
 *      Result of disk IO.
 *
 * Side effects:
 *
 *      log->end is updated to point after newly written data.
 *      Upon success *result contains a pointer to the position of the data on
 *      disk.
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus
LogFS_AppendLogAppend(LogFS_Log *log,
                      Async_Token * token,
                      SG_Array *sgArr,
                      log_id_t *result, int flags)
{
   VMK_ReturnStatus status;
   log_offset_t position;
   size_t count = SG_TotalLength(sgArr);
   int i;

   if (!LogFS_LogIsAppendLog(log)) {
      mk_invalid_version(inv);
      zprintf("%lu is not an appendlog!\n",log->index);
      NOT_REACHED();
      if (result)
         *result = inv;
      return VMK_BAD_PARAM;
   }

   position = Atomic_FetchAndAdd(&log->end, count);

   for(i=0;i<sgArr->length;++i) {
      sgArr->sg[i].offset += position;
   }

   status = LogFS_LogWriteBody(log, token, sgArr, flags);

   if (status != VMK_OK) {
      zprintf("bad status end %d , %s\n",
            Atomic_Read(&log->end),VMK_ReturnStatusToString(status));
      zprintf("pos %ld\n", position);
      zprintf("count %ld\n", count);
      return status;
   }

   if (result) {
      result->v.segment = log->index;
      result->v.blk_offset = position / BLKSIZE;
   }

   return VMK_OK;
}

VMK_ReturnStatus
LogFS_AppendLogAppendSimple(LogFS_Log *log,
                      Async_Token * token,
                      const void *buf,
                      log_size_t count,
                      log_id_t *result, int flags)
{
   VMK_ReturnStatus status;

   SG_Array sg;
   SG_SingletonSGArray(&sg, 0, (VA) buf, count, SG_VIRT_ADDR);
   status = LogFS_AppendLogAppend(log,token,&sg,result,0);
   return status;
}

/* We have to keep track of two pointers to the end of an appendable segment;
 * the offset where the next write should go, and where the last stable write
 * has completed. With the latter, we can properly truncate reads past the 
 * end of the log, instead of returning potentially random unstable data.
 */

int LogFS_LogIsAppendLog(LogFS_Log *log)
{
   return Atomic_Read(&log->isAppendLog);
}

VMK_ReturnStatus
LogFS_AppendLogClose(LogFS_Log *log, Async_Token * token, int flags)
{
   VMK_ReturnStatus status;

   LogFS_MetaLog *ml = log->metaLog;
   LogFS_Device *device = ml->device;

   Bool wait = FALSE;
   Async_IOHandle *ioh;


   Atomic_Write(&log->isAppendLog, 0);

   SP_Lock(&log->writeLock);
   log_offset_t end = Atomic_FetchAndAdd(&log->end, BLKSIZE);
   SP_Unlock(&log->writeLock);

   ASSERT(end + BLKSIZE <= LOG_MAX_SEGMENT_SIZE);

   if(token==NULL)
   {
      token = Async_AllocToken(0);
      wait = TRUE;
   }

   Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);
   Async_Token *bodyToken = Async_PrepareOneIO(ioh, NULL);

   /* Zero the rest of the blocks in the segment to prevent data leaks */
   size_t left = BLKSIZE*LOG_MAX_SEGMENT_BLOCKS - end;

   if (log->buffer) {

      memset(log->buffer+end,0,left);

      /* Set up a callback to free the buffer once we are done */

      *((void **)Async_PushCallbackFrame(bodyToken,
               LogFS_FreeSimpleBuffer,
               sizeof(void *))) = log->buffer;

      /* Write the log buffer */

      do {
         status = LogFS_DeviceWriteSimple(device, bodyToken, log->buffer, LOG_MAX_SEGMENT_SIZE,
                                    _cursor(log, 0), LogFS_LogSegmentsSection);
      } while (status == VMK_STORAGE_RETRY_OPERATION);

      ASSERT(status == VMK_OK);

   } else {

      /* Write the concluding zero blocks and create a callback to free them
       * when done. */

      void* zeroBlocks = aligned_malloc(left);
      ASSERT(zeroBlocks);
      memset(zeroBlocks,0,left);

      *((void **)Async_PushCallbackFrame(bodyToken, LogFS_FreeSimpleBuffer,
               sizeof(void *))) = zeroBlocks;

      SG_Array sgArr;
      SG_SingletonSGArray(&sgArr, end, (VA) zeroBlocks, left, SG_VIRT_ADDR);

      do { status =
             LogFS_LogWriteBody(log, bodyToken, &sgArr, flags);
      } while (status == VMK_STORAGE_RETRY_OPERATION);

   }

   Async_EndSplitIO(ioh, VMK_OK, FALSE);

   if (wait)
   {
      Async_WaitForIO(token);
      Async_ReleaseToken(token);
   }

   return status;
}


void LogFS_LogClose(LogFS_Log *log)
{
   log->alive = 0;
   SP_CleanupLock(&log->writeLock);
}
