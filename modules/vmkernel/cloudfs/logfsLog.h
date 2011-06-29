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
#ifndef __LOG_H__
#define __LOG_H__

#include "bTreeRange.h"
#include "logfsHash.h"

#include "logtypes.h"
// #include "lock.h"

typedef struct LogFS_Log {
   void *metaLog;
   int alive;
   log_segment_id_t index;
   Atomic_uint32 isAppendLog;
   Atomic_uint32 refCount;
   char *buffer;

   /* if AppendLog */
   SP_SpinLock writeLock;       /* protects write contexts chain */
   Atomic_uint32 end;
   log_offset_t stableEnd;

   void *prevWriteContext;
} LogFS_Log;

static inline log_offset_t _cursor(LogFS_Log *log, log_offset_t offset)
{
   if (!log->alive) {
      zprintf("log %ld is dead\n", log->index);
      zprintf("appendlog? %d\n", Atomic_Read(&log->isAppendLog));
   }
   ASSERT(log->alive);
   return log->index * LOG_MAX_SEGMENT_SIZE + offset;
}

static inline log_id_t
LogFS_AppendLogGetEnd(LogFS_Log *log)
{
   log_id_t v;
   v.v.segment = log->index;
   uint32 end = Atomic_Read(&log->end);
   v.v.blk_offset = end/BLKSIZE;
   return v;
}

struct LogFS_MetaLog;

void LogFS_LogInit(LogFS_Log *log, struct LogFS_MetaLog *metaLog,
                   log_segment_id_t index);

void LogFS_LogClose(LogFS_Log *log);

log_segment_id_t LogFS_LogGetSegment(LogFS_Log *log);
char *LogFS_LogEnableBuffering(LogFS_Log *log);
void LogFS_AppendLogInit(LogFS_Log *log, struct LogFS_MetaLog *metaLog,
                         log_segment_id_t index, log_offset_t end);

VMK_ReturnStatus
LogFS_LogWriteBody(LogFS_Log *log, Async_Token *token, SG_Array *sg, int flags);

VMK_ReturnStatus LogFS_LogReadBody(LogFS_Log *log, Async_Token * token,
                                   void *buf, log_size_t count,
                                   log_offset_t offset);
VMK_ReturnStatus LogFS_LogForceReadBody(LogFS_Log *log, Async_Token * token,
                                        void *buf, log_size_t count,
                                        log_offset_t offset);

VMK_ReturnStatus LogFS_AppendLogAppend(LogFS_Log *log, Async_Token * token,
      SG_Array *sgArr, log_id_t * result, int flags);

VMK_ReturnStatus LogFS_AppendLogAppendSimple(LogFS_Log *log, Async_Token *
      token, const void *buf, log_size_t count, log_id_t *result, int flags);

int LogFS_LogIsAppendLog(LogFS_Log *log);
VMK_ReturnStatus LogFS_AppendLogClose(LogFS_Log *log, Async_Token * token,
                                      int flags);
void LogFS_AppendLogPushEnd(LogFS_Log *log, log_offset_t end);

#endif                          /* __LOG_H__ */
