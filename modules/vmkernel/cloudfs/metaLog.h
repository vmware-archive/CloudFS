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
#ifndef __METALOG_H__
#define __METALOG_H__

#include "logfsLog.h"
#include "segmentlist.h"
#include "logfsIO.h"
#include "obsoleted.h"

#define MAX_OPEN_LOGS 128

struct LogFS_FingerPrint;

typedef struct LogFS_MetaLog {
   btree_t *superTree;
   Semaphore superTreeSemaphore;

   LogFS_SegmentList segment_list;
   LogFS_Device *device;

   LogFS_ObsoletedSegments obsoleted;
   LogFS_ObsoletedSegments dupes;

   LogFS_Log *openLogs[MAX_OPEN_LOGS];

   SP_SpinLock append_lock;
   SP_SpinLock refcounts_lock;

   List_Links remoteWaiters;

   LogFS_Log *activeLog;
   log_size_t spaceLeft;

   int lurt;

   struct LogFS_DiskLayout *diskLayout;
   
   Bool compactionInProgress;

   /* Dedupe related */
   struct LogFS_VebTree *vt;
   struct LogFS_HashDb *hd;
   struct LogFS_FingerPrint *fp;
   struct LogFS_FingerPrint *fingerPrints[0x100];

} LogFS_MetaLog;

void LogFS_MetaLogInit(LogFS_MetaLog *ml, struct LogFS_Device *device);
void LogFS_MetaLogCleanup(LogFS_MetaLog *ml);
VMK_ReturnStatus LogFS_MetaLogReopen(LogFS_MetaLog *ml, log_id_t position);
void LogFS_MetaLogFreeLog(LogFS_MetaLog *ml, LogFS_Log *log);
LogFS_Log *LogFS_MetaLogGetLog(LogFS_MetaLog *ml, log_segment_id_t segment);
LogFS_Log *LogFS_MetaLogPutLog(LogFS_MetaLog *ml, LogFS_Log *log);

VMK_ReturnStatus LogFS_MetaLogAppend(LogFS_MetaLog *ml, Async_Token * token,
      SG_Array *sgArr, log_id_t *retVersion, int flags);

void LogFS_MetaLogSignalNewData(LogFS_MetaLog *ml);

/* logCompactor.c */
log_id_t LogFS_MetaLogLookupIndirection(LogFS_MetaLog *ml, log_id_t position);

//void LogFS_MetaLogGC(LogFS_MetaLog* ml);

#endif                          /* __METALOG_H__ */
