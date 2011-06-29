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
#ifndef __REMOTELOG_H__
#define __REMOTELOG_H__

#include "logfsNet.h"
#include "logfsHash.h"

/* RemoteLog state is protected by the VDisk lock */

struct LogFS_VDisk;

typedef struct LogFS_RemoteLog {
   Atomic_uint32 refCount;
   Bool isSocketOpen;
   int shouldClose;
   LogFS_MetaLog *ml;
   vmk_Worldlet worldlet;
   uint64 lsn;
   LogFS_Hash hostId;
   World_ID serverWorld;
   List_Links outstandingWrites;
   uint32 numBuffered;
   struct LogFS_VDisk *vd;

   List_Links nextLog;
   List_Links next;
   SP_SpinLock lock;

   /* Position in the log, used when streaming asyncly */
   log_id_t position;

} LogFS_RemoteLog;

void LogFS_RemoteLogPreInit(void);
void LogFS_RemoteLogInit(LogFS_RemoteLog *rl,
                         LogFS_MetaLog *ml,
                         vmk_Worldlet worldlet,
                         struct LogFS_VDisk *vd,
                         uint64 lsn);
void LogFS_RemoteLogRef(LogFS_RemoteLog *rl);
void LogFS_RemoteLogRelease(LogFS_RemoteLog *rl);
void LogFS_RemoteLogClose(LogFS_RemoteLog *rl);
void LogFS_RemoteLogExit(LogFS_RemoteLog *rl);

VMK_ReturnStatus LogFS_RemoteLogAppend(LogFS_RemoteLog *rl, SG_Array *sgArr,
      Async_Token * token);

VMK_ReturnStatus LogFS_RemoteLogPopUpdate(LogFS_RemoteLog *rl, SG_Array **
      sgArr, Async_Token **);

void LogFS_RemoteLogSetCurrent(LogFS_RemoteLog *rl, Hash id);

static inline Bool LogFS_RemoteLogIsOpen(LogFS_RemoteLog *rl)
{
   return (rl->isSocketOpen && !rl->shouldClose);
}

VMK_ReturnStatus LogFS_RemoteLogSpoolFromRevision(LogFS_RemoteLog *rl);

static inline Bool LogFS_RemoteLogBufferFull(LogFS_RemoteLog *rl)
{
   return (rl->numBuffered > 0x400000);
}

#endif                          /* __REMOTELOG_H__ */
