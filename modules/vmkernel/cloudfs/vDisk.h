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
#ifndef __VDISK_H__
#define __VDISK_H__

#include "metaLog.h"
#include "bTreeRange.h"
#include "logfsHash.h"
// #include "remoteLog.h"

typedef struct LogFS_VDisk {

   Hash disk;
   Hash secret_parent;          /* when the vd is writable, we know this value */
   Hash parent;                 /* if not the primary copy of this vd, only the public parent is known */
   uint64 generation;
   Hash secretView;             /* random seed used when generating secret ids */

   Hash entropy;                /* Entropy resulting from last applied update, used for regenerating 
                                   secret on a forced view change */

   uint64 lsn;

   LogFS_MetaLog *log;
   List_Links remoteLogs;

   Bool stopRequested;
   Bool haveReservation;

   Bool isImmutable;

   Hash parentBaseId;
   struct LogFS_VDisk *parentDisk;

   LogFS_BTreeRangeMap *bt;

   SP_SpinLock lock;
   List_Links closeWaiters;
   List_Links tokenWaiters;
   List_Links openWaiters;

   Atomic_uint32 refCount;


} LogFS_VDisk;

void LogFS_VDiskInit(LogFS_VDisk *vd, LogFS_VDisk *parentDisk, Hash diskId,
                     LogFS_MetaLog *log);
void LogFS_VDiskCleanup(LogFS_VDisk *vd);

static inline void LogFS_VDiskRef(LogFS_VDisk *vd)
{
   Atomic_Inc(&vd->refCount);
}

VMK_ReturnStatus LogFS_VDiskReserve(LogFS_VDisk *vd);

Bool LogFS_VDiskIsWritable(LogFS_VDisk *vd);

VMK_ReturnStatus LogFS_VDiskBranch(LogFS_VDisk *vd, LogFS_Hash childId,
                                   LogFS_VDisk **result);

VMK_ReturnStatus LogFS_VDiskSnapshot(LogFS_VDisk *vd, LogFS_VDisk **snapshot);

void LogFS_VDiskRelease(LogFS_VDisk *vd);

Hash LogFS_VDiskGetBaseId(LogFS_VDisk *vd);
Hash LogFS_VDiskGetCurrentId(LogFS_VDisk *vd);
Hash LogFS_VDiskGetStable(LogFS_VDisk *vd);
Hash LogFS_VDiskGetParentDiskId(void *vd);
VMK_ReturnStatus LogFS_VDiskWrite(LogFS_VDisk *vd, Async_Token *,
                                  const char *buf, log_block_t blkno,
                                  size_t num_blocks, int flags);
VMK_ReturnStatus LogFS_VDiskSetSecret(LogFS_VDisk *vd, Hash secret,
                                      Hash secretView);
VMK_ReturnStatus LogFS_VDiskGetSecret(LogFS_VDisk *vd, Hash * secret,
                                      Bool failIfBusy);
VMK_ReturnStatus LogFS_VDiskAppend(LogFS_VDisk *vd, Async_Token *,
                                   struct LogFS_RefCountedBuffer *buf);
VMK_ReturnStatus LogFS_VDiskRead(LogFS_VDisk *vd, Async_Token * token,
                                 char *buf, log_block_t blkno,
                                 size_t num_blocks, int flags);
VMK_ReturnStatus LogFS_VDiskContinueRead(LogFS_VDisk *vd, Async_Token * token,
                                         const char *buf, log_block_t blkno,
                                         size_t num_blocks, int flags);
VMK_ReturnStatus LogFS_VDiskContinueWrite(LogFS_VDisk *vd, Async_Token * token,
                                          const char *buf, log_block_t blkno,
                                          size_t num_blocks, int flags);
LogFS_BTreeRangeMap *LogFS_VDiskGetVersionsMap(LogFS_VDisk *vd);

static inline void
LogFS_VDiskUpdateFromHead(LogFS_VDisk *vd, struct log_head *head)
{
   if (head->tag == log_entry_type) {
      LogFS_HashSetRaw(&vd->entropy, head->entropy);
      LogFS_HashSetRaw(&vd->parent, head->id);
   }
}

static inline unsigned long long LogFS_VDiskGetCapacity(LogFS_VDisk *vd)
{
   //static const unsigned long long FILE_SIZE=0x800000000ULL;
   //static const unsigned long long FILE_SIZE=0x400000000ULL;
   static const unsigned long long FILE_SIZE = 0x800000000ULL;
   return FILE_SIZE;
}

static inline int LogFS_VDiskIsOrphaned(LogFS_VDisk *vd)
{
   return (vd->parentDisk == NULL && LogFS_HashIsValid(vd->parentBaseId));
}

struct LogFS_RemoteLog;
int LogFS_VDiskAddRemoteLogIfSameVersion(LogFS_VDisk *vd, struct LogFS_RemoteLog *rl);

#endif
