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
#ifndef __BTREE_RANGE_H__
#define __BTREE_RANGE_H__

#include <system.h>

#include "logtypes.h"
// #include "lock.h"
#include "logfsHash.h"
#include "obsoleted.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "rangemap.h"
#ifdef __cplusplus
}
#endif
#define MAX_INSERTS 0x1800
struct LogFS_MetaLog;

struct ins_elem {
   uint64 lsn;
   log_block_t from;
   log_block_t to;
   log_id_t version;
};

typedef struct {

   struct LogFS_MetaLog *ml;

   struct ins_elem ins_buffer[MAX_INSERTS];

   /* We keep these as 32-bit, and do the modulo when accessing
    * into the array */
   Atomic_uint32 consumerIndex;
   Atomic_uint32 producerIndex;
   Atomic_uint32 producerStableIndex;

   Atomic_uint32 numBuffered;

   Hash diskId;
   uint64 lsn;
   Hash currentId;
   Hash entropy;

   log_segment_id_t lastLsnSegment;

   Semaphore sem;
   SP_SpinLock lock;
   btree_t *tree;
   btree_t *lsnTree;

   Bool isDirty;
   List_Links next;

} LogFS_BTreeRangeMap;

struct _LogFS_VDisk;
struct LogFS_MetaLog;

void LogFS_BTreeRangeMapPreInit(struct LogFS_MetaLog *ml);
void LogFS_BTreeRangeMapCleanupGlobalState(struct LogFS_MetaLog *ml);
void LogFS_BTreeRangeMapCleanup(LogFS_BTreeRangeMap *bt);

void LogFS_BTreeRangeMapInit(LogFS_BTreeRangeMap *bt, 
      struct LogFS_MetaLog *ml,
      Hash diskId,
      Hash currentId,
      Hash entropy);

void LogFS_BTreeRangeMapMemInit(LogFS_BTreeRangeMap *bt, void *mem);
void LogFS_BTreeRangeMapMemClear(LogFS_BTreeRangeMap *bt);
void LogFS_BTreeRangeMapFlush(LogFS_BTreeRangeMap *bt);
void LogFS_BTreeRangeMapInsert(LogFS_BTreeRangeMap *bt,
      log_block_t lsn,
      log_block_t from,
      log_block_t to,
      log_id_t version, 
      Hash currentId, Hash entropy);

VMK_ReturnStatus LogFS_BTreeRangeMapInsertSimple(LogFS_BTreeRangeMap *bt,
      log_block_t from,
      log_block_t to,
      log_id_t version);

log_id_t LogFS_BTreeRangeMapLookup(LogFS_BTreeRangeMap *bt, log_block_t x,
                                   log_block_t * endsat);

void LogFS_KickFlusher(void);

VMK_ReturnStatus LogFS_BTreeRangeMapAsyncLookup(LogFS_BTreeRangeMap *bt,
      log_block_t x,
      void (*callback) (range_t, log_block_t, void *),
      void *data, int flags,
      range_t* range,
      log_block_t * retEndsAt);

static inline int LogFS_BTreeRangeMapHighWater(LogFS_BTreeRangeMap *bt)
{
   return ((MAX_INSERTS - Atomic_Read(&bt->numBuffered) < 2048));
}

static inline int LogFS_BTreeRangeMapCanInsert(LogFS_BTreeRangeMap *bt)
{
   return (MAX_INSERTS - Atomic_Read(&bt->numBuffered) > 16);
}

void LogFS_BTreeRangeMapReplace(LogFS_BTreeRangeMap *bt,
                                log_block_t from, log_block_t to,
                                log_id_t oldvalue, log_id_t newvalue);
void LogFS_BTreeRangeMapCheckNoRefs(LogFS_BTreeRangeMap *bt,
                                    log_segment_id_t segment);
void LogFS_BTreeRangeMapShow(LogFS_BTreeRangeMap *bt);
void LogFS_BTreeRangeMapImport(LogFS_BTreeRangeMap *bt,
                               LogFS_BTreeRangeMap *other);
void LogFS_BTreeRangeMapClear(LogFS_BTreeRangeMap *bt);
void LogFS_BTreeRangeStartFlusher(struct LogFS_MetaLog *ml, uint64 generation);

VMK_ReturnStatus LogFS_BTreeRangeMapLookupLsn(
      LogFS_BTreeRangeMap *bt,
      uint64 lsn,
      log_segment_id_t *segment);

#endif                          /* __BTREE_RANGE_H__ */
