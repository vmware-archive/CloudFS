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
#include "system.h"
#include "globals.h"
// #include "lock.h"
#include "bTreeRange.h"
#include "obsoleted.h"
#include "pagedTree.h"
#include "logfsCheckPoint.h"
#include "vDisk.h"

/* Concurrency strategy:
 *
 * The B-tree rangemap is paged on disk, and cache misses may result in
 * blocking IO. The read (lookup) path has error handling for cache misses, the
 * write (insert) path has not.
 *
 * To allow non-blocking inserts and lookups, and to gain performance by
 * batching updates to the tree, inserts are batched in ins_buffer[], and only
 * flushed when this runs full or in case of a lookup from a blocking context.
 * Lookups can be served non-blocking from ins_buffer[], or if all involved
 * B-tree nodes are memory resident. When in non-blocking mode, lookups that
 * cause a cache miss will fail and be queued and handled by the rangemap
 * syncer thread.
 *
 * The tree is only ever flushed from non-blocking contexts, and flushes are
 * serialized with bt->sem. Normal inserts and lookups can be handled during
 * flush, but replace operations cannot. Thus they are also serialized by
 * bt->sem. Non-blocking lookups that cannot be serviced from ins_buffer[]
 * are postponed to be handled by the syncer thread.
 *
 *
 */

typedef struct {
   LogFS_BTreeRangeMap *bt;
   log_block_t block;
   log_block_t endsat;
   void (*callback) (range_t, log_block_t, void *);
   void *data;

   List_Links next;
} QueuedLookup;

static List_Links lookupQueue;
static SP_SpinLock lookupQueueLock;
static List_Links lookupWaitQueue;

static List_Links flusherQueue;
static SP_SpinLock flusherQueueLock;
static List_Links flusherWaitQueue;

/* Init state shared by all BTreeRangeMaps */

void LogFS_DelayedLookup(void *data);
void LogFS_Flusher(void *data);
void LogFS_KickFlusher(void);
static void LogFS_KickDelayedLookup(void);

static World_ID flusherWorld;
static World_ID syncerWorld;

static Bool flusherExit = FALSE;
static Bool syncerExit = FALSE;

static Bool bTreeShutdown = FALSE;
static Bool bTreeInitialized = FALSE;

void LogFS_BTreeRangeMapPreInit(LogFS_MetaLog *ml)
{
   VMK_ReturnStatus status;

   flusherExit = FALSE;
   syncerExit = FALSE;
   bTreeShutdown = FALSE;

   List_Init(&lookupWaitQueue);
   SP_InitLock("rangemaplookupq", &lookupQueueLock, SP_RANK_RANGEMAPQUEUES);
   List_Init(&lookupQueue);

   status = World_NewSystemWorld("logDelayedLookup", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &syncerWorld);
   ASSERT(status == VMK_OK);
   Sched_Add(World_Find(syncerWorld), LogFS_DelayedLookup, NULL);

   List_Init(&flusherWaitQueue);
   SP_InitLock("rangemapflushq", &flusherQueueLock, SP_RANK_RANGEMAPQUEUES);
   List_Init(&flusherQueue);
   bTreeInitialized = TRUE;

}

static uint64 logfsCheckPointGeneration = 0;

void LogFS_BTreeRangeStartFlusher(LogFS_MetaLog *ml, uint64 generation)
{
   logfsCheckPointGeneration = generation;
   VMK_ReturnStatus status;
   status = World_NewSystemWorld("logFlusher", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &flusherWorld);
   ASSERT(status == VMK_OK);
   Sched_Add(World_Find(flusherWorld), LogFS_Flusher, (void *)ml);

}

void LogFS_BTreeRangeMapCleanupGlobalState(LogFS_MetaLog *ml)
{
   if (!bTreeInitialized)
      return;

   bTreeShutdown = TRUE;

   flusherExit = TRUE;
   syncerExit = TRUE;

   LogFS_KickFlusher();
   LogFS_KickDelayedLookup();

   /* Threads will reset their exit flags back to false when done */
   World_WaitForExit(flusherWorld);
   World_WaitForExit(syncerWorld);

   SP_CleanupLock(&lookupQueueLock);
   SP_CleanupLock(&flusherQueueLock);

   bTreeInitialized = FALSE;
}

void LogFS_BTreeRangeMapInit(LogFS_BTreeRangeMap *bt, LogFS_MetaLog *ml,
      Hash diskId,
      Hash currentId,
      Hash entropy)
{
   Semaphore_Init("btrangesem", &bt->sem, 1, SEMA_RANK_UNRANKED);
   SP_InitLock("btrangelock", &bt->lock, SP_RANK_RANGEMAP);

   bt->tree = NULL;
   bt->ml = ml;

   bt->diskId = diskId;
   bt->currentId = currentId;
   bt->entropy = entropy;

   bt->lastLsnSegment = ~0ULL;

   Atomic_Write(&bt->consumerIndex, 0);
   Atomic_Write(&bt->producerIndex, 0);
   Atomic_Write(&bt->producerStableIndex, 0);
   Atomic_Write(&bt->numBuffered, 0);
}

void LogFS_BTreeRangeMapCleanup(LogFS_BTreeRangeMap *bt)
{
   Semaphore_Cleanup(&bt->sem);
   SP_CleanupLock(&bt->lock);
   if(bt->tree != NULL) {
      free(bt->tree);
   }
   if(bt->lsnTree != NULL) {
      free(bt->lsnTree);
   }
}

void LogFS_BTreeRangeMapCreateTrees(
      LogFS_BTreeRangeMap *bt,
      Hash diskId,
      Hash currentId,
      Hash entropy)
{

   LogFS_MetaLog *ml = bt->ml;

   SuperTreeElement *e = malloc(sizeof(SuperTreeElement));
   LogFS_HashCopy(e->key, diskId);

   Semaphore_Lock(&ml->superTreeSemaphore);

   if (tree_find(ml->superTree, (elem_t *) e, NULL)) {

      bt->tree = LogFS_PagedTreeReOpen(ml,e->value.root);
      bt->lsnTree = LogFS_PagedTreeReOpen(ml,e->value.lsnRoot);

   } else {

      bt->tree = LogFS_PagedTreeCreate(ml);
      bt->lsnTree = LogFS_PagedTreeCreate(ml);

      e->value.root = bt->tree->root;
      e->value.lsnRoot = bt->lsnTree->root;
      LogFS_HashCopy(e->value.currentId,currentId);
      LogFS_HashCopy(e->value.entropy,entropy);

      tree_insert(ml->superTree, (elem_t *) e, NULL);

      Hash nullId;
      LogFS_HashZero(&nullId);
   }
   Semaphore_Unlock(&ml->superTreeSemaphore);

   free(e);
}

static inline void createPagedTree( LogFS_BTreeRangeMap *bt)
{
   LogFS_BTreeRangeMapCreateTrees(bt,
         bt->diskId, bt->currentId, bt->entropy);
}

/* can only be called from a blocking context */
static void LogFS_BTreeRangeMapFlushLocked(LogFS_BTreeRangeMap *bt)
{
   if (bTreeShutdown)
      return;

   if (bt->tree == NULL) {
      createPagedTree(bt);
   }

   LogFS_MetaLog *ml = bt->ml;
   LogFS_ObsoletedSegments *os = &ml->obsoleted;

   uint32 i;
   uint32 from = Atomic_Read(&bt->consumerIndex);
   uint32 to = Atomic_Read(&bt->producerStableIndex);

   /* flush the inserts in the order they appeared.  it would be
    * tempting to sort them for better locality, but this would lead
    * to incorrect results. A better alternative might be to buffer
    * them in a in-memory rangemap, and then flush the rangemap,
    * but that would loose information needed for tracking obsolete 
    * segments.
    */

   for (i = from; i != to; ++i) {
      struct ins_elem *e = &bt->ins_buffer[i % MAX_INSERTS];

      log_block_t j;

      /* This insert may clobber existing data in the rangemap.
       * Count how many valid references where overwritten, and
       * update the priortity queue of obsoleted segments. */

      for (j = e->from; j < e->to;) {
         log_block_t endsat = MAXBLOCK;

         log_id_t r;
         r.raw = rangemap_get(bt->tree, j, &endsat);
         endsat = MIN(endsat, e->to);

         if (!is_invalid_version(r) && !(equal_version(r, e->version)) ) {

            ASSERT(r.v.segment < MAX_NUM_SEGMENTS);

            LogFS_ObsoletedSegmentsAdd(os, r.v.segment, endsat - j);
         }

         j = endsat;
      }

      rangemap_insert(bt->tree, e->from, e->to, e->version.raw);

      Atomic_Dec(&bt->numBuffered);
      Atomic_Inc(&bt->consumerIndex);

      log_segment_id_t s  = e->version.v.segment;
      if(!is_invalid_version(e->version) && bt->lastLsnSegment != s) {

         rangemap_insert(bt->lsnTree, e->lsn, e->lsn+1, s);
         bt->lastLsnSegment = s;

      }

   }
}

VMK_ReturnStatus LogFS_BTreeRangeMapLookupLsn(
      LogFS_BTreeRangeMap *bt,
      uint64 lsn,
      log_segment_id_t *segment)
{
   VMK_ReturnStatus status = VMK_NOT_FOUND;

   if(bt->lsnTree != NULL) {

      struct range lb;
      btree_iter_t it;
      struct range r;
      r.to = lsn;

      tree_result_t result = tree_lower_bound(bt->lsnTree, &it, (elem_t *) & r, NULL);

      if (result == tree_result_found) {

         tree_iter_read(&lb, &it, NULL);
         *segment = lb.version;
         status = VMK_OK;

      } else if (result == tree_result_end) {

         zprintf("rewind\n");

         if (tree_iter_dec(&it,NULL)==tree_result_ok) {
            tree_iter_read(&lb, &it, NULL);
            *segment = lb.version;
            status = VMK_OK;
         }

      }

   }

   return status;
}

void LogFS_BTreeRangeMapFlush(LogFS_BTreeRangeMap *bt)
{
   if (bTreeShutdown)
      return;

   /* We only bother flushing to disk when the tree has been updated, needs to be created,
    * or to update its current id */

   if (Atomic_Read(&bt->numBuffered) > 0 || bt->tree == NULL) {
      Semaphore_Lock(&bt->sem);
      LogFS_BTreeRangeMapFlushLocked(bt);
      Semaphore_Unlock(&bt->sem);
   }
}

void LogFS_BTreeRangeMapInsert(LogFS_BTreeRangeMap *bt,
      log_block_t lsn,
      log_block_t from,
      log_block_t to,
      log_id_t version, 
      Hash currentId, Hash entropy)
{
   ASSERT(from < to);
   if (bTreeShutdown)
      return;

   if (!LogFS_BTreeRangeMapCanInsert(bt)) {
      Panic("out of space!!\n");
   }

   SP_Lock(&bt->lock);
   if (List_IsUnlinkedElement(&bt->next)) {
      SP_Lock(&flusherQueueLock);
      List_Insert(&bt->next, LIST_ATREAR(&flusherQueue));
      SP_Unlock(&flusherQueueLock);
   }

   if (LogFS_BTreeRangeMapHighWater(bt)) {
      LogFS_KickFlusher();
   }

   /* We currently do not allow concurrent writers, to ensure proper
    * correspondence between producerIndex and producerStableIndex */

   uint32 idx = Atomic_FetchAndInc(&bt->producerIndex);
   Atomic_Inc(&bt->numBuffered);

   struct ins_elem *elem = &bt->ins_buffer[idx % MAX_INSERTS];

   elem->lsn = lsn;
   elem->from = from;
   elem->to = to;
   elem->version = version;

   /* Make sure elem is globally visible before updating stableIndex */

   CPU_MemBarrier();
   Atomic_Inc(&bt->producerStableIndex);
   bt->lsn = lsn;
   bt->currentId = currentId;
   bt->entropy = entropy;

   SP_Unlock(&bt->lock);
}

VMK_ReturnStatus LogFS_BTreeRangeMapInsertSimple(LogFS_BTreeRangeMap *bt,
      log_block_t from,
      log_block_t to,
      log_id_t version)
{
   ASSERT(from < to);
   if (bTreeShutdown)
      return VMK_NOT_SUPPORTED;

   if (!LogFS_BTreeRangeMapCanInsert(bt)) {
      //Panic("out of space!!\n");
      return VMK_BUSY;
   }

   SP_Lock(&bt->lock);
   if (List_IsUnlinkedElement(&bt->next)) {
      SP_Lock(&flusherQueueLock);
      List_Insert(&bt->next, LIST_ATREAR(&flusherQueue));
      SP_Unlock(&flusherQueueLock);
   }

   if (LogFS_BTreeRangeMapHighWater(bt)) {
      LogFS_KickFlusher();
   }

   /* We currently do not allow concurrent writers, to ensure proper
    * correspondence between producerIndex and producerStableIndex */

   uint32 idx = Atomic_FetchAndInc(&bt->producerIndex);
   Atomic_Inc(&bt->numBuffered);

   struct ins_elem *elem = &bt->ins_buffer[idx % MAX_INSERTS];

   elem->lsn = 0;
   elem->from = from;
   elem->to = to;
   elem->version = version;

   /* Make sure elem is globally visible before updating stableIndex */

   CPU_MemBarrier();
   Atomic_Inc(&bt->producerStableIndex);

   SP_Unlock(&bt->lock);

   return VMK_OK;
}

void LogFS_BTreeRangeMapClear(LogFS_BTreeRangeMap *bt)
{
   NOT_REACHED();

   Semaphore_Lock(&bt->sem);
   rangemap_clear(bt->tree);
   Semaphore_Unlock(&bt->sem);
}

void LogFS_BTreeRangeMapReplace(LogFS_BTreeRangeMap *bt,
                                log_block_t from, log_block_t to,
                                log_id_t oldvalue, log_id_t newvalue)
{
   if (bTreeShutdown)
      return;

   Semaphore_Lock(&bt->sem);

   LogFS_BTreeRangeMapFlushLocked(bt);

   rangemap_replace(bt->tree, from, to, oldvalue.raw, newvalue.raw);

   Semaphore_Unlock(&bt->sem);
}

VMK_ReturnStatus
LogFS_BTreeRangeMapLookupInBuffer( LogFS_BTreeRangeMap *bt,
      log_block_t x, 
      range_t* range,
      log_block_t * endsat)

{
   mk_invalid_version(inv);

   if (bTreeShutdown)
      return VMK_NOT_FOUND;

   /* We first check the buffered inserts, before consulting
    * the B-tree on disk */

   uint32 i;
   uint32 top;
   uint32 bottom;
   log_block_t greatest_less_than_x = 0;

   top = Atomic_Read(&bt->producerStableIndex);
   bottom = Atomic_Read(&bt->consumerIndex);

   for (i = top; i != bottom;) {
      --i;

      struct ins_elem *e = &bt->ins_buffer[i % MAX_INSERTS];

      /* ends before x */
      if (e->to <= x) {
         greatest_less_than_x = MAX(e->to, greatest_less_than_x);
      }

      /* starts after x */
      else if (x < e->from) {
         *endsat = MIN(e->from, *endsat);
      }

      /* overlaps x -- we have found it */
      else {

         range->from = MAX(e->from, greatest_less_than_x);
         *endsat = MIN(e->to, *endsat);

         log_id_t v = e->version;

         if(greatest_less_than_x > e->from) {
            v.v.blk_offset += (greatest_less_than_x - e->from);
         }

         range->version = v.raw;
         return VMK_OK;
      }
   }

   return VMK_NOT_FOUND;
}

log_id_t LogFS_BTreeRangeMapLookup(LogFS_BTreeRangeMap *bt,
                                   log_block_t x, log_block_t * endsat)
{
   /* We first check the buffered inserts, before consulting
    * the B-tree on disk */

   VMK_ReturnStatus status;
   range_t range;
   status = LogFS_BTreeRangeMapLookupInBuffer(bt, x, &range, endsat);

   if (status == VMK_NOT_FOUND) {
      /* The block was not covered by any buffered inserts, look in the B-tree instead */

      Semaphore_Lock(&bt->sem);

      /* Demand-create the B-tree if not there */
      if(bt->tree == NULL) {
         createPagedTree(bt);
      }

      __rangemap_get(bt->tree, x, &range, endsat, NULL);
      Semaphore_Unlock(&bt->sem);
   }
   log_id_t v;
   v.raw = range.version;
   return v;
}

/* In cases where B-tree lookups cannot be answered without blocking (due to
 * nodes being paged in from disk), this thread will retry the lookups from a
 * blocking context */

void LogFS_DelayedLookup(void *data)
{
   VMK_ReturnStatus status;

   for (;;) {
      SP_Lock(&lookupQueueLock);

      while (!List_IsEmpty(&lookupQueue)) {
         List_Links *elem;

         elem = List_First(&lookupQueue);
         QueuedLookup *l = List_Entry(elem, QueuedLookup, next);
         List_Remove(elem);

         log_block_t endsat = l->endsat;
         LogFS_BTreeRangeMap *bt = l->bt;

         SP_Unlock(&lookupQueueLock);

         Semaphore_Lock(&bt->sem);
         if(bt->tree == NULL) {
            createPagedTree(bt);
         }

         range_t range = {~0,};
         __rangemap_get(bt->tree, l->block, &range, &endsat, NULL);
         Semaphore_Unlock(&bt->sem);

         l->callback(range, endsat, l->data);
         free(l);

         SP_Lock(&lookupQueueLock);
      }

      status =
          CpuSched_Wait(&lookupWaitQueue, CPUSCHED_WAIT_SCSI, &lookupQueueLock);
      ASSERT(status == VMK_OK);

      if (syncerExit)
         World_Exit(VMK_OK);
   }
}

/* This thread takes care of flushing B-tree buffers when they run full. */

void LogFS_Flusher(void *data)
{
   VMK_ReturnStatus status;
   LogFS_MetaLog *ml = data;
   LogFS_CheckPoint *cp;
   int buffer = 0;

   cp = aligned_malloc(LOGFS_CHECKPOINT_SIZE);
   memset(cp, 0, LOGFS_CHECKPOINT_SIZE);

   /* Populate nodesBitmap by copying from the (potentially recovered)
    * global bitmap. All further changes will happen through the movedNodes
    * parameter to LogFS_CheckPointCommit(). */

   memcpy(cp->nodesBitmap, nodesBitmap, sizeof(cp->nodesBitmap));

   while(!flusherExit) {

      List_Links tmpList;
      List_Links movedNodes;  /* List of nodes that were moved or newly alloced */

      if(!ml->superTree) continue;

      List_Init(&movedNodes);

      status = LogFS_CheckPointPrepare(ml, cp, ++logfsCheckPointGeneration);

      /* Atomically move contents of flusherQueue to tmpList */

      SP_Lock(&flusherQueueLock);
      List_Init(&tmpList);
      List_Append(&tmpList, &flusherQueue);
      SP_Unlock(&flusherQueueLock);

      /* Flush any outstanding B-tree updates to disk, and 
       * count the dirty trees */

      List_Links *elem, *next;
      Hash currentId, entropy;

      LIST_FORALL_SAFE(&tmpList, elem, next) {
         LogFS_BTreeRangeMap *bt = List_Entry(elem, LogFS_BTreeRangeMap, next);
         int i;

         if(bt->tree == NULL) {
            createPagedTree(bt);
         }

         SP_Lock(&bt->lock);
         currentId = bt->currentId;
         entropy = bt->entropy;
         SP_Unlock(&bt->lock);

         Semaphore_Lock(&bt->sem);

         LogFS_BTreeRangeMapFlushLocked(bt);

         btree_t* trees[] = {bt->tree,bt->lsnTree};

         for(i=0;i<sizeof(trees)/sizeof(trees[0]);i++) {

            status = LogFS_PagedTreeSync(trees[i], bt->ml, &movedNodes);

            if(status != VMK_OK) {
               goto abort;
            }
         }

         btree_iter_t it;
         tree_result_t r;
         SuperTreeElement *e = malloc(sizeof(SuperTreeElement));
         LogFS_HashCopy(e->key, bt->diskId);

         r = tree_lower_bound(ml->superTree, &it, (elem_t*) e, NULL);
         ASSERT(r==tree_result_found);
         r = tree_iter_read(e, &it, NULL);
         ASSERT(r==tree_result_ok);

         e->value.root = bt->tree->root;
         e->value.lsn = bt->lsn;
         e->value.lsnRoot = bt->lsnTree->root;

         LogFS_HashCopy(e->value.currentId, currentId);
         LogFS_HashCopy(e->value.entropy, entropy);

         r = tree_iter_write(&it,e,NULL);
         ASSERT(r==tree_result_ok);

         free(e);

         /* Forcefully unlink to make List_IsUnlinkedElement() work */
         SP_Lock(&bt->lock);
         List_InitElement(&bt->next);
         SP_Unlock(&bt->lock);

         Semaphore_Unlock(&bt->sem);

      }
abort:

      /* Commit per-disk B-tree updates by syncing the top-level B-tree */


      if (status==VMK_OK) {

         LogFS_PagedTreeSync(ml->superTree, ml, &movedNodes);

         status = LogFS_CheckPointCommit(ml, cp, buffer, ml->superTree->root, &movedNodes);

         buffer ^= 1;
         ASSERT(status == VMK_OK);

         status = CpuSched_TimedWait(&flusherWaitQueue, CPUSCHED_WAIT_SCSI,
               NULL, 5 * 1000, NULL);

      } else {
         Panic("NOT COMMITTING!\n");
      }

   }

   LogFS_PagedTreeCleanupGlobalState(ml);

   aligned_free(cp);
   World_Exit(VMK_OK);
}

static void LogFS_KickDelayedLookup(void)
{
   CpuSched_Wakeup(&lookupWaitQueue);
}

void LogFS_KickFlusher(void)
{
   CpuSched_Wakeup(&flusherWaitQueue);
}

VMK_ReturnStatus LogFS_BTreeRangeMapAsyncLookup(LogFS_BTreeRangeMap *bt,
      log_block_t x,
      void (*callback) (range_t, log_block_t, void *),
      void *data, int flags,
      range_t* retRange,
      log_block_t *retEndsAt)
{
   VMK_ReturnStatus status;
   log_block_t endsat = MAXBLOCK;

   /* We first check the buffered inserts, before consulting
    * the B-tree on disk */

   range_t range = {~0,};
   status = LogFS_BTreeRangeMapLookupInBuffer(bt, x, &range, &endsat);

   if (status == VMK_OK) {
      if (retRange) {
         *retRange = range;
         *retEndsAt = endsat;
         return VMK_EXISTS;
      } else {
         callback(range, endsat, data);
      }
   }

   else {
      /* The block was not covered by any buffered inserts, look in the B-tree instead */

      int postpone = 0;

      void *cant_block = (flags & FS_CANTBLOCK) ? (void *)1 : NULL;

      if (Semaphore_TryLock(&bt->sem)) {
         log_block_t endsat2 = MAXBLOCK;

         if (bt->tree != NULL) {
            tree_result_t r =
                __rangemap_get(bt->tree, x, &range, &endsat2, cant_block);

            endsat = MIN(endsat2, endsat);

            if (r == tree_result_node_fault) {
               postpone = 1;
            }
         } else {
            /* the tree has not been created yet, so postpone answering */
            postpone = 1;
         }

         Semaphore_Unlock(&bt->sem);
      } else
         postpone = 1;

      if (postpone) {
         QueuedLookup *l = malloc(sizeof(QueuedLookup));
         l->bt = bt;
         l->block = x;
         l->endsat = endsat;
         l->callback = callback;
         l->data = data;

         SP_Lock(&lookupQueueLock);
         List_Insert(&l->next, LIST_ATREAR(&lookupQueue));
         SP_Unlock(&lookupQueueLock);

         LogFS_KickDelayedLookup();
      }

      else {
         if (retRange) {
            *retRange = range;
            *retEndsAt = endsat;
            return VMK_EXISTS;
         } else {
            callback(range, endsat, data);
         }
      }

   }

   return VMK_OK;
}

