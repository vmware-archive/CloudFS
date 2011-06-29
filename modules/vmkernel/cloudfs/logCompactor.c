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
#include "logtypes.h"
#include "globals.h"
#include "metaLog.h"
#include "vDisk.h"
#include "vDiskMap.h"

#define max_replaces 400000
#define max_remaps 200000

struct OutLogWrapper {
   LogFS_MetaLog *metalog;
   LogFS_Log *outlog;
   log_size_t spaceLeft;

   log_id_t prev;
};

static void LogFS_MetaLogGCCloseLog(struct OutLogWrapper *wrapper)
{
   /* the outlog is allocated out-of-band, so we must free it here */

   LogFS_AppendLogClose(wrapper->outlog, NULL, 0);
   free(wrapper->outlog);
   wrapper->outlog = NULL;
}

struct update {
   LogFS_BTreeRangeMap *map;
   log_block_t from, to;
   log_id_t curver, newver;
};

struct remap {
   log_id_t from, to;
};

struct referer {
   log_pointer_t direction;
   log_id_t from, to;
};

struct {
   struct update updates[max_replaces];
   int num_updates;

   struct remap remaps[max_remaps];
   int num_remaps;
   struct referer referers[max_remaps];
   int num_referers;
   LogFS_MetaLog *metalog;

} batched_updates;

void add_update(LogFS_BTreeRangeMap *map, log_block_t from, log_block_t to,
                log_id_t curver, log_id_t newver)
{
   struct update u = { map, from, to, curver, newver };
   /* we can only do this last, after all BufferedLogs have been 
    * written to disk, therefore we need to be able to buffer a 
    * lot of replaces */
   if (batched_updates.num_updates >= max_replaces) {
      Panic("out of space for rangemap updates!\n");
   }
   batched_updates.updates[batched_updates.num_updates++] = u;
}

void update_referer(log_pointer_t direction, log_id_t position,
                    log_id_t newvalue)
{
   struct referer rm = { direction, position, newvalue };
   if (batched_updates.num_referers >= max_remaps) {
      Panic("out of referer space\n");
   }
   batched_updates.referers[batched_updates.num_referers++] = rm;
}

static int cmp(const void *a, const void *b)
{
   struct update *ua = (struct update *)a;
   struct update *ub = (struct update *)b;

   return (ua->map != ub->map) ? (ua->map - ub->map) : (ua->from - ub->from);
}

static int cmp2(const void *a, const void *b)
{
   log_id_t fa = ((struct remap *)a)->from;
   log_id_t fb = ((struct remap *)b)->from;

   return (fa.v.segment !=
           fb.v.segment) ? (fa.v.segment - fb.v.segment) : (fa.v.blk_offset -
                                                            fb.v.blk_offset);
}

static int logCmp(const void *a, const void *b)
{
   LogFS_Log *la = (LogFS_Log *)a;
   LogFS_Log *lb = (LogFS_Log *)b;
   return (int)(la->index - lb->index);
}

static int cmp3(const void *a, const void *b)
{
   log_id_t fa = ((struct referer *)a)->from;
   log_id_t fb = ((struct referer *)b)->from;

   return (fa.v.segment !=
           fb.v.segment) ? (fa.v.segment - fb.v.segment) : (fa.v.blk_offset -
                                                            fb.v.blk_offset);
}

static void LogFS_MetaLogFlushCompactedReferences(LogFS_MetaLog *ml)
{
   int i;
   LogFS_BTreeRangeMap *last_map = NULL;
   /* sort the btree updates for better locality */
   qsort(batched_updates.updates, batched_updates.num_updates,
         sizeof(struct update), cmp);

   for (i = 0; i < batched_updates.num_updates; i++) {
      struct update *u = &batched_updates.updates[i];
      if (u->map != last_map) {
         if (last_map)
            LogFS_BTreeRangeMapFlush(last_map);
         last_map = u->map;
      }
      LogFS_BTreeRangeMapReplace(u->map, u->from, u->to, u->curver, u->newver);
   }
   batched_updates.num_updates = 0;
   if (last_map)
      LogFS_BTreeRangeMapFlush(last_map);
}

static inline struct remap *binary_search(struct remap *first,
                                          struct remap *last, log_id_t val)
{
   int half;
   struct remap *middle;
   int len = last - first;
   while (len > 0) {
      half = len >> 1;
      middle = first;

      middle += half;

      if ((middle->from.v.segment < val.v.segment) ||
          (middle->from.v.segment == val.v.segment
           && middle->from.v.blk_offset < val.v.blk_offset)) {
         first = middle;
         ++first;
         len = len - half - 1;
      } else
         len = half;
   }

   return (first->from.raw == val.raw) ? first : NULL;

}

log_id_t LogFS_MetaLogLookupIndirection(LogFS_MetaLog *ml, log_id_t position)
{

   ASSERT(ml->compactionInProgress);

   int i;
   mk_invalid_version(r);

   for (i = 0; i < batched_updates.num_remaps; i++) {
      struct remap *remap = batched_updates.remaps + i;
      if (remap->from.raw == position.raw) {
         r = remap->to;
         break;
      }
   }

   return r;
}

static 
void LogFS_MetaLogGCFlushRefs(LogFS_MetaLog *ml)
{
   VMK_ReturnStatus status;

   int i;

   qsort(batched_updates.referers, batched_updates.num_referers,
         sizeof(struct referer), cmp3);
   qsort(batched_updates.remaps, batched_updates.num_remaps,
         sizeof(struct remap), cmp2);

   struct log_head *head = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);
   memset(head, 0, LOG_HEAD_SIZE);

   ml->compactionInProgress = TRUE;

   LogFS_Log *log = NULL;
   for (i = 0; i < batched_updates.num_referers; i++) {
      struct referer *r = &batched_updates.referers[i];

      log_id_t position = r->from;

      struct remap *remap =
          binary_search(batched_updates.remaps,
                        batched_updates.remaps + batched_updates.num_remaps,
                        position);

      if (remap) {
         ASSERT(position.raw == remap->from.raw);
         position = remap->to;
      }

      log = LogFS_MetaLogGetLog(ml, position.v.segment);

      if (r->direction == log_next_ptr) {
         init_forward_pointer(head, r->to);
      } else {
         init_backward_pointer(head, r->to);
      }

      SG_Array sg;
      SG_SingletonSGArray(&sg, position.v.blk_offset * BLKSIZE, (VA) head,
            LOG_HEAD_SIZE, SG_VIRT_ADDR);

      status = LogFS_LogWriteBody(log, NULL, &sg, 0);

      ASSERT(status == VMK_OK);
      log = LogFS_MetaLogPutLog(ml, log);
   }
   ASSERT(log == NULL);

   aligned_free(head);

   batched_updates.num_referers = 0;
   batched_updates.num_remaps = 0;

   ml->compactionInProgress = FALSE;

}

static LogFS_Log *reserve(struct OutLogWrapper *wrapper, size_t sz)
{
   VMK_ReturnStatus status;
   Async_Token *token = NULL;
   LogFS_MetaLog *ml = wrapper->metalog;

   if (wrapper->outlog == NULL || wrapper->spaceLeft < sz + 24 * LOG_HEAD_SIZE) {
      struct log_head *end = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);
      struct log_head *begin = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);
      void *b = NULL;

      log_segment_id_t id;

      id = LogFS_SegmentListAllocSegment(&ml->segment_list);

      if (wrapper->outlog) {
         /* close the previous segment with a 'next' pointer */

         log_id_t p;
         p.v.segment = id;
         p.v.blk_offset = 0;

         init_forward_pointer(end, p);
         log_id_t n;
         status = LogFS_AppendLogAppendSimple(wrapper->outlog, token, (void *)(end),
                                        LOG_HEAD_SIZE, &n, 0);
         ASSERT(status == VMK_OK);
         wrapper->spaceLeft -= LOG_HEAD_SIZE;

         LogFS_MetaLogGCCloseLog(wrapper);

         init_backward_pointer(begin, n);
         b = begin;

      }
      wrapper->outlog = (LogFS_Log *)malloc(sizeof(LogFS_Log));
      ASSERT(wrapper->outlog != NULL);

      wrapper->spaceLeft = LOG_MAX_SEGMENT_SIZE;
      LogFS_AppendLogInit(wrapper->outlog, ml, id, 0);
      LogFS_LogEnableBuffering(wrapper->outlog);

      //wrapper->outfilter = (LogFS_BloomFilter*) malloc(sizeof(LogFS_BloomFilter));
      //ASSERT(wrapper->outfilter);
      //LogFS_BloomFilterInit(wrapper->outfilter);

      if (b) {
         status =
             LogFS_AppendLogAppendSimple(wrapper->outlog, token, b, LOG_HEAD_SIZE,
                                   NULL, 0);
         ASSERT(status == VMK_OK);
         wrapper->spaceLeft -= LOG_HEAD_SIZE;
      }

      aligned_free(begin);
      aligned_free(end);
   }
   wrapper->spaceLeft -= sz;
   return wrapper->outlog;
}

void schedule_gc(void);

void LogFS_MetaLogCompact(LogFS_MetaLog *ml, LogFS_Log **logs, int num_segments)
{
   VMK_ReturnStatus status;
   int i;

   zprintf("_____ gc %d ____ \n", num_segments);

   struct OutLogWrapper outlog_wrapper;
   memset(&outlog_wrapper, 0, sizeof(outlog_wrapper));
   outlog_wrapper.metalog = ml;

   LogFS_Log *outlog = reserve(&outlog_wrapper, 0);

   /* Before we start, lets make sure that changes to the obsoleted heap
    * get tracked somewhere. We start out by mapping everything to the
    * first new segment we will be writing. As that one fills up, we 
    * will update the map correspondingly
    *
    * XXX there is a race here between the popping of segments off the binHeap,
    * and this.
    *
    */

   for (i = 0; i < num_segments; i++) {
      LogFS_ObsoletedSegmentsRemapSegment(&ml->obsoleted,
                                          LogFS_LogGetSegment(logs[i]),
                                          LogFS_LogGetSegment(outlog));
   }

   batched_updates.num_referers = 0;
   batched_updates.num_remaps = 0;

   char *buffer = (char *)aligned_malloc(LOG_MAX_SEGMENT_SIZE);
   ASSERT(buffer);

   struct log_head *outhead = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);

   for (i = 0; i < num_segments; i++) {
      char *b = buffer;

      LogFS_Log *log = logs[i];
      printf("gc segment %" FMT64 "d\n", log->index);
      LogFS_LogReadBody(log, NULL, b, LOG_MAX_SEGMENT_SIZE, 0);

      schedule_gc();

      for (; !is_block_zero(b);) {
         struct log_head *head = (struct log_head *)b;
         int i;
         log_offset_t e = b - buffer;

         if (head->tag == log_entry_type) {
            size_t sz;
            struct sha1_ctx ctx;
            unsigned short num_blocks = head->update.num_blocks;
            sha1_init(&ctx);
            sha1_update(&ctx, sizeof(log_block_t),
                        (const uint8_t *)&head->update.blkno);
            sha1_update(&ctx, sizeof(num_blocks), (const uint8_t *)&num_blocks);

            LogFS_VDisk *vd =
                LogFS_DiskMapLookupDisk(LogFS_HashFromRaw(head->disk));
            if (!vd) {
               Panic("ranges lookup during GC\n");
            }
            LogFS_BTreeRangeMap *ranges = LogFS_VDiskGetVersionsMap(vd);

            /* Copy over the entry portion of the old log head and 
             * make sure to zero out refs bitmap which fill up the 
             * remained of the log head structure */

            size_t entrySize = (char *)head->update.refs - (char *)head;
            memcpy(outhead, head, entrySize);
            memset((char *)outhead + entrySize, 0, LOG_HEAD_SIZE - entrySize);

            int j;
            for (i = 0, j = 0, sz = LOG_HEAD_SIZE; i < head->update.num_blocks;
                 i++) {
               if (BitTest(head->update.refs,i)) {
                  log_block_t endsat = MAXBLOCK;

                  log_id_t v =
                      LogFS_BTreeRangeMapLookup(ranges, i + head->update.blkno,
                                                &endsat);

                  if (is_invalid_version(v) || v.v.segment == log->index) {
                     char *blkdata = b + LOG_HEAD_SIZE + j * BLKSIZE;
                     BitSet(outhead->update.refs,i);
                     sha1_update(&ctx, BLKSIZE, (const uint8_t *)blkdata);

                     sz += BLKSIZE;
                  } else {
                     BitClear(outhead->update.refs,i);
                  }

                  ++j;
               }
            }
            /* append the newly created log head */
            outlog = reserve(&outlog_wrapper, sz);
            LogFS_ObsoletedSegmentsRemapSegment(&ml->obsoleted,
                                                LogFS_LogGetSegment(log),
                                                LogFS_LogGetSegment(outlog));

            sha1_update(&ctx, LOG_HEAD_SIZE - sizeof(struct log_head),
                        (const uint8_t *)outhead->update.refs);
            sha1_digest(&ctx, SHA1_DIGEST_SIZE, outhead->update.checksum);

            log_id_t newver;
            status =
                LogFS_AppendLogAppendSimple(outlog, NULL, outhead, LOG_HEAD_SIZE,
                                      &newver, 0);
            ASSERT(status == VMK_OK);

            Hash p;
            LogFS_HashSetRaw(&p, head->parent);
            LogFS_HashReapply(&p);

            /* update references in the rangemap to point to the new log segment */
            /* XXX TODO flush the ranges to disk before freeing old segment! */
            log_id_t curver;
            curver.v.segment = log->index;
            curver.v.blk_offset = e / BLKSIZE;

            add_update(ranges, head->update.blkno,
                       head->update.blkno + head->update.num_blocks, curver,
                       newver);
         }

         else if (head->tag == log_pointer_type) {
            /* Even though we copy sub-segments to new ones, we have to keep 
             * track of their temporal orderings. To begin with, each segment
             * starts with a back-pointer to the one before it, and ends with
             * a forward pointer to the next one. As several segments get 
             * compacted into one, it is important that we keep those two-ways
             * pointers up to date. 
             *
             * During the compaction scan, we take note of all pending updates,
             * and after having written the new segments we patch the pointer
             * records on disk. 
             *
             * XXX This is the only part of the compaction that does not
             * preserve crash-consistency of the log, and we should journal
             * the 'referers' list to disk before performing the updates in
             * place.
             */
            outlog = reserve(&outlog_wrapper, LOG_HEAD_SIZE);

            log_id_t oldpos;
            oldpos.v.segment = log->index;
            oldpos.v.blk_offset = e / BLKSIZE;

            log_id_t newPos;
            status =
                LogFS_AppendLogAppendSimple(outlog, NULL, head, LOG_HEAD_SIZE, 
                                      &newPos, 0);
            ASSERT(status == VMK_OK);

            if (!is_invalid_version(head->target)) {
               log_pointer_t direction = (head->direction == log_prev_ptr) ?
                   log_next_ptr : log_prev_ptr;

               update_referer(direction, head->target, newPos);
            }

            struct remap r = { oldpos, newPos };
            ASSERT(batched_updates.num_remaps < max_remaps);
            batched_updates.remaps[batched_updates.num_remaps++] = r;
         }

         /* the log head has already been appended to the outlog, but
          * the actual blocks have not. Do this, and increment the input 
          * pointer to consume all blocks from the input entry */

         b += LOG_HEAD_SIZE;

         if (head->tag == log_entry_type) {
            for (i = 0; i < head->update.num_blocks; i++) {
               /* if present in new vector, append to output */
               if (BitTest(outhead->update.refs,i)) {
                  status =
                      LogFS_AppendLogAppendSimple(outlog, NULL, b, BLKSIZE,
                                            NULL, 0);
                  if (status != VMK_OK) {
                     Panic("GC log append failed!\n");
                  }
               }

               /* if present in old one, consume from input */
               if (BitTest(head->update.refs,i)) {
                  b += BLKSIZE;
               }
            }

         }
      }
   }

   aligned_free(outhead);
   aligned_free(buffer);

   /* flush the last open buffered log segment to disk */
   LogFS_MetaLogGCCloseLog(&outlog_wrapper);

   /* update references in the b-tree to point to the newly created segments */
   LogFS_MetaLogFlushCompactedReferences(ml);

   /* update pointer records used for replication */
   LogFS_MetaLogGCFlushRefs(ml);

   /* at this point, there are no longer any references to the compacted segments,
    * so we can free them for future use */

   for (i = 0; i < num_segments; i++) {
      LogFS_Log *log = logs[i];
      LogFS_MetaLogFreeLog(ml, log);
   }
   LogFS_ObsoletedSegmentsClearRemaps(&ml->obsoleted);

   printf("gc done.\n\n");
}

void LogFS_MetaLogGC(LogFS_MetaLog *ml)
{
   LogFS_Log *candidates[64];

   int numSegments = LogFS_ObsoletedSegmentsGetCandidates(&ml->obsoleted, ml,
                                                          (void **)candidates,
                                                          64);
   qsort(candidates, numSegments, sizeof(LogFS_Log *), logCmp);

   /* NB: If we bail out now, we will leak the log segments and confuse the
    * binary heap that is keeping track of things, so we trust that enough
    * segments were given to us and fire off the log compactor unconditionally
    *
    * Only legal reason to bail is numSegments==0
    */

   if (numSegments > 0)
      LogFS_MetaLogCompact(ml, candidates, numSegments);
}
