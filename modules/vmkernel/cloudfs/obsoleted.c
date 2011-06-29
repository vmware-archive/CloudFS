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
#include "obsoleted.h"
#include "metaLog.h"
#include "binHeap.h"

void LogFS_ObsoletedSegmentsInit(LogFS_ObsoletedSegments *os)
{

   LogFS_BinHeapInit(&os->heap, MAX_NUM_SEGMENTS);
   os->numCandidateSegments = 0;

   SP_InitLock("obslock", &os->lock, SP_RANK_OBSOLETED);
   LogFS_ObsoletedSegmentsClearRemaps(os);
}

void LogFS_ObsoletedSegmentsCleanup(LogFS_ObsoletedSegments *os)
{
   LogFS_BinHeapCleanup(&os->heap);
   SP_CleanupLock(&os->lock);
}

void LogFS_ObsoletedSegmentsClearRemaps(LogFS_ObsoletedSegments *os)
{
   int i;

   for (i = 0; i < LOGFS_OBS_MAX_REMAPS; i++) {
      os->remaps[i].from = INVALID_SEGMENT;
   }
   os->numRemaps = 0;
}

void LogFS_ObsoletedSegmentsRemapSegment(LogFS_ObsoletedSegments *os,
                                         log_segment_id_t from,
                                         log_segment_id_t to)
{
   int i;
   SP_Lock(&os->lock);

   for (i = 0; i < os->numRemaps; i++) {
      if (os->remaps[i].from == from)
         break;
   }

   ASSERT(i < LOGFS_OBS_MAX_REMAPS);

   os->numRemaps = i;
   os->remaps[i].from = from;
   os->remaps[i].to = to;

   SP_Unlock(&os->lock);
}

void LogFS_ObsoletedSegmentsAdd(LogFS_ObsoletedSegments *os,
                                log_segment_id_t segment, int howmany)
{
   int i;
   int value;

   ASSERT(segment < MAX_NUM_SEGMENTS);

   /* XXX
    * integrate segment_list and obsoleted under a single lock. Don't add
    * any more to segments not in use, to prevent them cropping up at the
    * top of the heap -- a'la:
    if(!LogFS_SegmentListSegmentInUse(&ml->segment_list,segment)) return;

    in fact we could integrate the free/used bit into the heap ordering function,
    so that free segments were _|_ and always resides last in the heap. Then you
    could alloc a segment in log(N) time (set bit in last elem and percolate up)
    To ensure continuity of allocs, we could order free segments by segment number

    however, we still have to account for that slack, even though the segment is 
    in the process of getting compacted. The new destination for the data actually
    needs to have its slack count set to the sum of the slack appearing in the old
    segment while they were getting cleaned. This also means that the old segments
    will drop back down as they are reset to zero again at the end of GC.
    */

   SP_Lock(&os->lock);

   for (i = 0; i < os->numRemaps; i++) {
      if (os->remaps[i].from == segment)
         segment = os->remaps[i].to;
   }

   value = LogFS_BinHeapAdjustUp(&os->heap, segment, howmany);

   int limit = LOG_MAX_SEGMENT_BLOCKS / 5;
   /* Did we cross the threshold and become GC fodder? */
   if ((value - howmany) < limit && value >= limit) {
      ++(os->numCandidateSegments);
   }

   SP_Unlock(&os->lock);
}

int LogFS_ObsoletedSegmentsGetCandidates(LogFS_ObsoletedSegments *os,
                                         LogFS_MetaLog *ml,
                                         void **candidates, int max_candidates)
{
   /* Hack around a circular dep in include files by using void* for the metalog :-( XXX not needed */

   if (os->numCandidateSegments < 6)
      return 0;

   int i;
   int howmany = 0;

   log_segment_id_t segments[max_candidates + 1];
   int values[max_candidates + 1];

   int n;

   SP_Lock(&os->lock);
   n = MIN(max_candidates, os->numCandidateSegments);

   for (i = 0; i < n; i++) {
      segments[i] = LogFS_BinHeapPopMax(&os->heap, &values[i]);
   }
   SP_Unlock(&os->lock);

   int j;
   for (i = j = 0; i < n; i++) {
      log_segment_id_t segment = segments[i];
      int value;

      LogFS_Log *log = LogFS_MetaLogGetLog(ml, segment);

      /* We do not want to attempt cleaning active segments, or segments which
       * have already been freed */

      value = (LogFS_SegmentListSegmentInUse(&ml->segment_list, segment)) ?
          values[i] : 0;

      if (value == 0 && values[i] != 0)
         zprintf("attempted to GC unalloced log segment %ld\n", segment);

      if (value > 0 && !LogFS_LogIsAppendLog(log)) {
         /* we reuse the values array in case we need to rollback below,
          * which works because j<=i */

         candidates[j] = log;
         values[j] = value;
         howmany += value;

         ++j;
      } else {
         SP_Lock(&os->lock);
         LogFS_BinHeapAdjustUp(&os->heap, segment, value);
         SP_Unlock(&os->lock);

         LogFS_MetaLogPutLog(ml, log);
      }
   }

   /* If we ended up not having enough segments for GC to be meaningful (free
    * up at least one), we have to rollback our changes to the binary heap and
    * drop references to the rest of the candidate log segments */

   if (j < 5) {
      for (i = 0; i < j; i++) {
         LogFS_Log *log = candidates[i];

         SP_Lock(&os->lock);
         LogFS_BinHeapAdjustUp(&os->heap, LogFS_LogGetSegment(log), values[i]);
         SP_Unlock(&os->lock);

         LogFS_MetaLogPutLog(ml, log);
      }
      n = 0;
   } else {
      n = j;
      SP_Lock(&os->lock);
      os->numCandidateSegments -= n;
      SP_Unlock(&os->lock);
      printf("returning %d segments with %d blocks free\n", n, howmany);
   }
   return n;
}
