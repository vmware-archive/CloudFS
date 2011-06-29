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

#ifndef __SEGMENTLIST_H__
#define __SEGMENTLIST_H__

#include "logfsConstants.h"
#include "logtypes.h"
#include "bitOps.h"

/** XXX locking makes no sense **/

typedef struct {
   SP_SpinLock lock;
   uint8 bitmap[MAX_NUM_SEGMENTS / 8 + 1];
} LogFS_SegmentList;

static inline void LogFS_SegmentListInit(LogFS_SegmentList *sl)
{
   memset(sl->bitmap, 0, sizeof(sl->bitmap));
   SP_InitLock("seglistlock", &sl->lock, SP_RANK_SEGMENTLIST);
}

static inline void LogFS_SegmentListFreeSegment(LogFS_SegmentList *sl,
                                                log_segment_id_t segment)
{
   SP_Lock(&sl->lock);
   BitClear(sl->bitmap,segment);
   SP_Unlock(&sl->lock);
}

static inline void LogFS_SegmentListStealSegment(LogFS_SegmentList *sl,
                                                 log_segment_id_t segment)
{
   SP_Lock(&sl->lock);
   BitSet(sl->bitmap,segment);
   SP_Unlock(&sl->lock);
}

static inline int LogFS_SegmentListSegmentInUse(LogFS_SegmentList *sl,
                                                log_segment_id_t segment)
{
   SP_Lock(&sl->lock);
   int r = BitTest(sl->bitmap,segment);
   SP_Unlock(&sl->lock);
   return r;
}

static inline log_segment_id_t LogFS_SegmentListAllocSegment(LogFS_SegmentList
                                                             *sl)
{
   SP_Lock(&sl->lock);

   log_segment_id_t r = INVALID_SEGMENT;

   int i;
   for (i = 0; i < MAX_NUM_SEGMENTS; i++) {
      if (!BitTest(sl->bitmap,i)) {
         BitSet(sl->bitmap,i);
         r = i;
         break;
      }
   }

   //log_segment_id_t r = find_first_zero_bit(sl->bitmap,LogFS_SegmentList_MAX_SEGMENTS/sizeof(unsigned long));
   SP_Unlock(&sl->lock);
   return r;
}

#endif                          /* __SEGMENTLIST_H__ */
