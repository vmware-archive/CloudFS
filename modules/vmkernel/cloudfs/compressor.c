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
#include "fingerPrint.h"
#include "metaLog.h"
#include "hashDb.h"
#include "vebTree.h"
#include "graph.h"

static inline Bool checkDuplicate(
      LogFS_HashDb *hd,
      LogFS_FingerPrint *fp,
      int idx,
      log_id_t *pos)
{
   return LogFS_HashDbLookupHash(hd, LogFS_HashFromRaw(fp->fullHashes +
            SHA1_DIGEST_SIZE * idx), pos);
}

void LogFS_DedupeSegment(LogFS_MetaLog *ml, 
      LogFS_Log *log,
      LogFS_VebTree *vt)
{
   zprintf("compress segment %lu\n",LogFS_LogGetSegment(log));

   VMK_ReturnStatus status;
   LogFS_FingerPrint *fp = ml->fingerPrints[ LogFS_LogGetSegment(log) ];

   int i;
   int ranges=0;
   int blocks = 0;
   mk_invalid_version(last);
   int begin;


   log_id_t pos;
   pos.v.segment = LogFS_LogGetSegment(log);
   pos.v.blk_offset = 0;
   Bool prev = checkDuplicate(ml->hd,ml->fp,0, &pos);
   Bool duplicate;

#if 1
   for(i=1, begin=0; ; ++i, prev=duplicate) {

      Bool stop = (i==LOG_MAX_SEGMENT_BLOCKS);

      pos.v.segment = LogFS_LogGetSegment(log);
      pos.v.blk_offset = i;

      duplicate = FALSE;

      if (stop || prev != ( duplicate = checkDuplicate(ml->hd, fp, i, &pos) ) ||
            fp->owners[i-1].vd != fp->owners[i].vd ||
            last.v.segment != pos.v.segment || 
            last.v.blk_offset+(i-begin)  != pos.v.blk_offset ) {

         /* If previous block was a duplicate, we flush since last begin */
         if (prev) {

            log_block_t from = fp->owners[begin].blkno;
            log_block_t to   = fp->owners[i-1].blkno + 1;

            if ( fp->owners[begin].vd ) {
               log_id_t dst = last;
               //dst.v.blk_offset++; /* Not sure why must point at successor? */
               LogFS_BTreeRangeMap *bt = LogFS_VDiskGetVersionsMap( fp->owners[begin].vd );

               blocks += to - from;
               for (;;) {
                  status = LogFS_BTreeRangeMapInsertSimple(bt,from,to,dst);
                  if (status != VMK_BUSY) {
                     break;
                  }
                  CpuSched_Sleep(100);
               }

               ++ranges;
            }
         }

         begin = i;
         last = pos;

         if (stop) break;

      }

      //fp->owners[i].vd = NULL;


   }
#endif

   /* Update old-generation VEB tree. */
   LogFS_FingerPrintFinish(fp,vt,LogFS_LogGetSegment(log));

   zprintf("compressed %u blocks, %u ranges\n", blocks, ranges);
}

struct data {
   LogFS_MetaLog *ml;
   LogFS_VebTree *vt;
};

static void callback(GraphNode *n, void *data)
{
   zprintf("cb %d\n",n->key);
   struct data *d = data;

   LogFS_Log *log = LogFS_MetaLogGetLog(d->ml,n->key);
   LogFS_DedupeSegment(d->ml, log, d->vt);
   LogFS_MetaLogPutLog(d->ml,log);

}

void LogFS_Compressor(LogFS_MetaLog *ml, Bool *gcExit)
{

   while(!*gcExit) {

      if(ml->lurt==8) {
         ml->lurt = 0;

         zprintf("do graph\n");
         Graph *g = malloc(sizeof(Graph));
         Graph_Init(g);
         LogFS_VebTreeMerge(ml->vt,ml->vt,g);

         LogFS_VebTree *vt = malloc(sizeof(LogFS_VebTree));
         LogFS_VebTreeInit(vt,NULL,0x200,0x10000);
         LogFS_VebTreeClear(vt);

         struct data d = {ml,vt};
         Graph_Traverse(g,callback,&d);
      } else {
         CpuSched_Sleep(200);
      }
   }


#if 0
   while(!*gcExit) {

      LogFS_Log *log = LogFS_ObsoletedSegmentsGetCandidate(&ml->dupes,ml);

      if(log != NULL) {
         LogFS_DedupeSegment(ml, log, vt);
      } else {
         CpuSched_Sleep(200);
      }

   }
#endif


}
