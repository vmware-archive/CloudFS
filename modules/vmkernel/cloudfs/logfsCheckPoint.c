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
#include "metaLog.h"
#include "globals.h"
#include "vDisk.h"
#include "vDiskMap.h"

VMK_ReturnStatus LogFS_RecoverCheckPoint(struct LogFS_MetaLog *ml,
      uint64 *generation, 
      log_id_t *logEnd, 
      disk_block_t *superTreeRoot)
{
   VMK_ReturnStatus status;
   int i;

   LogFS_SegmentList *sl = &ml->segment_list;

   LogFS_CheckPoint *latest = NULL;

   for (i = 0; i < 2; ++i) {
      LogFS_CheckPoint *cp = aligned_malloc(LOGFS_CHECKPOINT_SIZE);

      status = LogFS_DeviceRead(ml->device, NULL, cp, LOGFS_CHECKPOINT_SIZE, 0,
                                LogFS_CheckPointASection + i);
      if(status != VMK_OK) {

         zprintf("checkpoint read status %s\n",VMK_ReturnStatusToString(status));
         ASSERT(status==VMK_OK);

      }

      Hash checkSum = LogFS_HashChecksum((char *)cp + SHA1_DIGEST_SIZE,
                                         sizeof(LogFS_CheckPoint) -
                                         SHA1_DIGEST_SIZE);

      /* Did we find a valid checkpoint more recent than anything we had
       * before? */

      if (LogFS_HashEquals(checkSum, LogFS_HashFromRaw(cp->checksum)) 
            && ( latest==NULL || latest->generation < cp->generation)) {
            
            if(latest != NULL) {
               aligned_free(latest);
            }
            latest = cp;

      } else {
         aligned_free(cp);
      }

   }

   if (latest!=NULL) {

      /* Recover segment and B-tree allocation bitmaps */
      memcpy(sl->bitmap, latest->bitmap, sizeof(sl->bitmap));
      memcpy(nodesBitmap, latest->nodesBitmap, sizeof(nodesBitmap));

      for (i = 0; i < MAX_NUM_SEGMENTS; i++) {
         LogFS_BinHeapAdjustUp(&ml->obsoleted.heap, i, latest->heap[i]);
      }

      *generation = latest->generation;
      *logEnd = latest->logEnd;
      *superTreeRoot = latest->superTreeRoot;
      status = VMK_OK;

      aligned_free(latest);

   } else {
      zprintf("no good checksums found!\n");
      status = VMK_READ_ERROR;
   }

   return status;
}

extern VMK_ReturnStatus
LogFS_HandleViewChange(struct log_head *head,
                       LogFS_VDisk **retVd, LogFS_MetaLog *ml);

VMK_ReturnStatus LogFS_ReplayFromCheckPoint(LogFS_MetaLog *ml, log_id_t logEnd)
{
   VMK_ReturnStatus status;
   LogFS_SegmentList *sl = &ml->segment_list;

   struct log_head *head = aligned_malloc(LOG_HEAD_SIZE);

   LogFS_Log *log = LogFS_MetaLogGetLog(ml, logEnd.v.segment);
   zprintf("got log %ld\n", LogFS_LogGetSegment(log));
   log_offset_t offset = logEnd.v.blk_offset * BLKSIZE;

   size_t take;

   for (take = 0;; offset += take) {
      status = LogFS_LogForceReadBody(log, NULL, head, LOG_HEAD_SIZE, offset);
      ASSERT(status == VMK_OK);

      if (head->tag == log_entry_type) {
         LogFS_VDisk *vd;

         take = log_entry_size(head);

         if (head->tag == log_entry_type
             && head->update.blkno == METADATA_BLOCK) {
            zprintf("replay meta block\n");
            status = LogFS_HandleViewChange(head, &vd, ml);
         }

         if ((vd =
              LogFS_DiskMapLookupDiskForVersion(LogFS_HashApply
                                                (LogFS_HashFromRaw
                                                 (head->parent))))) {
            if (head->tag == log_entry_type) {
               zprintf("apply entry %ld\n", offset);
               LogFS_HashClear(&vd->secret_parent);

               log_id_t v;
               v.v.segment = LogFS_LogGetSegment(log);
               v.v.blk_offset = offset / BLKSIZE;

               Hash id;
               LogFS_HashSetRaw(&id, head->id);

               // Get the btrees in sync with the checkpoint
               LogFS_BTreeRangeMapInsert(LogFS_VDiskGetVersionsMap(vd),
                                         head->update.lsn,
                                         head->update.blkno,
                                         head->update.blkno +
                                         head->update.num_blocks, v,
                                         id,
                                         LogFS_HashFromRaw(head->entropy));
            }

            LogFS_VDiskUpdateFromHead(vd, head);

            /* Update logEnd to after latest entry with useful data */
            logEnd.v.segment = LogFS_LogGetSegment(log);
            logEnd.v.blk_offset = (take + offset) / BLKSIZE;

            /* Mark this segment in-use */
            log_segment_id_t segment = LogFS_LogGetSegment(log);
            LogFS_SegmentListStealSegment(sl, segment);
#if 0
            LogFS_BloomFilterInsert(ml->bloomFilter, segment,
                                    LogFS_HashApply(LogFS_HashFromRaw
                                                    (head->parent)));
#endif
         } else {
            /* entries that don't fit in any hash chain mark the end of the log */
            zprintf("reject entry/view\n");
            break;
         }

      }

      else if (head->tag == log_pointer_type && head->direction == log_prev_ptr) {
         take = LOG_HEAD_SIZE;
      }

      else if (head->tag == log_pointer_type && head->direction == log_next_ptr) {
         log_segment_id_t segment = head->target.v.segment;
         offset = head->target.v.blk_offset * BLKSIZE;

         zprintf("skip to log %ld\n", segment);
         log = LogFS_MetaLogPutLog(ml, log);
         log = LogFS_MetaLogGetLog(ml, segment);

         take = 0;
      } else {
         zprintf("unknown tag type %d\n", head->tag);
         break;
      }

   }

   status = LogFS_MetaLogReopen(ml, logEnd);
   ASSERT(status == VMK_OK);

   aligned_free(head);

   return status;
}

VMK_ReturnStatus
LogFS_CheckPointPrepare(LogFS_MetaLog *ml, LogFS_CheckPoint *cp, uint64 generation)
{
   int i;

   cp->generation = generation;
   memcpy(cp->bitmap, ml->segment_list.bitmap, sizeof(cp->bitmap));

   SP_Lock(&ml->append_lock);

   if (ml->activeLog) {
      cp->logEnd = LogFS_AppendLogGetEnd(ml->activeLog);
   } else {
      mk_invalid_version(inv);
      cp->logEnd = inv;
   }

   LogFS_BinHeap *heap = &ml->obsoleted.heap;
   for (i = 0; i < MAX_NUM_SEGMENTS; i++) {
      cp->heap[i] = heap->nodes[i].value;
   }

   SP_Unlock(&ml->append_lock);

   return VMK_OK;
}

VMK_ReturnStatus
LogFS_CheckPointCommit(LogFS_MetaLog *ml,
      LogFS_CheckPoint *cp, 
      int buffer,
      disk_block_t superTreeRoot,
      List_Links *movedNodes)
{
   VMK_ReturnStatus status;
   List_Links *elem, *next;

   cp->superTreeRoot = superTreeRoot;

   /* Apply changes to the checkpoint's copy of the node allocation bitmap. */

   LIST_FORALL(movedNodes, elem) {
      MovedNode *fn = List_Entry(elem, MovedNode, list);
      BitClear(cp->nodesBitmap, fn->from);
      BitSet(cp->nodesBitmap, fn->to);
   }

   Hash chk = LogFS_HashChecksum((char *)cp + SHA1_DIGEST_SIZE,
                                 sizeof(LogFS_CheckPoint) - SHA1_DIGEST_SIZE);
   LogFS_HashCopy(cp->checksum, chk);

   status = LogFS_DeviceWriteSimple(ml->device, NULL, cp, LOGFS_CHECKPOINT_SIZE,
                            0, LogFS_CheckPointASection + buffer);
   ASSERT(status==VMK_OK);

   /* Now that we wrote the checkpoint, we can free the disk blocks. */

   SP_Lock(&nodesLock);

   LIST_FORALL_SAFE(movedNodes, elem, next) {
      MovedNode *fn = List_Entry(elem, MovedNode, list);
      BitClear(nodesBitmap, fn->from);
      List_Remove(elem);
      free(fn);
   }

   SP_Unlock(&nodesLock);

   return status;
}
