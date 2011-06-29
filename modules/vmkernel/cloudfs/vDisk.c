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
#include "globals.h"
#include "common.h"
#include "vDisk.h"
#include "vDiskMap.h"
#include "logfsHttpd.h"
#include "logfsHttpClient.h"
#include "remoteLog.h"

//#define SYNCMODE  /* await remote IO completion forever */

//#define SP_Lock(_a) do{zprintf("%s:%d\n",__FUNCTION__,__LINE__); SP_Lock(_a); } while(0);

typedef struct {
   LogFS_VDisk *vd;
   LogFS_BTreeRangeMap *bt;

   LogFS_RefCountedBuffer *headBuffer;

   log_id_t version;
   Hash id;

} LogFS_VDiskSyncContext;

typedef struct {
   LogFS_VDisk *vd;
   Async_Token *token;
   char *buf;
   log_block_t blkno;
   size_t num_blocks;
   int flags;
} LogFS_VDiskReadParameters;

static void
LogFS_VDiskCommonInit(LogFS_VDisk *vd, LogFS_VDisk *parentDisk, Hash disk,
                      LogFS_MetaLog *log);

/* vd->lock must be held */
static inline LogFS_BTreeRangeMap *LogFS_VDiskGetVersionsMapLocked(LogFS_VDisk *vd)
{
   LogFS_BTreeRangeMap *bt;

   if (vd->bt == NULL) {
      bt = malloc(sizeof(LogFS_BTreeRangeMap));
      LogFS_BTreeRangeMapInit(bt, vd->log, vd->disk, vd->parent, vd->entropy);
      vd->bt = bt;

      LogFS_KickFlusher();
   } else
      bt = vd->bt;

   return bt;
}

LogFS_BTreeRangeMap *LogFS_VDiskGetVersionsMap(LogFS_VDisk *vd)
{
   LogFS_BTreeRangeMap *bt;

   SP_Lock(&vd->lock);
   bt = LogFS_VDiskGetVersionsMapLocked(vd);
   SP_Unlock(&vd->lock);

   return bt;
}

Hash LogFS_VDiskGetCurrentId(LogFS_VDisk *vd)
{
   LogFS_Hash r;
   SP_Lock(&vd->lock);
   r = vd->parent;
   SP_Unlock(&vd->lock);
   return r;
}

/* void*-compatible virtual methods */

Hash LogFS_VDiskGetBaseId(LogFS_VDisk *vd)
{
   return vd->disk;
}

Hash LogFS_VDiskGetParentDiskId(void *vd)
{
   return ((LogFS_VDisk *)vd)->parentBaseId;
}

static void LogFS_VDiskDeref(LogFS_VDisk *vd)
{
   SP_Lock(&vd->lock);
   if (Atomic_FetchAndDec(&vd->refCount) == 1) {
      CpuSched_Wakeup(&vd->closeWaiters);
   }
   SP_Unlock(&vd->lock);
}

typedef struct {
   LogFS_RefCountedBuffer *headBuffer;
   LogFS_RemoteLog *rl;
} LogFS_VDiskNetSendContext;

void LogFS_VDiskSendDone(Async_Token * token, void *data)
{
   LogFS_VDiskNetSendContext *c = data;

   if (token->transientStatus != VMK_OK) {
      zprintf("closing rl due to bad transientStatus\n");

      LogFS_RemoteLogClose(c->rl);
   }

   LogFS_RefCountedBufferRelease(c->headBuffer);

#ifdef SYNCMODE
   Async_TokenCallback(token);
#else
   Async_ReleaseToken(token);
#endif

}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_VDiskWriteDone --
 *
 *      Update LBA B-tree upon log append completion. This essentially 
 *      commits the writes so that they will be visible in future reads.
 *
 * Results:
 *      None.
 *
 * Side effects:
 *      B-tree will contain a mapping from the written LBAs to the log entry.
 *      After a crash, the recovery code will ensure the B-tree gets updated.
 *
 *-----------------------------------------------------------------------------
 */

void LogFS_VDiskWriteDone(Async_Token * token, void *data)
{
   LogFS_VDiskSyncContext *c = data;
   LogFS_VDisk *vd = c->vd;

   mk_invalid_version(inv);

   struct log_head *head = c->headBuffer->buffer;
   log_id_t v = c->version;

   SP_Lock(&vd->lock);

   int i;

   ++(v.v.blk_offset); /* skip over head */

   log_id_t *vs[] = {&inv,&v};
   log_block_t begin;

   for (i=1, begin=0 ; ; i++) {

      Bool stop = (i == head->update.num_blocks);
      int prev = BitTest(head->update.refs,i-1);

      if (stop || prev != BitTest(head->update.refs,i)) {

         LogFS_BTreeRangeMapInsert(vd->bt,
               head->update.lsn,
               head->update.blkno + begin,
               head->update.blkno + i,
               *vs[prev], c->id, vd->entropy);

         /* Increment disk pointer for non-zero blocks. */
         if (prev) {
            v.v.blk_offset += (i-begin);
         }

         begin = i;

         if (stop) break;
      }

   }
   SP_Unlock(&vd->lock);

   LogFS_RefCountedBufferRelease(c->headBuffer);
   LogFS_VDiskDeref(vd);

   ((SCSI_Result *) token->result)->status =
       SCSI_MAKE_STATUS(SCSI_HOST_OK, SDSTAT_GOOD);
   Async_TokenCallback(token);

}

typedef struct {
   LogFS_VDisk *vd;
   Async_Token *token;
   const char *buf;
   log_block_t blkno;
   size_t num_blocks;
   int flags;
   uint64 needsGeneration;
   List_Links next;
} LogFS_VDiskIoContext;

VMK_ReturnStatus
LogFS_VDiskContinueWrite(LogFS_VDisk *vd,
                         Async_Token * token,
                         const char *buf,
                         log_block_t blkno, size_t num_blocks, int flags)
{
   VMK_ReturnStatus status = VMK_OK;
   struct sha1_ctx ctx;

   int blocks_left;
   int take;

   Async_IOHandle *ioh;

   SP_Lock(&vd->lock);

   ASSERT(LogFS_VDiskIsWritable(vd));

   Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);
   const int sgMax = 1024;

   for (blocks_left = num_blocks; blocks_left > 0; blocks_left -= take) {

      take = MIN(blocks_left, LOG_HEAD_MAX_BLOCKS);

      SG_Array *sgArr = SG_Alloc(LogFS_GetHeap(), sgMax);
      sgArr->addrType = SG_VIRT_ADDR;

      struct log_head *head = aligned_malloc(LOG_HEAD_SIZE);
      LogFS_RefCountedBuffer *headBuffer = 
         LogFS_RefCountedBufferCreate(head,LOG_HEAD_SIZE);

      memset(head, 0, LOG_HEAD_SIZE);
      head->tag = log_entry_type;
      head->update.lsn = ++( vd->lsn );
      head->update.blkno = blkno;
      head->update.num_blocks = take;

      /* This update depends on the 'parent', the current version of this VDisk
       * (volume). To prevent others from appending to the VDisk, we obfuscate
       * the parent and store the back reference using the thus-far 'secret
       * parent' (aka 'secret id').  */

      memcpy(head->parent, vd->secret_parent.raw, SHA1_DIGEST_SIZE);

      ASSERT ( LogFS_HashEquals(LogFS_HashApply( vd->secret_parent ), vd->parent) );

      /* We also include the base volume id, as this is practical to have when
       * doing log compaction. */

      LogFS_HashCopy(head->disk, LogFS_VDiskGetBaseId(vd));

      /* We need a checksum for the update, including the block offset
       * and the length of the update, and of the actual blocks
       * following the header. We also include the bit vector used
       * for compressing away any zero blocks, but we do that last
       * to avoid looping over the block data twice. */

      sha1_init(&ctx);
      uint64 b = blkno;
      uint16 n = num_blocks;
      uint64 l = head->update.lsn;
      sha1_update(&ctx, sizeof(l), (const uint8_t *)&l);
      sha1_update(&ctx, sizeof(b), (const uint8_t *)&b);
      sha1_update(&ctx, sizeof(n), (const uint8_t *)&n);

      /* We only store non-zero blocks in the log, and represent zero blocks
       * as unset bits in the log header bit vector. If we could store a 'is
       * zero' version id directly in the B-tree we could save some tree
       * space and disk reads, however, right now the only null-value we have
       * is the invalid version, which means 'fall back to parent disk'
       * rather than 'return zeroes'. 
       */

      int i,j;

      /* Now prepare the SG array that we must pass on the the lower layers.
       * First entry points to the newly created log entry head */

      sgArr->sg[0].addr = (uint64) head;
      sgArr->sg[0].offset = 0;
      sgArr->sg[0].length = LOG_HEAD_SIZE;
      sgArr->length = 1;

      /* Then loop over the blocks and create SG entries that point to the
       * beginning of each non-zero block range, with offsets starting after
       * the log header. We do not yet know where on the disk the log entry
       * will get written, so we always start from zero. */

      int mode = 0;
      uint64 offset = LOG_HEAD_SIZE;

      for (i=0, j=0 ; i < take; i++) {
         char *blkdata = (char *)buf + i * BLKSIZE;

         if (!is_block_zero(blkdata)) {

            if(mode==0) {

               ++j;
               ASSERT(j<sgMax);

               sgArr->sg[j].addr = (uint64) blkdata;
               sgArr->sg[j].offset = offset;
               sgArr->sg[j].length = BLKSIZE;

               ++(sgArr->length);

            } else {
               ASSERT(j<sgMax);
               sgArr->sg[j].length += BLKSIZE;
            }

            BitSet(head->update.refs,i);
            sha1_update(&ctx, BLKSIZE, blkdata);
            offset += BLKSIZE;

            mode = 1;
         }
         else {
            BitClear(head->update.refs,i);
            mode = 0;
         }
      }

      /* Now that we have seen all the blocks, fixate the entry checksum */

      sha1_update(&ctx, LOG_HEAD_SIZE - sizeof(struct log_head),
            (const uint8_t *)head->update.refs);
      sha1_digest(&ctx, SHA1_DIGEST_SIZE, head->update.checksum);

      /* calculate 'entropy' coming from update context and contents.
       * This is an incremental hash covering the entire history of the
       * volume. We store this in a separate field because we may have
       * to modify the checksum later, during log compation, but we wish
       * to preserve the entropy for future use.
       *
       * XXX we could turn this into a HMAC and get update signing for free. */

      sha1_init(&ctx);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)head->parent);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)head->update.checksum);
      sha1_digest(&ctx, SHA1_DIGEST_SIZE, head->entropy);

      /* to prevent accidental split-brains, the entry id is used as a one-time
       * password that prevents others from appending a new entry after this one,
       * unless they know the previous entry id. So instead of storing the id 
       * as clear text, we store H(id), and then the cleartext-version in the subsequent
       * entry parent field */

      char tmp[SHA1_DIGEST_SIZE];
      sha1_init(&ctx);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, vd->secretView.raw);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, head->entropy);
      sha1_digest(&ctx, SHA1_DIGEST_SIZE, tmp);
      Hash secretId = LogFS_HashFromRaw(tmp);
      Hash id = LogFS_HashApply(secretId);
      LogFS_HashCopy(head->id,id); 

      vd->entropy = LogFS_HashFromRaw(head->entropy);
      vd->parent = id;
      vd->secret_parent = secretId;

      /* Size of the write in bytes on the physical disk, including log head */
      //log_size_t sz = LOG_HEAD_SIZE + bodySize;

      Async_Token *appendToken = Async_PrepareOneIO(ioh, NULL);
      mk_invalid_version(inv);


      LogFS_VDiskSyncContext *c = Async_PushCallbackFrame(appendToken,
               LogFS_VDiskWriteDone, sizeof (LogFS_VDiskSyncContext));

      LogFS_VDiskRef(vd);
      LogFS_RefCountedBufferRef(headBuffer);

      c->vd = vd;
      c->bt = LogFS_VDiskGetVersionsMapLocked(vd);
      c->headBuffer = headBuffer;
      c->version = inv;    /* Will be set by LogFS_AppendLogAppend() */
      c->id = id;

      status = LogFS_MetaLogAppend(vd->log, appendToken,
                                          sgArr, &c->version,
                                          flags);
      ASSERT(status==VMK_OK);

      /* Forward the update to remote replicas, in parallel with the write to
       * local storage.  The assumption here is that the local write does not
       * fail irrecoverably. In LogFS_LogWriteBody() we take great care to
       * retry writes etc., so that only permanent media errors can cause the
       * IO to fail.  Aborted write IOs are still persisted to disk, to
       * prevent replica divergence.  XXX Ideally we should quiesce incoming
       * IO when writes are failing, to make sure we get through to the disk
       * eventually. */

      List_Links *curr, *next;
      LIST_FORALL_SAFE(&vd->remoteLogs, curr, next) {
         LogFS_RemoteLog *rl = List_Entry(curr, LogFS_RemoteLog, nextLog);

         if (!LogFS_RemoteLogIsOpen(rl))
            continue;

#ifdef SYNCMODE
         Async_Token *netToken = Async_PrepareOneIO(ioh, NULL);
#else
         Async_Token *netToken = Async_AllocToken(0);
#endif

         LogFS_RefCountedBufferRef(headBuffer);

         LogFS_VDiskNetSendContext *c = Async_PushCallbackFrame(netToken,
               LogFS_VDiskSendDone, sizeof (LogFS_VDiskNetSendContext *));
         c->rl = rl;
         c->headBuffer = headBuffer;

         VMK_ReturnStatus netStatus;
         netStatus = LogFS_RemoteLogAppend(rl, sgArr, netToken);

         if (netStatus == VMK_WOULD_BLOCK) {
            zprintf("detaching still open RL\n");
            List_Remove(curr);
#ifdef SYNCMODE
            //Panic("buffer full in syncmode!\n");
#endif
         } else if (netStatus != VMK_OK) {
            zprintf("detaching RL\n");
            List_Remove(curr);
            LogFS_RemoteLogClose(rl);
#ifdef SYNCMODE
            netToken->transientStatus = status;
            Async_TokenCallback(netToken);
#endif
         }
      }

      LogFS_RefCountedBufferRelease(headBuffer);
      SG_Free(LogFS_GetHeap(), &sgArr);

      /* increment counters */
      blkno += take;
      buf += take * BLKSIZE;
   }

   Async_EndSplitIO(ioh, VMK_OK, FALSE);

   SP_Unlock(&vd->lock);
   return status;
}

/* Continue IO, caller must have bumped refcount to prevent the secret token being stolen */

void LogFS_VDiskContinueIo(LogFS_VDiskIoContext * c)
{
   LogFS_VDisk *vd = c->vd;
   Async_Token *token = c->token;
   const char *buf = c->buf;
   log_block_t blkno = c->blkno;
   size_t num_blocks = c->num_blocks;
   int flags = c->flags;

   uint64 now = Timer_SysUptime();
   if (token->absTimeoutMS != 0 && now > token->absTimeoutMS) {
      zprintf("aborting expired IO!\n");
      token->transientStatus = VMK_TIMEOUT;
      Async_TokenCallback(token);
   }

   else if (LogFS_VDiskIsWritable(vd)) {
      if (flags & FS_READ_OP) {
         LogFS_VDiskContinueRead(vd, token, buf, blkno, num_blocks, flags);
      } else if (flags & FS_WRITE_OP) {
         LogFS_VDiskContinueWrite(vd, token, buf, blkno, num_blocks, flags);

      }
   } else {
      zprintf("failing io to %ld\n", blkno);
      NOT_REACHED();
      token->transientStatus = VMK_RESERVATION_CONFLICT;
      Async_TokenCallback(token);
   }
}

VMK_ReturnStatus
LogFS_VDiskWrite(LogFS_VDisk *vd,
                 Async_Token * token,
                 const char *buf,
                 log_block_t blkno, size_t num_blocks, int flags)
{
   VMK_ReturnStatus status = VMK_OK;

   printf("write %" FMT64 "d+%lu\n", blkno, num_blocks);

 retry:
   SP_Lock(&vd->lock);
   LogFS_BTreeRangeMap *bt = vd->bt;

   if (bt && LogFS_BTreeRangeMapHighWater(bt)) {
      if (flags & FS_CANTBLOCK) {
         if (!LogFS_BTreeRangeMapCanInsert(bt)) {
            zprintf("can't insert, b-tree full!\n");
            status = VMK_WOULD_BLOCK;
            goto out;
         }
      } else {
         SP_Unlock(&vd->lock);
         zprintf("force flush\n");
         LogFS_BTreeRangeMapFlush(bt);
         goto retry;
      }
   }

   if (LogFS_VDiskIsWritable(vd)) {
      LogFS_VDiskRef(vd);
      SP_Unlock(&vd->lock);

      status =
          LogFS_VDiskContinueWrite(vd, token, buf, blkno, num_blocks, flags);
      LogFS_VDiskDeref(vd);
      return status;
   } else {
      Hash inv;

      LogFS_HashClear(&inv);
      Hash id = LogFS_HashIsValid(vd->secretView) ? vd->parent : inv;

      status = LogFS_HttpClientRequest(vd->disk, id,
                                       token, (char *)buf, blkno, num_blocks,
                                       flags & FS_WRITE_OP);
   }
 out:
   SP_Unlock(&vd->lock);
   return status;

}

void LogFS_VDiskAddRemoteLog(LogFS_VDisk *vd, LogFS_RemoteLog *rl)
{
   LogFS_RemoteLogRef(rl);
   List_Insert(&rl->nextLog, LIST_ATREAR(&vd->remoteLogs));
}

int LogFS_VDiskAddRemoteLogIfSameVersion(LogFS_VDisk *vd, LogFS_RemoteLog *rl)
{
   int added = 0;
   SP_Lock(&vd->lock);
   if (vd->lsn == rl->lsn) {
      zprintf("adding sync log\n");
      LogFS_VDiskAddRemoteLog(vd, rl);
      added = 1;
   }
   SP_Unlock(&vd->lock);
   return added;
}

void LogFS_VDiskContinueWithSecret(LogFS_VDisk *vd)
{
   List_Links *curr, *next;
   LIST_FORALL_SAFE(&vd->tokenWaiters, curr, next) {
      LogFS_VDiskIoContext *c = List_Entry(curr, LogFS_VDiskIoContext, next);

      SP_Lock(&vd->lock);
      Bool canProceed = (vd->generation == c->needsGeneration);
      SP_Unlock(&vd->lock);

      if (canProceed) {
         LogFS_VDiskContinueIo(c);
         List_Remove(curr);
         free(c);
      }
   }
}

VMK_ReturnStatus LogFS_VDiskSetSecret(LogFS_VDisk *vd, Hash secret,
                                      Hash secretView)
{
   VMK_ReturnStatus status;

   SP_Lock(&vd->lock);

   if (LogFS_HashIsNull(secret)) {
      vd->secretView = secretView;
      status = VMK_OK;
   }

   else if (LogFS_HashEquals(vd->parent, LogFS_HashApply(secret))) {
      vd->secret_parent = secret;
      vd->secretView = secretView;
      vd->stopRequested = FALSE;
      ++(vd->generation);
      status = VMK_OK;
      zprintf("secret set ok\n");
   } else {
      Warning("bad secret input!\n");
      status = VMK_BAD_PARAM;
   }

   LogFS_VDiskRef(vd);

   SP_Unlock(&vd->lock);

   if (status == VMK_OK)
      LogFS_VDiskContinueWithSecret(vd);
   LogFS_VDiskDeref(vd);

   return status;
}

void LogFS_VDiskRelease(LogFS_VDisk *vd)
{
   SP_Lock(&vd->lock);
   vd->haveReservation = FALSE;
   SP_Unlock(&vd->lock);
   LogFS_VDiskDeref(vd);
}

VMK_ReturnStatus LogFS_VDiskReserve(LogFS_VDisk *vd)
{
   SP_Lock(&vd->lock);
   LogFS_VDiskRef(vd);
   vd->haveReservation = TRUE;
   SP_Unlock(&vd->lock);

   return VMK_OK;
}

VMK_ReturnStatus LogFS_VDiskSnapshot(LogFS_VDisk *vd, LogFS_VDisk **snapshot)
{
   VMK_ReturnStatus status = VMK_OK;
   LogFS_VDisk *carcass;

   zprintf("snapshotting disk %s\n", LogFS_HashShow(&vd->disk));

   /* We need two new vDisks, one to hold the immutable state of the base
    * branch, and one for storing changes to the new one.
    * Rather than trying to figure out who might be holding references to
    * the existing vd, we keep the old one and fix up the bt ptr */

   /* Create the 'carcass'; the readonly snapshot of the vd the way it looks
    * right now, inheriting its baseId and version map.
    * If the current vd is the same version as its parent (an older carcass),
    * we reuse that one.
    */

   /* We currently do not support branching an empty disk as this makes little
    * sense. Better to just create a new one. */

   if (LogFS_HashEquals(vd->disk, vd->parent)) {
      zprintf("cannot snapshot empty disk!\n");
      status = VMK_NOT_SUPPORTED;
      goto out;
   }

   if (vd->parentDisk != NULL &&
       LogFS_HashEquals(vd->parent, vd->parentDisk->parent)) {
      carcass = vd->parentDisk;
   } else {
      carcass = malloc(sizeof(LogFS_VDisk));
      if (carcass == NULL) {
         status = VMK_NO_MEMORY;
         goto out;
      }

      LogFS_VDiskCommonInit(carcass, NULL, vd->disk, vd->log);
      carcass->bt = vd->bt;
      carcass->parent = vd->parent;
      carcass->isImmutable = TRUE;
   }

   /* Though we snapshotted the vd, we reuse the old vd struct to hold the
    * writable overlay that retains the old base name. We cannot create a new
    * b-tree for its changes right away, because we need a new log entry to
    * serve as it's first head, which we don't have before the next write
    * comes in. */

   SP_Lock(&vd->lock);

   vd->bt = NULL;

   /* All future unresolved reads will fall back to the immutable carcass */
   vd->parentDisk = carcass;

   SP_Unlock(&vd->lock);

   /* No point in having unflushed inserts in the carcass bt map */
   LogFS_BTreeRangeMapFlush(carcass->bt);
   *snapshot = carcass;
   status = VMK_OK;
 out:
   return status;
}

#if 0
VMK_ReturnStatus
LogFS_VDiskBranch(LogFS_VDisk *vd, LogFS_Hash childId, LogFS_VDisk **result)
{
   VMK_ReturnStatus status;
   LogFS_VDisk *child;

   LogFS_VDiskBase *snapshot;
   status = LogFS_VDiskSnapshot(vd, &snapshot);

   if (status != VMK_OK)
      goto out;

   child = malloc(sizeof(LogFS_VDisk));
   if (child == NULL) {
      status = VMK_NO_MEMORY;
      goto out;
   }
   LogFS_VDiskInit(child, snapshot, childId, vd->log);
// child->parent = childId;

 out:
   return status;
}
#endif

VMK_ReturnStatus
LogFS_VDiskGetSecret(LogFS_VDisk *vd, Hash * secret, Bool failIfBusy)
{
   /* Wait for outstanding writes finishing, and client VM closing the disk
    * before returning the secret key to the vd */

   VMK_ReturnStatus status;

   SP_Lock(&vd->lock);

   if (failIfBusy && Atomic_Read(&vd->refCount) > 0) {
      status = VMK_BUSY;
      goto out;
   }

   /* Quiesce IO */

   if (LogFS_HashIsValid(vd->secret_parent)) {
      vd->stopRequested = TRUE;
      status = VMK_OK;
   } else {
      status = VMK_FAILURE;
      goto out;
   }

   while (Atomic_Read(&vd->refCount) > 0) {
      status = CpuSched_Wait(&vd->closeWaiters, CPUSCHED_WAIT_SCSI, &vd->lock);
      if (status != VMK_OK)
         return status;
      SP_Lock(&vd->lock);
   }

   ASSERT(!vd->haveReservation);

   if (LogFS_HashIsValid(vd->secret_parent)) {
      *secret = vd->secret_parent;
      LogFS_HashClear(&vd->secret_parent);
      status = VMK_OK;
   } else {
      LogFS_HashClear(secret);
      zprintf("failed getting secret after wakeup!\n");
      status = VMK_FAILURE;
   }

 out:
   SP_Unlock(&vd->lock);
   return status;
}

/*typedef struct {
   LogFS_VDisk *vd;
   LogFS_RemoteLog *rl;
} LogFS_VDiskRemoteAppendContext; */

VMK_ReturnStatus LogFS_VDiskAppend(LogFS_VDisk *vd,
                                   Async_Token * token,
                                   LogFS_RefCountedBuffer *rb)
{
   VMK_ReturnStatus status;
   Hash parent, id, entropy;
   uint64 lsn;

   struct log_head *head = (struct log_head *) rb->buffer;

   LogFS_HashSetRaw(&parent, head->parent);
   LogFS_HashSetRaw(&id, head->id);
   LogFS_HashSetRaw(&entropy, head->entropy);
   lsn = head->update.lsn;

   SP_Lock(&vd->lock);

   /* View-change records can be valid with a parent pointer either to itself
    * or to a parent in clear-text, but in that case it will have been treated
    * as a branch record, and not get this far. Here, data and view entries
    * are valid only if the H(head->parent) == vd->parent. */

   if (head->tag == log_entry_type &&
         head->update.lsn == vd->lsn +1 &&
       (LogFS_HashEquals(LogFS_HashApply(parent), vd->parent) ||
        (LogFS_HashEquals(id, vd->parent) && LogFS_HashEquals(id, vd->disk)))
       ) {
      status = VMK_OK;
   } else {
      zprintf("bad or unknown log entry appended, tag %u\n", head->tag);
      zprintf("parent was %s\n", LogFS_HashShow(&parent));
      zprintf("should be %s\n", LogFS_HashShow(&vd->parent));
      status = VMK_WRITE_ERROR;
      goto out;
   }

   vd->parent = id;
   vd->lsn = lsn;
   LogFS_HashClear(&vd->secret_parent);
   LogFS_HashSetRaw(&vd->entropy, head->entropy);

   /* MetaLog and BTree are protected by their own locking schemes, so
    * we do not (cannot) hold the vd->lock when updating those */

   head = NULL;                 /* when the split IOs complete the buffer holding the head may
                                   have been freed */


   LogFS_VDiskRef(vd);
   LogFS_RefCountedBufferRef(rb);

   LogFS_VDiskSyncContext *c = Async_PushCallbackFrame(token,
         LogFS_VDiskWriteDone, sizeof (LogFS_VDiskSyncContext));

   mk_invalid_version(inv);
   c->vd = vd;
   c->bt = LogFS_VDiskGetVersionsMapLocked(vd);
   c->headBuffer = rb;
   c->version = inv;    /* Will be set by LogFS_AppendLogAppend() */
   c->id = id;

   SG_Array sgArr;
   SG_SingletonSGArray(&sgArr, 0, (VA) rb->buffer, rb->count, SG_VIRT_ADDR);
   status = LogFS_MetaLogAppend(vd->log, token, &sgArr, &c->version, 0);
   ASSERT(status==VMK_OK);

   SP_Unlock(&vd->lock);

   return status;

 out:
   SP_Unlock(&vd->lock);
   return status;
}

static inline log_offset_t
LogFS_VDiskGetAbsoluteDiskPosition(LogFS_MetaLog *ml,
                                   LogFS_Log *log, log_offset_t offset)
{
   LogFS_Device *device = ml->device;

   return _cursor(log, offset)
       + LogFS_DiskLayoutGetOffset(&device->diskLayout,
                                   LogFS_LogSegmentsSection);

}

typedef struct {
   Async_Token *token;
   LogFS_MetaLog *log;
   LogFS_Log *sublog;
   struct log_head *head;
   char *buf;
   log_block_t blkno;
   log_size_t sz;
   log_offset_t position;
   log_size_t readahead;
} LogFS_VDiskHeadReadContext ;

void LogFS_VDiskFreeGenericBuffer(Async_Token * token, void *data)
{
   void **bufPtr = data;
   free(*bufPtr);
   Async_TokenCallback(token);
}



typedef struct {
   LogFS_VDisk *vd;
   log_block_t blkno;
   size_t num_blocks;
   char *buf;
   Async_IOHandle *ioh;
   log_block_t end;
   int flags;
   int depth;

} LogFS_VDiskLookupContext;

void LogFS_VDiskProcessLookups(range_t range, log_block_t endsat, void *data)
{
   VMK_ReturnStatus status;

   LogFS_VDiskLookupContext *c = (LogFS_VDiskLookupContext *) data;
   ++(c->depth);
   if (c->depth > 5)
      zprintf("recursion depth %d\n", c->depth);

   log_id_t v;
   v.raw = range.version;

   for (;;) {

      //zprintf("process %lu-> (%lu) blkno %lu\n",range.from,endsat,c->blkno);

      char *buf = c->buf;
      LogFS_VDisk *vd = c->vd;
      LogFS_MetaLog *log = vd->log;

      log_block_t i = c->blkno;

      log_block_t end = c->blkno + c->num_blocks;
      size_t sz = MIN(endsat, end) - i;

      ASSERT(sz > 0);

      /* never written? */
      if (is_invalid_version(v)) {
         void *pd = vd->parentDisk;

         if (pd != NULL) {
            ASSERT(pd != vd);
            printf("forwarding read %ld+%ld to parent\n", i, sz);
            Async_Token *childToken = Async_PrepareOneIO(c->ioh, NULL);
            status = LogFS_VDiskRead(pd, childToken, buf, i, sz, c->flags);
            ASSERT(status == VMK_OK);
         } else {
            /* We may need a parent, but not actually have one. In that case,
             * the read should fail. */
            if (LogFS_HashIsValid(vd->parentBaseId)) {
               status = VMK_READ_ERROR;
            }
            /* Otherwise, just zero the buffer */
            else {
               memset(buf, 0, BLKSIZE * sz);
               status = VMK_OK;
            }
         }
      }

      /* data for these blocks is present in the log. */
      else {

         int relPos = c->blkno - range.from;
         ASSERT(range.from <= c->blkno);

         Async_Token *childToken = Async_PrepareOneIO(c->ioh, NULL);

         LogFS_Log *sublog = LogFS_MetaLogGetLog(log, v.v.segment);
         log_offset_t position = ( relPos + v.v.blk_offset ) * BLKSIZE;

         log_offset_t offset = 
            LogFS_VDiskGetAbsoluteDiskPosition(log, sublog, position);

         LogFS_MetaLogPutLog(log,sublog);

         SG_Array sgArr;
         SG_SingletonSGArray(&sgArr, offset, (VA) buf, sz*BLKSIZE, SG_VIRT_ADDR);
         FDS_Handle *fdsHandleArray[1];
         fdsHandleArray[0] = log->device->fd;

         status = FDS_AsyncIO(fdsHandleArray, &sgArr, FS_READ_OP, childToken);
         ASSERT(status == VMK_OK);

      }

      if (sz < c->num_blocks) {
         c->blkno += sz;
         c->num_blocks -= sz;
         c->buf += sz * BLKSIZE;

         status = LogFS_BTreeRangeMapAsyncLookup(LogFS_VDiskGetVersionsMap(vd),
                                                 c->blkno,
                                                 LogFS_VDiskProcessLookups, c,
                                                 c->flags, &range, &endsat);

         /* If the lookup gets satisfied immediately we can use the result
          * right away rather than do the processing in a callback. This
          * saves us doing tail recursion in the common case. */

         if (status == VMK_EXISTS) {
            continue;
         }
      }

      else {
         Async_EndSplitIO(c->ioh, VMK_OK, FALSE);
         free(c);
      }

      break;
   }

}

// We are the primary read it from the disk
VMK_ReturnStatus
LogFS_VDiskContinueRead(LogFS_VDisk *vd,
                        Async_Token * token,
                        const char *buf,
                        log_block_t blkno, size_t num_blocks, int flags)
{
   VMK_ReturnStatus status = VMK_OK;

   LogFS_BTreeRangeMap *bt;

   bt = LogFS_VDiskGetVersionsMap(vd);

   LogFS_VDiskLookupContext *c = malloc(sizeof(LogFS_VDiskLookupContext));
   c->end = blkno + num_blocks;
   c->flags = flags;

   /* we need to split the read into two layers, first a sequence of reads of
    * all involved log entry heads, and then the sequence of reads of the
    * actual data from the disk. When the latter sequence is completely done,
    * we can signal completion to the user */

   Async_IOHandle *ioh;
   Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);
   c->ioh = ioh;

   c->vd = vd;
   c->blkno = blkno;
   c->num_blocks = num_blocks;
   c->buf = (char *)buf;
   c->depth = 0;

   status = LogFS_BTreeRangeMapAsyncLookup(bt, blkno,
         LogFS_VDiskProcessLookups, c,
         c->flags, NULL, NULL);

   ASSERT(status == VMK_OK);
   return status;
}


VMK_ReturnStatus LogFS_VDiskRead(LogFS_VDisk *vd,
                                 Async_Token * token,
                                 char *buf, log_block_t blkno,
                                 size_t num_blocks, int flags)
{
   VMK_ReturnStatus status = VMK_OK;

   //zprintf("read %" FMT64 "d+%lu\n", blkno, num_blocks);

   SP_Lock(&vd->lock);

   if (vd->isImmutable || LogFS_VDiskIsWritable(vd)) {
      /* We cannot hold vd->lock during normal read, as it may recurse into a
       * parent vd, and cause a lock rank violation. On the other hand we
       * don't want anyone stealing the secret write token, so we need to 
       * bump the refcount before releasing the lock */

      LogFS_VDiskRef(vd);
      SP_Unlock(&vd->lock);

      status =
          LogFS_VDiskContinueRead(vd, token, buf, blkno, num_blocks, flags);

      LogFS_VDiskDeref(vd);
      
      /* return here because we already unlocked */
      return status;
   } else {
      if ((flags & FS_SKIPZERO) == 0) {
         Hash inv;

         LogFS_HashClear(&inv);
         Hash id = LogFS_HashIsValid(vd->secretView) ? vd->parent : inv;

         status = LogFS_HttpClientRequest(vd->disk, id,
                                          token, buf, blkno, num_blocks,
                                          flags & FS_READ_OP);
      } else
         status = VMK_FAILURE;
   }

   SP_Unlock(&vd->lock);

   return status;
}

Bool LogFS_VDiskIsWritable(LogFS_VDisk *vd)
{
   return (LogFS_HashIsValid(vd->secret_parent)
         && (!vd->stopRequested || vd->haveReservation));
}

static void
LogFS_VDiskCommonInit(LogFS_VDisk *vd,
                      LogFS_VDisk *parentDisk, Hash disk, LogFS_MetaLog *log)
{
   memset(vd, 0, sizeof(*vd));

   SP_InitLock("vdisklock", &vd->lock, SP_RANK_VDISK);

   vd->disk = disk;
   vd->log = log;

   /* A list of remote parties to relay writes to */
   List_Init(&vd->remoteLogs);

   LogFS_HashClear(&vd->secretView);

   List_Init(&vd->closeWaiters);
   List_Init(&vd->tokenWaiters);

   /* the VDisk starts out read-only, but can be upgraded to writable
    * if secret_parent is set correctly */
   LogFS_HashClear(&vd->secret_parent);
   vd->parent = disk;

   if (parentDisk != NULL) {
      vd->parentDisk = parentDisk;
      vd->parentBaseId = LogFS_VDiskGetBaseId(parentDisk);
   } else
      vd->parentDisk = NULL;

   Atomic_Write(&vd->refCount, 0);
   vd->stopRequested = FALSE;
   vd->haveReservation = FALSE;
   vd->isImmutable = FALSE;
   vd->lsn = 0;
}

void LogFS_VDiskInit(LogFS_VDisk *vd, LogFS_VDisk *parentDisk, Hash diskId,
                     LogFS_MetaLog *log)
{
   LogFS_VDiskCommonInit(vd, parentDisk, diskId, log);
}

void LogFS_VDiskCleanup(LogFS_VDisk *vd)
{
   if(vd->bt != NULL) {
      LogFS_BTreeRangeMapCleanup(vd->bt);
      free(vd->bt);
   }
}
