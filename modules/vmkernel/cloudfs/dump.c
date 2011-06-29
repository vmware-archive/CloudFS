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
#include "vmware.h"
#include "vmkStatusToString.h"
#include "list.h"

#include "system.h"
#include "btree.h"
#include "logtypes.h"
#include "logfsHash.h"
#include "logfsDiskLayout.h"
#include "logfsCheckPoint.h"

#define _FILE_OFFSET_BITS 64
#include <unistd.h>

typedef struct {
   Hash diskId;
   Hash currentId;

   List_Links next;
} DiskInfo;

List_Links diskList;

char *nodes;
int refcount;

static const node_t *get_node_mem(btree_t *t, disk_block_t block, void *context)
{
   return (const node_t *)(&nodes[block * TREE_BLOCK_SIZE]);
}

static node_t *edit_node_mem(btree_t *t, disk_block_t block, const node_t *p,
                             void *context)
{
   return (node_t *)get_node_mem(t, block, context);
}

static void put_node_mem(btree_t *t, const node_t *n, void *context)
{
}

static disk_block_t alloc_node_mem(btree_t *t, void *context)
{
   disk_block_t r = ++(t->num_nodes);
   return r;
}

static int compareHeads(void *a, void *b)
{
   struct log_head *headA = a;
   struct log_head *headB = b;
   return memcmp(headA->id, headB->id, SHA1_DIGEST_SIZE);
}

void free_node_mem(btree_t *t, const node_t *n)
{
}

static inline int LogFS_PagedRangeMapVerifyChecksum(const node_t *n)
{
   const int hdr = 20 + sizeof(uint64_t);

   unsigned char sum[SHA1_DIGEST_SIZE];
   const uint8_t *nn = (const uint8_t *)n;
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, TREE_BLOCK_SIZE - hdr, nn + hdr);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, sum);

   Hash a, b;
   LogFS_HashSetRaw(&a, sum);
   LogFS_HashSetRaw(&b, n->chk);

   if (!LogFS_HashEquals(a, b)) {
      printf("BAD SUM %s != %s\n", LogFS_HashShow(&a), LogFS_HashShow(&b));
      //exit(1);
   }
   return (memcmp(sum, n->chk, SHA1_DIGEST_SIZE) == 0);
}

disk_block_t recover_subtree(btree_t *t, disk_block_t disk_node, int generation,
                             int depth)
{
   const node_t *n = get_node(t, disk_node, NULL);
   int i;

   for (i = 0; i < depth; i++)
      printf(" ");
   printf("recover node %d, generation %llu\n", disk_node, n->generation);
   ASSERT(n->nodeIdx == disk_node);
   LogFS_PagedRangeMapVerifyChecksum(n);
   while (n->generation > generation) {
      printf("generation in node %llu\n", n->generation);
      printf("rolling back %d to %d\n", disk_node, n->replacesIdx);
      disk_node = n->replacesIdx;
      put_node(t, n, NULL);
      n = get_node(t, disk_node, NULL);
   }

   if (!n->leaf)
      for (i = 0; i < n->num_elems + 1; i++) {
         ((node_t *)n)->children[i] =
             recover_subtree(t, n->children[i], generation, depth + 1);
      }

   put_node(t, n, NULL);
   return disk_node;
}

void recover(btree_t *t)
{

   const btree_info_t *info = get_info(t, NULL);
   printf("info at %p\n", info);
   printf("generation %llu\n", info->generation);
   printf("root is %d\n", info->root);

#if 0
   const node_t *root = get_node(t, info->root, NULL);
   Hash chk = LogFS_HashFromRaw(root->chk);
   printf("chk %s\n", LogFS_HashShow(&chk));
#endif

   ((btree_info_t *)info)->root =
       recover_subtree(t, info->root, info->generation, 0);
}

struct range {
   /* key */
   log_block_t to;

   /* value */
   int length:31;
   unsigned int pristine:1;
   log_id_t version;

} __attribute__ ((__packed__));

LogFS_DiskLayout layout;
static inline log_offset_t _cursor(log_segment_id_t segment,
                                   log_offset_t offset)
{
   return LogFS_DiskLayoutGetOffset(&layout, LogFS_LogSegmentsSection) +
       segment * LOG_MAX_SEGMENT_SIZE + offset;
}

int main(int argc, char **argv)
{
   int f;
   List_Init(&diskList);

   assert(argc == 2);

   f = open(argv[1], O_RDONLY);
   assert(f >= 0);

   printf("size %d\n", sizeof(layout));
   int r = pread(f, &layout, sizeof(layout), 0);
   printf("r %d\n", r);

   printf("magic %s\n", layout.magic);

   nodes = malloc(TREE_BLOCK_SIZE * 1024);
   assert(nodes);
   r = pread(f, nodes, TREE_BLOCK_SIZE * 1024,
             LogFS_DiskLayoutGetOffset(&layout, LogFS_BTreeSection));
   assert(r >= 0);

   btree_t tree;
   btree_callbacks_t callbacks = {
      .cmp = compareHeads,
      .alloc_node = alloc_node_mem,
      .edit_node = edit_node_mem,
      .get_node = get_node_mem,
      .put_node = put_node_mem,
      .free_node = free_node_mem,
   };

   tree_reopen(&tree, 1, &callbacks, TREE_BLOCK_SIZE, NULL, NULL);
   recover(&tree);

   //print_tree(&tree,stdout);

   btree_iter_t it;
   tree_result_t result = tree_begin(&tree, &it, NULL);

   while (result != tree_result_end) {
      SuperTreeElement e;
      //struct range e;
      tree_iter_read(&e, &it, NULL);

      struct log_head *head = (struct log_head *)e.value.firstHead;

      btree_t subTree;
      tree_reopen(&subTree, e.value.info_block, &callbacks, TREE_BLOCK_SIZE,
                  NULL, NULL);
      recover(&subTree);

      DiskInfo *info = malloc(sizeof(DiskInfo));

      LogFS_HashSetRaw(&info->diskId, head->id);
      LogFS_HashSetRaw(&info->currentId, get_info(&subTree, NULL)->id);

      Hash entropy;
      LogFS_HashSetRaw(&entropy, get_info(&subTree, NULL)->entropy);
      List_Insert(&info->next, LIST_ATREAR(&diskList));
      Hash parent;
      LogFS_HashSetRaw(&parent, head->parent);

      printf("%s : parent %s : %u, id %s entropy %s\n",
             LogFS_HashShow(&info->diskId),
             LogFS_HashShow2(LogFS_HashApply(parent))

             , e.value.info_block,
             LogFS_HashShow(&info->currentId), LogFS_HashShow(&entropy));

      result = tree_iter_inc(&it, NULL);
   }

   LogFS_CheckPoint *cp = malloc(sizeof(LogFS_CheckPoint));

   int i;
   for (i = 0; i < 2; i++) {
      r = pread(f, cp, sizeof(LogFS_CheckPoint),
                LogFS_DiskLayoutGetOffset(&layout,
                                          LogFS_CheckPointASection + i));
      Hash chk = LogFS_HashFromRaw(cp->checksum);
      Hash chk2 = LogFS_HashChecksum((char *)cp + SHA1_DIGEST_SIZE,
                                     sizeof(LogFS_CheckPoint) -
                                     SHA1_DIGEST_SIZE);
      printf("checksum %s vs %s\n", LogFS_HashShow(&chk),
             LogFS_HashShow(&chk2));
   }

   log_id_t logEnd = cp->logEnd;
   log_segment_id_t segment = logEnd.v.segment;

   log_offset_t e;
   char *entry = malloc(LOG_HEAD_SIZE);
   struct log_head *head = (struct log_head *)entry;

   printf("segment %lld\n", segment);

   for (e = logEnd.v.blk_offset * BLKSIZE;; e += log_entry_size(head)) {
      int r = pread(f, head, LOG_HEAD_SIZE, _cursor(segment, e));
      if (r <= 0)
         break;

      if (is_block_zero(entry))
         break;

      if (head->tag == log_pointer_type && head->direction == log_next_ptr) {
         segment = head->target.v.segment;
         e = head->target.v.blk_offset * BLKSIZE;

         continue;
      }

      if (head->tag == log_entry_type) {
         Bool applied = FALSE;
         LogFS_Hash id, parent;
         LogFS_HashSetRaw(&id, head->id);
         LogFS_HashSetRaw(&parent, head->parent);

         List_Links *curr;
         LIST_FORALL(&diskList, curr) {
            DiskInfo *info = List_Entry(curr, DiskInfo, next);
            if (LogFS_HashEquals(info->currentId, LogFS_HashApply(parent))) {
               printf("apply %lld + %d\n", head->update.blkno,
                      head->update.num_blocks);
               info->currentId = id;
               applied = TRUE;
               break;
            }
         }

         if (!applied)
            break;
      }
   }

   return 0;
}
