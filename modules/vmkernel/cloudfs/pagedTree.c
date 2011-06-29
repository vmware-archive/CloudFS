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
#include "logtypes.h"
#include "logfsConstants.h"
#include "globals.h"
#include "logfsIO.h"
#include "pagedTree.h"
#include "vDisk.h"
#include "vDiskMap.h"

SP_SpinLock nodesLock;   /* protects allocation bitmap */
uint8 nodesBitmap[TREE_MAX_BLOCKS / 8 + 1];
static List_Links treeList;

static LogFS_PagedTreeCache* theCache;

static Bool pagedTreeShutdownInProgress = FALSE;

extern int compare_u64(const void *a, const void *b);

/* Forward declarations */

static int nodeMapCmp(const void *va, const void *vb)
{
   int ia = *((int*) va);
   int ib = *((int*) vb);

   NodeInfo *a = theCache->lines[ia];
   NodeInfo *b = theCache->lines[ib];

   if(a==NULL && b==NULL) return 0;
   else if(b==NULL) return -1;
   else if(a==NULL) return 1;

   if(a->nodeIdx < b->nodeIdx) return -1; 
   else if(a->nodeIdx==b->nodeIdx) return 0;
   else return 1;
}

static inline NodeInfo *refInfo(NodeInfo *info)
{
   Atomic_Inc(&info->refCount);
   return info;
}

static inline void releaseInfo(NodeInfo *info)
{
   if (Atomic_FetchAndDec(&info->refCount) == 1) {
      free((node_t *)info->node);
      free(info);
   }
}

void LogFS_PagedTreeCacheNode(btree_t *t, NodeInfo *info)
{
   int i;
   int child;
   int line;

   TreeInfo *treeInfo = t->user_data;
   LogFS_PagedTreeCache* cache = treeInfo->cache;

   /* select a cache line for replacement using pseudo-LRU */

   for (i = 0, child = 0; i < LOGLINES; i++) {
      int parent = child;
      child = 2 * parent + 1 + cache->bits[parent];
      cache->bits[parent] ^= 1;
   }
   line = child - INNER_NODES;

   NodeInfo *replacedInfo = cache->lines[line];

   if (replacedInfo != NULL) {
      zprintf("replaced line %u %u\n",line,replacedInfo->nodeIdx);
      releaseInfo(replacedInfo);
   }

   cache->lines[line] = refInfo(info);
   qsort(cache->nodeMap, LINES, sizeof(int), nodeMapCmp);
}

/* Search for the cache line with the node numbered nodeIdx 
 * using binary search */

static inline int LogFS_PagedTreeFindLine(LogFS_PagedTreeCache* cache,
      disk_block_t nodeIdx)
{

   int *nodeMap = cache->nodeMap;
   NodeInfo *info;

   int len = LINES;
   int first = 0;
   int half, middle;

   while (len > 0) {
      half = len >> 1;
      middle = first + half;

      info = cache->lines[ nodeMap[middle] ];

      if (info && info->nodeIdx < nodeIdx) {
         first = middle;
         ++first;
         len = len - half - 1;
      } else
         len = half;
   }

   info = cache->lines[ nodeMap[first] ];

   if (info && info->nodeIdx == nodeIdx)
      return first;
   else
      return -1;
}

/* Lookup a node in the cache, and update the pseudo-LRU
 * pointer tree to make eviction less likely */

static inline NodeInfo *createNodeInfo(btree_t *t, void *context, disk_block_t block)
{
   NodeInfo *info = malloc(sizeof(NodeInfo));
   ASSERT(info);

   info->node = NULL;
   info->nodeIdx = block;
   info->freed = FALSE;
   Atomic_Write(&info->refCount,1);

   List_InitElement(&info->dirtyList);
   List_Init(&info->waiters);
   return info;
}

typedef struct {
   LogFS_PagedTreeCache* cache;
   NodeInfo *info;
} ReadInfo;

void LogFS_RangeMapGotNode(Async_Token *token, void *data)
{
   ReadInfo *c = data;
   LogFS_PagedTreeCache *cache = c->cache;
   NodeInfo *info = c->info;

   node_t *incoming = (node_t *)info->incoming;

   if (!LogFS_PagedTreeVerifyChecksum(incoming)) {
      Panic("bad checksum node %u", info->nodeIdx);
   }


   /* Wake up threads waiting for this node */
   SP_Lock(&cache->lock);
   incoming->user_data = (uint64)info;
   info->incoming = NULL;
   info->node = incoming;
   CpuSched_Wakeup(&info->waiters);
   SP_Unlock(&cache->lock);

   releaseInfo(info);

   Async_TokenCallback(token);
}

static const node_t *get_node_disk(btree_t *t, disk_block_t block,
                                   void *context)
{
   VMK_ReturnStatus status;
   int line;
   int idx;

   if (block >= TREE_MAX_BLOCKS) {
      Panic("out of room node %u",block);
   }

   if (!BitTest(nodesBitmap,block)) {
      Panic("trying to get unalloced node %u\n",block);
   }

   NodeInfo *info;
   TreeInfo *treeInfo = t->user_data;
   LogFS_PagedTreeCache *cache = treeInfo->cache;

   for(;;) {

      /* First we linearly scan for the node with the wanted nodeIdx, and then we
       * update the 'bits' binary search tree so that all pointers in the path to
       * the found line point AWAY from this one, making it an unlikely candidate
       * for eviction */

      SP_Lock(&cache->lock);

      idx = LogFS_PagedTreeFindLine(cache,block);
      if (idx >= 0) {
         line = cache->nodeMap[idx];
         info = cache->lines[line];

         /* Flip the bits in the reverse path from leaf to root */

         int child;
         for (child = line + INNER_NODES; child != 0;) {
            int parent = (child - 1) / 2;
            cache->bits[parent] = (child == (2 * parent + 1));  /* inverse test to save xor */
            child = parent;
         }
         SP_Unlock(&cache->lock);
         goto found;
      }

      /* The node is not in the cache, but it may still be around but
       * scheduled for eviction */

      List_Links *curr;
      SP_Lock(&treeInfo->dirtyNodesListLock);

      LIST_FORALL(&treeInfo->dirtyNodesList, curr) {
         info = List_Entry(curr, NodeInfo, dirtyList);
         if (info->nodeIdx == block) {

            LogFS_PagedTreeCacheNode(t, info);
            SP_Unlock(&treeInfo->dirtyNodesListLock);

            SP_Unlock(&cache->lock);
            goto found;
         }
      }
      SP_Unlock(&treeInfo->dirtyNodesListLock);

      /* The node is not in the cache, create a NodeInfo for storing in-memory
       * extended info such as the refcount for it, and cache it to prevent
       * other from trigger fetching of the same node. */

      info = createNodeInfo(t, context, block);
      info->node = NULL;
      LogFS_PagedTreeCacheNode(t, info);

      SP_Unlock(&cache->lock);

      /* Read the node from disk */

      Async_Token *token = Async_AllocToken(0);

      TreeInfo* treeInfo = t->user_data;
      LogFS_MetaLog *ml = treeInfo->ml;

      info->incoming = malloc(TREE_BLOCK_SIZE);

      ReadInfo *c = Async_PushCallbackFrame(token, LogFS_RangeMapGotNode, 
            sizeof(ReadInfo));
      c->cache = cache;
      c->info = info;

      status = LogFS_DeviceRead(ml->device, token, info->incoming, TREE_BLOCK_SIZE,
            info->nodeIdx * TREE_BLOCK_SIZE, LogFS_BTreeSection);
      ASSERT(status==VMK_OK);

      goto found;
   }

found:
   /* Is caller able to wait for node data getting paged in? */

   if(info->node!=NULL) {

      return refInfo(info)->node;

   } else {

      if(context==NULL) { /* can sleep */

         for(;;) { /* wait for node data becoming available */
            SP_Lock(&cache->lock);
            if(info->node==NULL) {
               status = CpuSched_Wait(&info->waiters, CPUSCHED_WAIT_SCSI, &cache->lock);
            } else {
               SP_Unlock(&cache->lock);
               return refInfo(info)->node;
            }
         }
      } else {
         return NULL;
      }

   }
}


static inline void rememberDirtyNode(btree_t *t, NodeInfo *info)
{
   TreeInfo *treeInfo = t->user_data;
   if (treeInfo!=NULL) {
      SP_Lock(&treeInfo->dirtyNodesListLock);

      if (List_IsUnlinkedElement(&info->dirtyList)) {
         refInfo(info);
         List_Insert(&info->dirtyList, LIST_ATREAR(&treeInfo->dirtyNodesList));
      }
      SP_Unlock(&treeInfo->dirtyNodesListLock);
   }
   else NOT_REACHED();
}

static node_t *edit_node_disk(btree_t *t, disk_block_t block, const node_t *p,
                              void *context)
{
   const node_t *r = get_node_disk(t, block, context);
   if (r == NULL)
      return NULL;

   NodeInfo *info = (NodeInfo *)r->user_data;

   rememberDirtyNode(t, info);

   return (node_t *)r;
}

static void put_node_disk(btree_t *t, const node_t *n, void *context)
{
   NodeInfo *info = (NodeInfo *)n->user_data;
   releaseInfo(info);
}

static inline disk_block_t alloc_phys_node(void)
{
   int i;

   SP_Lock(&nodesLock);

   /* note that we are starting from 1 */
   for (i = 1; i < TREE_MAX_BLOCKS; i++) {
      if (!BitTest(nodesBitmap,i)) {
         BitSet(nodesBitmap,i);
         break;
      }
   }
   if (i == TREE_MAX_BLOCKS)
      Panic("Out of tree nodes!\n");

   SP_Unlock(&nodesLock);

   return i;
}

static disk_block_t allocDiskNode(btree_t *t, void *context)
{
   NodeInfo *info = createNodeInfo(t, context, alloc_phys_node());

   node_t *n = malloc(t->real_node_size);
   memset(n, 0, sizeof(node_t));
   n->user_data = (uint64)info;
   info->node = n;

   rememberDirtyNode(t, info);
   disk_block_t r = info->nodeIdx;
   releaseInfo(info);

   return r;
}

static void free_node_disk(btree_t *t, const node_t *n)
{
   NodeInfo *info = (NodeInfo *)n->user_data;
   zprintf("WARNING: Should free node %p %u\n", info, info->nodeIdx);
   //info->freed = TRUE;
   //rememberDirtyNode(t, info);
}


static inline disk_block_t
remapBlock(disk_block_t in, disk_block_t * map)
{
   //if(map[in] != tree_null_block) zprintf("%u becomes %u\n",in,map[in]);
   return (map[in] != tree_null_block) ? map[in] : in;
}

static inline void
remapChildren(node_t *n, disk_block_t * map)
{
   if (!n->leaf) {
      int i;
      for (i = 0; i < n->num_elems + 1; i++) {
         n->children[i] = remapBlock(n->children[i], map);
      }
   }
}

#if 0
void LogFS_PagedTreeReIndexNode(
      LogFS_PagedTreeCache* cache,
      NodeInfo *oldInfo, NodeInfo *newInfo)
{
   int i;
   int step;
   int end;
   int newIdx = newInfo->nodeIdx;

   /* First find the location of the NodeInfo we are replacing
      (oldInfo may be NULL) */
   NodeMapItem *nodeMap = cache->nodeMap;

   for (i = 0; i < LINES; i++)
   {
      if ( cache->lines [nodeMap[i].line] == oldInfo) {
         break;
      }
   }

   /* Now i points at the element that needs reindexing. We want to maintain
    * that the table is sorted on info->nodeIdx, so find out which direction to
    * push array elements by comparing against current nodeIdx value we are
    * replacing. */

   if (oldInfo!=NULL && oldInfo->nodeIdx < newIdx) {
      step = 1;
      end = LINES - 1;
   } else {
      step = -1;
      end = 0;
   }

   int tmp = nodeMap[i].line;
   
   for (;; i += step) {

      NodeInfo *info = nodeMap[i+step].info;
      int idx = (info==NULL) ? TREE_MAX_BLOCKS : info->nodeIdx;

      if (i == end || (step > 0 && idx > newIdx) || (step < 0 && idx < newIdx)) {
         nodeMap[i].info = newInfo;
         nodeMap[i].line = tmp;
         break;
      } else
         nodeMap[i] = nodeMap[i + step];
   }
}
#endif

static inline VMK_ReturnStatus
LogFS_PagedTreeWriteNode(LogFS_MetaLog *ml,
                             Async_Token * token,
                             const node_t *n, disk_block_t nodePos)
{
   size_t blockSize = ml->superTree->real_node_size;

   return LogFS_DeviceWriteSimple(ml->device, token, (void *)n, blockSize,
                            nodePos * blockSize, LogFS_BTreeSection);
}

/*
 *-----------------------------------------------------------------------------
 *
 * LogFS_PagedTreeSync --
 *
 *      Flush B-tree nodes to disk.
 *
 * Results:
 *
 *      Entries will be added to the movedNodes list parameter, to allow 
 *      caller to update its persistent allocation bitmap.
 *
 * Side effects:
 *
 *      B-tree nodes will get remapped to new locations and written to disk.
 *      References from parent tree nodes will get remapped, and those nodes
 *      will also be written to disk.
 *
 *      t->root gets updated to reflect new location of root node. Caller must
 *      persist the new root location to effectively commit the update
 *      transaction.
 *
 *-----------------------------------------------------------------------------
 */

VMK_ReturnStatus LogFS_PagedTreeSync(btree_t *t, LogFS_MetaLog *ml, List_Links *movedNodes)
{
   VMK_ReturnStatus status;
   int numNodes = 0;
   List_Links *curr, *next;

   TreeInfo *treeInfo = t->user_data;
   LogFS_PagedTreeCache *cache = treeInfo->cache;

   disk_block_t *map = malloc(TREE_MAX_BLOCKS* sizeof(disk_block_t));
   memset(map, 0, TREE_MAX_BLOCKS* sizeof(disk_block_t));

   Async_Token *token = Async_AllocToken(0);
   ASSERT(token);
   Async_IOHandle *ioh;

   /* Atomically empty and copy the dirtyNodesList. */

   List_Links dirtyNodesList;
   List_Init(&dirtyNodesList);

   SP_Lock(&treeInfo->dirtyNodesListLock);
   List_Append(&dirtyNodesList,&treeInfo->dirtyNodesList);
   SP_Unlock(&treeInfo->dirtyNodesListLock);

   /* We first have to relocate all the dirty nodes to new addresses.
    * The block allocator works simply by scanning a free nodes bitmap
    * for empty spots, so the nodes will be written in order, but with
    * some degree of fragmentation.  */

   LIST_FORALL_SAFE(&dirtyNodesList, curr, next) {
      NodeInfo *info = List_Entry(curr, NodeInfo, dirtyList);

      MovedNode *mn = malloc(sizeof(MovedNode));

      if (!BitTest(nodesBitmap,info->nodeIdx)) {
         Panic("trying to CoW unalloced node %u\n",info->nodeIdx);
      }

      if(info->freed) {

         mn->from = info->nodeIdx;
         mn->to = tree_null_block;
         zprintf("free node %u\n",mn->from);

      } else {

         /* If the node is still referenced, and has been written to disk
          * previously, copy it to a new location and record the old so that
          * the disk block may be freed in the next checkpoint. */

         mn->from = info->nodeIdx;
         map[mn->from] = mn->to = alloc_phys_node();
      }

      List_Insert(&mn->list, LIST_ATREAR(movedNodes));
      ++numNodes;
   }

   /* Now that we know the new locations of the nodes to be written,
    * we can fix up inter-node references so that nodes are referenced
    * at the new locations. After fixing up a node, we queue for write.
    * Ideally we should use an SGArray instead of potentially lots of
    * split IOs here.
    */

   Async_StartSplitIO(token, Async_DefaultChildDoneFn, 0, &ioh);

   LIST_FORALL(&dirtyNodesList, curr) {

      NodeInfo *info = List_Entry(curr, NodeInfo, dirtyList);
      node_t *n = (node_t *)info->node;

      Async_Token *t1 = Async_PrepareOneIO(ioh, NULL);

      /* Finally write out the node after remapping its children */

      if(!n->leaf) {
         remapChildren(n, map);
      }
      LogFS_PagedTreeChecksum(n);

      status = LogFS_PagedTreeWriteNode(ml, t1, n, remapBlock(info->nodeIdx,map) );
      ASSERT(status == VMK_OK);

   }
   Async_EndSplitIO(ioh, VMK_OK, FALSE);


   Async_WaitForIO(token);
   Async_ReleaseToken(token);

   /* The final step is to reindex the cache with the new node locations.  We
    * need to do this with the cache lock held, because the cache nodeMap will
    * be temporarily unsorted. */

   SP_Lock(&cache->lock);

   LIST_FORALL(&dirtyNodesList, curr) {
      NodeInfo *info = List_Entry(curr, NodeInfo, dirtyList);
      info->nodeIdx = remapBlock(info->nodeIdx,map);
   }

   /* Keep nodeMap sorted to allow binary search lookups. XXX do this lazily 
    * or use a search tree instead. */
   qsort(cache->nodeMap, LINES, sizeof(int), nodeMapCmp);

   SP_Unlock(&cache->lock);

   /* Drop references to the no-longer dirty nodes. This
    * will cause evicted nodes to get freed in memory. */

   LIST_FORALL_SAFE(&dirtyNodesList, curr, next) {
      NodeInfo *info = List_Entry(curr, NodeInfo, dirtyList);
      List_Remove(curr);
      List_InitElement(curr);
      releaseInfo(info);
   }


   /* Finally commit the updates by updating the tree root pointer */
   t->root = remapBlock(t->root, map);

#if 0
   if (numNodes > 0) {
      zprintf("wrote %u, new root at %u\n", numNodes, t->root);
   }
#endif


   free(map);
   return VMK_OK;
}

void LogFS_PagedTreeFillinCallbacks(btree_callbacks_t *callbacks)
{
   callbacks->alloc_node = allocDiskNode;
   callbacks->free_node = free_node_disk;
   callbacks->edit_node = edit_node_disk;
   callbacks->get_node = get_node_disk;
   callbacks->put_node = put_node_disk;
}

static TreeInfo *LogFS_PagedTreeCreateTreeInfo(btree_t *t,
      LogFS_MetaLog *ml)
{
   TreeInfo *treeInfo = malloc(sizeof(TreeInfo));
   memset(treeInfo, 0, sizeof(TreeInfo));

   SP_InitLock("dirtyNodesList", &treeInfo->dirtyNodesListLock,
               SP_RANK_RANGEMAPQUEUES);
   List_Init(&treeInfo->dirtyNodesList);
   treeInfo->ml = ml;
   treeInfo->cache = theCache;
   List_Insert(&treeInfo->nextInfo, LIST_ATREAR(&treeList));
   return treeInfo;
}

void LogFS_PagedTreeDestroyTreeInfo(TreeInfo *treeInfo)
{
   SP_CleanupLock(&treeInfo->dirtyNodesListLock);
   free(treeInfo);
}

btree_t *LogFS_PagedTreeReOpen(LogFS_MetaLog *ml, disk_block_t root)
{
   btree_callbacks_t callbacks;
   LogFS_PagedTreeFillinCallbacks(&callbacks);
   callbacks.cmp = compare_u64;
   btree_t *t = malloc(sizeof(btree_t));
   TreeInfo *treeInfo = LogFS_PagedTreeCreateTreeInfo(t,ml);

   tree_reopen(t, &callbacks,root,
               RANGEMAP_KEY_SIZE, RANGEMAP_VALUE_SIZE, TREE_BLOCK_SIZE, 
               treeInfo, NULL);

   return t;
}

btree_t *LogFS_PagedTreeCreate(LogFS_MetaLog *ml)
{
   btree_callbacks_t callbacks;
   LogFS_PagedTreeFillinCallbacks(&callbacks);
   callbacks.cmp = compare_u64;
   btree_t *t = malloc(sizeof(btree_t));
   TreeInfo *treeInfo = LogFS_PagedTreeCreateTreeInfo(t,ml);

   zprintf("create new tree\n");

   tree_create(t,&callbacks,
         RANGEMAP_KEY_SIZE, RANGEMAP_VALUE_SIZE, TREE_BLOCK_SIZE, 
         treeInfo, NULL);
   return t;
}

void LogFS_PagedTreeCleanupGlobalState(LogFS_MetaLog *ml)
{
   int i;
   int alive;

   LogFS_PagedTreeCache *cache = theCache;

   pagedTreeShutdownInProgress = TRUE;

   SP_Lock(&cache->lock);
   for (i = 0, alive = 0; i < LINES; i++) {
      NodeInfo *info;
      if ((info = cache->lines[i])) {
         releaseInfo(info);
      }
   }
   SP_Unlock(&cache->lock);

   List_Links *curr, *next;
   LIST_FORALL_SAFE(&treeList, curr, next) {
      TreeInfo *treeInfo = List_Entry(curr, TreeInfo, nextInfo);
      List_Remove(curr);

      LogFS_PagedTreeDestroyTreeInfo(treeInfo);
   }

   free(ml->superTree);
   SP_CleanupLock(&cache->lock);
   SP_CleanupLock(&nodesLock);
   free(cache);
}

extern void LogFS_Syncer(void *data);

void LogFS_PagedTreeInitCache(
      LogFS_PagedTreeCache* cache)
{

}

VMK_ReturnStatus LogFS_PagedTreeDiskReopen(LogFS_MetaLog *ml, disk_block_t superTreeRoot)
{
   int i;
   ASSERT((TREE_MAX_BLOCKS & (sizeof(unsigned long) * 8 - 1)) == 0);
   List_Init(&treeList);

   /* Initialize block cache */

   LogFS_PagedTreeCache* cache = malloc(sizeof(LogFS_PagedTreeCache));
   memset(cache,0,sizeof(LogFS_PagedTreeCache)); 
   for (i = 0; i < LINES; i++) {
      cache->nodeMap[i] = i;
   }
   SP_InitLock("pagedtreemapcache", &cache->lock, SP_RANK_RANGEMAPCACHE);
   SP_InitLock("pagedtreemapalloc", &nodesLock, SP_RANK_RANGEMAPNODES);
   theCache = cache;

   /* Initialize top-level B-tree */

   btree_callbacks_t callbacks;
   LogFS_PagedTreeFillinCallbacks(&callbacks);
   callbacks.cmp = NULL;

   Semaphore_Init("supertree", &ml->superTreeSemaphore, 1, SEMA_RANK_UNRANKED);

   ml->superTree = (btree_t *)malloc(sizeof(btree_t));
   TreeInfo *treeInfo = LogFS_PagedTreeCreateTreeInfo(ml->superTree, ml);

   SuperTreeElement *dummyElement = NULL;

   if(superTreeRoot==tree_null_block) {

      tree_create(ml->superTree, &callbacks,
            sizeof(dummyElement->key),
            sizeof(dummyElement->value), TREE_BLOCK_SIZE, treeInfo, NULL);

      return VMK_OK;

   } else {

      tree_reopen(ml->superTree, &callbacks, 
            superTreeRoot,
            sizeof(dummyElement->key), 
            sizeof(dummyElement->value), TREE_BLOCK_SIZE, treeInfo, NULL);
   }

   /* Rescan top-level B-tree and create in-memory state for VDisks */

   Hash nullId;
   LogFS_HashZero(&nullId);
   btree_iter_t it;
   tree_result_t result = tree_begin(ml->superTree, &it, NULL);

   while (result != tree_result_end) {
      SuperTreeElement e;
      tree_iter_read(&e, &it, NULL);

      zprintf("recover existing tree at %u\n", e.value.root);

      LogFS_VDisk *vd = malloc(sizeof(LogFS_VDisk));
      Hash diskId = LogFS_HashFromRaw(e.key);

      LogFS_VDiskInit(vd, NULL, diskId, ml);
      LogFS_HashSetRaw(&vd->parent, e.value.currentId);
      LogFS_HashSetRaw(&vd->entropy, e.value.entropy);
      vd->lsn = e.value.lsn;
      LogFS_DiskMapInsert(vd);

      result = tree_iter_inc(&it, NULL);
   }

   /* Link VDisks with their parents */

#if 0
   int i;
   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];

#if 0
      Hash parent = vd->parentBaseId;

      if (!LogFS_HashIsNull(parent)) {
         LogFS_VDisk *pd =
             LogFS_DiskMapLookupDiskForVersion(LogFS_HashApply(parent));
         if (pd == NULL)
            pd = LogFS_DiskMapLookupDiskForVersion(parent);

         ASSERT(pd);
         zprintf("%s has parent %s\n",
                 LogFS_HashShow(&vd->disk), LogFS_HashShow(&pd->disk));
         vd->parentDisk = pd;
      }
#endif
      LogFS_MakeDiskDevice(vd);
   }
#endif

   return VMK_OK;
}
