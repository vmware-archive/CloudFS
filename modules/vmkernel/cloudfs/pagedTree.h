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
#ifndef  _PAGEDTREE_H_
#define  _PAGEDTREE_H_


#define LOGLINES 11 /* 2**1 * 32kB == 64MB of cache */
#define LINES (1<<LOGLINES)

#define INNER_NODES (LINES-1)

typedef struct NodeInfo {
   disk_block_t nodeIdx;
   const node_t *node;
   node_t *incoming;
   Atomic_uint32 refCount;

   Bool freed;

   List_Links dirtyList;
   List_Links waiters;
} NodeInfo;

typedef struct {
   List_Links dirtyNodesList;
   List_Links nextInfo;
   SP_SpinLock dirtyNodesListLock;
   btree_t *tree;
   struct LogFS_MetaLog *ml;
   struct LogFS_PagedTreeCache* cache;
} TreeInfo;


/* A sorted dict mapping nodeIdx -> NodeInfo* */

typedef struct LogFS_PagedTreeCache {
   SP_SpinLock lock;

   /* the bits array is a binary tree with LINES-1 inner nodes */
   char bits[INNER_NODES];
   NodeInfo *lines[LINES];
   int nodeMap[LINES]; /* dict mapping NodeInfo* to cache lines */

} LogFS_PagedTreeCache ;

VMK_ReturnStatus LogFS_PagedTreeDiskReopen(struct LogFS_MetaLog *ml, disk_block_t superTreeRoot);

void LogFS_PagedTreeCleanupGlobalState(struct LogFS_MetaLog *ml);

struct btree;
struct LogFS_MetaLog;

struct btree *LogFS_PagedTreeReOpen(struct LogFS_MetaLog *ml, disk_block_t root);
struct btree *LogFS_PagedTreeCreate(struct LogFS_MetaLog *ml);

void LogFS_PagedTreeCleanup(btree_t* t);

VMK_ReturnStatus LogFS_PagedTreeSync(btree_t *t, struct LogFS_MetaLog *ml, List_Links *movedNodes);

VMK_ReturnStatus LogFS_PagedTreeRescan(struct LogFS_MetaLog *);

static inline void LogFS_PagedTreeChecksum(node_t *n)
{
   const int hdr = 20 + sizeof(void *);

   const uint8_t *nn = (const uint8_t *)n;
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, TREE_BLOCK_SIZE - hdr, nn + hdr);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, n->chk);
}

static inline int LogFS_PagedTreeVerifyChecksum(node_t *n)
{
   const int hdr = 20 + sizeof(void *);

   unsigned char sum[SHA1_DIGEST_SIZE];
   const uint8_t *nn = (const uint8_t *)n;
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, TREE_BLOCK_SIZE - hdr, nn + hdr);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, sum);
   return (memcmp(sum, n->chk, SHA1_DIGEST_SIZE) == 0);
}

#endif //_PAGEDTREE_H_
