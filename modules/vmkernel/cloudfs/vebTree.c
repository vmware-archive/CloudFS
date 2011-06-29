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
#include "logfsIO.h"
#include "aligned.h"
#include "vebTree.h"
#include "bitops.h"
#include "metaLog.h"

#include "graph.h"

static inline void* CONTEXT(LogFS_VebTree* vt)
{
   return NULL;
}

static inline void* getTreeBase(btree_t* t)
{
   LogFS_VebTree* vt = t->user_data;
   size_t bankSz = (vt->device==NULL) ? vt->treeSz : 0;
	return ((char*)vt->mem)+ bankSz*vt->activeBank;
}

static const node_t* get_node_mem(btree_t* t,disk_block_t block,void* context)
{
   char* base = getTreeBase(t);
	return (const node_t*) (base + block*t->real_node_size);
}

static node_t* edit_node_mem(btree_t* t,disk_block_t block,const node_t* p, void* context)
{
	node_t* n = (node_t*) get_node_mem(t,block,context);
   LogFS_VebTree* vt = t->user_data;
   vt->bankDirty = 1;
	return n;
}

static void put_node_mem(btree_t* t,const node_t* n,void* context)
{
}

static disk_block_t alloc_node_mem(btree_t* t,void* context)
{
   int i;
	void* rawmem = getTreeBase(t);

   for(i=1;i<4096;i++)
   {
      if(!BitTest(rawmem,i)) break;
   }
   BitSet(rawmem,i);
   return i;
}

void free_node_mem(btree_t* t, const node_t* _n)
{
}
static btree_callbacks_t callbacks = {
	.cmp = NULL,
	.alloc_node = alloc_node_mem,
	.edit_node = edit_node_mem,
	.get_node = get_node_mem,
	.put_node = put_node_mem,
	.free_node = free_node_mem,
};

static inline size_t ioSize(LogFS_VebTree* vt)
{
   return vt->treeSz;
   //return 0x10000;//XXX
   //return vt->nodeSz * (1+ vt->counters[vt->activeBank]);
}

void
LogFS_VebTreeSetBank(
	LogFS_VebTree* vt,
	int bank)
{
	if(vt->activeBank != bank)
	{
      if(vt->device)
      {
         if(vt->activeBank>=0 && vt->bankDirty)
         {
            LogFS_DeviceWriteSimple(vt->device, NULL, vt->mem, 
                  ioSize(vt),
                  vt->treeSz*vt->activeBank, LogFS_VebTreeSection);
         }

         LogFS_DeviceRead(vt->device, NULL, vt->mem, 
               ioSize(vt),
               vt->treeSz*bank, LogFS_VebTreeSection);
      }

      vt->t = &vt->tree;
		vt->activeBank = bank;
      vt->bankDirty = 0;

		tree_reopen(vt->t,&callbacks,1,sizeof(uint64_t),0, vt->nodeSz, vt, CONTEXT(vt));
	}
}



void
LogFS_VebTreeInit(
	LogFS_VebTree* vt,
	LogFS_Device* device,
	size_t nodeSz,
	size_t treeSz)
{
   size_t sz;
	vt->device = device;
	vt->nodeSz = nodeSz;
	vt->treeSz = treeSz;

	vt->t = NULL;
	vt->activeBank = -1;

   sz = (device==NULL) ? vt->treeSz*0x100 : vt->treeSz;
   vt->mem = aligned_malloc(sz);
   memset(vt->mem,0,sz);
}

void LogFS_VebTreeCleanup(LogFS_VebTree *vt)
{
   free(vt->mem);
   vt->mem = NULL;
}

void
LogFS_VebTreeFlush( LogFS_VebTree* vt, LogFS_Device* device )
{
   LogFS_DeviceWriteSimple(device, NULL, vt->mem,
         vt->treeSz * 0x100, 0, LogFS_VebTreeSection);
}

void
LogFS_VebTreeClear(
	LogFS_VebTree* vt)
{
	int i;
	for(i=0;i<0x100;i++)
	{
      vt->activeBank = i;

		tree_create(&vt->tree,&callbacks,sizeof(uint64_t),0, vt->nodeSz, vt, CONTEXT(vt));

      if(vt->device)
      {
         LogFS_DeviceWriteSimple(vt->device, NULL, vt->mem,
               ioSize(vt),
               vt->treeSz*vt->activeBank, LogFS_VebTreeSection);
      }
   }
}

static inline uint64_t keyValuePair(uint32_t key, uint32_t value)
{
	return ((uint64_t)key<<32) | value;
}

static inline uint32_t getKey(uint64_t keyValue)
{
   return keyValue >> 32;
}

static inline uint32_t getValue(uint64_t keyValue)
{
   return keyValue & 0xffffffff;
}

static inline uint8_t getBank(uint64_t key)
{
   return key>>32;
}

void LogFS_VebTreeInsert(
	LogFS_VebTree* vt,
   uint8_t bank,
	uint32_t key,
	uint32_t value)
{
	/* Insert the key-value pair so that it can be found in the future */

	uint64_t e = keyValuePair(key,value);
	LogFS_VebTreeSetBank(vt,bank);
	tree_insert(vt->t,(elem_t*)&e,CONTEXT(vt));
}

void LogFS_VebTreeDelete(
	LogFS_VebTree* vt,
   uint8_t bank,
	uint32_t key,
	uint32_t value)
{
	LogFS_VebTreeSetBank(vt, bank);

	uint64_t e  = keyValuePair(key,value);
   tree_delete(vt->t, (elem_t*) &e, CONTEXT(vt));
}


void LogFS_VebTreeMerge(
	LogFS_VebTree* base,
	LogFS_VebTree* overlay,
   Graph *g)
{
	int i;

   const int nHeaps = 256;
   LogFS_BinHeap** heaps;
   heaps = malloc(sizeof(LogFS_BinHeap*) * nHeaps);

   for(i=0;i<nHeaps;i++)
   {
      heaps[i] = malloc(sizeof(LogFS_BinHeap));
      LogFS_BinHeapInit(heaps[i],0x1000);
   }


	for(i=0;i<0x100;i++)
	{
		LogFS_VebTreeSetBank(overlay,i);
      LogFS_VebTreeSetBank(base,i);

		btree_iter_t overlayIt;
		tree_result_t tr = tree_begin(overlay->t,&overlayIt,CONTEXT(overlay));

		while( tr != tree_result_end)
		{

         uint64_t have;
			tree_iter_read(&have,&overlayIt,CONTEXT(overlay));

         //zprintf("key %08x value %08x\n",getKey(have),getValue(have));
         uint64_t lookup = keyValuePair( getKey(have), 0 );

         btree_iter_t baseIt;
         tree_result_t r = tree_lower_bound(base->t,&baseIt,(elem_t*)&lookup,CONTEXT(base));

         while(r==tree_result_found)
         {
            uint64_t found;
            tree_iter_read(&found,&baseIt,CONTEXT(base));

            if(getKey(lookup) == getKey(found))
            {
               //zprintf("%08x%02x\n",getKey(lookup),i);
               uint32_t valueA = getValue(have);
               uint32_t valueB = getValue(found);
               if(valueA != valueB)
               {
                  LogFS_BinHeapAdjustUp(heaps[valueA],valueB,1);
               }
            }
            else break;

            r = tree_iter_inc(&baseIt,CONTEXT(base));
         }

			tr = tree_iter_inc(&overlayIt,CONTEXT(overlay));
		}
	}

   GraphNode **nodes = malloc(0x1000*sizeof(GraphNode*));
   memset(nodes,0,0x1000*sizeof(GraphNode*));

   for(i=0;i<nHeaps;i++)
   {
      LogFS_BinHeap* heap = heaps[i];


      for(;;)
      {
         int value;
         uint32_t friend = LogFS_BinHeapPopMax(heap,&value);
         if(value > 50)
         {
            GraphNode *me = nodes[i];
               
            if (me==NULL) {
               me = malloc(sizeof(GraphNode));
               Graph_InitNode(g,me,i);
               nodes[i] = me;
            }

            GraphNode *other = nodes[friend];

            if(other == NULL) {
               other = malloc(sizeof(GraphNode));
               Graph_InitNode(g,other,friend);
               nodes[friend] = other;
            }

            //zprintf("add edge from %d to %d\n",i,friend);
            Graph_AddEdge(g,me,other,value);

            //LogFS_ObsoletedSegmentsAdd(os, i, value*64);
            //LogFS_ObsoletedSegmentsAdd(os, friend, value*64);

#if 0
            if(value > max)
            {
               *a = i;
               *b = friend;
               max = value;
            }
#endif
         }
         else break;
      }
      LogFS_BinHeapCleanup(heaps[i]);
      free(heap);
   }

   free(nodes);
   free(heaps);

}

void LogFS_VebTreePrint(
	LogFS_VebTree* vt)
{
	int i;
	for(i=0;i<0x100;i++)
	{
		LogFS_VebTreeSetBank(vt,i);

		btree_iter_t it;
		tree_result_t tr = tree_begin(vt->t,&it,CONTEXT(vt));

		while( tr != tree_result_end)
		{
			uint64_t e;
			tree_iter_read(&e,&it,CONTEXT(vt));

			tr = tree_iter_inc(&it,CONTEXT(vt));
		}
	}


}
