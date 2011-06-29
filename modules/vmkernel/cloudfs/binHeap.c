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
#include "binHeap.h"

static inline int lessThan(LogFS_BinHeap *hp, int a, int b)
{
   return (hp->nodes[hp->heap[a]].value < hp->nodes[hp->heap[b]].value);
}

void LogFS_BinHeapInit(LogFS_BinHeap *hp, int maxElems)
{
   hp->maxElems = maxElems;

   hp->heap = malloc(maxElems * sizeof(int));
   ASSERT(hp->heap);
   hp->nodes = malloc(maxElems * sizeof(HeapNode));
   ASSERT(hp->nodes);

   int i;
   for (i = 0; i < maxElems; i++) {
      HeapNode *node = &hp->nodes[i];
      node->value = 0;
      node->heapIndex = i;

      hp->heap[i] = i;
   }
}

void LogFS_BinHeapCleanup(LogFS_BinHeap *hp)
{
   free(hp->heap);
   free(hp->nodes);
}

static inline void swapNodes(LogFS_BinHeap *hp, int parent, int child)
{
   hp->nodes[hp->heap[parent]].heapIndex = child;
   hp->nodes[hp->heap[child]].heapIndex = parent;

   int tmp = hp->heap[parent];
   hp->heap[parent] = hp->heap[child];
   hp->heap[child] = tmp;
}

static inline void SiftUp(LogFS_BinHeap *hp, int child)
{
   int parent;
   for (; child; child = parent) {
      parent = (child - 1) / 2;

      if (lessThan(hp, parent, child)) {
         swapNodes(hp, parent, child);
      } else
         break;
   }
}

static inline void SiftDown(LogFS_BinHeap *hp, int parent)
{
   int child;
   for (;; parent = child) {
      child = 2 * parent + 1;

      if (child >= hp->maxElems)
         break;

      /* choose the larger of the two children */
      if (child < hp->maxElems - 1 && lessThan(hp, child, child + 1)) {
         ++child;
      }

      if (lessThan(hp, child, parent))
         break;

      swapNodes(hp, parent, child);
   }
}

int LogFS_BinHeapAdjustUp(LogFS_BinHeap *hp, int nodeIndex, unsigned howmuch)
{
   ASSERT(nodeIndex < hp->maxElems);

   HeapNode *node = hp->nodes + nodeIndex;
   node->value += howmuch;

   SiftUp(hp, node->heapIndex);

   return node->value;
}

uint32 LogFS_BinHeapPopMax(LogFS_BinHeap *hp, int *value)
{
   int top = hp->heap[0];

   HeapNode *node = hp->nodes + top;
   *value = node->value;
   node->value = 0;
   SiftDown(hp, 0);

   return top;
}

#if 0
int main(int argc, char **argv)
{
   int i, j;
   int value;
   int oldValue;

   LogFS_BinHeap heap;
   LogFS_BinHeapInit(&heap, 6400);

   for (j = 0; j < 380; j++)
      for (i = 0; i < heap.maxElems; i++) {
         LogFS_BinHeapAdjustUp(&heap, i, rand() % 10000);

#if 1
         if ((i % 7) == 0) {
            int r = LogFS_BinHeapPopMax(&heap, &value);
            if ((i % 14) == 0) {
               LogFS_BinHeapAdjustUp(&heap, r, value);
            }
         }
#endif
      }

   int r;
   value = oldValue = 0x11111111;
   int oldR = r = 0x11111111;

   int n = heap.maxElems;
   for (i = 0; i < n; i++) {
      oldValue = value;
      oldR = r;
      r = LogFS_BinHeapPopMax(&heap, &value);
      printf("r %d max = %d.\n", r, value);
      assert(value <= oldValue);
      //assert(r<=oldR);
   }
}
#endif
