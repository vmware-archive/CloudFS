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
#ifndef VMKERNEL 
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#else
#include "system.h"
#endif

#include "graph.h"

void Graph_InitNode(Graph *g, GraphNode *n, int key)
{
   PriQueue_Init(&n->edges);
   n->marked = 0;
   n->key = key;

   List_Insert(&n->nodesLinks, LIST_ATREAR(&g->nodesList));
}

void Graph_Init(Graph *g)
{
   List_Init(&g->nodesList);
   g->generation = 0;
}

void Graph_AddEdge(Graph *g, GraphNode *from, GraphNode *to, int weight)
{
   GraphEdge *e = malloc(sizeof(GraphEdge));
   e->n = to;
   PriQueue_Insert(&from->edges,&e->queueLinks, weight);
}

void Graph_Connect(Graph *g, GraphNode *from, GraphNode *to, int weight)
{
   Graph_AddEdge(g,from,to,weight);
   Graph_AddEdge(g,to,from,weight);
}

void 
Graph_Traverse(Graph *g, void (*callback) (GraphNode*,void*), void *data)
{
   List_Links *curr, *next;
   List_Links queue;
   List_Init(&queue);

   ++(g->generation);

   LIST_FORALL_SAFE(&g->nodesList, curr, next) {

      GraphNode *root = List_Entry( curr, GraphNode, nodesLinks);

      if (root->marked == g->generation) continue;
      root->marked = g->generation;

      List_Insert(&root->listLinks, LIST_ATREAR(&queue));


      while(!List_IsEmpty(&queue)) {
         GraphNode *n;
         PriQueue_Links *qe;

         n = List_Entry(List_First(&queue), GraphNode, listLinks);

         PRIQUEUE_FORALL(&n->edges, qe) {

            GraphEdge *e = PriQueue_Entry(qe, GraphEdge, queueLinks);
            GraphNode *p = e->n;
            if(p->marked != g->generation) {
               p->marked = g->generation;
               List_Insert(&p->listLinks, LIST_ATREAR(&queue));

               /* To save some looping over the nodeList, we remove the element
                * and move it to the front of the list. In that we do not loose
                * list contents, but save traversing over already visited nodes
                * in this run. */

               List_Remove(curr);
               List_Insert(curr, LIST_ATFRONT(&g->nodesList));
            }
         }

         callback(n,data);

         List_Remove(&n->listLinks);
      }
   }
}
static void graphCleanCallback(GraphNode *n, void *data)
{
   PriQueue_Links *qe, *next;
   PRIQUEUE_FORALL_SAFE(&n->edges, qe, next) {
      GraphEdge *e = PriQueue_Entry(qe, GraphEdge, queueLinks);
      PriQueue_Remove(qe);
      free(e);
   }
   free(n);
}

void Graph_Cleanup(Graph *g)
{

   Graph_Traverse(g, graphCleanCallback, NULL);


}


#ifndef VMKERNEL
void callback(GraphNode *n, void *data)
{
   printf("call %d\n",n->key);

}

int main(int argc, char** argv)
{
   Graph *g = malloc(sizeof(Graph));

   GraphNode* a = malloc(sizeof(GraphNode));
   GraphNode* b = malloc(sizeof(GraphNode));
   GraphNode* c = malloc(sizeof(GraphNode));
   GraphNode* d = malloc(sizeof(GraphNode));
   GraphNode* e = malloc(sizeof(GraphNode));

   Graph_Init(g);
   Graph_InitNode(g,a,1);
   Graph_InitNode(g,b,2);
   Graph_InitNode(g,c,3);
   Graph_InitNode(g,d,4);

   Graph_InitNode(g,e,5);

   Graph_Connect(g,a,b,1);
   Graph_Connect(g,a,c,2);
//   Graph_Connect(g,c,d,19);


   Graph_Traverse(g,callback,NULL);
   Graph_Cleanup(g);

   return 0;
}
/*
 * Panic --
 *	Print a message to stderr and die.
 */
void
Panic(const char *fmt, ...)
{
   va_list ap;
   va_start(ap, fmt);
   fprintf(stderr, "cloudfs PANIC: ");
   vfprintf(stderr, fmt, ap);
   va_end(ap);
   exit(1);
}
#endif
