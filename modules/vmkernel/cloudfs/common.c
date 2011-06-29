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
#include "common.h"
#include "system.h"
#include "heapsort.h"

/* use a specific heap to help confine memory utiltization */
#define LogFS_HEAP                        "vmlogfs"
#define LogFS_HEAP_INITIAL               (128*1024)

#ifdef TRACK_BLOCK_ORIGINS
#define LogFS_HEAP_MAX                   (196*1024*1024)
#else
//#define LogFS_HEAP_MAX                   (128*1024*1024)
#define LogFS_HEAP_MAX                   (384*1024*1024)
#endif

vmk_HeapID logfsHeap = INVALID_HEAP_ID;


void qsort(void *base, size_t nmemb, size_t size,
           int (*compar) (const void *, const void *))
{
   void *tmp = malloc(size);
   heapsort(base, nmemb, size, compar, tmp);
   free(tmp);
}

/*
*----------------------------------------------------------------------
*
* LogFS_Alloc()
*
*      Allocates memory from the logfsHeap.
*
* Results:
*      A pointer to a memory region of size size if the memory
*      was available.  Otherwise NULL.
*
* Side effects:
*      None
*
*----------------------------------------------------------------------
*/

//#define TRACE_ALLOCS
#ifdef TRACE_ALLOCS
struct {
   void *a;
   char place[64];
} allocs[0x20000];
int na;
#endif

void *LogFS_Alloc(size_t size, const char *fn, int line)
{
   // zprintf("malloc %ld, from %s:%d\n",size,fn,line);
   void *ptr;
   if ((ptr = vmk_HeapAlloc(logfsHeap, size))) {

#ifdef TRACE_ALLOCS
      allocs[na].a = ptr;
      sprintf(allocs[na].place, "%s:%d\n", fn, line);
      ++na;
      ASSERT(na < sizeof(allocs)/sizeof(allocs[0]));
#endif

      memset(ptr, '\0', size);
   } else {
      zprintf("No memory available! Called from %p, %s:%d, %lu bytes",
         __builtin_return_address(0), fn, line, size);
      NOT_REACHED();
   }
   return ptr;
}

/*
*----------------------------------------------------------------------
*
* LogFS_Free()
*
*      Frees the memory pointed to by ptr.
*
* Results:
*      None
*
* Side effects:
*      None
*
*----------------------------------------------------------------------
*/
void LogFS_Free(void *ptr)
{
   if (!ptr)
      NOT_REACHED();

#ifdef TRACE_ALLOCS
   int i;
   for (i = 0; i < na; i++) {
      if (allocs[i].a == ptr) {
         allocs[i].a = NULL;
      }

   }
#endif
   vmk_HeapFree(logfsHeap, ptr);
}

vmk_HeapID
LogFS_GetHeap(void)
{
   return logfsHeap;
}


VMK_ReturnStatus 
LogFS_CommonInit(void)
{
   logfsHeap = Heap_Create("logfs",
                           VMK_PAGE_SIZE,
                           LogFS_HEAP_MAX,
                           MM_PHYS_ANY_CONTIGUITY,
                           MM_TYPE_ANY);

   if (logfsHeap == INVALID_HEAP_ID) {
      Warning("Unable to create heap for logfs module");
      return VMK_NO_MEMORY;
   }

   return VMK_OK;
}

void
LogFS_CommonCleanup(void)
{

#ifdef TRACE_ALLOCS
   int i;
   for (i = 0; i < na; i++) {
      if (allocs[i].a)
         zprintf("missing: %s\n", allocs[i].place);
   }
   na = 0;
#endif

   if (logfsHeap != INVALID_HEAP_ID) {
      vmk_HeapDestroy(logfsHeap);
   }
}

