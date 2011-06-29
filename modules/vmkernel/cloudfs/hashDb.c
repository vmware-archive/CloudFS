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
/* **********************************************************
 * **********************************************************/

/*
 * hashDb.c --
 *      
 *      hash table with pseudo-LRU replacement.
 *      Resolves from block hashes to their log offsets, and
 *      updates hash database at the same time, if this is 
 *      a hash not seen before.
 *
 */

#include "logtypes.h"
#include "hashDb.h"

int LogFS_HashDbLookupHash(LogFS_HashDb* hd, Hash h, log_id_t *pos)
{
   uint16_t key = *((uint16_t*)h.raw);
   LogFS_HashDbEntry *he = hd->hashTable[key];
   LogFS_HashDbEntry *prev = NULL;

   /* If hash collision, follow linked linked list until the
    * end or a match is found. */

   while(he!=NULL && memcmp(he->hash,h.raw,SHA1_DIGEST_SIZE)!=0)
   {
      prev = he;
      he = he->next;
   }

   /* If not found, insert <h,pos> into hashtable. Otherwise return
    * found pos. */

   if(he==NULL)
   {
      /* Find existing element to evict, using pseudo-LRU */

      int i;
      int child;
      for (i = 0, child = 0; i < LogFS_HashDb_LOGLINES; i++) {
         int parent = child;
         child = 2 * parent + 1 + hd->bits[parent];
         hd->bits[parent] ^= 1;
      }
      int line = child - LogFS_HashDb_INNER_NODES;
      he = &hd->entries[line];

      /* Unlink existing element from linked list */

      if(he->prev) {
         he->prev->next = he->next;
      }
      if(he->next) {
         he->next->prev = he->prev;
      }

      if(prev==NULL) {
         /* First element with this hash */

         hd->hashTable[key] = he;
         he->next = NULL;

         /* Set 'prev' to point back to hash table entry, so that
          * it gets unlinked like other elements upon replace. */

         he->prev = (struct LogFS_HashDbEntry *) &hd->hashTable[key];


      } else {

         /* Part of a linked list of entries */

         prev->next = he;
         he->prev = prev;
      }
      LogFS_HashCopy(he->hash,h);
      he->pos = *pos;
      he->next = NULL;

      return 0;
   }
   else
   {
      /* Flip the bits in the reverse path from leaf to root */

      int line = he - hd->entries;
      int child;
      for (child = line + LogFS_HashDb_INNER_NODES; child != 0;) {
         int parent = (child - 1) / 2;

         ASSERT(parent < sizeof(hd->bits));

         hd->bits[parent] = (child == (2 * parent + 1));  /* inverse test to save xor */
         child = parent;
      }

      *pos = he->pos;
      return 1;
   }

}

void LogFS_HashDbInit(LogFS_HashDb *hd)
{
   memset(hd->hashTable,0,sizeof(hd->hashTable));
}

#if 0
int main(int argc,char** argv)
{

   LogFS_HashDb *hd = malloc(sizeof(LogFS_HashDb));

   printf("sz %lu\n",sizeof(LogFS_HashDb));
   LogFS_HashDbInit(hd);

   Hash h;
   LogFS_HashRandomize(&h);
   log_id_t v;
   v.v.segment = 117;
   v.v.blk_offset = 546;

   LogFS_HashDbLookupHash(hd,h,v);

   Hash a;
   LogFS_HashRandomize(&a);

   for(;;) {
      LogFS_HashRandomize(&h);
      log_id_t v2;
      v2.v.segment = 0;
      v2.v.blk_offset = 547;
      v2 = LogFS_HashDbLookupHash(hd,h,v2);

      log_id_t v3;
      v3.v.segment = 7;
      v3.v.blk_offset = 1234;

      v3 = LogFS_HashDbLookupHash(hd,a,v3);
   }


   return 0;
}
#endif
