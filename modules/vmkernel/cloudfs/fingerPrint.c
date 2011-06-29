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
#include "fingerPrint.h"
#include "vebTree.h"

struct LogFS_VDisk;

extern int compare_u64(const void *a, const void *b);

void LogFS_FingerPrintInit(LogFS_FingerPrint *fp)
{
   memset(fp,0,sizeof(LogFS_FingerPrint));
   fp->tmp = malloc(sizeof(uint64) * LOG_MAX_SEGMENT_BLOCKS);
   fp->fullHashes = malloc(LOG_MAX_SEGMENT_BLOCKS*SHA1_DIGEST_SIZE);
}


Hash 
LogFS_FingerPrintLookupHash(LogFS_FingerPrint* fp, Hash h)
{
   uint16_t key = *((uint16_t*)h.raw);

   HashEntry* he = fp->hashTable[key];
   HashEntry* prev = NULL;

   while(he!=NULL && !LogFS_HashEquals(he->h,h))
   {
      prev = he;
      he = he->next;
   }

   if(he==NULL)
   {
      ASSERT(fp->numHashEntries<sizeof(fp->entries)/sizeof(fp->entries[0]));
      he = &fp->entries[fp->numHashEntries++];
      if(prev==NULL)
      {
         fp->hashTable[key] = he;
      }
      else
      {
         prev->next = he;
      }
      he->h = h;
      he->c = h;
      he->next = NULL;
   }
   else
   {
      LogFS_HashReapply(&he->c);
   }

   return he->c;

}

void LogFS_FingerPrintAddHash(
   LogFS_FingerPrint* fp,
   Hash h,
   struct LogFS_VDisk *vd,
   log_block_t blkno)
{
   ASSERT(fp->numFullHashes<LOG_MAX_SEGMENT_BLOCKS);

   int n = fp->numFullHashes++;
   /* Store full hash for use in log manifest */
   uint8* dst = fp->fullHashes + SHA1_DIGEST_SIZE * n;
   memcpy(dst,h.raw,SHA1_DIGEST_SIZE);
   fp->owners[n].vd = vd;
   fp->owners[n].blkno = blkno;

   if (vd==NULL) {
      return;
   }

}

void LogFS_FingerPrintFinish(LogFS_FingerPrint *fp,
      LogFS_VebTree* vt,
      uint32 value)
{
   int i;
   uint64* tmp = fp->tmp;

   /* Mangle repeated hashes to ensure proportional representation */

   for(i=0; i<LOG_MAX_SEGMENT_BLOCKS; ++i) {

      Hash h = LogFS_FingerPrintLookupHash(fp, LogFS_HashFromRaw(
               fp->fullHashes + SHA1_DIGEST_SIZE * i));

      /* We only have room for 40 of the 160 bits in the hash */
      uint64_t key;
      memcpy(&key,h.raw,sizeof(key));
      key>>=24;

      /* Subsample to only store hashes with a certain bit pattern.
       * This is based on the observations in the HP FAST 2009 dedupe paper */

      if((h.raw[9]&0xf)==0) /* Arbitrarily subsample hashes 1:16 */
      {
         ASSERT(fp->numHashes<LOG_MAX_SEGMENT_BLOCKS);
         tmp[fp->numHashes++] = key;
      }

   }

   int q = fp->numHashes;
   qsort(tmp,q,sizeof(uint64_t),compare_u64); /* XXX use a minheap */

   int j,k;
   for(j=0,k=0 ; j<q && k<LogFS_FingerPrintMaxKeys; j++)
   {
      /* Only insert unique keys */
      if(j==0 || tmp[j-1]!=tmp[j])
      {
         LogFS_VebTreeInsert(vt, tmp[j] & 0xff, tmp[j]>>8, value );
         ++k;
      }
   }

}

void LogFS_FingerPrintCleanup(LogFS_FingerPrint* fp)
{
   free(fp->tmp);
   fp->tmp = NULL;
   free(fp->fullHashes);
   fp->fullHashes = NULL;
}
