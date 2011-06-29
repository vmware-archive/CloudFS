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
#include "logfsHash.h"

typedef struct _HashEntry {
Hash h;
Hash c;
struct _HashEntry* next;

} HashEntry;

#define LogFS_FingerPrintMaxKeys 512

struct LogFS_VDisk;

typedef struct LogFS_FingerPrint {
   HashEntry entries[LOG_MAX_SEGMENT_BLOCKS];
   HashEntry* hashTable[0x10000];
   int numHashEntries;
   uint64 *tmp;
   uint64 keys[LogFS_FingerPrintMaxKeys];
   int numHashes;

   int numFullHashes;
   uint8* fullHashes;
   struct FingerPrintOwner {
      struct LogFS_VDisk *vd;
      log_block_t blkno;
   } owners[LOG_MAX_SEGMENT_BLOCKS];

} LogFS_FingerPrint;

static inline int
LogFS_FingerPrintIsFull(LogFS_FingerPrint* fp)
{
   return fp->numHashes<LogFS_FingerPrintMaxKeys;
}

struct LogFS_VebTree;

void LogFS_FingerPrintInit(LogFS_FingerPrint* fp);

void LogFS_FingerPrintAddHash(
   LogFS_FingerPrint* fp,
   Hash h,
   struct LogFS_VDisk *vd,
   log_block_t blkno);

void LogFS_FingerPrintFinish(LogFS_FingerPrint *fp,
      struct LogFS_VebTree* vt,
      uint32 value);

void LogFS_FingerPrintCleanup(LogFS_FingerPrint* fp);
