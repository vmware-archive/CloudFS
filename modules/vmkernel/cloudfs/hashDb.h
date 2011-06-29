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
#define LogFS_HashDb_LOGLINES 18
#define LogFS_HashDb_INNER_NODES ((1<<LogFS_HashDb_LOGLINES)-1)


typedef struct LogFS_HashDbEntry {
   struct LogFS_HashDbEntry *next; /* next ptr must be first! */
   struct LogFS_HashDbEntry *prev;
   uint8_t hash[SHA1_DIGEST_SIZE];
   log_id_t pos;
} LogFS_HashDbEntry ;

typedef struct LogFS_HashDb {
   LogFS_HashDbEntry entries[1<<LogFS_HashDb_LOGLINES];
   LogFS_HashDbEntry *hashTable[1<<16];
   int numHashEntries;
   char bits[LogFS_HashDb_INNER_NODES];

} LogFS_HashDb ;

void LogFS_HashDbInit(LogFS_HashDb *hd);
int LogFS_HashDbLookupHash(LogFS_HashDb* hd, Hash h, log_id_t *pos);
