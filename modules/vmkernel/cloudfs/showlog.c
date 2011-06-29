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
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "logtypes.h"
#include "logfsHash.h"

int main(int argc, char **argv)
{

   char *entry = malloc(LOG_HEAD_SIZE + LOG_HEAD_MAX_BLOCKS * BLKSIZE);
   struct log_head *head = (struct log_head *)entry;
   char *body = entry + BLKSIZE;

   int f = open(argv[1], O_RDONLY);
   int g = -1;

   log_offset_t e;
   LogFS_Hash last;
   int line;

   if (f < 0) {
      printf("could not open %s\n", argv[1]);
      exit(1);
   }

   if (argc > 3) {
      open(argv[2], O_CREAT | O_RDWR);
      if (g < 0) {
         printf("could not open %s\n", argv[1]);
         exit(1);
      }
   }

   for (e = 0, line = 0;; line++) {
      char s_id[SHA1_HEXED_SIZE];
      char checksum[SHA1_DIGEST_SIZE];
      LogFS_Hash id, parent;

      int r = pread(f, head, LOG_HEAD_SIZE, e);
      if (r <= 0)
         break;
      if (log_body_size(head) > 0) {
         r = pread(f, body, log_body_size(head), e + LOG_HEAD_SIZE);
         if (r <= 0)
            break;
      }

      LogFS_HashSetRaw(&id, head->id);
      LogFS_HashSetRaw(&parent, head->parent);
      LogFS_HashPrint(s_id, &id);
      printf("%05d lsn: %llu %d: %s parent %s\n", line, head->update.lsn, head->tag, s_id,
             LogFS_HashShow2(parent));
      printf("%lld+%d : %s\n", head->update.blkno, head->update.num_blocks,
             s_id);

      log_entry_checksum(checksum, head, ((char *)head) + BLKSIZE,
                         log_body_size(head));

      if (head->tag == log_entry_type) {
         if (memcmp(checksum, head->update.checksum, SHA1_DIGEST_SIZE) != 0) {
            Hash a, b;
            LogFS_HashSetRaw(&a, checksum);
            LogFS_HashSetRaw(&b, head->update.checksum);
            printf("bad checksum %s vs %s\n",
                   LogFS_HashShow(&a), LogFS_HashShow(&b));
            //exit(1);
         }
      }


      if (head->tag == log_entry_type) {
         if (head->update.lsn > 1 && !LogFS_HashEquals(LogFS_HashApply(parent), last)) {
            printf("Chain invalid\n");
            //break;
         }
      }
      if (g >= 0)
         pwrite(g, head, log_entry_size(head), e);

      last = id;
      e += log_entry_size(head);
   }

   return 0;
}
