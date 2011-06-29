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
#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <linux/unistd.h>
#include <mntent.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <dirent.h>

#include <str.h>

#include "hex.h"
#include "aligned.h"

#include "logfsHash.h"
#include "cloudfslib.h"
#include "parseHttp.h"
#include "cloudfsdb.h"
#include "sqlite3.h"

#define O_DIRECT      040000

extern void createBranch(void *buf, Hash parent, int numHosts, Hash * hostIds, Hash
      secretView, Hash * retSecret);

int openLog(void)
{
   static int log_fd = -1;
   char *fn = "/vmfs/volumes/cloudfs/log";

   if (log_fd >= 0)
      return log_fd;

   log_fd = open(fn, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, (mode_t) 0644);
   if (log_fd < 0) {
      fprintf(stderr, "could not open %s for write\n", fn);
      exit(1);
   }
   return log_fd;
}


int main(int argc, char **argv)
{
   int r;

   /* XXX hack to seed random gen */
   Hash a;
   LogFS_HashRandomize(&a);
   srand( a.raw[0] << 24 | a.raw[1] << 16 | a.raw[2] << 8 | a.raw[3] );

   if (argc >= 2 && strcmp(argv[1], "newdisk") == 0) {
      Hash secret;
      Hash secretView;
      Hash diskId;
      Hash zero;
      HostIdRecord hrs;
      Hash inv;
      char fn[64];

      char sDiskId[SHA1_HEXED_SIZE];

      int numHosts;
      int howMany = argc == 3 ? atoi(argv[2]) : 3;
      void* buf;
      
      LogFS_HashZero(&zero);
      LogFS_HashClear(&inv);
      LogFS_HashRandomize(&secretView);

      initDatabase();

      for (;;) {
         hrs.numIds = 0;
         selectNRandomPeers(&hrs, howMany);

         numHosts = MIN(howMany, hrs.numIds);

         if (numHosts < howMany) {
            fprintf(stderr, "only %d host(s) found, retrying...\n", numHosts);
            sleep(2);
         } else
            break;
      }


      buf = malloc(LOG_HEAD_SIZE + BLKSIZE);

      createBranch(buf, zero, hrs.numIds, hrs.hostIds, secretView, &secret);
      forwardBranch(buf, numHosts, numHosts, hrs.hostNames, secret, secretView);

      diskId = LogFS_HashApply(secret);

      /* print disk id to stdout so scripts can pick it up easily */
      LogFS_HashPrint(sDiskId, &diskId);
      printf("%s", sDiskId);
      Str_Sprintf(fn,sizeof(fn),"/vmfs/volumes/cloudfs/%s",sDiskId);
      readdir(opendir(fn));
   }

   else if (argc == 4 && strcmp(argv[1], "force") == 0) {

      Hash diskId;
      Hash exclude;
      Hash secretView;
      Hash secret;
      Hash entropy;
      Hash publicView;
      Hash zero;
      Hash inv;
      Hash nextSecret;
      Hash nextSecretView;

      HostIdRecord hrs = { 0, };
      int numHosts;

      struct sha1_ctx ctx;
      unsigned char tmp[SHA1_DIGEST_SIZE];
      void* buf;
      struct log_head *head;

      LogFS_HashSetString(&diskId, argv[2]);
      LogFS_HashSetString(&exclude, argv[3]);

      initDatabase();

      secretView = getSecretView(diskId);

      if (!LogFS_HashIsValid(secretView)) {
         fprintf(stderr, "This host does not seem to be in the replica set.\n");
         exit(1);
      }

      publicView = LogFS_HashApply(secretView);

      /* Calc secret */
      entropy = getEntropy(diskId);
      assert(LogFS_HashIsValid(entropy));

      sha1_init(&ctx);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, secretView.raw);
      sha1_update(&ctx, SHA1_DIGEST_SIZE, entropy.raw);
      sha1_digest(&ctx, SHA1_DIGEST_SIZE, tmp);
      LogFS_HashSetRaw(&secret, tmp);

      printf("secret for view %s must be %s\n", LogFS_HashShow(&publicView),
             LogFS_HashShow(&secret));

      selectHostsInView(&hrs, publicView, exclude);

      /* Grow the view with one host */
      r = selectHostsNotInView(&hrs, publicView, 1);
      assert(r == SQLITE_OK);

      numHosts = hrs.numIds;
      printf("%d hosts now\n", numHosts);
      assert(numHosts == 3);

      LogFS_HashZero(&zero);
      LogFS_HashClear(&inv);

      buf = malloc(LOG_HEAD_SIZE + BLKSIZE);

      LogFS_HashRandomize(&nextSecretView);

      createBranch(buf, secret, hrs.numIds, hrs.hostIds, nextSecretView,
                   &nextSecret);

      /* XXX hack */
      head = (struct log_head *)buf;
      LogFS_HashCopy(head->disk, diskId);

      forwardBranch(buf, numHosts, numHosts, hrs.hostNames, nextSecret,
                    nextSecretView);
   }
#if 0
   else if (argc == 3 && strcmp(argv[1], "branch") == 0) {
      struct log_head *head = (struct log_head *)aligned_malloc(LOG_HEAD_SIZE);
      memset(head, 0, LOG_HEAD_SIZE);

      LogFS_Hash secret, parent;
      if (LogFS_HashSetString(&parent, argv[2])) {
         Hash secretView;
         LogFS_HashRandomize(&secretView);

         appendBranch(openLog(), parent, getHostId(), secretView, &secret);

         Hash id = LogFS_HashApply(secret);
         setSecret(id, secret, secretView);
         /* print disk id to stdout so scripts can pick it up easily */
         printf("%s", LogFS_HashShow2(id));
      } else
         printf("input error\n");
   }
#endif

#if 1
   else if (argc == 4 && strcmp(argv[1], "setsecret") == 0) {
      Hash id;
      Hash secret;
      Hash secretView;

      if (!LogFS_HashSetString(&id, argv[2])) {
         printf("invalid id input\n");
         exit(1);
      }

      if (!LogFS_HashSetString(&secret, argv[3])) {
         printf("invalid secret input\n");
         exit(1);
      }

      LogFS_HashRandomize(&secretView);
      setSecret(id, secret, secretView);
   }
#endif
   else if (argc == 3 && strcmp(argv[1], "nuke") == 0) {
      const size_t sz = 0x10000;
      int f = open(argv[2], O_WRONLY | O_CREAT | O_DIRECT);
      int i;
      char *buf = aligned_malloc(sz);
      memset(buf, 0, sz);
      for (i = 0; i < 256; i++) {
         int r = pwrite(f, buf, sz, sz * i);
         if (r != sz) {
            perror("write failed");
            exit(1);
         }
      }

      close(f);
   } else if ((argc == 3 || argc == 4) && strcmp(argv[1], "test") == 0) {

      int i,n;
      int f;

      char *buf = aligned_malloc(4096);

      f = open(argv[2], O_WRONLY | O_CREAT | O_DIRECT);
      ASSERT(f >= 0);

      n = (argc == 4) ? atoi(argv[3]) : 1;
      printf("n %d\n", n);
   
      for (i = 0; i < n; i++) {
         memset(buf, 0, 4096);
         strcpy(buf+741, "hejmorogfar\n");
         strcpy(buf+2041, "hejmorogfar\n");
         buf[7] = 'a' + i;

         if( pwrite(f, buf, 4096,i * 4096) != 4096) {
            perror("write failed");
            exit(1);
         }
#if 0
         if( pwrite(f, buf, 4096, 8192 * i) != 4096) {
            perror("write failed");
            exit(1);
         }
#endif
      }

      close(f);
   } else if ((argc == 4) && strcmp(argv[1], "read") == 0) {

      int p,r;
      char *buf = aligned_malloc(4096);
      int f = open(argv[2], O_RDONLY | O_DIRECT);
      ASSERT(f >= 0);
      memset(buf, 0, 4096);

      p = atoi(argv[3]);

      r = pread(f, buf, 4096, 4096 * p);
      printf("r %d\n", r);
      perror("err");
      assert(r == 4096);
      //write(0,buf,4096);

      close(f);
   } else {
      printf("Usage: cloudfs newdisk [num-replicas]\n");
      printf(" or:   cloudfs force <disk> <excluded-hostid>\n");
      printf(" or:   cloudfs setsecret <disk> <secret> (normally not needed)\n");
      printf(" or:   cloudfs branch <revision> (not working at the moment)\n");
      printf(" or:   cloudfs deletedisk <disk> (not implemented yet)\n");
   }

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
