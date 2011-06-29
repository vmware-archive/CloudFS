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
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdarg.h>
#include <str.h>
#include <dirent.h>

#include "aligned.h"
#include "logtypes.h"
#include "logfsHash.h"
#include "cloudfslib.h"
#include "parseHttp.h"
#include "cloudfsdb.h"
#include "bitOps.h"

Hash getSecret(Hash diskId)
{
   char fn[256];
   char sDiskId[SHA1_HEXED_SIZE];
   int fd;
   Hash value;

   LogFS_HashPrint(sDiskId, &diskId);
   Str_Sprintf(fn, sizeof(fn), "/dev/cloudfs/%s.secret", sDiskId);

   /* O_DIRECT is important to avoid caching! */
   fd = open(fn, O_RDONLY | O_DIRECT);
   LogFS_HashClear(&value);

   if (fd < 0) {
      fprintf(stderr, "could not open %s\n", fn);
   } else {
      int r;
      char *sValue = aligned_malloc(BLKSIZE);
      memset(sValue, 0, BLKSIZE);

      r = read(fd, sValue, BLKSIZE);

      if (r >= SHA1_HEXED_SIZE_UNTERMINATED) {
         sValue[SHA1_HEXED_SIZE - 1] = '\0';
         LogFS_HashSetString(&value, sValue);
      } else
         fprintf(stderr, "read returned %d\n", r);
      aligned_free(sValue);
      close(fd);
   }
   return value;
}

Hash getCurrentId(Hash diskId)
{
   char fn[256];
   char sDiskId[SHA1_HEXED_SIZE];
   Hash id;
   FILE* f;
   char sId[SHA1_HEXED_SIZE];

   assert(LogFS_HashIsValid(diskId));
   LogFS_HashPrint(sDiskId, &diskId);
   Str_Sprintf(fn, sizeof(fn), "/dev/cloudfs/%s.id", sDiskId);

   LogFS_HashClear(&id);

   f = fopen(fn, "r");
   if (f == NULL)
      return id;

   if (fread(sId, SHA1_HEXED_SIZE_UNTERMINATED, 1, f) == 1) {
      LogFS_HashSetString(&id, sId);
   }
   fclose(f);

   return id;
}

uint64 getLsn(Hash diskId)
{
   char url[256];

   int sock;
   HTTPParserState ps = {0,};
   HTTPSession ss = {0,};
   char response[4096];
   int r;
   struct sockaddr_in server;
   uint64 lsn = 0;

   Str_Sprintf(url, sizeof(url), "GET /lsn?%s HTTP/1.1\r\nHost: 127.0.0.01:8090\r\nAccept: */*\r\n\r\n",
         LogFS_HashShow(&diskId));

   sock = socket(PF_INET, SOCK_STREAM, 0);

   server.sin_family = AF_INET;
   server.sin_addr.s_addr = htonl(0x7f000001);
   server.sin_port = htons(8090);
   connect(sock, (struct sockaddr *)&server, sizeof(server));


   /* Connected */

   r = write(sock, url, strlen(url));

   if (r < 0) {
      goto out;
   }

   /* Has responded */

   do {

      r = read(sock, response, sizeof(response));
      printf("r %d\n",r);
      if (r > 0)
         r = parseHttp(&ps, response, r, &ss);
   } while (r > 0);

   if (!ss.complete || ss.status != 200) {
      goto out;
   }

   sscanf(response + ps.justParsed, "%llu", &lsn);

out:

   close(sock);
   return lsn;
}

Hash getEntropy(Hash diskId)
{
   char fn[256];
   Hash id;
   char sDiskId[SHA1_HEXED_SIZE];
   char sId[SHA1_HEXED_SIZE];
   FILE* f;
   size_t r;

   assert(LogFS_HashIsValid(diskId));
   LogFS_HashPrint(sDiskId, &diskId);
   Str_Sprintf(fn, sizeof(fn), "/dev/cloudfs/%s.info", sDiskId);

   LogFS_HashClear(&id);

   f = fopen(fn, "r");
   if (f == NULL)
      return id;

   r = fread(sId, SHA1_HEXED_SIZE_UNTERMINATED, 1, f);
   assert(r==1);
   fclose(f);

   LogFS_HashSetString(&id, sId);
   return id;
}


void
createBranch(void *buf, Hash parent, int numHosts, Hash * hostIds,
             Hash secretView, Hash * retSecret)
{
   int i;
   const size_t sz = LOG_HEAD_SIZE + BLKSIZE;
   struct log_head *head = buf;
   struct viewchange *v = (struct viewchange *)((char *)head + LOG_HEAD_SIZE);
   unsigned char tmp[SHA1_DIGEST_SIZE];
   struct sha1_ctx ctx;

   Hash id;
   Hash secret;

   /* We log this change of ownership as a view-change record, that has
    * the previous secret as parent, and gets its own new id */

   memset(head, 0, sz);
   head->tag = log_entry_type;
   head->update.lsn = 1;
   head->update.blkno = METADATA_BLOCK;
   head->update.num_blocks = 1;
   BitSet(head->update.refs,0);

   LogFS_HashCopy(head->parent, parent);

   LogFS_HashCopy(v->view, LogFS_HashApply(secretView));

   v->num_replicas = numHosts;
   for (i = 0; i < numHosts; i++) {
      LogFS_HashCopy(&v->hosts[i][0], hostIds[i]);
   }

   LogFS_HashCopy(v->invalidates_view, secretView);

   /* calculate 'entropy' coming from update context and contents */
   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)head->parent);  /* == 0 */
   sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)v->view);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, head->entropy);

   /* calculate the secret entry id, based on the secret view and the entropy */
   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, secretView.raw);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, head->entropy);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, tmp);

   /* Calculate the public id, which is also going to be the base id for
    * the new volume */
   LogFS_HashSetRaw(&secret, tmp);
   id = LogFS_HashApply(secret);
   LogFS_HashCopy(head->id, id);
   LogFS_HashCopy(head->disk, id);

   log_entry_checksum(head->update.checksum, head, v, BLKSIZE);

   if (retSecret)
      *retSecret = secret;
}

int setSecret(Hash diskId, Hash secret, Hash secretView)
{
   int r = 0;
   char fn[256];
   char sDiskId[SHA1_HEXED_SIZE];
   char *sSecret = aligned_malloc(BLKSIZE);
   int f;

   memset(sSecret, 0, BLKSIZE);

   LogFS_HashPrint(sDiskId, &diskId);
   LogFS_HashPrint(sSecret, &secret);
   LogFS_HashPrint(sSecret + SHA1_HEXED_SIZE_UNTERMINATED, &secretView);

   Str_Sprintf(fn, sizeof(fn), "/dev/cloudfs/%s.secret", sDiskId);

   f = open(fn, O_WRONLY | O_DIRECT);

   if (f >= 0) {
      r = pwrite(f, sSecret, BLKSIZE, 0);
      close(f);
   } else
      printf("could not open %s\n", fn);

   aligned_free(sSecret);
   return (r >= 0);
}

#if 0
int forceSecret(int fd, Hash diskId, Hash secretView, Hash hostId, Hash newView,
                Hash * retSecret)
{
   Hash secret;
   struct sha1_ctx ctx;
   unsigned char tmp[SHA1_DIGEST_SIZE];
   Hash entropy = getValue(diskId, "entropy");

   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, secretView.raw);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, entropy.raw);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, tmp);
   LogFS_HashSetRaw(&secret, tmp);

   return appendBranch(fd, secret, hostId, newView, retSecret);
}
#endif

#define wr(__a) do{size_t l=strlen(__a);memcpy(m->outBuf,__a,l); m->outBuf+=l; *m->outBuf++='\r'; *m->outBuf++='\n';}while(0);
#define FLUSH(__s) do{ if(m->outBuf!=m->outBuf0) {\
		write(__s,m->outBuf0,m->outBuf-m->outBuf0);\
		m->outBuf=m->outBuf0;\
} }while(0);

int isHostInReplicaSet(Hash hostId, struct viewchange *v)
{
   int i;
   for (i = 0; i < v->num_replicas; i++) {
      if (LogFS_HashEquals(hostId, LogFS_HashFromRaw(&v->hosts[i][0])))
         return 1;
   }
   return 0;
}

int forwardBranch(struct log_head *head,
                  int numHosts,
                  int threshold, char **hostNames, Hash secret, Hash secretView)
{
   int i;

   int numOpen = 0;
   int numAgreeing = 0;
   int numBad = 0;
   int numDone = 0;

   Hash diskId;
   Hash parent;

   char url[256];

   typedef struct {
      char *hostName;
      int sock;
      HTTPParserState ps;
      HTTPSession ss;
      int state;
      char outBuf0[1024];
      char *outBuf;
      size_t sent;
   } Member;
   Member members[5];

   struct timeval oneSec;

   oneSec.tv_sec = 1;
   oneSec.tv_usec = 0;

   diskId = LogFS_HashFromRaw(head->disk);
   parent = LogFS_HashFromRaw(head->parent);


   Str_Sprintf(url, sizeof(url), "PUT /log?%s&%s HTTP/1.1",
           LogFS_HashShow(&diskId), LogFS_HashShow(&parent));

   memset(&members, 0, sizeof(members));

   for (i = 0; i < numHosts; i++) {
      Member *m = &members[i];
      struct hostent *hp;
      int sock;
      struct sockaddr_in server;

      fprintf(stderr, "host %s\n", hostNames[i]);

      if ((hp = gethostbyname(hostNames[i])) == 0) {
         fprintf(stderr, "host resolution error for %s\n", hostNames[i]);
         perror("gethostbyname");
         return -1;
      }

      sock = socket(PF_INET, SOCK_STREAM, 0);

      fcntl(sock, F_SETFL, fcntl(sock, F_GETFL) | O_NONBLOCK);

      server.sin_family = AF_INET;
      server.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
      server.sin_port = htons(8091);
      connect(sock, (struct sockaddr *)&server, sizeof(server));

      m->sock = sock;
      m->state = 1;
      m->outBuf = m->outBuf0;
      m->hostName = hostNames[i];
   }

   for (;;) {
      /* Initialize select() state anew each time */

      int r;

      fd_set rFds, wFds;
      int maxFd = -1;

      FD_ZERO(&rFds);
      FD_ZERO(&wFds);

      for (i = 0; i < numHosts; i++) {
         Member *m = &members[i];
         if (m->state > 0 && (m->state < 3 || numAgreeing >= threshold)) {
            fd_set *fdSet = (m->state & 1) ? &wFds : &rFds;
            FD_SET(m->sock, fdSet);

            maxFd = (m->sock > maxFd) ? m->sock : maxFd;
         }
      }

      r = select(maxFd + 1, &rFds, &wFds, NULL, &oneSec);

      for (i = 0; i < numHosts; i++) {
         Member *m = &members[i];

         /* Odd states write, even states read */

         fd_set *fdSet = (m->state & 1) ? &wFds : &rFds;

         if (!FD_ISSET(m->sock, fdSet))
            continue;

         /* Connected */
         if (m->state == 1) {
            size_t sz;

            m->outBuf = m->outBuf0;
            wr(url);

            if (LogFS_HashIsValid(secret)) {
               char s[128];
               char sSecret[SHA1_HEXED_SIZE];
               char sSecretView[SHA1_HEXED_SIZE];

               if (i == 0) {
                  LogFS_HashPrint(sSecret, &secret);
               } else {
                  Hash zero;
                  LogFS_HashZero(&zero);
                  LogFS_HashPrint(sSecret, &zero);
               }

               LogFS_HashPrint(sSecretView, &secretView);
               Str_Sprintf(s, sizeof(s), "Secret: \"%s,%s\"", sSecret, sSecretView);
               wr(s);
            }

            wr("Content-Length: 1024");
            wr("Expect: 100-continue");
            wr("");

            sz = m->outBuf - m->outBuf0;
            r = write(m->sock, m->outBuf0 + m->sent, sz - m->sent);

            if (r < 0) {
               fprintf(stderr, "bad host %s\n", m->hostName);
               m->state = 0;

               if (++numBad > numHosts - threshold) {
                  goto out;
               }
            } else {
               m->sent += r;

               if (m->sent == sz) {
                  ++(m->state);
                  ++numOpen;

                  /* Reset fields for future reuse */
                  m->outBuf = m->outBuf0;
                  m->sent = 0;
               }
            }

         }

         /* Has responded */
         else if (m->state == 2) {
            char response[4096];
            do {
               r = read(m->sock, response, sizeof(response));
               if (r > 0)
                  r = parseHttp(&m->ps, response, r, &m->ss);
            } while (r > 0);

            if (!m->ss.complete || m->ss.status != 100) {
               fprintf(stderr, "host %s objects\n", hostNames[i]);
               goto out;
            } else {
               memset(&m->ps, 0, sizeof(m->ps));
               memset(&m->ss, 0, sizeof(m->ss));
               ++numAgreeing;
               ++(m->state);
            }
         }

         else if (m->state == 3 && numAgreeing >= threshold) {
            const size_t sz = LOG_HEAD_SIZE + BLKSIZE;

            char *data = (char *)head;
            r = write(m->sock, data + m->sent, sz - m->sent);

            if (r < 0) {
               fprintf(stderr, "failed forwarding to %s\n", m->hostName);
               fflush(stderr);
               return 1;
            }

            else {
               m->sent += r;

               if (m->sent == sz) {
                  ++(m->state);
               }
            }
         }

         /* Get final ok */
         else if (m->state == 4) {
            char response[4096];
            do {
               r = read(m->sock, response, sizeof(response));
               if (r > 0)
                  r = parseHttp(&m->ps, response, r, &m->ss);
            } while (r > 0);

            if (m->ss.complete && (m->ss.status == 200 || m->ss.status == 204)) {
               if (++numDone == numAgreeing) {
                  goto out;
               }

               ++(m->state);
            }
         }

      }
   }

 out:

   for (i = 0; i < numHosts; i++) {
      Member *m = &members[i];
      close(m->sock);
   }
   return 0;
}

void createCloudVolume(char *name)
{
   Hash secret;
   Hash secretView;
   Hash diskId;
   Hash zero;
   HostIdRecord hrs;
   Hash inv;

   int numHosts;
   int howMany = 1;
   void* buf;
   char fn[256];

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
   LogFS_HashPrint(name,&diskId);
   Str_Sprintf(fn,sizeof(fn),"/vmfs/volumes/cloudfs/%s",name);
   readdir(opendir(fn));
}
