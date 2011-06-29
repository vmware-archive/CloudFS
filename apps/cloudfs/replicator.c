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
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include <curl/curl.h>
#include <curl/types.h>
#include <curl/easy.h>

#include "list.h"

#include "logtypes.h"
#include "logfsHash.h"
#include "aligned.h"
#include "cloudfslib.h"
#include "parseHttp.h"
#include "cloudfsdb.h"

#include <str.h>

//#include "vm_basic_asm.h"     // for RDTSC

#define MAXURL 1024
#define MAXHOSTNAME 256

void *diskWorker(void *vdisk);

pthread_mutex_t listMutex;

pthread_mutex_t httpClientListLock;
pthread_cond_t httpClientListCv;

Hash localHostId;

List_Links diskList;
List_Links httpClients;

pthread_t createThread(void *(*start_routine) (void *), void *arg)
{
   pthread_t tid;
   int r = pthread_create(&tid, NULL, start_routine, arg);
   if (r != 0) {
      fprintf(stderr, "pthread error %d\n", r);
      fflush(stderr);
   }
   assert(r == 0);
   return tid;
}

typedef struct {
   Hash diskId;
   Hash currentId;

   List_Links streams;

   int secretFd;

   pthread_mutex_t syncMutex;

   List_Links next;

   CURLM *curlMultiHandle;

   Hash proposedView;

   int logFd;

} DiskInfo;

typedef struct _StreamInfo {
   DiskInfo *diskInfo;

   Hash hostId;
   CURL *curlHandle;
   Bool paused;
   Hash pausedWaitingFor;
   char *backLog;
   size_t bytesInBackLog;

   List_Links next;
} StreamInfo;

struct cb_data {
   char *buffer;
   size_t headleft;             // = LOG_HEAD_SIZE;
   size_t bodyleft;
   int buf_offs;
   int state;                   // = 1;
   int count;
   int bytesReceived;
   StreamInfo *stream;
};

DiskInfo *lookupDisk(Hash diskId);

/* Cache a few stats to avoid fetching from from DB over and over */

static int numDisks;
static int numPeers;

static size_t data_callback(void *ptr, size_t size, size_t nmemb, void *data);

int sockOptCallback(void *clientp, curl_socket_t curlfd, curlsocktype purpose)
{
   int size;
   int r;
   for (size = 0x100000; size != 0; size >>= 1) {
      r = setsockopt(curlfd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
      if (r == 0)
         break;
   }
   assert(r == 0);
   return r;
}

void initStream(StreamInfo * stream, DiskInfo * info, Hash hostId)
{
   Bool haveDeviceNode = FALSE;
   Hash currentId;
   Hash useId;

   char sDiskId[SHA1_HEXED_SIZE];
   char hostName[MAXHOSTNAME];
   uint64 lsn;

   char url[MAXURL];
   struct cb_data *data = malloc(sizeof(struct cb_data));

   CURL *curl_handle;
   curl_handle = curl_easy_init();

   memset(stream, 0, sizeof(StreamInfo));
   stream->diskInfo = info;
   stream->hostId = hostId;

   currentId = getCurrentId(info->diskId);
   if (LogFS_HashIsValid(currentId))
      printf("kernel id %s.\n", LogFS_HashShow(&currentId));

   if (LogFS_HashIsValid(currentId)) {
      useId = currentId;
      haveDeviceNode = TRUE;
   } else
      useId = info->diskId;

   if (!LogFS_HashIsValid(info->currentId))
      info->currentId = useId;


   data->buffer =
       (char *)aligned_malloc(LOG_HEAD_SIZE + LOG_HEAD_MAX_BLOCKS * BLKSIZE);
   data->headleft = LOG_HEAD_SIZE;
   data->bodyleft = 0;
   data->buf_offs = 0;
   data->state = 1;
   data->count = 0;
   data->stream = stream;
   data->bytesReceived = 0;

   if (!haveDeviceNode) {
      LogFS_HashZero(&useId);
   }

   LogFS_HashPrint(sDiskId, &info->diskId);

   for (;;) {
      getPeerForHost(hostName, hostId);
      if (*hostName)
         break;
      fprintf(stderr, "waiting for host %s to appear\n",
              LogFS_HashShow(&hostId));
      sleep(2);
   }

   lsn = getLsn(info->diskId);

   /* XXX remote host may not be ready yet, we should retry a few times */
   Str_Sprintf(url, sizeof(url), "http://%s:8090/stream?%s&%llu", hostName, sDiskId, lsn);

   fprintf(stderr, "get %s\n", url);
   fflush(stderr);
   curl_easy_setopt(curl_handle, CURLOPT_URL, url);
   curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, data_callback);
   curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)data);
   curl_easy_setopt(curl_handle, CURLOPT_SOCKOPTFUNCTION, sockOptCallback);
   //curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1);
   curl_multi_add_handle(info->curlMultiHandle, curl_handle);

   stream->curlHandle = curl_handle;

}

#if 1
DiskInfo *rememberDisk(Hash diskId, Hash hostId)
{
   DiskInfo *info = NULL;
   Bool startThread = FALSE;
   List_Links *curr;
   StreamInfo *stream;

   pthread_mutex_lock(&listMutex);

   LIST_FORALL(&diskList, curr) {
      DiskInfo *i = List_Entry(curr, DiskInfo, next);
      if (LogFS_HashEquals(i->diskId, diskId)) {
         info = i;
         break;
      }
   }

   if (info == NULL) {
      char sDiskId[SHA1_HEXED_SIZE];

      info = malloc(sizeof(DiskInfo));

      memset(info, 0, sizeof(DiskInfo));
      info->secretFd = -1;
      info->logFd = -1;

      pthread_mutex_init(&info->syncMutex, NULL);

      info->curlMultiHandle = curl_multi_init();

      List_Init(&info->streams);

      LogFS_HashPrint(sDiskId, &diskId);
      info->diskId = diskId;
      List_Insert(&info->next, LIST_ATREAR(&diskList));

      startThread = TRUE;
   }

   LIST_FORALL(&info->streams, curr) {
      StreamInfo *stream = List_Entry(curr, StreamInfo, next);
      if (LogFS_HashEquals(stream->hostId, hostId))
         goto found;
   }

   stream = malloc(sizeof(StreamInfo));
   initStream(stream, info, hostId);
   List_Insert(&stream->next, LIST_ATREAR(&info->streams));

 found:

   if (startThread) {
      fprintf(stderr, "create thread for %s @ %s\n", LogFS_HashShow(&diskId),
              LogFS_HashShow(&hostId));
      fflush(stderr);
      createThread(diskWorker, (void *)info);
   }

   pthread_mutex_unlock(&listMutex);
   return info;
}
#endif

static const char str_id[] = "/id";
static char str_blocks[] = "/blocks";
static char ok_string[] = "HTTP/1.1 200 OK";
static char no_content[] = "HTTP/1.1 204 No Content";
static char err_string[] = "HTTP/1.1 404 Not Found";

static char date_string[] = "Date: Tue Jan 15 13:47:48 CET 2007";
static char type_string[] = "Content-type: text/plain";
static char html_type_string[] = "Content-type: text/html";
static char moved_string[] = "HTTP/1.1 301 Moved Permanently";
//static char nomod_string[] = "HTTP/1.1 304 Not Modified";

#define wr(__a) do{size_t l=strlen(__a);memcpy(outBuf,__a,l); outBuf+=l; *outBuf++='\r'; *outBuf++='\n';}while(0);
#define FLUSH() do{ if(outBuf!=cs->outBuf) {\
		ignored=write(cs->clientSock,cs->outBuf,outBuf-cs->outBuf);\
		outBuf=cs->outBuf;\
} }while(0);

typedef struct {
   int clientSock;
   int bytesReceived;
   char outBuf[1024];
   char request[4096];
   char *in;
   int protocolState;

   struct sockaddr_in client_addr;

   HTTPParserState ps;
   HTTPSession ss;

   List_Links list;
} UWConnectionState;

int handleHttpClient(UWConnectionState * cs, int canRead, int canWrite)
{

   char *outBuf = cs->outBuf;
   HTTPParserState *ps = &cs->ps;
   HTTPSession *ss = &cs->ss;
   int ignored;

   if (canRead && cs->bytesReceived == 0) {
      int r = read(cs->clientSock, cs->request, 0x1000);

      if (r == EWOULDBLOCK)
         return 1;

      cs->bytesReceived = r;
      cs->in = cs->request;

      if (cs->bytesReceived == 0) {
         return 0;
      }

   }

   /* In header state? */

   if (cs->protocolState == 0) {

      Bool more = parseHttp(ps, cs->in, cs->bytesReceived, ss);

      cs->in += ps->justParsed;
      cs->bytesReceived -= ps->justParsed;

      if (!more) {
         if (ss->complete) {
            cs->protocolState = 1;
         } else {
            printf("PARSER ERROR '%s'\n", cs->in - ps->justParsed);
            return 0;
         }

      }
   }

   /* In body state? */

   if (cs->protocolState == 1) {

      Hash diskId;
      Hash hostId;
      Hash id;
      Hash oldSecret;
      Hash oldSecretView;
      char sDiskId[SHA1_HEXED_SIZE + 1];
      char sHostId[SHA1_HEXED_SIZE + 1];
      char sId[SHA1_HEXED_SIZE + 1];


      if (ss->verb == HTTP_PUT &&
          sscanf(ss->fileName, "/log?%40s&%40s", sDiskId, sId) == 2
          && LogFS_HashSetString(&diskId, sDiskId)
          && LogFS_HashSetString(&id, sId)) {

         Hash publicView;
         char *buf;
         void *body;
         struct log_head *head;
         int i;
         int r;
         int left,take;
         unsigned char checksum[SHA1_DIGEST_SIZE];
         struct viewchange *v;
         /* Check that we have the corresponding disk at the correct
          * version */

         DiskInfo *info = NULL;

         /* If we have this volume mirrored already, lock the stream
          * for it and sanity-check the proposed new id */

         info = lookupDisk(diskId);

         if (info != NULL) {
            if (pthread_mutex_trylock(&info->syncMutex) != 0) {
               printf("locking fails\n");
               goto error;
            }

            /* Check if volume is owned by this host, in
             * which case we will refuse the request */

            oldSecretView = getSecretView(diskId);

            if (LogFS_HashIsValid(oldSecretView)) {

               oldSecret = getSecret(diskId);

               if (LogFS_HashIsValid(oldSecret)) {
                  assert(LogFS_HashIsValid(oldSecretView));
                  printf("I was the master already!\n");
                  setSecret(diskId, oldSecret, oldSecretView);
                  pthread_mutex_unlock(&info->syncMutex);
                  goto error;
               }
            }

            if (!LogFS_HashEquals(LogFS_HashApply(id), getCurrentId(diskId))) {
               printf("id check fails\n");
               pthread_mutex_unlock(&info->syncMutex);
               goto error;
            }
         }

         wr("HTTP/1.1 100 Continue");
         wr("");
         FLUSH();

         buf = malloc(ss->contentLength);
         head = (struct log_head *)buf;
         body = buf + LOG_HEAD_SIZE;

         r = 0;

         left = ss->contentLength;
         take = MIN(left, cs->bytesReceived);

         memcpy(head, cs->in, take);

         buf += take;
         left -= take;

         cs->in += take;
         cs->bytesReceived -= take;

         if (left > 0) {
            r = read(cs->clientSock, buf, left);
            if (r == EWOULDBLOCK)
               return 1;
         }

         log_entry_checksum(checksum, head, body, log_body_size(head));

         if (r != left || head->update.blkno != METADATA_BLOCK ||
             memcmp(checksum, head->update.checksum, SHA1_DIGEST_SIZE) != 0) {
            if (info)
               pthread_mutex_unlock(&info->syncMutex);
            goto error;
         }

         v = (struct viewchange *)body;

         diskId = LogFS_HashFromRaw(head->disk);

         /* If this host is going to be a mirror, append to log device */

         publicView = LogFS_HashFromRaw(v->view);
         for (i = 0; i < v->num_replicas; i++) {
            insertViewMember(publicView, LogFS_HashFromRaw(&v->hosts[i][0]));
         }

         if (isHostInReplicaSet(localHostId, v)) {

            int logFd;
            logFd = open("/vmfs/volumes/cloudfs/log",
                         O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, (mode_t) 0644);
            r = write(logFd, head, LOG_HEAD_SIZE + BLKSIZE);
            close(logFd);

            if (info == NULL) {
               for (i = 0; i < v->num_replicas; i++) {
                  Hash hostId = LogFS_HashFromRaw(&v->hosts[i][0]);

                  if (!LogFS_HashEquals(hostId, localHostId)) {
                     rememberDisk(diskId, hostId);
                  }
               }
            }

            /* secret may be valid but null, in which case only
             * secretView will be set in kernel */

            if (LogFS_HashIsValid(ss->secret)) {
               setSecret(diskId, ss->secret, ss->secretView);
               insertSecretView(diskId, ss->secretView);
            }
         }

         /* Now that we commited the write, we don't need to 
          * prevent others from touching the volume any longer */

         if (info)
            pthread_mutex_unlock(&info->syncMutex);

         /* Ack the write back to the client */

         wr(no_content);
         wr("");
         FLUSH();

#if 1
         if (insertDisk(diskId) == 0) {

            Hash inv;
            char nextPeer[MAXHOSTNAME];
            char randomPeer[MAXHOSTNAME];
            char *hosts[] = { nextPeer, randomPeer };

            LogFS_HashClear(&inv);
            ++numDisks;         /* XXX make atomic */

            getNextPeer(nextPeer, localHostId);
            getRandomPeer(randomPeer, localHostId);
            forwardBranch(head, 2, 0, hosts, inv, inv);
         }
#endif

      }

      else if (memcmp(ss->fileName, str_blocks, strlen(str_blocks)) == 0) {
         char sDiskId[SHA1_HEXED_SIZE];
         char hostName[MAXHOSTNAME];

         Hash diskId;

         if (sscanf(ss->fileName, "/blocks?%40s", sDiskId) == 1
             && LogFS_HashSetString(&diskId, sDiskId)) {
            char url[128];

            int port;

            getRandomViewMember(hostName, diskId);

            if (*hostName != '\0') {
               port = 8090;
               fprintf(stderr, "may be known at %s\n", hostName);
               fflush(stderr);
            }

            else {
               /*or else pick next host to ensure progress */

               getNextPeer(hostName, localHostId);
               //getRandomPeer(hostName,localHostId);
               port = 8091;
            }

            Str_Sprintf(url, sizeof(url), "Location: http://%s:%d/blocks?%40s", hostName, port,
                    sDiskId);

            wr(moved_string);
            wr(date_string);
            wr(url);
            wr("");
            FLUSH();
         } else
            goto error;

      } else if (memcmp(ss->fileName, str_id, strlen(str_id)) == 0) {

         char sSz[64];

         wr(ok_string);
         wr(date_string);
         wr(type_string);

         Str_Sprintf(sSz, sizeof(sSz), "Content-Length: %d", SHA1_HEXED_SIZE_UNTERMINATED);

         LogFS_HashPrint(sHostId, &localHostId);

         wr(sSz);
         wr("");
         FLUSH();

         ignored = write(cs->clientSock, sHostId, SHA1_HEXED_SIZE_UNTERMINATED);
      }
#if 1
      else if (sscanf(ss->fileName, "/peers?%40s", sHostId) == 1
               && LogFS_HashSetString(&hostId, sHostId)) {

         char sLocalHostId[SHA1_HEXED_SIZE];
         char result[4096];
         char *c = result;
         char sSz[32];
         char peerName[MAXHOSTNAME];

         wr(ok_string);
         wr(date_string);
         wr(type_string);

         LogFS_HashPrint(sLocalHostId, &localHostId);
         Str_Sprintf(c, 4096, "%s @ 127.0.0.1\n", sLocalHostId);
         c += strlen(c);

         selectRandomPeers(&c, localHostId, 10);

         Str_Sprintf(sSz, sizeof(sSz), "Content-Length: %d", c - result);

         wr(sSz);
         wr("");
         FLUSH();

         ignored = write(cs->clientSock, result, c - result);
         /*    #include <arpa/inet.h>
          *
          *           const char *inet_ntop(int af, const void *src,
          *                                        char *dst, socklen_t size);
          *
          */

         inet_ntop(AF_INET, &cs->client_addr.sin_addr, peerName,
                   sizeof(peerName));

         if (insertPeer(peerName, hostId) == 0) {
            ++numPeers;
         }
      }
#endif
      else {
 error:
         //printf("bad fileName %s\n",ss->fileName);
         wr(err_string);
         wr(date_string);
         wr(html_type_string);
         wr("");
         wr("<html><head><title>Not Found</title></head><body><p1>NOT FOUND</p1></body></html>\n");
         FLUSH();
      }

   }

   return ss->keepAlive;
}

/* Simple threadpool to handle blocking HTTP serving.  Userworlds do not like
 * it if we spawn pthreads too often, so use this as a workaround.  XXX
 * eventually we should change this to use nonblocking/select IO with fewer
 * threads.
 */

void *handleHttpClients(void *data)
{
   List_Links *list = data;

   for (;;) {
      List_Links *curr;
      UWConnectionState *cs;

      pthread_mutex_lock(&httpClientListLock);

      while (List_IsEmpty(list)) {
         pthread_cond_wait(&httpClientListCv, &httpClientListLock);
      }

      curr = List_First(list);
      cs = List_Entry(curr, UWConnectionState, list);
      List_Remove(curr);

      pthread_mutex_unlock(&httpClientListLock);

      if (handleHttpClient(cs, 1, 1) == 0) {
         close(cs->clientSock);
         free(cs);
      } else {
         pthread_mutex_lock(&httpClientListLock);
         List_Insert(&cs->list, LIST_ATREAR(list));
         pthread_mutex_unlock(&httpClientListLock);
      }
   }

   return NULL;
}

void *httpServer(void *d)
{

   int flag = 1;
   struct sockaddr_in server;

   int sock = socket(PF_INET, SOCK_STREAM, 0);
   setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));

   server.sin_family = AF_INET;
   server.sin_addr.s_addr = htonl(INADDR_ANY);
   server.sin_port = htons(8091);

   if (bind(sock, (struct sockaddr *)&server, sizeof(server)) < 0) {
      fprintf(stderr, "could not bind to port 8091!\n");
      exit(-1);
   }

   for (;;) {
      UWConnectionState *cs;
      int client_addr_len;

      if (listen(sock, 1) < 0) {
         fprintf(stderr, "listen failed\n");
         exit(-1);
      }

      cs = malloc(sizeof(UWConnectionState));
      memset(cs, 0, sizeof(UWConnectionState));

      client_addr_len = sizeof(cs->client_addr);
      cs->clientSock =
          accept(sock, (struct sockaddr *)&cs->client_addr, &client_addr_len);

      if (cs->clientSock >= 0) {

         cs->bytesReceived = 0;
         cs->in = cs->request;
         cs->protocolState = 0;

         memset(&cs->ps, 0, sizeof(HTTPParserState));
         memset(&cs->ss, 0, sizeof(HTTPSession));

         pthread_mutex_lock(&httpClientListLock);
         List_Insert(&cs->list, LIST_ATREAR(&httpClients));
         pthread_mutex_unlock(&httpClientListLock);

         pthread_cond_signal(&httpClientListCv);
      }
   }

   return NULL;
}

#if 0
static size_t voteCallback(void *ptr, size_t size, size_t nmemb, void *d)
{
   LogFS_FetchCommand *cmd = d;
   DiskInfo *info = lookupDisk(cmd->id);
   assert(info);

   char s[SHA1_HEXED_SIZE];
   memcpy(s, ptr, SHA1_HEXED_SIZE - 1);
   s[SHA1_HEXED_SIZE - 1] = '\0';
   Hash fragment;

   if (LogFS_HashSetString(&fragment, s)) {
      printf("got vote %s\n", LogFS_HashShow(&fragment));

      Hash secretView;
      Hash fragmentA = getValue(info->diskId, "fragmentA");
      Hash fragmentB = getValue(info->diskId, "fragmentB");

      if (!LogFS_HashEquals(fragment, fragmentA)) {
         printf("mixing with A\n");
         secretView = LogFS_HashXor(fragmentA, fragment);
      } else {
         printf("mixing with B\n");
         secretView = LogFS_HashXor(fragmentB, fragment);
      }

      if (!LogFS_HashEquals
          (LogFS_HashApply(secretView), getValue(info->diskId, "view")))
         return 0;

      printf("secretView %s\n", LogFS_HashShow(&secretView));

      pthread_mutex_lock(&logMutex);
      pthread_mutex_lock(&info->syncMutex);

      Hash secret;
      if (forceSecret
          (info->logFd, info->diskId, secretView, self->hostId,
           info->proposedView, &secret) == LOG_HEAD_SIZE) {
         assert(0);             // XXX generate new secretView
         printf("forcesecret ok\n");
         info->currentId = getCurrentId(info->diskId);
         info->primaryHostId = self->hostId;
         setSecret(info->diskId, secret, secretView);
      }
      pthread_mutex_unlock(&info->syncMutex);
      pthread_mutex_unlock(&logMutex);

      return size * nmemb;
   }

   return 0;

}
#endif

DiskInfo *lookupDisk(Hash diskId)
{
   DiskInfo *info = NULL;
   List_Links *curr;

   pthread_mutex_lock(&listMutex);

   LIST_FORALL(&diskList, curr) {
      DiskInfo *i = List_Entry(curr, DiskInfo, next);
      if (LogFS_HashEquals(i->diskId, diskId)) {
         info = i;
         break;
      }
   }
   pthread_mutex_unlock(&listMutex);

   return info;
}

static size_t peersCallback(void *ptr, size_t size, size_t nmemb, void *data)
{
   char **result = data;
   memcpy(*result, ptr, nmemb * size);
   *result += nmemb * size;
   return size * nmemb;
}

static void discardBackLog(StreamInfo * stream)
{
   free(stream->backLog);
   stream->backLog = NULL;
   stream->bytesInBackLog = 0;
}

static void replayBackLog(DiskInfo * info, StreamInfo * stream)
{
   char *b = stream->backLog;
   char *c;
   for (c = b; c - b < stream->bytesInBackLog;) {
      Hash parent, id;
      Hash compareWith;

      struct log_head *head = (struct log_head *)c;
      size_t sz = log_entry_size(head);
      LogFS_HashSetRaw(&parent, head->parent);
      LogFS_HashSetRaw(&id, head->id);

      switch (head->tag) {
      case log_entry_type:
         compareWith = LogFS_HashApply(parent);
         break;

      default:
         assert(0);
         break;
      }

      if (LogFS_HashEquals(info->currentId, compareWith)) {
         int r;

         printf("apply %s\n", LogFS_HashShow(&id));

         do {
            r = write(info->logFd, head, sz);
         } while (r < 0 && (errno == EAGAIN || errno == EINTR));

         assert(r == sz);
         info->currentId = id;
      } else
         printf("reject %s\n", LogFS_HashShow(&id));

      c += sz;
   }
   free(stream->backLog);
   stream->backLog = NULL;
   stream->bytesInBackLog = 0;
}

Hash extractSecret(Hash diskId, Hash secretView, Hash entropy)
{
   Hash secret;
   struct sha1_ctx ctx;
   unsigned char tmp[SHA1_DIGEST_SIZE];

   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, secretView.raw);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, entropy.raw);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, tmp);
   LogFS_HashSetRaw(&secret, tmp);

   return secret;
}

void flushUpdate(int fd, struct log_head *head, size_t sz, StreamInfo * stream)
{
   Hash id = LogFS_HashFromRaw(head->id);
   DiskInfo *info = stream->diskInfo;

   int r;
   do {
      r = write(stream->diskInfo->logFd, head, sz);
   } while (r < 0 && (errno == EAGAIN || errno == EINTR));

   if (r >= 0) {
      List_Links *curr;
      info->currentId = id;

      LIST_FORALL(&info->streams, curr) {
         StreamInfo *otherStream = List_Entry(curr, StreamInfo, next);

         if (!LogFS_HashEquals(stream->hostId, localHostId)) {
            if (otherStream->paused &&
                LogFS_HashEquals(otherStream->pausedWaitingFor,
                                 info->currentId)) {
               printf("caught up with other stream, %u bytes\n",
                      otherStream->bytesInBackLog);

               otherStream->paused = FALSE;
               LogFS_HashClear(&otherStream->pausedWaitingFor);
               replayBackLog(info, otherStream);

               assert(r >= 0);
               info->currentId = getCurrentId(info->diskId);
            }
         }
      }

   } else {
      fprintf(stderr, "error writing head with tag %u, id %s\n", head->tag,
              LogFS_HashShow2(LogFS_HashFromRaw(head->id)));
      fflush(stderr);
   }
   return;
}

static size_t data_callback(void *ptr, size_t size, size_t nmemb, void *data)
{
   struct cb_data *d = (struct cb_data *)data;
   struct log_head *head = (struct log_head *)d->buffer;
   StreamInfo *stream = d->stream;
   DiskInfo *info = stream->diskInfo;
   const size_t backLogSize = 0x100000;

   char *in = (char *)ptr;
   int inputleft = size * nmemb;
   void *firstHead = NULL;
   size_t totalSz = 0;

   d->bytesReceived += inputleft;



   pthread_mutex_lock(&info->syncMutex);

   while (inputleft > 0) {
      if (d->state == 1)        // head
      {
#if 0
         if (firstHead == NULL && d->headleft == LOG_HEAD_SIZE) {
            printf("setting firstHead\n");
            firstHead = in;
         }
#endif

         int take = inputleft < d->headleft ? inputleft : d->headleft;

         memcpy(d->buffer + d->buf_offs, in, take);
         d->buf_offs += take;
         in += take;

         d->headleft -= take;
         inputleft -= take;

         if (d->headleft == 0) {
            /* tiny sanity check */
            if (head->tag == log_entry_type
                && head->update.num_blocks > LOG_HEAD_MAX_BLOCKS) {
               Hash id, parent;
               LogFS_HashSetRaw(&id, head->id);
               LogFS_HashSetRaw(&parent, head->parent);
               fprintf(stderr, "too many blocks %u\n", head->update.num_blocks);
               exit(1);
            }

            d->bodyleft = log_body_size(head);

            if (d->bodyleft > LOG_HEAD_MAX_BLOCKS * BLKSIZE) {
               fprintf(stderr, "too large %u\n", d->bodyleft);
               exit(1);
            }

            d->state = 2;
         }
      }

      if (d->state == 2)        // body
      {
         int take = inputleft < d->bodyleft ? inputleft : d->bodyleft;

         memcpy(d->buffer + d->buf_offs, in, take);
         d->buf_offs += take;
         in += take;

         d->bodyleft -= take;
         inputleft -= take;

         if (d->bodyleft == 0) {

            Hash parent;
            Hash id;
            Hash compareWith;


            LogFS_HashSetRaw(&parent, head->parent);
            LogFS_HashSetRaw(&id, head->id);

            d->count++;


            switch (head->tag) {
            case log_entry_type:
               compareWith = LogFS_HashApply(parent);
               break;

            default:
               fprintf(stderr, "bad tag %d\n", head->tag);
               {
                  Hash id, parent;
                  LogFS_HashSetRaw(&id, head->id);
                  LogFS_HashSetRaw(&parent, head->parent);
                  fprintf(stderr, "%d: %s parent %s\n", head->tag,
                          LogFS_HashShow2(id), LogFS_HashShow2(parent));
                  exit(1);

               }
               return 0;
               break;
            }

            if (stream->paused) {
               if (LogFS_HashEquals(compareWith, info->currentId)) {
                  //printf("paused stream starts making sense\n");
                  stream->paused = FALSE;
                  discardBackLog(stream);
                  LogFS_HashClear(&stream->pausedWaitingFor);
               } else {
                  size_t sz = log_entry_size(head);
                  if (sz + stream->bytesInBackLog >= backLogSize) {
                     printf("closing connection!\n");
                     discardBackLog(stream);
                     //pthread_mutex_unlock(&logMutex);
                     pthread_mutex_unlock(&info->syncMutex);
                     return 0;
                  }
                  memcpy(stream->backLog + stream->bytesInBackLog, head,
                         log_entry_size(head));
                  stream->bytesInBackLog += sz;
               }
            }

            if (LogFS_HashIsNull(parent) ||
                LogFS_HashEquals(compareWith, info->currentId) ||
                LogFS_HashEquals(compareWith, getCurrentId(info->diskId))) {
               if (firstHead == NULL) {
                  flushUpdate(info->logFd, (struct log_head *)d->buffer,
                              d->buf_offs, stream);
               } else
                  totalSz += d->buf_offs;
            }

            d->buf_offs = 0;

            d->state = 1;
            d->headleft = LOG_HEAD_SIZE;

         }
      }

   }
   if (firstHead != NULL) {
      printf("flushing %d from firstHead\n", totalSz);
      flushUpdate(info->logFd, firstHead, totalSz, stream);
   }
   pthread_mutex_unlock(&info->syncMutex);

   return size * nmemb;
}

void *diskWorker(void *d)
{
   DiskInfo *info = d;
   char *fn = "/vmfs/volumes/cloudfs/log";

   info->logFd = open(fn, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, (mode_t) 0644);
   if (info->logFd < 0) {
      fprintf(stderr, "WARNING: could not open %s for write\n", fn);
      exit(1);
   }

   for (;;) {
      int running;
      CURLMcode r;

      for (;;) {
         int numMsgs;
 next:
         do
            r = curl_multi_perform(info->curlMultiHandle, &running);
         while (r == CURLM_CALL_MULTI_PERFORM);


         do {
            CURLMsg *msg;
            msg = curl_multi_info_read(info->curlMultiHandle, &numMsgs);
            if (msg) {
               long response;
               curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE,
                                 &response);
               if (response == 404) {
                  curl_multi_remove_handle(info->curlMultiHandle,
                                           msg->easy_handle);
                  curl_multi_add_handle(info->curlMultiHandle,
                                        msg->easy_handle);
                  r = 1;
                  sleep(1);
               }

               if (msg->data.result == CURLE_WRITE_ERROR) {
                  List_Links *curr;
                  LIST_FORALL(&info->streams, curr) {
                     StreamInfo *stream = List_Entry(curr, StreamInfo, next);

                     if (stream->curlHandle == msg->easy_handle) {
                        printf("remove closed handle\n");
                        curl_multi_remove_handle(info->curlMultiHandle,
                                                 stream->curlHandle);
                        curl_easy_cleanup(stream->curlHandle);

                        goto next;
                     }
                  }
               }
            }
         } while (numMsgs);

         if (r == 0) {
            fd_set readSet;
            fd_set writeSet;
            fd_set excSet;
            int maxFd;
            struct timeval tv;
            tv.tv_sec = 3;
            tv.tv_usec = 0;

            FD_ZERO(&readSet);
            FD_ZERO(&writeSet);
            FD_ZERO(&excSet);

            curl_multi_fdset(info->curlMultiHandle, &readSet, &writeSet,
                             &excSet, &maxFd);
            select(maxFd + 1, &readSet, &writeSet, &excSet, &tv);
         }

      }
   }

}

void *replicator(void *a)
{
   const char *sPeerName = a;
   CURL *curl_handle;
   char url[MAXURL];
   char sLocalHostId[SHA1_HEXED_SIZE];
   char result[4096];
   char *c;

   LogFS_HashPrint(sLocalHostId, &localHostId);

   /* init the curl session */
   curl_handle = curl_easy_init();


   for (;;) {
      int i;
      int len;

      Str_Sprintf(url, sizeof(url), "http://%s:8091/peers?%s", sPeerName, sLocalHostId);

      c = result;

      curl_easy_setopt(curl_handle, CURLOPT_URL, url);
      curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, peersCallback);
      curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, &c);
      curl_easy_perform(curl_handle);

      len = c - result;

      for (i = 0; i < len; i++) {
         if (i == 0 || result[i - 1] == '\n') {
            Hash hostId;
            char sHostId[SHA1_HEXED_SIZE];
            char *sHostName = malloc(MAXHOSTNAME);

            if (sscanf(result + i, "%40s @ %s\n", sHostId, sHostName) != 2)
               break;

            LogFS_HashSetString(&hostId, sHostId);

            if (strcmp(sHostName, "127.0.0.1") == 0) {
               Str_Strcpy(sHostName, sPeerName, MAXHOSTNAME);
            }

            if (insertPeer(sHostName, hostId) == 0) {
               printf("new peer %s\n", sHostName);
               ++numPeers;
               createThread(replicator, (void *)sPeerName);
            }
         }

      }

      sleep(10);
   }

   /* cleanup curl stuff */
   curl_easy_cleanup(curl_handle);
   return NULL;
}

void spawnReplicator(char *host)
{
   createThread(replicator, host);
}

typedef struct {
   int num;
   struct {
      Hash a, b;
   } pairs[64];
} HashPairTable;

static int getHashPair(void *pArg, int nArg, char **azArg, char **azCol)
{
   HashPairTable *hpt = pArg;

   int num = hpt->num++;

   LogFS_HashSetString(&hpt->pairs[num].a, azArg[0]);
   LogFS_HashSetString(&hpt->pairs[num].b, azArg[1]);

   return 0;
}

int main(int argc, char **argv)
{
   int i;
   char *sLocalHostId;
   HashPairTable hpt = { 0, };
   pthread_t tid;

   curl_global_init(CURL_GLOBAL_ALL);

   pthread_mutex_init(&listMutex, NULL);
   pthread_mutex_init(&httpClientListLock, NULL);
   pthread_cond_init(&httpClientListCv, NULL);

   initDatabase();

   List_Init(&diskList);
   List_Init(&httpClients);

   if (argc < 3) {
      puts("Usage: replicator host-id HOST1-IP ...");
      exit(1);
   }

   sLocalHostId = argv[1];

   LogFS_HashSetString(&localHostId, sLocalHostId);
   assert(LogFS_HashIsValid(localHostId));

   /* Re-populate peer list anew each time to hack around insertPeer() check */
   SQL(NULL, NULL, "delete from peer;");


   SQL(getHashPair, &hpt, "select diskId,hostId from secretView,viewMember "
       " where secretView.publicView=viewMember.publicView "
       "  and secretView.publicView in "
       "  (select publicView from viewMember where "
       "   hostId='%s') and hostId!='%s'; ", sLocalHostId, sLocalHostId);

   printf("%d existing\n", hpt.num);
   for (i = 0; i < hpt.num; i++) {
      Hash diskId = hpt.pairs[i].a;
      Hash hostId = hpt.pairs[i].b;
      rememberDisk(diskId, hostId);
   }

   pthread_create(&tid, NULL, httpServer, (void *)NULL);

   for (i = 0; i < 8; i++) {
      createThread(handleHttpClients, &httpClients);
   }

   for (i = 1; i < argc - 1; i++) {
      spawnReplicator(argv[1 + i]);
   }
   for (;;)
      sleep(1000);

   /* we're done with libcurl, so clean it up */
   curl_global_cleanup();
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
