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
#include "system.h"
#include "globals.h"
#include "logfsNet.h"

#include "globals.h"
#include "metaLog.h"

#include "logfsHttpClient.h"
#include "parseHttp.h"

#include "vDisk.h"
#include "vDiskMap.h"

#define wr(__a) do{strcpy(outBuf,__a); outBuf+=strlen(__a); *outBuf++='\r'; *outBuf++='\n';}while(0);
#define wr2(__a,__l) do{memcpy(outBuf,__a,__l); outBuf+=__l;}while(0);

#define FLUSH() do{ if(outBuf!=outBuf0) {\
		status=LogFS_NetWrite(he->clientSock,outBuf0,outBuf-outBuf0,NULL);\
		if(status!=VMK_OK) zprintf("BAD FLUSH STATUS %s\n",VMK_ReturnStatusToString(status));\
		outBuf=outBuf0;\
} }while(0);

static char space[] = "";

static List_Links demandFetchWaiters;
static List_Links demandFetchIds;

static SP_SpinLock demandFetchLock;

typedef struct {
   Async_Token *token;
   Hash diskId;
   Hash id;
   char *buf;
   log_block_t blkno;
   size_t num_blocks;
   int flags;

   uint32 host;
   uint16 port;

   int retries;

   List_Links next;
} LogFS_HttpClientFetch;

typedef struct HTTPConnection {
   Hash diskId;

   Bool isOpen;
   Net_Socket clientSock;

   //uint32 host;
   //uint16 port;
   struct HTTPConnection *next;

} HTTPConnection;

static HTTPConnection *hashTable[0x10000];
static HTTPConnection entries[0x1000];
static int numHashEntries = 0;

static void resetHashTable(void)
{
   numHashEntries = 0;
   memset(hashTable, 0, sizeof(hashTable));
}

static inline void redirectToLocal(LogFS_HttpClientFetch * fetch)
{
   fetch->host = htonl(0x7f000001);
   fetch->port = htons(8091);
}

/* Lookup an existing connection, XXX use a smaller hash table */

HTTPConnection *lookupConnection(Hash diskId)
{
   uint16_t key = *((uint16_t *)diskId.raw);

   HTTPConnection *he = hashTable[key];
   HTTPConnection *prev = NULL;

   while (he != NULL && !LogFS_HashEquals(he->diskId, diskId)) {
      prev = he;
      he = he->next;
   }

   /* New value */

   if (he == NULL) {
      he = &entries[numHashEntries++];
      if (prev == NULL) {
         hashTable[key] = he;
      } else {
         prev->next = he;
      }

      /* Create connection state */

      he->diskId = diskId;
      he->isOpen = FALSE;

      he->next = NULL;
   }
   ASSERT(he);
   return he;
}

void
LogFS_HttpClientAckFetch(LogFS_HttpClientFetch * fetch, VMK_ReturnStatus status)
{
   SP_Lock(&demandFetchLock);
   List_Remove(&fetch->next);
   SP_Unlock(&demandFetchLock);

   Async_Token *token = fetch->token;
   token->transientStatus = status;
   Async_TokenCallback(token);
   free(fetch);
}

static Bool httpClientExit = FALSE;

static void LogFS_HttpClientThread(void *data)
{
   VMK_ReturnStatus status = VMK_OK;

   char *outBuf0 = malloc(0x1000);
   char *outBuf = outBuf0;

   HTTPParserState *ps = malloc(sizeof(HTTPParserState));
   HTTPSession *ss = malloc(sizeof(HTTPSession));

   while(!httpClientExit) {
      LogFS_HttpClientFetch *fetch = NULL;
      List_Links *elem;

      SP_Lock(&demandFetchLock);

      if (!List_IsEmpty(&demandFetchIds)) {
         elem = List_First(&demandFetchIds);
         fetch = List_Entry(elem, LogFS_HttpClientFetch, next);
         ++(fetch->retries);
      }

      else {
         status =
             CpuSched_Wait(&demandFetchWaiters, CPUSCHED_WAIT_SCSI,
                           &demandFetchLock);
         continue;
      }

      /* See if there is an existing connection that we can use, or get the description for a fresh one */

      HTTPConnection *he = lookupConnection(fetch->diskId);

      SP_Unlock(&demandFetchLock);

      if (fetch->retries > 40) {
         zprintf("too many redirects\n");
         LogFS_HttpClientAckFetch(fetch,
                                  (fetch->
                                   flags & FS_READ_OP) ? VMK_READ_ERROR :
                                  VMK_WRITE_ERROR);
         fetch = NULL;

         continue;
      }

      if (!he->isOpen) {
         status = Net_CreateSocket(PF_INET, SOCK_STREAM, IPPROTO_TCP,
                                   &he->clientSock, DEFAULT_STACK);

         if (status != VMK_OK) {
            if (status == VMK_NOT_SUPPORTED) {
               Panic
                   ("Can't create client clientSock.  Is the TCP/IP module loaded?");
            } else {
               Panic("Can't create client clientSock: %s",
                     VMK_ReturnStatusToString(status));
            }
         }
         status = LogFS_NetSetSocketOptions(he->clientSock);
         if (status != VMK_OK) {
            zprintf("could not set window size %s\n",
                    VMK_ReturnStatusToString(status));
         }

         struct timeval setTimeo;

         setTimeo.tv_sec = 2;
         setTimeo.tv_usec = 0;

         status = Net_SetSockOpt(he->clientSock, SOL_SOCKET, SO_RCVTIMEO,
                                 (void *)&setTimeo, sizeof setTimeo,
                                 DEFAULT_STACK);
         if (status != VMK_OK) {
            LOG(0, "Net_SetSockOpt(SO_RCVTIMEO) failed (%s)",
                VMK_ReturnStatusToString(status));
         }

         status = Net_SetSockOpt(he->clientSock, SOL_SOCKET, SO_SNDTIMEO,
                                 (void *)&setTimeo, sizeof setTimeo,
                                 DEFAULT_STACK);
         if (status != VMK_OK) {
            LOG(0, "Net_SetSockOpt(SO_SNDTIMEO) failed (%s)",
                VMK_ReturnStatusToString(status));
         }

         sockaddr_in_bsd serverAddr;
         memset(&serverAddr, 0, sizeof(serverAddr));
         serverAddr.sin_addr.s_addr = fetch->host;
         serverAddr.sin_len = sizeof(serverAddr);
         serverAddr.sin_family = AF_INET;
         serverAddr.sin_port = fetch->port;

         status = Net_ConnectSocket(he->clientSock,
                                    (struct sockaddr *)&serverAddr,
                                    (int)sizeof(serverAddr), DEFAULT_STACK);

         if (status == VMK_OK)
            he->isOpen = TRUE;
         else {
            zprintf("Connect to %x,%u : %s\n", ntohl(fetch->host),
                    ntohs(fetch->port), VMK_ReturnStatusToString(status));
            redirectToLocal(fetch);
         }

      }

      if (status == VMK_OK) {
         char *cmd = malloc(256);
         char sDiskId[SHA1_HEXED_SIZE];
         int bytesReceived = 0;

         LogFS_HashPrint(sDiskId, &fetch->diskId);

         sprintf(cmd, "%s /blocks?%40s HTTP/1.1",
                 (fetch->flags & FS_WRITE_OP) ? "PUT" : "GET", sDiskId);
         wr(cmd);
         char range[48];
         sprintf(range, "Range: bytes=%lu-%lu", BLKSIZE * fetch->blkno,
                 BLKSIZE * (fetch->blkno + fetch->num_blocks));
         wr(range);

         wr("Connection: keep-alive");
         wr("Host: 127.0.0.1:8091");
         wr("Accept: */*");

         /* Tell the other side which version we have, it will respond with
          * a 204 if we can safely read that instead */

         Bool speculativeWrite;

         if (LogFS_HashIsValid(fetch->id)) {
            char sId[SHA1_HEXED_SIZE];
            LogFS_HashPrint(sId, &fetch->id);
            char sIfIdStr[64];
            sprintf(sIfIdStr, "If-None-Match: \"%s\"", sId);
            wr(sIfIdStr);

            speculativeWrite = FALSE;
         } else
            speculativeWrite = TRUE;

         wr(space);             /* end of headers */
         FLUSH();

         /* We expect to get an 100 Continue response, unless we sent the
          * If-None-Match header, in which case we expect a 304 with the
          * secret for the volume */

         if (fetch->flags & FS_WRITE_OP && speculativeWrite) {
            int bytesWritten;
            status =
                LogFS_NetWrite(he->clientSock, fetch->buf,
                               BLKSIZE * fetch->num_blocks, &bytesWritten);
         }

         const size_t bufferSize = 0x10000;
         char *response = malloc(bufferSize);
         char *c = NULL;

 nextResponse:

         /* Initialize parser state to zero */
         memset(ps, 0, sizeof(HTTPParserState));
         memset(ss, 0, sizeof(HTTPSession));

         for (;;) {
            if (bytesReceived == 0) {
               status =
                   LogFS_NetRead(he->clientSock, response, bufferSize,
                                 &bytesReceived);
               c = response;
            }

            if (status != VMK_OK || bytesReceived == 0) {
               break;
            }

            if (status != VMK_OK) {
               zprintf("read status %s\n", VMK_ReturnStatusToString(status));
               NOT_REACHED();
            }

            int more = parseHttp(ps, c, bytesReceived, ss);
            bytesReceived -= ps->justParsed;
            c += ps->justParsed;

            if (!more)
               break;

         }

         if (ss->complete) {
            if (ss->status == 204) {
               if (fetch->flags & FS_WRITE_OP) {
                  LogFS_HttpClientAckFetch(fetch, status);
                  fetch = NULL;
               }
            }

            else if (ss->status == 301) {
               zprintf("redirected to host: %s port: %u\n", ss->hostName,
                       ss->port);
               if (ss->hostName) {
                  inet_pton4(ss->hostName, &fetch->host);
                  fetch->port = htons(ss->port);
               } else
                  redirectToLocal(fetch);

               status = Net_CloseSocket(he->clientSock, DEFAULT_STACK);
               he->isOpen = FALSE;
            }

            else if (ss->status == 304) {
               /* Server says go ahead and use your own cached copy of the data */

               LogFS_VDisk *vd = LogFS_DiskMapLookupDiskForVersion(fetch->id);

               if (vd != NULL) {
                  if (LogFS_HashIsValid(ss->secret)) {
                     zprintf("gave me secret!\n");
                     LogFS_VDisk *vd =
                         LogFS_DiskMapLookupDiskForVersion(fetch->id);
                     if (vd != NULL) {
                        status =
                            LogFS_VDiskSetSecret(vd, ss->secret,
                                                 vd->secretView);
                        ASSERT(status == VMK_OK);
                     }
                  }

                  if (fetch->flags & FS_READ_OP) {
                     ASSERT(fetch->token->refCount > 0);
                     status =
                         LogFS_VDiskContinueRead(vd, fetch->token, fetch->buf,
                                                 fetch->blkno,
                                                 fetch->num_blocks,
                                                 fetch->flags);
                  } else if (fetch->flags & FS_WRITE_OP) {
                     if (LogFS_VDiskIsWritable(vd)) {
                        status = LogFS_VDiskWrite(vd, fetch->token, fetch->buf,
                                                  fetch->blkno,
                                                  fetch->num_blocks,
                                                  fetch->flags);
                     } else {
                        Panic("error: 304 for PUT but not secret returned");
                        status = VMK_WRITE_ERROR;
                     }
                  }

                  if (status == VMK_OK) {
                     SP_Lock(&demandFetchLock);
                     List_Remove(elem);
                     free(fetch);
                     SP_Unlock(&demandFetchLock);
                  } else
                     Panic("local cache status %s",
                           VMK_ReturnStatusToString(status));
               } else {
                  /* Clear fetch->id to avoid a possibly infinite loop for a heavily mutated volume */
                  LogFS_HashClear(&fetch->id);
               }
            }

            else if (ss->status == 200) {
               if (fetch->flags & FS_READ_OP) {
                  char *buf = fetch->buf;

                  size_t left = BLKSIZE * fetch->num_blocks;

                  for (;;) {
                     size_t take = MIN(bytesReceived, left);

                     assert(take <= bufferSize);
                     memcpy(buf, c, take);
                     buf += take;
                     left -= take;

                     if (left == 0)
                        break;

                     //zprintf("reading %lu, %lu left\n",MIN(left,bufferSize),left);
                     status =
                         LogFS_NetRead(he->clientSock, response,
                                       MIN(left, bufferSize), &bytesReceived);
                     c = response;

                     if (status != VMK_OK || bytesReceived == 0) {
                        zprintf("break early\n");
                        break;
                     }
                  }
               }

               LogFS_HttpClientAckFetch(fetch, status);
               fetch = NULL;
            }

            else if (ss->status == 100) {
               if (speculativeWrite == FALSE && (fetch->flags & FS_WRITE_OP)) {
                  int bytesWritten;
                  status =
                      LogFS_NetWrite(he->clientSock, fetch->buf,
                                     BLKSIZE * fetch->num_blocks,
                                     &bytesWritten);
               }
               goto nextResponse;
            }
         }

         else {
            zprintf("incomplete or server error %d!\n", ss->status);
            zprintf("response (c) '%s'\n", c);

            redirectToLocal(fetch);

            status = Net_CloseSocket(he->clientSock, DEFAULT_STACK);
            he->isOpen = FALSE;
         }

         free(response);
         free(cmd);

      }

   }
   free(ss);
   free(ps);
   free(outBuf);
   httpClientExit = FALSE;
   World_Exit(VMK_OK);
}

static World_ID clientWorld;

VMK_ReturnStatus LogFS_InitHttpClient(void)
{
   VMK_ReturnStatus status;

   SP_InitLock("demandFetchLock", &demandFetchLock, SP_RANK_DEMANDFETCH);

   List_Init(&demandFetchWaiters);
   List_Init(&demandFetchIds);

   status = World_NewSystemWorld("httpClient", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &clientWorld);
   ASSERT(status == VMK_OK);

   Sched_Add(World_Find(clientWorld), LogFS_HttpClientThread, NULL);

   resetHashTable();

   return status;
}

void LogFS_CleanupHttpClient(void)
{
   httpClientExit = TRUE;
   while (httpClientExit) {
      CpuSched_Wakeup(&demandFetchWaiters);
      CpuSched_Sleep(1);
   }
}

VMK_ReturnStatus
LogFS_HttpClientRequest(Hash diskId,
                        Hash id,
                        Async_Token * token,
                        char *buf,
                        log_block_t blkno, size_t num_blocks, int flags)
{
   //VMK_ReturnStatus status;

   LogFS_HttpClientFetch *fetch = malloc(sizeof(LogFS_HttpClientFetch));
   fetch->diskId = diskId;
   fetch->id = id;
   ASSERT(token);
   ASSERT(token->refCount > 0);
   fetch->token = token;
   fetch->buf = buf;
   fetch->blkno = blkno;
   fetch->num_blocks = num_blocks;
   fetch->flags = flags;
   fetch->retries = 0;

   /* IOs initially go to user world on localhost, which will respond with a
    * 301 redirect to the current location. This frees the kernel module from
    * having to know about any high-level cluster topology stuff, while keeping
    * the IO path kernel-to-kernel */

   redirectToLocal(fetch);

   SP_Lock(&demandFetchLock);

   List_Insert(&fetch->next, LIST_ATREAR(&demandFetchIds));
   CpuSched_Wakeup(&demandFetchWaiters);

   SP_Unlock(&demandFetchLock);

   return VMK_OK;
}
