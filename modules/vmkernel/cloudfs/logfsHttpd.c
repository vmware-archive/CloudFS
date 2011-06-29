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
#include "logfsNet.h"
#include "metaLog.h"
#include "vDisk.h"
#include "globals.h"
#include "vDiskMap.h"
#include "parseHttp.h"
#include "remoteLog.h"

#include "sched_sysacct.h"
#include "worldlet.h"

extern int sscanf(const char *ibuf, const char *fmt, ...);

static char ok_string[] = "HTTP/1.1 200 OK";
static char no_content[] = "HTTP/1.1 204 No Content";
static char err_string[] = "HTTP/1.1 404 Not Found";
//static char err417_string[] = "HTTP/1.1 417 Expectation Failed";

static char date_string[] = "Date: Tue Jan 15 13:47:48 CET 2007";
static char type_string[] = "Content-type: text/plain";
static char html_type_string[] = "Content-type: text/html";
static char moved_string[] = "HTTP/1.1 301 Moved Permanently";
static char nomod_string[] = "HTTP/1.1 304 Not Modified";
static char space[] = "";

static const char str_heads[] = "/heads";
static const char str_blocks[] = "/blocks";

static Bool httpExit = FALSE;

static const size_t bufferSize = 0x10000;

#define wr(__a) do{strcpy(outBuf,__a); outBuf+=strlen(__a); *outBuf++='\r'; *outBuf++='\n';}while(0);
#define wr2(__a,__l) do{memcpy(outBuf,__a,__l); outBuf+=__l;}while(0);

#define FLUSH() do{ if(outBuf!=cs->outBuf) {\
		status=LogFS_NetWrite(cs->clientSock,cs->outBuf,outBuf-cs->outBuf,NULL);\
		if(status!=VMK_OK) zprintf("BAD FLUSH STATUS %s\n",VMK_ReturnStatusToString(status));\
		outBuf=cs->outBuf;\
} }while(0);

#define all_ok() wr(ok_string); wr(date_string); wr(type_string); wr(space); FLUSH();

static Net_Socket httpdSocket;

typedef struct LogFS_ConnectionState {
   Atomic_uint32 refCount;
   Net_Socket clientSock;
   LogFS_MetaLog *log;
   vmk_Worldlet worldlet;
   HTTPParserState *ps;
   HTTPSession *ss;
   char *request;
   int bytesReceived;
   char *in;
   char *outBuf;
   int *shouldClose;
   Atomic_uint32 protocolState;
    VMK_ReturnStatus(*process) (struct LogFS_ConnectionState * cs, char *in,
                                size_t sz, void *data, size_t * consumed);
   void *data;
} LogFS_ConnectionState;

typedef struct {
   Atomic_uint32 done;
   LogFS_VDisk *vd;
   log_block_t blkno;
   size_t contentLength;
   size_t processed;
   Bool ioStarted;
   char *buf;
   vmk_Worldlet worldlet;
} LogFS_HttpdNetIoState;

typedef struct {
   LogFS_RemoteLog *rl;
   vmk_Worldlet worldlet;
   SG_Array *sgArr;
   size_t count;
   size_t sent;
   Async_Token *token;
} LogFS_HttpdStreamState;

typedef struct {
   LogFS_HttpdNetIoState *ioState;
} LogFS_HttpdIoContext;

static inline LogFS_ConnectionState *LogFS_HttpdGetConnectionState(void *data)
{
   LogFS_ConnectionState *cs = data;
   Atomic_Inc(&cs->refCount);
   return cs;
}

static inline 
void LogFS_HttpdPutConnectionState(LogFS_ConnectionState * cs)
{
   if (Atomic_FetchAndDec(&cs->refCount) == 1) {
      free(cs->request);
      free(cs->outBuf);
      free(cs->ss);
      free(cs->ps);
      free(cs);
   }
}

static void LogFS_HttpdReadDone(Async_Token * token, void *data)
{
   LogFS_HttpdIoContext *c = data;
   LogFS_HttpdNetIoState *sendState = c->ioState;
   sendState->processed = sendState->contentLength;
   Async_ReleaseToken(token);
   vmk_WorldletActivate(sendState->worldlet);
}

static void LogFS_HttpdWriteDone(Async_Token * token, void *data)
{
   ASSERT(token->transientStatus == VMK_OK);

   LogFS_HttpdIoContext *c = data;
   LogFS_HttpdNetIoState *receiveState = c->ioState;

   ASSERT(receiveState);

   Atomic_Write(&receiveState->done, 1);

   Async_ReleaseToken(token);
   vmk_WorldletActivate(receiveState->worldlet);
}

static VMK_ReturnStatus
LogFS_HttpdStreamUpdates(LogFS_ConnectionState * cs,
                         char *in, size_t sz, void *data, size_t * consumed)
{
   VMK_ReturnStatus status;
   int bytesSent;

   *consumed = 0;

   LogFS_HttpdStreamState *streamState = data;
   LogFS_RemoteLog *rl = streamState->rl;

   /* Loop for as long as there are updates queued for transmission, and the
    * socket accepts new data without blocking */

   for (;;) {
      /* Do we need to to pop the next update? */

      if (streamState->count == 0) {

         status = LogFS_RemoteLogPopUpdate(rl, &streamState->sgArr,
                                      &streamState->token);
         if (status != VMK_OK)
            break;

         streamState->count = SG_TotalLength(streamState->sgArr);
         streamState->sent = 0;
      }

      /* We have data to send */

      size_t sum = 0;
      int i;

      SG_Array *sgArr = streamState->sgArr;

      for(i=0; i<sgArr->length; ++i) {

         sum += sgArr->sg[i].length;

         if(sum > streamState->sent) {

            size_t send = sum - streamState->sent;
            size_t offset = sgArr->sg[i].length - send;
            ASSERT(send>0);

            bytesSent = 0;

            if(offset>0) zprintf("send i %d, offset %lu len %lu \n",i,offset,send);
            status = LogFS_NetWrite(cs->clientSock, (void*) (sgArr->sg[i].addr + offset),
                  send, &bytesSent);

            streamState->sent += bytesSent;
            streamState->count -= bytesSent;

            if(status!=VMK_OK) {
               break;
            }
         }

      }

      /* For a partial send, update counters */

      if (status != VMK_OK)
         break;

      /* If everything got sent OK, activate token callback for the last update */

      if (streamState->count == 0) {
         SG_Free(logfsHeap,&streamState->sgArr);
         Async_Token *token = streamState->token;
         token->transientStatus = status;
         Async_TokenCallback(token);
      }

   }

   return status;

}

static VMK_ReturnStatus
LogFS_HttpdSendBlocks(LogFS_ConnectionState * cs,
                      char *in, size_t sz, void *data, size_t * consumed)
{
   VMK_ReturnStatus status;

   LogFS_HttpdNetIoState *sendState = data;

   if (sendState->processed == sendState->contentLength) {
      status =
          LogFS_NetWrite(cs->clientSock, sendState->buf,
                         sendState->contentLength, NULL);

      aligned_free(sendState->buf);
      status = VMK_LIMIT_EXCEEDED;
   } else
      status = VMK_WOULD_BLOCK;

   *consumed = 0;
   return status;
}

static void
LogFS_HttpdTimer(void *data, UNUSED_PARAM(Timer_AbsCycles timestamp))
{
   zprintf("timer triggered\n");
   LogFS_HttpdNetIoState *receiveState = data;
   vmk_WorldletActivate(receiveState->worldlet);
}

static VMK_ReturnStatus
LogFS_HttpdReceiveBlocks(LogFS_ConnectionState * cs,
                         char *in, size_t sz, void *data, size_t * consumed)
{
   VMK_ReturnStatus status;

   LogFS_HttpdNetIoState *receiveState = data;

   ssize_t left;
   size_t take;

   if (Atomic_Read(&receiveState->done) == 1) {
      char *outBuf = cs->outBuf;

      aligned_free(receiveState->buf);
      receiveState->buf = NULL;

      wr(no_content);
      wr(space);
      FLUSH();

      status = VMK_LIMIT_EXCEEDED;  /* no more callbacks needed */
      take = 0;
      goto out;
   }

   left = receiveState->contentLength - receiveState->processed;
   take = MIN(sz, left);

   memcpy(receiveState->buf + receiveState->processed, in, take);
   receiveState->processed += take;

   if (receiveState->processed == receiveState->contentLength) {
      /* Only trigger the first time */
      if (!receiveState->ioStarted) {
         Async_Token *token = Async_AllocToken(0);
         LogFS_HttpdIoContext *c = Async_PushCallbackFrame(token,
                                                           LogFS_HttpdWriteDone,
                                                           sizeof
                                                           (LogFS_HttpdIoContext));

         ASSERT(receiveState->buf);
         c->ioState = receiveState;

         size_t num_blocks = receiveState->contentLength / BLKSIZE;

         status = LogFS_VDiskWrite(receiveState->vd, token,
                                   receiveState->buf, receiveState->blkno,
                                   num_blocks, FS_WRITE_OP | FS_CANTBLOCK);

         if (status == VMK_WOULD_BLOCK) {
            /* Free token and callback */
            Async_FreeCallbackFrame(token);
            Async_ReleaseToken(token);
            /* Set up a timer to make sure we get activated later */
            Timer_Add(MY_PCPU, LogFS_HttpdTimer, 20, TIMER_ONE_SHOT,
                      receiveState);
         } else if (status == VMK_OK)
            receiveState->ioStarted = TRUE;
         else
            zprintf("warning: bad status %s\n",
                    VMK_ReturnStatusToString(status));

         //ASSERT(status==VMK_OK);
      }

      /* No more data to read, block on local write completing */
      status = VMK_WOULD_BLOCK;
   }

   else {
      status = VMK_OK;
   }

 out:
   *consumed = take;
   return status;
}

/* Worldlet for HTTP server processing. Worldlets cannot block, so the socket has been
 * set to nonblocking mode prior to getting here. */

static VMK_ReturnStatus
LogFS_HttpResponseWorldlet(vmk_Worldlet wdt, void *data,
                           vmk_WorldletRunData* runData)
{
   VMK_ReturnStatus status = VMK_OK;

   LogFS_ConnectionState *cs = LogFS_HttpdGetConnectionState(data);

   if (Atomic_Read(&cs->protocolState) == 2) {
      Worldlet_Disable((Worldlet *) cs->worldlet);
      runData->state = VMK_WDT_RELEASE;

      LogFS_HttpdPutConnectionState(cs);
      goto out;
   }

   Bool keepAlive = TRUE;

   char *outBuf = cs->outBuf;

   ASSERT(cs);

   HTTPParserState *ps = cs->ps;
   HTTPSession *ss = cs->ss;
   LogFS_VDisk *vd;
   uint64 lsn = ~0ULL;

   while (keepAlive && Net_CheckSocket(cs->clientSock, DEFAULT_STACK) == 0) {
      /* If we were activated with a processing handler set, call that first */

      if (cs->process != NULL) {
         size_t consumed;
         status =
             cs->process(cs, cs->in, cs->bytesReceived, cs->data, &consumed);

         cs->in += consumed;
         cs->bytesReceived -= consumed;

         if (status == VMK_LIMIT_EXCEEDED) {
            cs->process = NULL;

            ASSERT(cs->data);
            free(cs->data);
            cs->data = NULL;

            goto nextHeader;
         } else if (status == VMK_WOULD_BLOCK) {
            goto yield;
         } else if (status != VMK_OK) {
            zprintf("unhandled status %s\n", VMK_ReturnStatusToString(status));
            break;
         }
      }

      /* Do we need to read more data off the network? This is the only place we read from the socket,
       * to avoid doing WOULD_BLOCK error-handling elsewhere */

      if (cs->bytesReceived == 0) {
         cs->in = cs->request;
         status =
             LogFS_NetRead(cs->clientSock, cs->request, bufferSize,
                           &cs->bytesReceived);

         if (status == VMK_WOULD_BLOCK) {
            ASSERT(cs->bytesReceived == 0);
            /* Yield for more data */

            goto yield;
         }

         if (status != VMK_OK || cs->bytesReceived == 0) {
            break;
         }

      }

      /* In header state? */

      if (Atomic_Read(&cs->protocolState) == 0) {
         Bool more = parseHttp(ps, cs->in, cs->bytesReceived, ss);

         cs->in += ps->justParsed;
         cs->bytesReceived -= ps->justParsed;

         if (!more) {
            if (ss->complete) {
               Atomic_Inc(&cs->protocolState);
            } else {
               zprintf("PARSER ERROR '%s'\n", cs->in - ps->justParsed);
               break;
            }

         }
      }

      /* In body state? */

      if (Atomic_Read(&cs->protocolState) == 1) {
         /* Produce an adequate response for the request. If further processing is required, set cs->process
          * to handle that */

         if (cs->process == NULL) {
            char sDiskId[SHA1_HEXED_SIZE];
            char sSecret[SHA1_HEXED_SIZE];
            char sSecretView[SHA1_HEXED_SIZE];
            Hash diskId,secret,secretView;

            /* List all VDisks and their current versions. Used only for
             * debugging purposes */

            char *sz_str = (char *)malloc(64);
            if (strncmp(ss->fileName, str_heads, sizeof(str_heads)) == 0) {
               size_t sz;
               char *work = aligned_malloc(0x2000);

               wr(ok_string);
               wr(date_string);
               wr(type_string);

               extern size_t LogFS_DiskMapListDisks(char *out);
               sz = LogFS_DiskMapListDisks(work);
               work[sz] = '\0';

               sprintf(sz_str, "Content-Length: %lu", sz);
               wr(sz_str);
               wr(space);
               wr2(work, sz);
               FLUSH();

               aligned_free(work);

               keepAlive &= FALSE;
            }

            else if( sscanf(ss->fileName, "/setsecret?%40s&%40s&%40s", sDiskId, sSecret, sSecretView) == 3 
                  && LogFS_HashSetString(&diskId, sDiskId)
                  && LogFS_HashSetString(&secret, sSecret)
                  && LogFS_HashSetString(&secretView, sSecretView)
                  && (vd=LogFS_DiskMapLookupDisk(diskId))  ) {

               zprintf("setting secret\n");
               status = LogFS_VDiskSetSecret(vd,secret,secretView);

               wr(ok_string);
               wr(date_string);
               wr(type_string);

               sprintf(sz_str, "Content-Length: 0");
               wr(sz_str);
               wr(space);
               FLUSH();

               keepAlive &= FALSE;
            }

            else if( sscanf(ss->fileName, "/getsecret?%40s", sDiskId) == 1 
                  && LogFS_HashSetString(&diskId, sDiskId)
                  && (vd=LogFS_DiskMapLookupDisk(diskId))  ) {

               zprintf("getting secret\n");
               status = LogFS_VDiskGetSecret(vd,&secret,FALSE);

               if (status==VMK_OK) {
                  wr(ok_string);
                  wr(date_string);
                  wr(type_string);
                  LogFS_HashPrint(sSecret,&secret);

                  sprintf(sz_str, "Content-Length: 40");
                  wr(sz_str);
                  wr(space);
                  wr(sSecret);
               }
               FLUSH();

               keepAlive &= FALSE;
            }

            else if( sscanf(ss->fileName, "/lsn?%40s", sDiskId) == 1 
                  && LogFS_HashSetString(&diskId, sDiskId)
                  && (vd=LogFS_DiskMapLookupDisk(diskId))  ) {

               char sLsn[32];
               sprintf(sLsn,"%lu",vd->lsn);

               wr(ok_string);
               wr(date_string);
               wr(type_string);
               LogFS_HashPrint(sSecret,&secret);

               sprintf(sz_str, "Content-Length: %lu",strlen(sLsn));
               wr(sz_str);
               wr(space);
               wr(sLsn);
               FLUSH();

               keepAlive &= FALSE;
            }

            else if( sscanf(ss->fileName, "/stream?%40s&%lu", sDiskId, &lsn) == 2 
                  && LogFS_HashSetString(&diskId, sDiskId)
                  && (vd=LogFS_DiskMapLookupDisk(diskId))  ) {

               /* Create a remoteLog to hold the state of the stream. If necesarry, the remoteLog
                * will sync up from disk in a separate thread, and when it catches up to the head
                * version will switch over to synchronus mode. Every time an update is ready the
                * worldlet we are running in will get activated, and the LogFS_HttpdStreamUpdates
                * process handler will handle the actual sending of the data down the socket */

               LogFS_RemoteLog *rl =
                   (LogFS_RemoteLog *)malloc(sizeof(LogFS_RemoteLog));
               LogFS_RemoteLogInit(rl, cs->log, cs->worldlet, vd, lsn);

               LogFS_HttpdStreamState *streamState =
                   malloc(sizeof(LogFS_HttpdStreamState));
               streamState->worldlet = cs->worldlet;
               streamState->rl = rl;
               streamState->sgArr = NULL;
               streamState->count = 0;
               streamState->sent = 0;
               streamState->token = NULL;

               cs->process = LogFS_HttpdStreamUpdates;
               cs->data = streamState;
               cs->shouldClose = &rl->shouldClose;

               keepAlive &= TRUE;

               /* Restart loop to make sure handler gets called within the current activation */

               continue;
            }

            else if (strncmp(ss->fileName, str_blocks, strlen(str_blocks)) == 0) {

               char sDiskId[SHA1_HEXED_SIZE];

               sscanf(ss->fileName, "/blocks?%40s", sDiskId);

               log_block_t blkno = ss->from / BLKSIZE;
               log_block_t to = ss->to / BLKSIZE;
               ASSERT(blkno <= to);
               size_t num_blocks = to - blkno;

               if (num_blocks == 0) {
                  zprintf("NO RANGE HEADER\n");
                  num_blocks = ss->contentLength / BLKSIZE;
               }

               LogFS_Hash diskId;
               LogFS_HashSetString(&diskId, sDiskId);

               LogFS_VDisk *vd = LogFS_DiskMapLookupDisk(diskId);

               /* We can only serve the IO if the we know the VDisk, and if we
                * are the current primary for it */

               if (vd != NULL && LogFS_VDiskIsWritable(vd)) {
                  /* First check if the other host possesses an up to date
                   * replica already, as in this case its easier just to
                   * turn over control and let it process the IO locally.
                   * */

                  Hash id = LogFS_VDiskGetCurrentId(vd);

                  Hash secret;
                  LogFS_HashClear(&secret);

                  if (LogFS_HashEquals(id, ss->id) &&
                      (LogFS_VDiskGetSecret(vd, &secret, TRUE) == VMK_OK ||
                       ss->verb == HTTP_GET)
                      ) {
                     zprintf("sending 304\n");
                     wr(nomod_string);
                     wr(date_string);

                     /* In case we managed to extract the secret, 
                      * return it the client in an HTTP header */

                     if (LogFS_HashIsValid(secret)) {
                        zprintf("give away secret %s\n",
                                LogFS_HashShow(&secret));

                        char s[128];
                        char sSecret[SHA1_HEXED_SIZE];

                        LogFS_HashPrint(sSecret, &secret);

                        /* The secret view will have been pre-shared */
                        sprintf(s, "Secret: \"%s\"", sSecret);
                        wr(s);
                     }

                     wr(space);
                     FLUSH();
                  }

                  else if (ss->verb == HTTP_GET) {
                     char eTag[64];
                     char sId[SHA1_HEXED_SIZE];
                     LogFS_HashPrint(sId, &id);

                     sprintf(eTag, "ETag: \"%s\"", sId);

                     if (!LogFS_HashEquals(id, ss->id)) {
                        if (num_blocks > 0) {
                           LogFS_HttpdNetIoState *sendState =
                               malloc(sizeof(LogFS_HttpdNetIoState));

                           sendState->worldlet = cs->worldlet;
                           sendState->vd = vd;
                           sendState->blkno = blkno;
                           sendState->contentLength = BLKSIZE * num_blocks;
                           sendState->processed = 0;
                           sendState->buf =
                               aligned_malloc(sendState->contentLength);
                           sendState->ioStarted = FALSE;
                           Atomic_Write(&sendState->done, 0);

                           cs->process = LogFS_HttpdSendBlocks;
                           cs->data = sendState;

                           /* Start an IO to read the blocks into sendState->buf */

                           Async_Token *token = Async_AllocToken(0);

                           LogFS_HttpdIoContext *c =
                               Async_PushCallbackFrame(token,
                                                       LogFS_HttpdReadDone,
                                                       sizeof
                                                       (LogFS_HttpdIoContext));

                           c->ioState = sendState;

                           /* Stealing the skipzero flag to mean don't forward
                            * this IO across the network, which might result in a 
                            * forwarding loop */

                           status =
                               LogFS_VDiskRead(sendState->vd, token,
                                               sendState->buf, blkno,
                                               num_blocks,
                                               FS_READ_OP | FS_SKIPZERO |
                                               FS_CANTBLOCK);

                           /* If the vd is not read/writable, the read will
                            * fail and we must clean up. The bad status code
                            * will trigger a redirect below.  */

                           if (status != VMK_OK) {
                              cs->process = NULL;
                              cs->data = NULL;

                              free(sendState);
                              Async_ReleaseToken(token);
                           }
                        } else {
                           status = VMK_OK;
                        }

                        if (status == VMK_OK) {
                           wr(ok_string);
                           wr(date_string);
                           wr(type_string);
                           wr(eTag);
                           sprintf(sz_str, "Content-Length: %lu",
                                   num_blocks * BLKSIZE);
                           wr(sz_str);
                           wr(space);
                           FLUSH();
                        }

                        /* The read may have already returned, so make sure the process handler is called if set */

                        if (cs->process)
                           continue;

                     }

                  } else if (ss->verb == HTTP_PUT) {
                     if (LogFS_VDiskIsWritable(vd)) {
                        wr("HTTP/1.1 100 Continue");
                        wr(space);
                        FLUSH();

                        LogFS_HttpdNetIoState *receiveState =
                            malloc(sizeof(LogFS_HttpdNetIoState));

                        receiveState->worldlet = cs->worldlet;
                        receiveState->vd = vd;
                        receiveState->blkno = blkno;
                        receiveState->contentLength = BLKSIZE * num_blocks;
                        receiveState->processed = 0;
                        receiveState->buf =
                            aligned_malloc(receiveState->contentLength);
                        receiveState->ioStarted = FALSE;
                        Atomic_Write(&receiveState->done, 0);

                        cs->process = LogFS_HttpdReceiveBlocks;
                        cs->data = receiveState;

                        /* So far it's all looking good. We may have lost
                         * control of the VDisk before the client has had time
                         * to return any data for us to write, so we have to be
                         * prepared to deal with that. However, it's better
                         * than trying to reserve the vd, because the client
                         * may crash etc. which will force us to hold on to the
                         * VDisk forever.
                         *
                         * XXX add error handling if VDiskWrite() fails. */

                        status = VMK_OK;
                     } else {
                        zprintf("status conflict\n");
                        status = VMK_RESERVATION_CONFLICT;
                     }

                  }
               } else {
                  status = VMK_RESERVATION_CONFLICT;
               }

               /* For some reason cannot deal with the request here. Punt
                * client back to his own user world daemon */

               if (vd == NULL || status != VMK_OK) {
                  char url[128];
                  sprintf(url, "Location: http://127.0.0.1:8091/blocks?%40s",
                          sDiskId);
                  wr(moved_string);
                  wr(date_string);
                  wr(url);
                  wr(space);
                  FLUSH();

                  keepAlive &= FALSE;
               }

            }

            else {

               zprintf("bad fileName %s\n", ss->fileName);
               wr(err_string);
               wr(date_string);
               wr(html_type_string);
               wr("Content-Length: 0");
               wr("Connection: close");
               wr(space);
               FLUSH();

               keepAlive &= FALSE;
            }

         }

         /* If getting here, we have parsed the body and are ready for the next header */

 nextHeader:
         if (cs->process == NULL) {
            Atomic_Write(&cs->protocolState, 0);   /* Ready to parse next HTTP request */

            keepAlive &= ss->keepAlive;

            memset(ps, 0, sizeof(HTTPParserState));
            memset(ss, 0, sizeof(HTTPSession));
         }

      }

   }

   /* If we get here the connection should be closed */

   if (Atomic_ReadWrite(&cs->protocolState, 2) != 2) {
      if(cs->shouldClose) {
         *(cs->shouldClose) = 1;
      }
      Net_CloseSocket(cs->clientSock, DEFAULT_STACK);
   }

   runData->state = VMK_WDT_READY;
   goto out;

 yield:
   runData->state = VMK_WDT_SUSPEND;

 out:

   LogFS_HttpdPutConnectionState(cs);
   return VMK_OK;
}

static void LogFS_ReceiveCallback(Net_Socket so, void *data, int unused)
{
   LogFS_ConnectionState *cs = LogFS_HttpdGetConnectionState(data);
   vmk_WorldletActivate(cs->worldlet);

   LogFS_HttpdPutConnectionState(cs);
}

static void LogFS_SendCallback(Net_Socket so, void *data, int unused)
{
   LogFS_ConnectionState *cs = LogFS_HttpdGetConnectionState(data);
   vmk_WorldletActivate(cs->worldlet);

   LogFS_HttpdPutConnectionState(cs);
}

static void LogFS_Listen(void *data)
{
   LogFS_MetaLog *log = (LogFS_MetaLog *)data;

   VMK_ReturnStatus status = VMK_OK;

   while (!httpExit) {
      int events;

      events = Net_PollSocket(httpdSocket, NET_SOCKET_POLLIN, DEFAULT_STACK);

      if (events > 0) {
         Net_Socket clientSock;
         sockaddr_in_bsd addr;
         int addrLen = sizeof addr;

         status =
             Net_Accept(httpdSocket, FALSE, (struct sockaddr *)&addr, &addrLen,
                        &clientSock, DEFAULT_STACK);

         if (status == VMK_OK) {
            status = LogFS_NetSetSocketOptions(clientSock);
            if (status != VMK_OK) {
               zprintf("could not set window size %s\n",
                       VMK_ReturnStatusToString(status));
            }

            LogFS_ConnectionState *cs = malloc(sizeof(LogFS_ConnectionState));
            Atomic_Write(&cs->refCount, 1);
            cs->shouldClose = NULL;

#if 1

#ifndef SO_NONBLOCKING
#define SO_NONBLOCKING    0x1015
#endif
            int optVal = 1;
            status = Net_SetSockOpt(clientSock, SOL_SOCKET,
                                    SO_NONBLOCKING, &optVal, sizeof(optVal),
                                    DEFAULT_STACK);

            if (status != VMK_OK) {
               printf("Non-blocking sockopt failed: %s",
                      VMK_ReturnStatusToString(status));
            }
#endif

            HTTPParserState *ps = malloc(sizeof(HTTPParserState));
            HTTPSession *ss = malloc(sizeof(HTTPSession));
            memset(ps, 0, sizeof(HTTPParserState));
            memset(ss, 0, sizeof(HTTPSession));

            cs->clientSock = clientSock;
            cs->log = log;
            cs->ss = ss;
            cs->ps = ps;
            cs->request = malloc(bufferSize);
            cs->outBuf = malloc(0x1000);
            cs->in = cs->request;

            vmk_Name wdtName;
            vmk_NameFormat(&wdtName, "httpResponse");


            status = vmk_WorldletCreate(&cs->worldlet, &wdtName,
                  SCHED_SYS_ACCT_TYPE_OTHER,
                  World_GetLastModID(),
                  logfsHeap,
                  LogFS_HttpResponseWorldlet, cs);
            ASSERT(status == VMK_OK);

            status = Net_RegisterSocketRecvBufferCallback(cs->clientSock,
                                                          LogFS_ReceiveCallback,
                                                          cs, DEFAULT_STACK);

            status = Net_RegisterSocketSendBufferCallback(cs->clientSock,
                                                          LogFS_SendCallback,
                                                          cs, FALSE,
                                                          DEFAULT_STACK);

            vmk_WorldletActivate(cs->worldlet);
         }
      }

      else {
         World_SelectBlock();
      }
   }

   Net_CloseSocket(httpdSocket, DEFAULT_STACK);

   zprintf("exit http listen thread\n");

   httpExit = FALSE;
   World_Exit(VMK_OK);

}

static World_ID serverWorld;

VMK_ReturnStatus LogFS_InitHttpd(LogFS_MetaLog *ml)
{
   VMK_ReturnStatus status;

   LogFS_RemoteLogPreInit();

   printf("creating socket!\n");
   status = LogFS_NetCreateAcceptSocket(&httpdSocket);
   ASSERT(status == VMK_OK);

   status = World_NewSystemWorld("repServer", 0, WORLD_GROUP_DEFAULT,
                                 NULL, SCHED_GROUP_PATHNAME_DRIVERS,
                                 &serverWorld);
   ASSERT(status == VMK_OK);

   Sched_Add(World_Find(serverWorld), LogFS_Listen, (void *)ml);

   return status;
}

void LogFS_CleanupHttp(void)
{
   httpExit = TRUE;
   while (httpExit) {
      World_SelectWakeup(serverWorld);
      CpuSched_Sleep(1);
   }
}
