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
#include "parseHttp.h"

/* Parse a URL into hostname, port number, and filename portions.  Upon return,
 * hostName and fileName will point to the relevant substrings.  A zero return
 * value means that parsing failed. */

static int parseUrl(char *c, char **hostName, uint16_t *port, char **fileName)
{
   int p;
   *hostName = c;

   for (c = (*hostName); *c != ':' && *c != '/'; c++) {
      if (*c == '\0')
         return 0;
   }

   if (*c == ':') {
      *c++ = '\0';
      for (p = 0; *c >= '0' && *c <= '9'; c++) {
         p = 10 * p + *c - '0';
      }
   } else
      p = 80;
   *port = p;

   if (*c == '/') {
      *c++ = '\0';
      *fileName = c;
   }
   return 1;

}

/* Similar to atoi, but for non-null terminated strings.  Returns the address
 * of the symbol that causes parsing to stop. Callers can check for success by
 * checking that input str is different return value. */

static inline char *parseSize(size_t * val, char *str)
{
   for (*val = 0; '0' <= *str && *str <= '9'; ++str) {
      *val = (*val * 10) + (*str - '0');
   }
   return str;
}

/* Callback handlers for parser state transitions. A zero return value means
 * that parsing should stop.
 */

static int parseHostname(char *in, void *data)
{
   HTTPSession *session = data;
   char *hostName = NULL;
   char *fileName = NULL;

   if (parseUrl(in, &hostName, &session->port, &fileName)) {
      strncpy(session->hostName, hostName, sizeof(session->hostName) - 1);
      session->hostName[sizeof(session->hostName) - 1] = '\0';

      strncpy(session->fileName, fileName, sizeof(session->fileName) - 1);
      session->fileName[sizeof(session->fileName) - 1] = '\0';

      return 1;
   } else
      return 0;
}

static int parseLength(char *in, void *data)
{
   HTTPSession *session = data;
   return (parseSize(&session->contentLength, in) != in);
}

static int parseRange(char *in, void *data)
{
   HTTPSession *session = data;

   if (memcmp(in, "bytes=", 6) == 0) {
      char *first = in + 6;
      char *second = parseSize(&session->from, first);

      if (second != first && *second++ == '-') {
         if (parseSize(&session->to, second) != second)
            return 1;
      }
   }

   return 0;
}

static int parseId(char *in, void *data)
{
   HTTPSession *session = data;
   LogFS_HashSetString(&session->id, in + 1);   // XXX skipping over first quote
   return 1;
}

static int parseSecret(char *in, void *data)
{
   HTTPSession *session = data;
   LogFS_HashSetString(&session->secret, in + 1);

   /* XXX hack */
   if (in[1 + SHA1_HEXED_SIZE_UNTERMINATED] == ',') {
      LogFS_HashSetString(&session->secretView,
                          in + 1 + SHA1_HEXED_SIZE_UNTERMINATED + 1);
   } else
      LogFS_HashClear(&session->secretView);

   return 1;
}

static int parseConnection(char *in, void *data)
{
   HTTPSession *session = data;
   if (strcmp(in, "keep-alive") == 0)
      session->keepAlive = 1;
   else
      session->keepAlive = 0;
   return 1;
}

static int parse100(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 100;
   return 1;
}

static int parse200(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 200;
   return 1;
}

static int parse204(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 204;
   return 1;
}

static int parse301(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 301;
   return 1;
}

static int parse304(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 304;
   return 1;
}

static int parse404(char *in, void *data)
{
   HTTPSession *session = data;
   session->status = 404;
   return 1;
}

static inline void parseGetOrPut(char *in, void *data)
{
   HTTPSession *session = data;
   strncpy(session->fileName, in, sizeof(session->fileName) - 1);
   session->fileName[sizeof(session->fileName) - 1] = '\0';
}

static int parseGet(char *in, void *data)
{
   HTTPSession *session = data;
   session->verb = HTTP_GET;
   parseGetOrPut(in, data);
   return 1;
}

static int parsePut(char *in, void *data)
{
   HTTPSession *session = data;
   session->verb = HTTP_PUT;
   parseGetOrPut(in, data);
   return 1;
}

static int setComplete(char *in, void *data)
{
   HTTPSession *session = data;
   session->complete = 1;
   return 1;
}

HTTPState header = {
   "header", {
              {"Date: *", &header},
              {"Location: http://*", &header, parseHostname},
              {"User-Agent: *", &header},
              {"Content-Length: *", &header, parseLength},
              {"Range: *", &header, parseRange},
              {"Accept: *", &header},
              {"Host: *", &header},
              {"Connection: *", &header, parseConnection},
              {"If-None-Match: *", &header, parseId},
              {"ETag: *", &header, parseId},
              {"Secret: *", &header, parseSecret},
              {"\r\n", NULL, setComplete},
              {NULL, &header}}
};

HTTPState status = {
   "status", {
              {"100 *", &header, parse100},
              {"200 *", &header, parse200},
              {"204 *", &header, parse204},
              {"301 *", &header, parse301},
              {"304 *", &header, parse304},
              {"404 *", &header, parse404},
              {NULL, NULL}}
};

HTTPState begin2 = {
   "begin2", {
              {"HTTP/1.0*", &header},
              {"HTTP/1.1*", &header},
              {NULL, NULL}},
};

HTTPState begin = {
   "begin", {
             {"HTTP/1.0 ", &status},
             {"HTTP/1.1 ", &status},
             {"GET #", &begin2, parseGet},
             {"PUT #", &begin2, parsePut},
             {NULL, NULL}},
};

int parseHttp(HTTPParserState *ps, char *in, int inputLeft, void *data)
{
   char *buf;

   ps->justParsed = 0;

   if (ps->state == NULL)
      ps->state = &begin;

   buf = ps->buf;

   for (;;) {
      int i;
      HTTPTransition *t = NULL;

      if (inputLeft == 0)
         return 1;

      else if (ps->bufOffset == 0) {
         for (;;) {
            char c = *in++;

            /* This is usually a case of feeding the parser non-header data */
            if (ps->putOffset >= sizeof(ps->buf))
               return 0;

            buf[ps->putOffset++] = c;

            ps->totalParsed++;
            ps->justParsed++;

            --inputLeft;

            if (c == '\n') {
               break;
            } else if (inputLeft == 0)
               return 1;
         }

         ps->putOffset = 0;
      }
      //zprintf("parse: %s '%s'\n",ps->state->name,buf+ps->bufOffset);


      /* Select a transition to the next state */

      for (i = 0;; i++) {
         t = &ps->state->transitions[i];

         if (t->key != NULL) {
            int len = strlen(t->key);

            char wildcard;
            char last = t->key[len - 1];

            switch (last) {
            case '*':
            case '#':
               --len;
               wildcard = last;
               break;
            default:
               wildcard = 0;
               break;
            }

            if (memcmp(buf + ps->bufOffset, t->key, len) == 0) {

               char *arg;
               ps->bufOffset += len;

               /* Poor man's lexer - allow simple wildcards for variable data */

               if (wildcard) {

                  char endl = (wildcard == '#') ? ' ' : '\n';
                  arg = buf + ps->bufOffset;

                  while (buf[ps->bufOffset] != endl) {
                     ++(ps->bufOffset);
                  }

                  /* hack away \r if there! */
                  if (buf[ps->bufOffset - 1] == '\r') {
                     buf[ps->bufOffset - 1] = '\0';
                  }

                  buf[ps->bufOffset++] = '\0';

                  if (wildcard == '*') {
                     ps->bufOffset = 0;
                  }

               } else
                  arg = NULL;

               /* Optionally call a callback associated with transition, using wildcard as argument if present */

               if (t->callback) {
                  int r = t->callback(arg, data);
                  if (r == 0) {
                     zprintf("callback %s breaks parsing!\n", t->key);
                     return 0;
                  }
               }

               break;

            }
         } else {
            ps->bufOffset = 0;
            break;
         }

      }
      ps->state = t->state;
      if (ps->state == NULL)
         return 0;

   }

}

#if 0                           //ndef VMKERNEL
int main()
{
   char session1[] =
       "HTTP/1.0 301 Moved Permanently\r\nDate: Tue Jan 15 13:47:48 CET 2007\r\nLocation: http://10.20.63.28:8090/blocks?ec50fee2300b5f5988e84a653b88c7abf8336c31&blk=1536&num=256\r\n\r\n";

   char session2[] =
       "HTTP/1.0 200 OK\r\nDate: some date\r\nContent-Length: 217\r\n\r\n";

   char session3[] =
       "HTTP/1.0 200 OK\r\nDate: Tue Jan 15 13:47:48 CET 2007\r\nContent-Length: 4096\r\n\r\n";

   char session4[] =
       "GET /lurt?num=1 HTTP/1.1\r\nRange: bytes=512-1024\r\nUser-Agent: curl/7.20.0\r\nHost: localhost:9000\r\nAccept: */*\r\n\r\n";

   char session5[] = "HTTP/1.1 100 Continue\r\n\r\n";

   char *sessions[] = { session1, session2, session3, session4, session5 };

   int i;
   for (i = 0; i < sizeof(sessions) / sizeof(sessions[0]); i++) {
      printf("================ session %d ================\n", 1 + i);
      HTTPParserState ps = { 0 };

      HTTPSession sessionHTTPState = { 0 };
      sessionHTTPState.complete = 0;

      int r =
          parseHttp(&ps, sessions[i], strlen(sessions[i]), &sessionHTTPState);
      printf("r %d, status %d\n", r, sessionHTTPState.status);
      printf("fileName %s\n", sessionHTTPState.fileName);
      assert(sessionHTTPState.complete == 1);
   }

   return 0;
}
#endif
