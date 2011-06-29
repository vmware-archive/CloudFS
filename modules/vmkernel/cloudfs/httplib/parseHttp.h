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

typedef struct HTTPTransition {
   char *key;
   struct HTTPState *state;
   int (*callback) (char *c, void *data);
} HTTPTransition;

typedef struct HTTPState {
   char *name;
   HTTPTransition transitions[16];
} HTTPState;

typedef enum {
   HTTP_GET = 1,
   HTTP_PUT = 2,
} HTTPVerb;

typedef struct {
   int status;
   HTTPVerb verb;
   int complete;
   int keepAlive;
   char hostName[256];
   char fileName[512];
   uint16_t port;
   size_t contentLength;

   size_t from;
   size_t to;

   Hash id;
   Hash secret;
   Hash secretView;

} HTTPSession;

typedef struct {
   HTTPState *state;
   char buf[512];
   size_t putOffset;
   size_t bufOffset;
   size_t totalParsed;
   size_t justParsed;
} HTTPParserState;

int parseHttp(HTTPParserState *ps, char *in, int inputLeft, void *data);
