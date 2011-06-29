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

#include "globals.h"
#include "metaLog.h"

#include "socket.h"

VMK_ReturnStatus LogFS_NetSetSocketOptions(Net_Socket sock)
{
   uint32 sndSize;
   uint32 actualSockSize = 0;
   uint32 sockSizeLen = 0;
   int optVal = 1;
   VMK_ReturnStatus status;

   for (sndSize = 0x40000; sndSize != 0; sndSize >>= 1) {
      status = Net_SetSockOpt(sock, SOL_SOCKET, SO_SNDBUF,
                              &sndSize, sizeof(sndSize), DEFAULT_STACK);

      if (status != VMK_OK) {
         continue;
      }

      sockSizeLen = sizeof(actualSockSize);
      status = Net_GetSockOpt(sock, SOL_SOCKET, SO_SNDBUF, &actualSockSize,
                              &sockSizeLen, DEFAULT_STACK);
      break;
   }

   status = Net_SetSockOpt(sock, IPPROTO_TCP, TCP_NODELAY,
                           &optVal, sizeof(optVal), DEFAULT_STACK);

   return status;
}

VMK_ReturnStatus LogFS_NetCreateAcceptSocket(Net_Socket * acceptSocketOut)
{
   VMK_ReturnStatus status;
   sockaddr_in_bsd myAddr;
   int optval;
   Net_Socket acceptSocket;

   Net_SetSockVal(acceptSocketOut, NULL);

   status = Net_CreateSocket(PF_INET, SOCK_STREAM, IPPROTO_TCP,
                             &acceptSocket, DEFAULT_STACK);
   if (status != VMK_OK) {
      if (status == VMK_NOT_SUPPORTED) {
         printf("Can't create accept socket.  Is the TCP/IP module loaded?");
      } else {
         printf("Can't create accept socket: %s",
                VMK_ReturnStatusToString(status));
      }
      return status;
   }

   memset(&myAddr, 0, sizeof(myAddr));
   myAddr.sin_len = sizeof(myAddr);
   myAddr.sin_family = AF_INET;
   myAddr.sin_port = htons(8090);

   optval = 1;
   status = Net_SetSockOpt(acceptSocket, SOL_SOCKET, SO_REUSEPORT,
                           &optval, sizeof optval, DEFAULT_STACK);
   if (status != VMK_OK) {
      printf("Reuse port sockopt failed: %s", VMK_ReturnStatusToString(status));
      Net_CloseSocket(acceptSocket, DEFAULT_STACK);
      return status;
   }

   status = Net_Bind(acceptSocket, (struct sockaddr *)&myAddr, sizeof myAddr,
                     DEFAULT_STACK);
   if (status != VMK_OK) {
      printf("Bind failed: %s", VMK_ReturnStatusToString(status));
      Net_CloseSocket(acceptSocket, DEFAULT_STACK);
      return status;
   }

   status = Net_Listen(acceptSocket, 5, DEFAULT_STACK);
   if (status != VMK_OK) {
      printf("Error trying to listen: %s", VMK_ReturnStatusToString(status));
      Net_CloseSocket(acceptSocket, DEFAULT_STACK);
      return status;
   }

   *acceptSocketOut = acceptSocket;
   return VMK_OK;
}

VMK_ReturnStatus LogFS_NetRead(Net_Socket socket, void *data, size_t len,
                               int *bytesReceived)
{
   int b;
   return Net_RecvFrom(socket, 0, data, len, NULL,
                       NULL, bytesReceived ? bytesReceived : &b, DEFAULT_STACK);
}

VMK_ReturnStatus LogFS_NetWrite(Net_Socket socket, void *data, size_t len,
                                int *bytesSent)
{
   int b;
   return Net_SendTo(socket, 0, NULL, data, len, bytesSent ? bytesSent : &b,
                     DEFAULT_STACK);
}
