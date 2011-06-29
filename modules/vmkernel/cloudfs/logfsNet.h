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
#ifndef __LOGFSNET_H__
#define __LOGFSNET_H__

#ifdef VMKERNEL
#include "migBSDNet.h"

#include "sys/types.h"
#include "netinet/in.h"
#include "netinet/tcp.h"
#define FIXED_FOR_ESX
#include "sys/socket.h"

#define _SYS_INTTYPES_H_
#define __FreeBSD__

#include "vm_types.h"
#include "vm_libc.h"
#include "vmkernel.h"
#include "return_status.h"
#include "world.h"
#include "vmk_net.h"
#include "vmkcfgopts_public.h"
#include "net.h"
#include "timer.h"
#include "bh_dist.h"
#include "util.h"
#include "memalloc_dist.h"
#include "vob.h"

#else
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <linux/reboot.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

#include <netinet/tcp.h>

#include "system.h"

typedef int Net_Socket;

#define Net_CloseSocket(_a,_b) close((_a))

#endif

struct LogFS_MetaLog;

VMK_ReturnStatus LogFS_NetCreateAcceptSocket(Net_Socket * acceptSocketOut);
VMK_ReturnStatus LogFS_NetSetSocketOptions(Net_Socket sock);
VMK_ReturnStatus LogFS_NetRead(Net_Socket socket, void *data, size_t len,
                               int *bytesReceived);
VMK_ReturnStatus LogFS_NetWrite(Net_Socket socket, void *data, size_t len,
                                int *bytesSent);

#endif                          /* __LOGFSNET_H__ */
