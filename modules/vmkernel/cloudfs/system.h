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
#ifndef __SYSTEM_H__
#define __SYSTEM_H__

#ifndef VMKERNEL

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 500
#endif

#ifndef MIN
#define MIN(__a,__b) ( (__a)<(__b) ? (__a) : (__b ))
#endif

#ifndef MAX
#define MAX(__a,__b) ( (__a)>(__b) ? (__a) : (__b ))
#endif

#include <sys/types.h>

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <assert.h>
#include <unistd.h>

#define O_DIRECT      040000

#ifndef ASSERT
#define ASSERT(_a) assert(_a)
#endif

#ifndef USERLEVEL
#define Panic(_a) abort()
//#define exit(_a) ASSERT(0)
//#define abort() ASSERT(0)
//#define NOT_REACHED() abort()

typedef int VMK_ReturnStatus;
#define VMK_OK 0
#define VMK_BAD_PARAM (-EINVAL)
#define VMK_WRITE_ERROR (-EIO)
#define VMK_WOULD_BLOCK (-EWOULDBLOCK)

#define FALSE (0)
#define TRUE (1)

#define FMT64 "L"
#endif

#define zprintf(fmt,args...) printf(fmt , ## args)

#else

#include "vm_types.h"
#include "vm_libc.h"
#include "vmkernel.h"
#include "splock.h"
#include "world.h"
#include "fss_int.h"
#include "logfs_int.h"
#include "fs_common.h"
#include "fsSwitch.h"
#include "scattergather.h"
#include "scsi_ext.h"
#include "scsi_vmware.h"
#include "timer.h"
#include "helper_ext.h"
#include "semaphore_ext.h"
#include "util.h"
#include "config.h"
#include "libc.h"
#include "volumeCache.h"
#include "objectCache.h"
//#include "reservation.h"
#include "vcfs.h"
#include "stopwatch.h"
#include "kseg_dist.h"
#include "srm_ext.h"
#include "fsvcb_public.h"
#include "volumeCache.h"
#include "world_user.h"

/* fake <stdint.h> for VMKERNEL */

#ifndef _INT8_T_DECLARED

typedef uint64 uint64_t;
typedef uint32 uint32_t;
typedef uint16 uint16_t;
typedef uint8 uint8_t;
#define _UINT8_T_DECLARED
#define _UINT16_T_DECLARED
#define _UINT32_T_DECLARED
#define _UINT64_T_DECLARED

typedef int64 int64_t;
typedef int32 int32_t;
typedef int16 int16_t;
typedef int8 int8_t;
#define _INT8_T_DECLARED
#define _INT16_T_DECLARED
#define _INT32_T_DECLARED
#define _INT64_T_DECLARED

#endif

#define LOGLEVEL_MODULE FS3
#define LOGLEVEL_MODULE_LEN 3
#include "log.h"

#define LOGFS_FSTYPENUM 0x10f2

#define printf(fmt,args...)     //Log(fmt , ## args)
#define zprintf(fmt,args...) Log(fmt , ## args)

void qsort(void *base, size_t nmemb, size_t size,
           int (*compar) (const void *, const void *));

void *LogFS_Alloc(size_t, const char *, int);
void LogFS_Free(void *);
#define malloc(_a) LogFS_Alloc(_a,__FUNCTION__,__LINE__)
#define free(_a) LogFS_Free(_a)

#define aligned_malloc(_a) LogFS_Alloc(_a,__FUNCTION__,__LINE__)
#define aligned_free(_a) LogFS_Free(_a)

#ifndef DVMX86_DEBUG
#undef ASSERT
#define ASSERT(_a) {if(!(_a)) Panic("Botched assert file %s line %u",__FILE__,__LINE__);}
#endif

#define assert(_a) ASSERT(_a)

#endif

#endif                          /* __SYSTEM_H__ */
