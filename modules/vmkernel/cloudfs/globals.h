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
#ifndef __GLOBALS_H__
#define __GLOBALS_H__

#include "logfsHash.h"

#ifdef VMKERNEL
extern vmk_ModuleID logfsModuleID;
extern int LogFS_FlushAllBuffers(void);
extern vmk_HeapID logfsHeap;

struct Async_Token;

extern void LogFS_FreeSimpleBuffer(struct Async_Token *token, void *data);
extern void LogFS_UnmapBuffer(struct Async_Token *token, void *data);
extern void LogFS_FreeSimpleBufferAndToken(struct Async_Token *token,
                                           void *data);
extern void LogFS_ReleaseBuffer(struct Async_Token *token, void *data);

#include "vm_types.h"
typedef struct LogFS_RefCountedBuffer {
   Atomic_uint32 refCount;
   void *buffer;
   size_t count;
} LogFS_RefCountedBuffer;

static inline LogFS_RefCountedBuffer *LogFS_RefCountedBufferCreate(void *buffer,
                                                                   size_t sz)
{
   LogFS_RefCountedBuffer *rb = malloc(sizeof(LogFS_RefCountedBuffer));
   Atomic_Write(&rb->refCount, 1);
   rb->buffer = buffer;
   rb->count = sz;
   return rb;
}

static inline void LogFS_RefCountedBufferRef(LogFS_RefCountedBuffer *rb)
{
   Atomic_Inc(&rb->refCount);
}
static inline void LogFS_RefCountedBufferRelease(LogFS_RefCountedBuffer *rb)
{
   if (Atomic_FetchAndDec(&rb->refCount) == 1) {
      aligned_free(rb->buffer);
      free(rb);
   }
}

#endif

#endif                          /* __GLOBALS_H__ */
