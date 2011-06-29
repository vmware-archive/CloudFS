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
#ifndef __HASH_H__
#define __HASH_H__

#include "system.h"
#include "shalib/sha.h"

#include "hex.h"
#include "logtypes.h"

typedef struct __LogFS_Hash {
   unsigned char raw[SHA1_DIGEST_SIZE];
   int isValid;

#ifdef __cplusplus
   bool operator <(struct __LogFS_Hash other) const {
      return (memcmp(raw, other.raw, SHA1_DIGEST_SIZE) < 0);
   }
#endif
}
LogFS_Hash;

static inline void LogFS_HashSetRaw(LogFS_Hash * h, const unsigned char *in)
{
   memcpy(h->raw, in, SHA1_DIGEST_SIZE);
   h->isValid = 1;
}

static inline LogFS_Hash LogFS_HashFromRaw(const unsigned char *in)
{
   LogFS_Hash h;
   memcpy(h.raw, in, SHA1_DIGEST_SIZE);
   h.isValid = 1;
   return h;
}

static inline LogFS_Hash LogFS_HashChecksum(const void *in, size_t sz)
{
   LogFS_Hash h;
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, sz, (const uint8_t *)in);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, h.raw);
   h.isValid = 1;
   return h;
}

static inline void LogFS_HashCopy(unsigned char *buf, LogFS_Hash h)
{
#ifdef VMKERNEL
   ASSERT(h.isValid);
#endif
   memcpy(buf, h.raw, SHA1_DIGEST_SIZE);
}

static inline void LogFS_HashClear(LogFS_Hash * h)
{
   memset(h->raw, 0, SHA1_DIGEST_SIZE);
   h->isValid = 0;
}

static inline int LogFS_HashZero(LogFS_Hash * h)
{
   memset(h->raw, 0, SHA1_DIGEST_SIZE);
   h->isValid = 1;
   return h->isValid;
}

static inline int LogFS_HashSetString(LogFS_Hash * h, const char *in)
{
   if (strlen(in) < SHA1_HEXED_SIZE_UNTERMINATED) {
#if 0
      printf("invalid hash input %s\n", in);
#endif
      h->isValid = 0;
   } else if (unhex(h->raw, (char *)in) == 0) {
      h->isValid = 1;
   }
   return h->isValid;
}

static inline void LogFS_HashRandomize(LogFS_Hash * h)
{
#ifdef VMKERNEL
   zprintf("weak randomize in kernel\n");
   uint32 seed = Util_RandSeed();
   int i;
   for (i = 0; i < SHA1_DIGEST_SIZE; i++)
      h->raw[i] = seed = Util_FastRand(seed);
   h->isValid = 1;
#else
   int r;
   int f = open("/dev/urandom", O_RDONLY);
   if (f < 0) {
      perror("could not open /dev/urandom!\n");
      exit(1);
   }
   r = read(f, h->raw, sizeof(h->raw));
   assert(r==sizeof(h->raw));
   close(f);
   h->isValid = 1;
#endif
}

static inline void LogFS_HashReapply(LogFS_Hash * h)
{
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)h->raw);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, h->raw);
}

static inline LogFS_Hash LogFS_HashApply(LogFS_Hash h)
{
#ifdef VMKERNEL
   ASSERT(h.isValid);
#endif
   LogFS_Hash r;
   struct sha1_ctx ctx;
   sha1_init(&ctx);
   sha1_update(&ctx, SHA1_DIGEST_SIZE, (const uint8_t *)h.raw);
   sha1_digest(&ctx, SHA1_DIGEST_SIZE, r.raw);
   r.isValid = 1;
   return r;
}

static inline LogFS_Hash LogFS_HashXor(LogFS_Hash a, LogFS_Hash b)
{
   int i;
   LogFS_Hash r;

#ifdef VMKERNEL
   ASSERT(a.isValid && b.isValid);
#else
   assert(a.isValid && b.isValid);
#endif

   for (i = 0; i < SHA1_DIGEST_SIZE; i++) {
      r.raw[i] = a.raw[i] ^ b.raw[i];
   }
   r.isValid = a.isValid & b.isValid;
   return r;
}

static inline int LogFS_HashIsValid(LogFS_Hash h)
{
   return h.isValid;
}

static inline int LogFS_HashIsNull(LogFS_Hash h)
{
   if (h.isValid) {
      int i;
      for (i = 0; i < SHA1_DIGEST_SIZE; i++)
         if (h.raw[i] != 0)
            return 0;
      return 1;
   } else
      return 0;
}

static inline int LogFS_HashCompare(const LogFS_Hash * a, const LogFS_Hash * b)
{
#ifdef VMKERNEL
   ASSERT(a->isValid && b->isValid);
#endif
   return memcmp(a->raw, b->raw, SHA1_DIGEST_SIZE);
}

static inline int LogFS_HashEquals(const LogFS_Hash a, const LogFS_Hash b)
{
   if (a.isValid == 0 || b.isValid == 0)
      return 0;
   return (LogFS_HashCompare(&a, &b) == 0);
}

static inline char *LogFS_HashPrint(char *out, const LogFS_Hash * h)
{
   if (h->isValid)
      hex(out, (char *)h->raw);
   else
      strcpy(out, "++++++++++[ INVALID HASH ]+++++++++++++\0");
   return out;
}

static inline char *LogFS_HashShow(const LogFS_Hash * h)
{
   char *out = (char *)malloc(SHA1_HEXED_SIZE);
   return LogFS_HashPrint(out, h);
}

static inline char *LogFS_HashShow2(const LogFS_Hash h)
{
#ifdef VMKERNEL
   NOT_REACHED();
#endif
   char *out = (char *)malloc(SHA1_HEXED_SIZE);
   return LogFS_HashPrint(out, &h);
}

typedef LogFS_Hash Hash;

#endif
