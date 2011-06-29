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
#ifndef __HEX_H__
#define __HEX_H__

#include "system.h"

#define SHA1_HEXED_SIZE_UNTERMINATED 40
#define SHA1_HEXED_SIZE (SHA1_HEXED_SIZE_UNTERMINATED+1)

static inline int unhex(unsigned char *out, const char *in)
{
   int i;
   int shift = 4;
   int digit = 0;

   for (i = 0; i < 40; i++) {
      char c = *in++;
      if (c >= '0' && c <= '9')
         c -= '0';
      else if (c >= 'a' && c <= 'f')
         c -= ('a' - 0xa);
      else if (c >= 'A' && c <= 'F')
         c -= ('A' - 0xa);
      else
         return -1;

      digit |= c << shift;
      shift ^= 4;

      if (shift) {
         *out++ = digit;
         digit = 0;
      }
   }
   return 0;
}

static inline void hex(char *out, char *in)
{
   int i;
   char *o = out;
   char digits[] = "0123456789abcdef";
   for (i = 0; i < 20; i++) {
      char c = *in++;
      *o++ = digits[(c & 0xf0) >> 4];
      *o++ = digits[c & 0xf];
   }
   *o = '\0';
}
#endif                          /* __HEX_H__ */
