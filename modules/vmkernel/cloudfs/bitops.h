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
/* Adapted from VMware's bitvector.h */

#ifndef __BITOPS_H__
#define __BITOPS_H__

static inline void
BitSet(void *v, int n)
{
#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))
   asm volatile("btsl %1, (%0)"
                :: "r" (v), "r" (n)
                : "cc", "memory");
#else
   bv->vector[BITVECTOR_INDEX(n)] |= BITVECTOR_MASK(n);
#endif
}

static inline void
BitClear(void *v, int n)
{
#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))
   asm volatile("btrl %1, (%0)"
                :: "r" (v), "r" (n)
                : "cc", "memory");
#else
   bv->vector[BITVECTOR_INDEX(n)] &= ~BITVECTOR_MASK(n);
#endif
}

static inline int
BitTest(const void *v, int n)
{
#if defined(__GNUC__) && (defined(__i386__) || defined(__x86_64__))
   {
      uint32_t tmp;

      asm("btl  %2, (%1); "
          "sbbl %0, %0"
          : "=r" (tmp)
          : "r" (v), "r" (n)
          : "cc");
      return (tmp != 0);
   }
#else
   return ((bv->vector[BITVECTOR_INDEX(n)] & BITVECTOR_MASK(n)) != 0);
#endif
}

#endif /* __BITOPS_H__ */
