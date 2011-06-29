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
/* **********************************************************
 * **********************************************************/

/*
 * logtypes.h --
 *
 *      Core data structures and macros for CloudFS network protocol and disk
 *      format.
 *
 *      we use stdint.h types such as uint64_t, because we intend to include
 *      this file in user space tools etc. outside of VMware's normal build
 *      infrastructure.
 *
 *      XXX we currently ignore all endianness issues.
 *
 *      XXX we currently rely on bitops.h and the libnettle SHA-1 functions,
 *      both of which are GPL. We need to switch to our own bitops, and to
 *      find an equally fast SHA-1 that we can legally ship.
 *
 */

#ifndef __LOGTYPES_H__
#define __LOGTYPES_H__

#include "shalib/sha.h"
#include "bitops.h"

typedef uint64_t log_segment_id_t;
typedef uint64_t log_size_t;
typedef int64_t log_ssize_t;
typedef uint64_t log_offset_t;
typedef uint64_t log_block_t;
typedef uint8_t log_ref_t;

#define MAXBLOCK (~0ULL)

#ifdef VMKERNEL
typedef FDS_Handle *log_device_handle_t;
#else
typedef int log_device_handle_t;
#endif

/* We do everything at disk sector precision because we can. Longer term the
 * advent of 4kB sector disks may force us to align our disk accesses at 4kB,
 * but we believe it is going to be feasible to emulate a 512B interface for
 * VMs and applications. */

#define BLKSIZE 512
#define BLKSIZE_ALIGNUP(_a) ( (_a+BLKSIZE-1)/BLKSIZE * BLKSIZE )

/* Log segments are 16MB each. We are planning to change this to be 32MB soon. */

#define LOG_MAX_SEGMENT_BLOCKS (0x8000)
#define LOG_MAX_SEGMENT_SIZE (LOG_MAX_SEGMENT_BLOCKS*BLKSIZE)

typedef struct __log_id {
   union {
      struct {
         unsigned short blk_offset:16;
         unsigned long long segment:48;
      } v;
      uint64_t raw;
   };
}
log_id_t;

#define METADATA_BLOCK (0xff00000000000000ULL/BLKSIZE)

#define LOGID_TO_UINT64(_a)  (_a.raw)

#define INVALID_SEGMENT 0xffffffffffffULL
#define INVALID_BLK_OFFSET 0xffff
#define mk_invalid_version(__name) log_id_t __name; __name.raw = 0xffffffffffffffffULL;
#define equal_version(__a,__b) ( __a.raw==__b.raw )
#define is_invalid_version(__a) ( (__a).raw==0xffffffffffffffffULL )

typedef enum { log_eof = 0,
   log_pointer_type,
   log_entry_type,
} log_tag_t;

typedef enum { log_prev_ptr = 1, log_next_ptr } log_pointer_t;

struct log_head {

   log_tag_t tag:32;

   uint8_t disk[20];
   uint8_t parent[20];
   uint8_t id[20];
   uint8_t entropy[20];

   union {

      struct {                  /* log_entry_type */
         uint8_t checksum[20];  /* actual checksum for this log entry */
         uint64_t lsn;

         /* Extent info */
         log_block_t blkno;
         unsigned int num_blocks:16;

         /* For potential RAIN uses in the future */
         uint16_t slice;
         uint16_t slices_total;
         uint16_t num_parity;
         uint16_t ununsed;
         /* We conclude with a bit-vector used for compressing away all-zero blocks.
          * This is both to save space and bandwidth, but also to simplify log-
          * compaction, where blocks that are later overwritten can be compressed
          * down to a single zero bit here. */
         log_ref_t refs[0];

      } __attribute__ ((__packed__)) update;

      struct {                  /* log_pointer_type */
         log_pointer_t direction;
         log_id_t target;
      } __attribute__ ((__packed__));
   };

} __attribute__ ((__packed__));


#define LOG_HEAD_SIZE BLKSIZE
#define LOG_HEAD_MAX_BLOCKS (8*(LOG_HEAD_SIZE- (unsigned long)( ((struct log_head*)NULL)->update.refs) )  )
/* FixME: use offsetof above! */

static inline void init_forward_pointer(struct log_head *head, log_id_t target)
{
   memset(head, 0, LOG_HEAD_SIZE);
   head->tag = log_pointer_type;
   head->direction = log_next_ptr;
   head->target = target;
}

static inline void init_backward_pointer(struct log_head *head, log_id_t target)
{
   memset(head, 0, LOG_HEAD_SIZE);
   head->tag = log_pointer_type;
   head->direction = log_prev_ptr;
   head->target = target;
}

static inline size_t log_body_size(struct log_head *head)
{
   size_t sum;
   log_block_t i;

   if (head->tag != log_entry_type)
      return 0;

   for (i = 0, sum = 0; i < head->update.num_blocks; i++) {
      if (BitTest(head->update.refs,i))
         sum += BLKSIZE;
   }

   return sum;
}

static inline size_t log_entry_size(struct log_head *head)
{
   return log_body_size(head) + LOG_HEAD_SIZE;
}

static inline int is_block_zero(char *blkdata)
{
   unsigned int i;
   int *data = (int *)blkdata;

   for (i = 0; i < BLKSIZE / sizeof(int); i++) {
      if (data[i])
         return 0;
   }
   return 1;
}

static inline void log_entry_checksum(unsigned char *sum, struct log_head *head,
                                      const void *body, size_t sz)
{
   struct sha1_ctx ctx;

   /* We hash from copies on the stack to prevent the compiler messing with the
    * field sizes. */

   uint64_t b = head->update.blkno;
   uint16_t n = head->update.num_blocks;
   uint64_t l = head->update.lsn;

   sha1_init(&ctx);
   sha1_update(&ctx, sizeof(l), (const uint8_t *)&l);
   sha1_update(&ctx, sizeof(b), (const uint8_t *)&b);
   sha1_update(&ctx, sizeof(n), (const uint8_t *)&n);
   sha1_update(&ctx, sz, (const uint8_t *)body);

   /* digest the refs last for practical reasons */
   sha1_update(&ctx, LOG_HEAD_SIZE - sizeof(struct log_head),
               (const uint8_t *)head->update.refs);

   sha1_digest(&ctx, SHA1_DIGEST_SIZE, sum);
}

#endif                          /* __LOGTYPES_H__ */
