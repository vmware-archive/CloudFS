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
#ifndef __LOGFSDISKLAYOUT_H__
#define __LOGFSDISKLAYOUT_H__

#include "logtypes.h"
#include "globals.h"
#include "btree.h"
#include "logfsCheckPoint.h"

typedef enum {
   LogFS_DiskHeaderSection = 0,
   LogFS_CheckPointASection,
   LogFS_CheckPointBSection,
   LogFS_BTreeSection,
   LogFS_VebTreeSection,
   LogFS_LogSegmentsSection,

   LogFS_LogNumDiskSegments,
} LogFS_DiskSegmentType;

struct __section {
   uint32 type;
   log_offset_t offset;
} __attribute__ ((__packed__));

typedef struct __LogFS_DiskLayout {
   char magic[8];
   struct __section sections[LogFS_LogNumDiskSegments];
} __attribute__ ((__packed__))
LogFS_DiskLayout;

typedef struct {
   uint8 key[SHA1_DIGEST_SIZE];
   struct {
      disk_block_t root;
      disk_block_t lsnRoot;
      uint64 lsn;
      uint8 currentId[SHA1_DIGEST_SIZE];
      uint8 entropy[SHA1_DIGEST_SIZE];
   } __attribute__ ((__packed__)) value;
} __attribute__ ((__packed__))
SuperTreeElement;

static inline VMK_ReturnStatus
LogFS_DiskLayoutInit(LogFS_DiskLayout *dl, log_size_t diskCapacity)
{
   log_size_t headerSize = sizeof(LogFS_DiskLayout);
   log_size_t checkPointSize = LOGFS_CHECKPOINT_SIZE;
   log_size_t bTreeSize = diskCapacity / 128;

   log_size_t sizes[] = {
      headerSize,
      checkPointSize, checkPointSize,  /* Double buffered */
      bTreeSize,
      bTreeSize, /* XXX used by VebTree */
   };

   LogFS_DiskSegmentType type;
   log_offset_t pos;

   strcpy(dl->magic, "CloudFS");

   for (type = LogFS_DiskHeaderSection, pos = 0;
        type != LogFS_LogNumDiskSegments; type++) {
      dl->sections[type].type = type;
      dl->sections[type].offset = pos;
      pos += BLKSIZE_ALIGNUP(sizes[type]);
   }

   return VMK_OK;
}

static inline log_offset_t
LogFS_DiskLayoutGetOffset(LogFS_DiskLayout *dl, LogFS_DiskSegmentType type)
{
   return dl->sections[type].offset;
}

#endif                          /* __LOGFSDISKLAYOUT_H__ */
