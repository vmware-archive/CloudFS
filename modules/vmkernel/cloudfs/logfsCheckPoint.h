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
#ifndef __LOGFS_CHECKPOINT_H__
#define __LOGFS_CHECKPOINT_H__

#include "logfsConstants.h"
#include "logtypes.h"

struct SP_SpinLock;

extern uint8 nodesBitmap[TREE_MAX_BLOCKS / 8 + 1];
extern struct SP_SpinLock nodesLock;

typedef struct {
   uint8_t checksum[20];
   uint64 generation;
   log_id_t logEnd;
   disk_block_t superTreeRoot;
   uint8 bitmap[MAX_NUM_SEGMENTS / 8 + 1];
   uint16 heap[MAX_NUM_SEGMENTS];
   uint8 nodesBitmap[TREE_MAX_BLOCKS / 8 + 1];
} __attribute__ ((__packed__))
LogFS_CheckPoint;

typedef struct {
   disk_block_t from,to;
   List_Links list;
} MovedNode;

#define LOGFS_CHECKPOINT_SIZE (BLKSIZE_ALIGNUP(sizeof(LogFS_CheckPoint)))

struct LogFS_MetaLog;

VMK_ReturnStatus
LogFS_CheckPointPrepare(struct LogFS_MetaLog *ml,
      LogFS_CheckPoint *cp,
      uint64 generation);

VMK_ReturnStatus LogFS_CheckPointCommit(struct LogFS_MetaLog *ml,
      LogFS_CheckPoint *cp, 
      int buffer,
      disk_block_t superTreeRoot,
      List_Links *freedList);

VMK_ReturnStatus LogFS_RecoverCheckPoint(struct LogFS_MetaLog *ml,
      uint64 *generation, log_id_t * logEnd, disk_block_t *superTreeRoot);

VMK_ReturnStatus LogFS_ReplayFromCheckPoint(struct LogFS_MetaLog *ml,
                                            log_id_t logEnd);

#endif                          /* __LOGFS_CHECKPOINT_H__ */
