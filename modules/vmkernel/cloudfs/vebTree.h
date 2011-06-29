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
#ifndef __VEBTREE_H__
#define __VEBTREE_H__

#include "system.h"
#include "btree.h"

#ifndef VMKERNEL

typedef void LogFS_Device;

#endif


typedef struct LogFS_VebTree {
	struct LogFS_Device* device;
	size_t nodeSz;
	size_t totalSz;
	size_t treeSz;
	btree_t* t;
	void* mem;

	int activeBank;
   int bankDirty;
   btree_t tree;
} LogFS_VebTree;



struct LogFS_Device;
struct LogFS_ObsoletedSegments;
struct Graph;

void LogFS_VebTreeInit( LogFS_VebTree* vt, struct LogFS_Device* device, size_t nodeSz, size_t treeSz);
void LogFS_VebTreeCleanup(LogFS_VebTree *vt);
void LogFS_VebTreeFlush( LogFS_VebTree* vt, struct LogFS_Device* device );
void LogFS_VebTreeClear( LogFS_VebTree* vt);
void LogFS_VebTreeSetBank( LogFS_VebTree* vt, int bank);
void LogFS_VebTreeInsert( LogFS_VebTree* vt, uint8_t bank, uint32_t key, uint32_t value);

void LogFS_VebTreeMerge(
	LogFS_VebTree* base,
	LogFS_VebTree* overlay,
   struct Graph *g);

void LogFS_VebTreePrint( LogFS_VebTree* vt); 

#endif /* __VEBTREE_H__ */
