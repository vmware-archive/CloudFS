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
#ifndef __LOGFSCONSTANTS_H__
#define __LOGFSCONSTANTS_H__

#define MAX_NUM_SEGMENTS 0x1000

#define TREE_MAX_BLOCKS 2048
#define TREE_BLOCK_SIZE (8*4096)
#define MAX_FILE_SIZE (TREE_BLOCK_SIZE*TREE_MAX_BLOCKS)

// KJO: copied from older version.. Revisit the lock hierarchy and see what we really need


#define SP_RANK_POSIX SP_RANK_FSDRIVER_LOWEST

#define SP_RANK_VDISK SP_RANK_FSDRIVER_LOWEST
#define SP_RANK_DEMANDFETCH (SP_RANK_VDISK+1)

#define SP_RANK_VDISKBUFFEREDRANGES (SP_RANK_VDISK+1)

#define SP_RANK_METALOG (SP_RANK_VDISK+1)
#define SP_RANK_APPENDLOG (SP_RANK_METALOG+1)
#define SP_RANK_REFCOUNTS (SP_RANK_METALOG+1)
#define SP_RANK_SEGMENTLIST (SP_RANK_METALOG+1)

#define SP_RANK_DDISK (SP_RANK_VDISK+1)
#define SP_RANK_REMOTELOG (SP_RANK_VDISK+1)

#define SP_RANK_BTREERANGE (SP_RANK_VDISK+1)
#define SP_RANK_RANGEMAP (SP_RANK_BTREERANGE+1)
#define SP_RANK_RANGEMAPCACHE (SP_RANK_RANGEMAP+1)
#define SP_RANK_RANGEMAPQUEUES (SP_RANK_RANGEMAPCACHE+1)
#define SP_RANK_RANGEMAPNODES (SP_RANK_RANGEMAPCACHE+1)

#define SP_RANK_OBSOLETED (SP_RANK_BTREERANGE+1)

#endif                          /* __LOGFSCONSTANTS_H__ */
