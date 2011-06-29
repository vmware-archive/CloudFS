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
#ifndef __RANGEMAP_H__
#define __RANGEMAP_H__

#include "btree.h"

/* Internal representation of a range within the B-tree. */

struct range {
   /* key */
   uint64_t to;

   /* value */
   uint16_t length;
   uint64_t version;

} __attribute__ ((__packed__)) ;

/* Struct used when returning lookup results. */

typedef struct {
   /* key */
   uint64_t from;
   uint64_t version;

} range_t ;

#define RANGEMAP_KEY_SIZE (sizeof(uint64_t))
#define RANGEMAP_VALUE_SIZE (sizeof(struct range)-sizeof(uint64_t))

void rangemap_meminit(btree_t*, void *);
int rangemap_insert(btree_t *tree, uint64_t from,
                    uint64_t to, uint64_t version);
void rangemap_merge(btree_t*, btree_t*);
void rangemap_show(btree_t*);
void rangemap_clear(btree_t*);
uint64_t rangemap_get(btree_t*, uint64_t, uint64_t *);
tree_result_t __rangemap_get(btree_t *tree, uint64_t block,
      range_t *result,
      uint64_t * endsat, void *context);
void rangemap_replace(btree_t*, uint64_t, uint64_t, uint64_t,
                      uint64_t);
int rangemap_check(btree_t*);

#endif
