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
#ifndef __BTREE_H__
#define __BTREE_H__

#ifndef NULL
#define NULL 0
#endif

#ifndef VMKERNEL
#include <assert.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#else
#include "system.h"
#endif


/* btree types */

typedef void *elem_t;

typedef uint32_t disk_block_t;
#define tree_null_block 0UL

typedef enum {
   tree_result_found = 0,
   tree_result_ok = 0,
   tree_result_end = 1,
   tree_result_node_fault = 2,

} tree_result_t;

/* the tree node */

typedef struct _node_t {
	uint8_t chk[20];\
	uint64_t user_data;\

   uint16_t num_elems;
   uint16_t leaf;
   disk_block_t children[0];
   char elems[0];
} __attribute__ ((__packed__))
node_t;

/* callbacks invoked during btree operation */
struct btree;
typedef struct {
   disk_block_t(*alloc_node) (struct btree *, void *context);
   void (*free_node) (struct btree * t, const node_t *);
   node_t *(*edit_node) (struct btree * t, disk_block_t block, const node_t *,
                         void *context);
   const node_t *(*get_node) (struct btree * t, disk_block_t block,
                              void *context);
   void (*put_node) (struct btree * t, const node_t *n, void *context);
   int (*cmp) (const void *, const void *);
} btree_callbacks_t;

/* in-memory tree metadata that does not make sense to keep on disk */
typedef struct btree {
   disk_block_t root;
   int num_nodes;

   btree_callbacks_t callbacks;
   void *user_data;

   uint32_t branch;
   uint32_t key_size;
   uint32_t value_size;
   uint32_t real_node_size;
} btree_t;

#define TREE_MAX_DEPTH 32

/* btree iterator */
typedef struct _btree_iter_t {
   btree_t *tree;
   unsigned short stack[TREE_MAX_DEPTH];
   int depth;

   /* if pointing to a leaf node, cache its location here */
   disk_block_t block;
} btree_iter_t;

/* forward declarations */

void tree_create(btree_t *t, btree_callbacks_t *callbacks,
                     int key_size, int value_size, int node_size,
                     void *user_data, void *context);

void tree_reopen(btree_t *t, 
      btree_callbacks_t *callbacks, disk_block_t root,
      int key_size, int value_size, int node_size,
      void *user_data, void *context);

void tree_insert(btree_t *, elem_t *, void *);

void tree_delete(btree_t *, elem_t *, void *);
tree_result_t tree_iter_read(void *dst, btree_iter_t *it, void *context);
tree_result_t tree_iter_write(btree_iter_t *it, void *src, void *context);
int tree_find(btree_t *, elem_t *, void *);

tree_result_t tree_lower_bound(btree_t *t, btree_iter_t *it, elem_t * e,
                               void *);

tree_result_t tree_begin(btree_t *, btree_iter_t *, void *context);
tree_result_t tree_end(btree_t *, btree_iter_t *, void *context);
tree_result_t tree_iter_inc(btree_iter_t *, void *context);
void tree_iter_touch(btree_iter_t *it, void *context);
void tree_iter_deref(void *, btree_iter_t *it, void *);
tree_result_t tree_iter_dec(btree_iter_t *, void *context);

/* inline helpers */

static inline const node_t *get_node(btree_t *t, disk_block_t b,  void *context,int line)
{
   //assert(b > 0);
#ifdef VMKERNEL
   if(b==0) {
      zprintf("zero block at line %d\n",line);
      NOT_REACHED();
   }
#endif
   return t->callbacks.get_node(t, b, context);
}
#define get_node(_a,_b,_c) get_node(_a,_b,_c,__LINE__)

static inline node_t *edit_node(btree_t *t, disk_block_t b, node_t *p,
                                 void *context)
{
   return t->callbacks.edit_node(t, b, p, context);
}

static inline void put_node(btree_t *t, const node_t *node, void *context,int line)
{
   t->callbacks.put_node(t, node, context);
}
#define put_node(_a,_b,_c) put_node(_a,_b,_c,__LINE__)

static inline void free_node(btree_t *t, const node_t *n)
{
   t->callbacks.free_node(t, n);
}

#define get_child(_t,_n,_p,_c) get_node(_t,(_n)->children[(_p)],_c)
#define edit_child(_t,_n,_p,_c) edit_node(_t,_n->children[_p],_n,_c)

static inline int _compare_u32(const void *a, const void *b)
{
   uint32_t ia = *(uint32_t *)a;
   uint32_t ib = *(uint32_t *)b;
   if (ia < ib)
      return -1;
   else if (ia == ib)
      return 0;
   else
      return 1;
}

static inline int _compare_u64(const void *a, const void *b)
{
   uint64_t ia = *(uint64_t *)a;
   uint64_t ib = *(uint64_t *)b;
   if (ia < ib)
      return -1;
   else if (ia == ib)
      return 0;
   else
      return 1;
}

/* Instead of setting a comparison callback, you can leave the field NULL and
 * we will default to an inlined standard compare suitable for the key size.
 * Branch prediction makes this slightly faster than jumping through a 
 * function pointer, without loss of generality. 
 *
 * XXX the jury is still out on whether or not this is such as great idea after
 * all.
 */

static inline int compare(btree_t *t, const void *a, const void *b)
{
   if (t->callbacks.cmp != NULL)
      return t->callbacks.cmp(a, b);
   else {
      int key_size = t->key_size;
      switch (key_size) {
      case 8:
         return _compare_u64(a, b);
      case 4:
         return _compare_u32(a, b);
      default:
         return memcmp(a, b, key_size);
      }
   }
}

static inline int elem_size(btree_t *t)
{
   return t->key_size + t->value_size;
}

static inline int value_size(btree_t *t)
{
   return t->value_size;
}

static inline int node_size(btree_t *t)
{
   return sizeof(node_t) + sizeof(disk_block_t) * 2 * t->branch +
       elem_size(t) * (2 * t->branch - 1);
}

static inline int calc_branch_factor(btree_t *t, int nodesize)
{
   int s = nodesize;
   int b = sizeof(node_t);
   int c = sizeof(disk_block_t);
   int d = elem_size(t);
   int branch = (s - b + d) / (2 * (c + d));
   assert(branch > 0);
   return branch;
}

static inline const elem_t *nth_elem(btree_t *t, const node_t *n, int i)
{
   char *elems = (char *)(&n->children[2 * t->branch]);
   return (elem_t *) (elems + elem_size(t) * i);
}

static inline elem_t *nth_elem_l(btree_t *t, node_t *n, int i)
{
   return (elem_t *) nth_elem(t,n,i);
}

static inline void *elem_value(btree_t *t, const elem_t * e)
{
   return (void *)((char *)e + t->key_size);
}

static inline void *elem_key(btree_t *tree, const elem_t * e)
{
   return (void *)e;
}
#endif                          // __BTREE_H__
