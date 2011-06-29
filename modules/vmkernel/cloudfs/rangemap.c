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
#include "system.h"
#include "rangemap.h"
#include "logfsConstants.h"


#define rprintf(fmt,args...)    //zprintf(fmt, ## args)

#define qprintf(fmt,args...)    //printf(fmt, ## args)

int compare_u64(const void *a, const void *b)
{
   uint64_t *ia = (uint64_t *) a;
   uint64_t *ib = (uint64_t *) b;
   if (*ia < *ib)
      return -1;
   else if (*ia == *ib)
      return 0;
   else
      return 1;
}

static const node_t *get_node_mem(btree_t *t, disk_block_t block, void *context)
{
   char *mapped_offset = (char *)t->user_data;
   const node_t *r =
       (const node_t *)(mapped_offset + t->real_node_size * block);
   return r;
}

static node_t *edit_node_mem(btree_t *t, disk_block_t block, const node_t *p,
                             void *context)
{
   char *mapped_offset = (char *)t->user_data;
   node_t *r = (node_t *)(mapped_offset + t->real_node_size * block);
   return r;
}

static void put_node_mem(btree_t *t, const node_t *node, void *context)
{
}

static disk_block_t alloc_node_mem(btree_t *t, void *context)
{
   qprintf("%s\n", __FUNCTION__);
   return ++(t->num_nodes);
}

static void free_node_mem(btree_t *t, const node_t *n)
{
}

void rangemap_replace(btree_t *tree,
                      uint64_t from,
                      uint64_t to, uint64_t oldvalue, uint64_t newvalue)
{
   qprintf("%s\n", __FUNCTION__);
   struct range r;
   r.to = to;

   btree_iter_t it;

   //zprintf("replace %ld -> %ld\n",from,to);

   tree_result_t result = tree_lower_bound(tree, &it, (elem_t *) & r, NULL);
   //ASSERT(result==tree_result_found);
   //zprintf("replace %lx.%x with %lx.%x\n",oldvalue.v.segment,oldvalue.v.blk_offset,
   //    newvalue.v.segment,newvalue.v.blk_offset);

   if (result == tree_result_found) {
      struct range lb;
      tree_iter_read(&lb, &it, NULL);
      // zprintf("lb: %ld : %lx.%x\n",lb.to,lb.version.v.segment,lb.version.v.blk_offset);

      for (;;) {
         tree_iter_read(&lb, &it, NULL);
         if (lb.version == oldvalue) {
            lb.version = newvalue;
            tree_iter_write(&it, &lb, NULL);
         }
#if 0
         else {
            zprintf("failed replace (%lx.%x) with (%lx.%x)\n",
                    lb.version.v.segment, lb.version.v.blk_offset,
                    newvalue.v.segment, newvalue.v.blk_offset);
            zprintf("old supposed to be %lx.%x\n", oldvalue.v.segment,
                    oldvalue.v.blk_offset);
            zprintf("from %lld -> %lld\n", from, to);
            NOT_REACHED();
         }
#endif

         if (tree_iter_dec(&it, NULL) == tree_result_found) {
            struct range lb2;
            tree_iter_read(&lb2, &it, NULL);
            if (lb2.to < from)
               break;
         } else
            break;

      }
   }
}

static inline void r2r(range_t* out, struct range *in)
{
   out->from = in->to - in->length;
   out->version = in->version;
}

tree_result_t __rangemap_get(btree_t *tree, uint64_t block,
      range_t *ret,
      uint64_t * endsat, void *context)
{
   qprintf("%s\n", __FUNCTION__);
   struct range r;
   r.to = block + 1;

   const uint64_t invalid = ~0ULL;

   struct range lb;

   btree_iter_t it;

   tree_result_t result = tree_lower_bound(tree, &it, (elem_t *) & r, context);
   ASSERT(it.depth < TREE_MAX_DEPTH);

   if (result == tree_result_found) {
      tree_iter_read(&lb, &it, context);
      r2r(ret,&lb);

      uint64_t to = lb.to;
      uint64_t from = lb.to - lb.length;

      if (block >= from && block != to) {
         if (endsat)
            *endsat = MIN(*endsat, to);
      } else {
         if (endsat) *endsat = MIN(*endsat, from);

         ret->from = block;
         ret->version = invalid;
      }
   } else if (result == tree_result_end) {
      ret->version = invalid;
      ret->from = block;
   } else if (result == tree_result_node_fault) {
   }

   return result;

}

uint64_t rangemap_get(btree_t *tree, uint64_t block,
                      uint64_t * endsat)
{
   qprintf("%s\n", __FUNCTION__);
   range_t r;

   __rangemap_get(tree, block, &r, endsat, NULL);

   return r.version;
}

int rangemap_insert(btree_t *tree, uint64_t from,
                    uint64_t to, uint64_t version)
{
   if(!(from < to)) {
      zprintf("from %lu to %lu\n",from,to);

   }
   ASSERT(from < to);

   void *context = NULL;        /* OK to block */

   struct range e;
   btree_iter_t it;
   uint64_t* dels;
	dels = malloc(1000*sizeof(uint64_t));
   int num_dels = 0;
   int inserted = 0;

   struct range extra;
   extra.to = 0;
   extra.length = 0;
   extra.version = 0;

   /* When a range is first inserted, it is marked as 'pristine', meaning
    * that it has a 1:1 correspondence with the log entry stored on disk.
    * If it is later partially occluded by another range, it is no longer
    * pristine. This is used when deciding to do readahead or not */

   e.to = to;
   struct range right;
   struct range left;
   btree_iter_t rightiter;

   const uint64_t invalid = ~0ULL;

   ASSERT(to - from <= 0x7fffff);   /* 23 bits of space for the length field */

   if (tree_lower_bound(tree, &it, (elem_t *) & e, context) ==
       tree_result_found) {
      tree_iter_read(&right, &it, context);
      rightiter = it;

      uint64_t right_from = right.to - right.length;
      rprintf("at right [%lx/g;%ld[ ver %lx\n", right_from, right.to,
              right.version);

      if (right_from <= to && to <= right.to
          && right.version == version + (right_from - from)) {

          //&& equal_version(right.version, version)) {
         rprintf("merge with right 0x%lx!\n",right.version);

         right.length = right.to - ((from < right_from) ? from : right_from);
         right.version = version;
         /* for sake of left checks below, update from value */
         to = right.to;
         from = to - right.length;
         inserted = 1;

         tree_iter_write(&rightiter, &right, context);

         goto cleanleft;
      }

      /* overlapping entry to the right? */
      else if (right.to == to) {
         /* complete overlap */
         if (right.length <= (to - from)) {
            if(version == invalid) {
               /* overlap with empty range, so remove old right */
               dels[num_dels++] = right.to;
            } else {
               /* reuse existing key but overwrite its data. We
                * replace everything and do not have to scoot. */
               right.to = to;
               right.length = to - (right_from < from ? right_from : from);
               ASSERT(right.length == to-from);
               right.version = version;
               inserted = 1;
            }
            rprintf("overwrote right with %lx/g:%d\n", right.to, right.length);
         }
         /* only partial overlap, place old right entry as my new left */
         else {
            rprintf("pull right one to the left of me\n");
            right.length -= (to - from);
            /* We do not have to update relative pointer in this
             * case, as the beginning of the range will stay 
             * the same. */
            right.to = from;
            rprintf("now at right [%lx/g;%ld[\n", right.to - right.length,
                    right.to);
         }

         tree_iter_write(&rightiter, &right, context);
         goto cleanleft;
      }

      /* Splitting an existing entry by writing inside its range results in the
       * creation of an extra key to the left of the new one being inserted. */

      else if (right_from < from && to < right.to) {
         extra.to = from;
         extra.length = from - right_from;
         extra.version = right.version;
      }

      /* overlap with existing key to the right */

      long int overlap = to - right_from;

      if (overlap > 0) {
         rprintf("right adjust %lx/g\n", overlap);
         right.length -= overlap;
         right.version += overlap;
         tree_iter_write(&rightiter, &right, context);
      }

   }
 cleanleft:

   if (tree_iter_dec(&it, context) == tree_result_found) {
      tree_iter_read(&left, &it, context);

      //if (equal_version(left.version, version) && left.to >= from) {

      if (left.to >= from
            && left.version + (from - (left.to - left.length)) == version) {

         rprintf("merge with left [%lx/g;%ld[ ver %lx\n",
                 left.to - left.length, left.to, left.version);

         long int left_from = left.to - left.length;
         left.to = to;
         if (from < left_from)
            left.length = to - from;
         else
            left.length = to - left_from;

         tree_iter_write(&it, &left, context);

         /* in the case where we merge both to the right and to the left,
          * one has to go */

         if (inserted) {
            /* copy left to right in tree */
            tree_iter_write(&rightiter, &left, context);
            left.to--;
            tree_iter_write(&it, &left, context);
            dels[num_dels++] = left.to;
         }

         inserted = 1;

         /* is there anything more to the left? */
         if (tree_iter_dec(&it, context) != tree_result_found)
            goto nothing_to_delete;
      }

   }

   /* delete overlapped nodes to the left (XXX is there any reason for storing
    * all of them, surely they must be ordered so storing top & bottom should
    * suffice??) */

   for (;;) {
      tree_iter_read(&left, &it, context);

      if (left.to >= to)
         goto next;

      int length = left.length;
      uint64_t left_from = left.to - length;

      if (left_from > from) {
         ASSERT(num_dels <= 990);   // XXX very hacky
         {
            rprintf("length %d\n", length);
         }

         rprintf("eat left [%lu:%lu[\n", left_from, left.to);
         dels[num_dels++] = left.to;
      } else
         break;

 next:
      if (tree_iter_dec(&it, context) != tree_result_found)
         goto nothing_to_delete;
   }

   tree_iter_read(&left, &it, context);

   if (left.to > from) {
      rprintf("checking left %lx/g\n", left.to);

      long int overlap = left.to - from;
      rprintf("overlap %lx/g\n", overlap);

      if (overlap >= left.length) {
         rprintf("eat overlap\n");
         dels[num_dels++] = left.to;
      }

      else {
         /* We do not need to scoot blk_offset in this case */
         left.to -= overlap;
         left.length -= overlap;
         tree_iter_write(&it, &left, context);
      }
   }
 nothing_to_delete:

   if (!inserted && version!=invalid) {
      e.to = to;
      e.length = to - from;
      e.version = version;
      rprintf("actually insert %lx/g+%d\n", e.to, e.length);
      tree_insert(tree, (elem_t *) & e, context);
   }

   if (extra.length > 0) {
      rprintf("extra ins [%lx/g:%ld[\n", extra.to - extra.length, extra.to);
      tree_insert(tree, (elem_t *) & extra, context);
   }

   int i;
   for (i = 0; i < num_dels; i++) {
      struct range del = { dels[i], 0 };
      rprintf("del %lx/g\n", dels[i]);
      tree_delete(tree, (elem_t *) & del, context);
   }

	free(dels);
   return 0;
}

void rangemap_merge(btree_t *dst, btree_t *src)
{
   void *context = NULL;        /* OK to block */

   btree_iter_t it;
   tree_result_t result = tree_begin(src, &it, context);

   while (result != tree_result_end) {
      struct range r;
      uint64_t from, to;
      uint64_t version;

      tree_iter_read(&r, &it, context);

      from = r.to - r.length;
      to = r.to;
      version = r.version;

      rangemap_insert(dst, from, to, version);

      result = tree_iter_inc(&it, context);
   }
}

void rangemap_meminit(btree_t *tree, void *memory)
{
   btree_callbacks_t callbacks = {
      .cmp = compare_u64,
      .alloc_node = alloc_node_mem,
      .free_node = free_node_mem,
      .edit_node = edit_node_mem,
      .get_node = get_node_mem,
      .put_node = put_node_mem,
   };

   tree_create(tree, &callbacks,
               RANGEMAP_KEY_SIZE, RANGEMAP_VALUE_SIZE, 0x200, memory, NULL);
}

/* 
 * next: use single mmapped file for all btrees
 * add a tree-of-trees mapping hashes to diskblocks (tree roots) at top-level
 *
 */

void rangemap_clear(btree_t *tree)
{
   //tree_clear(&tree->tree);
   rangemap_meminit(tree, tree->user_data);
   //tree->tree.num_nodes = 1;  //XXX to leave room for info block
}

void rangemap_show(btree_t *tree)
{
#ifdef VMKERNEL
   NOT_IMPLEMENTED();
#endif
}

int rangemap_check(btree_t *tree)
{
   btree_iter_t it;
   tree_result_t result = tree_end(tree, &it, NULL);
   int n = 0;

   while (result != tree_result_end) {

      struct range r;
      tree_iter_read(&r, &it, NULL);
      uint64_t from, to;
      from = r.to - r.length;
      to = r.to;

      printf("from %lu to %lu off %u\n",from,to,r.version.v.blk_offset);
      if (from == to) {

         return -1;
      }

      ++n;

      result = tree_iter_dec(&it, NULL);
   }

   return n;

}
