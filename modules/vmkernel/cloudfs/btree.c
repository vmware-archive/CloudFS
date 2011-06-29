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
#include "btree.h"

#ifndef VMKERNEL

#if 1
#include <stdio.h>
extern int refcount;
#define IN int refc = refcount;printf("rc %d\n",refc);
#define OUT if(refcount!=refc) {printf("fail %s:%d %d instead of %d\n",__FUNCTION__,__LINE__,refcount,refc);assert(0);}
//define OUT1 if(refcount!=refc+1) {printf("fail %s:%d %d instead of %d\n",__FUNCTION__,__LINE__,refcount,refc);assert(0);}
#endif

//#define IN
//#define OUT

#else

#define IN
#define OUT

#endif

/** 
	The B-Tree
**/

static int lower_bound(btree_t *t, const node_t *n, const elem_t * e)
{
   int len = n->num_elems;
   int first = 0;
   int half, middle;
   while (len > 0) {
      half = len >> 1;
      middle = first;
      middle += half;
      if (compare(t, elem_key(t, nth_elem(t, n, middle)), elem_key(t, e)) < 0) {
         first = middle;
         ++first;
         len = len - half - 1;
      } else
         len = half;
   }
   return first;
}

static int upper_bound(btree_t *t, const node_t *n, elem_t * e)
{
   int len = n->num_elems;
   int first = 0;
   int half, middle;
   while (len > 0) {
      half = len >> 1;
      middle = first;
      middle += half;
      if (compare(t, elem_key(t, e), elem_key(t, nth_elem(t, n, middle))) < 0) {
         len = half;
      } else {
         first = middle;
         ++first;
         len = len - half - 1;
      }
   }
   return first;
}

static void insert_elem(btree_t *t, node_t *n, int pos, const elem_t* e,
                        disk_block_t disk_child, int cp, void *context)
{
   int ne = n->num_elems;

   memmove(nth_elem_l(t, n, pos + 1), nth_elem(t, n, pos),
           (ne - pos) * elem_size(t));

   memcpy(nth_elem_l(t, n, pos), e, elem_size(t));

   if (!n->leaf) {
      void *dst = n->children + pos + cp + 1;
      void *src = n->children + pos + cp;
      int len = (ne + 1) - (pos + cp);

      memmove(dst, src, len * sizeof(disk_block_t));

      n->children[pos + cp] = disk_child;
   }
   n->num_elems++;
}

/* Erase an element from a node, and erase one of the corresponding
 * child pointers (cp==0 means left-of, cp==1 means right-of) */

static void erase_elem(btree_t *t, node_t *n, int pos, int cp)
{
   int cpos = pos + cp;

   n->num_elems--;

   if (pos <= n->num_elems) {
      memmove(nth_elem_l(t, n, pos), nth_elem(t, n, pos + 1),
              (n->num_elems - pos) * elem_size(t));

      if (!n->leaf) {
         memmove(n->children + cpos, n->children + cpos + 1,
                 (1 + n->num_elems - cpos) * sizeof(disk_block_t));
      }
   }
}

static void split_child(btree_t *, node_t *, int keyindex, node_t *targetnode,
                        void *context);

static void insert_non_full(btree_t *t, disk_block_t disk_node,
                            node_t *parent, elem_t *e, void *context)
{
   int branch = t->branch;

   node_t *n = edit_node(t, disk_node, parent, context);

   int pos = upper_bound(t, n, e);
   if (n->leaf) {
      insert_elem(t, n, pos, e, 0, 1, context);
   } else {
      const node_t *child = get_child(t, n, pos, context);
      int num_child = child->num_elems;
      put_node(t, child, context);

      if (num_child == 2 * branch - 1) {
         node_t *wc = edit_child(t, n, pos, context);

         split_child(t, n, pos, wc, context);
         pos = upper_bound(t, n, e);

         put_node(t, wc, context);
      }

      insert_non_full(t, n->children[pos], n, e, context);

   }

   put_node(t, n, context);
}

/**
	Merges the right node into the left, with e as the glue.
	left::[e]::right
**/

static void merge_nodes(btree_t *t, node_t *left, const node_t *right,
                        const elem_t* e, void *context)
{
   int nl;
   int nr;

   IN;

   nl = left->num_elems;
   nr = right->num_elems;

   /* copy glue element to end of left */

   memcpy(nth_elem_l(t, left, nl), e, elem_size(t));

   /* copy elements from right, after the newly inserted glue element (nl+1) */

   memcpy(nth_elem_l(t, left, nl + 1), nth_elem(t, right, 0), nr * elem_size(t));

   /* copy child pointers */
   if (!left->leaf) {
      memcpy(left->children + (nl + 1), right->children,
             sizeof(disk_block_t) * (nr + 1));
   }

   left->num_elems = nl + 1 + nr;

   free_node(t, right);

   OUT;
}

/* Delete the element e from node disk_node */

static void delete_from(btree_t *t, disk_block_t disk_node,
                        node_t *parent, const elem_t * e, void *context)
{
   int pos;
   int branch = t->branch;
   node_t *n;
   IN;

   n = edit_node(t, disk_node, parent, context);
   pos = lower_bound(t, n, e);

   if (n->leaf) {
      IN;
      if (n->num_elems != pos &&
          compare(t, elem_key(t, nth_elem(t, n, pos)), elem_key(t, e)) == 0) {
         erase_elem(t, n, pos, -1);
      }
      OUT;
      goto out;
   }

   /* do we have the element? */
   if (pos != n->num_elems
       && compare(t, elem_key(t, nth_elem(t, n, pos)), elem_key(t, e)) == 0) {

      const node_t *left_child;
      const node_t *right_child;
      int num_left;
      int num_right;

      IN;

      left_child = get_child(t, n, pos, context);
      right_child = get_child(t, n, pos + 1, context);
      num_left = left_child->num_elems;
      num_right = right_child->num_elems;

      put_node(t, left_child, context);
      put_node(t, right_child, context);

      if (num_left >= branch)   // take from left sub-tree
      {
         const node_t *c;
         disk_block_t sub_node = n->children[pos];

         for (;;) {
            c = get_node(t, sub_node, context);
            sub_node = c->children[c->num_elems];
            if (c->leaf)
               break;

            put_node(t, c, context);
         }

         memcpy(nth_elem_l(t, n, pos), nth_elem(t, c, c->num_elems - 1),
                elem_size(t));
         put_node(t, c, context);

         delete_from(t, n->children[pos], n, nth_elem(t, n, pos), context);
      } else if (num_right >= branch)  // take from right sub-tree
      {
         const node_t *c;
         disk_block_t sub_node = n->children[pos + 1];

         for (;;) {
            c = get_node(t, sub_node, context);
            sub_node = c->children[0];
            if (c->leaf)
               break;

            put_node(t, c, context);
         }

         memcpy(nth_elem_l(t, n, pos), nth_elem(t, c, 0), elem_size(t));

         put_node(t, c, context);

         delete_from(t, n->children[pos + 1], n, nth_elem(t, n, pos), context);
      } else {
         node_t *lc;
         const node_t *rc;
         elem_t* tmp = malloc(elem_size(t));

         memcpy(tmp, nth_elem(t, n, pos), elem_size(t));

         lc = edit_child(t, n, pos, context);
         rc = get_child(t, n, pos + 1, context);
         merge_nodes(t, lc, rc, tmp, context);
         put_node(t, rc, context);
         put_node(t, lc, context);

         erase_elem(t, n, pos, 1);

         delete_from(t, n->children[pos], n, tmp, context);
         free(tmp);

      }

      OUT;
   } else                       // we dont, but we know who does...
   {
      int num_left = 0;
      int num_right = 0;
      disk_block_t disk_subtree;
      const node_t *s;
      int num_elems;

      if (pos > 0) {
         const node_t *left = get_child(t, n, pos - 1, context);
         num_left = left->num_elems;
         put_node(t, left, context);
      }

      if (pos < n->num_elems) {
         const node_t *right = get_child(t, n, pos + 1, context);
         num_right = right->num_elems;
         put_node(t, right, context);
      }

      disk_subtree = n->children[pos];
      s = get_node(t, disk_subtree, context);
      num_elems = s->num_elems;
      put_node(t, s, context);

      if (num_elems == branch - 1) {
         IN;
         // does the right sibling have enough??
         if (num_right > branch - 1) {

            node_t *subtree;
            node_t *right_sibling;
            IN;

            subtree = edit_node(t, disk_subtree, n, context);
            right_sibling = edit_child(t, n, pos + 1, context);

            insert_elem(t, subtree, num_elems, nth_elem(t, n, pos),
                        right_sibling->children[0], 1, context);

            memcpy(nth_elem_l(t, n, pos), nth_elem(t, right_sibling, 0),
                   elem_size(t));
            erase_elem(t, right_sibling, 0, 0);

            put_node(t, right_sibling, context);
            put_node(t, subtree, context);

            OUT;
         } else if (num_left > branch - 1) {

            node_t *subtree;
            node_t *left_sibling;
            disk_block_t disk_child;

            IN;

            subtree = edit_node(t, disk_subtree, n, context);
            left_sibling = edit_child(t, n, pos - 1, context);

            disk_child =
                left_sibling->children[left_sibling->num_elems];

            insert_elem(t, subtree, 0, nth_elem(t, n, pos - 1), disk_child, 0,
                        context);

            memcpy(nth_elem_l(t, n, pos - 1),
                   nth_elem(t, left_sibling, left_sibling->num_elems - 1)
                   , elem_size(t));

            erase_elem(t, left_sibling, left_sibling->num_elems, 1);

            put_node(t, left_sibling, context);
            put_node(t, subtree, context);

            OUT;
         } else {
            if (num_right != 0) {
               node_t *subtree;
               const node_t *right_sibling;

               IN;
               subtree = edit_node(t, disk_subtree, n, context);
               right_sibling = get_child(t, n, pos + 1, context);

               merge_nodes(t, subtree, right_sibling, nth_elem(t, n, pos),
                           context);

               put_node(t, right_sibling, context);
               put_node(t, subtree, context);

               erase_elem(t, n, pos, 1);

               OUT;
            } else {
               int cp = (pos == n->num_elems) ? 1 : 0;

               const node_t *subtree = get_node(t, disk_subtree, context);
               node_t *left_sibling = edit_child(t, n, pos - 1, context);

               merge_nodes(t, left_sibling, subtree, nth_elem(t, n, pos - cp),
                           context);

               put_node(t, left_sibling, context);
               put_node(t, subtree, context);

               erase_elem(t, n, pos - cp, cp);

               delete_from(t, n->children[pos - 1], n, e, context);

               goto out;        /* do not recurse any further down the tree */
            }
         }
      }

      delete_from(t, disk_subtree, n, e, context);

   }
 out:
   put_node(t, n, context);
   OUT;

}

static void split_child(btree_t *t, node_t *parent, int keyindex, node_t *child,
                        void *context)
{
   int i;
   int branch = t->branch;

   disk_block_t disk_new_node = t->callbacks.alloc_node(t, context);
   node_t *new_node = edit_node(t, disk_new_node, parent, context);
   new_node->num_elems = 0;
   new_node->leaf = 1;

   if (!child->leaf) {
      new_node->leaf = 0;

      for (i = 0; i < child->num_elems + 1 - branch; i++) {
         new_node->children[i] = child->children[i + branch];
      }
   }

   new_node->num_elems = child->num_elems - branch;

   memcpy(nth_elem_l(t, new_node, 0), nth_elem(t, child, branch),
          new_node->num_elems * elem_size(t));

   insert_elem(t, parent, keyindex, nth_elem(t, child, branch - 1),
               disk_new_node, 1, context);

   for (i = branch; i < child->num_elems + 1; i++)
      child->children[i] = 0;

   child->num_elems = branch - 1;

   put_node(t, new_node, context);

}

void tree_insert(btree_t *t, elem_t * e, void *context)
{
   const node_t *old_root = get_node(t, t->root, context);
   assert(old_root);

   /* If the root node is full, we replace it with a new one */

   if (old_root->num_elems == 2 * t->branch - 1) {

      node_t *r;
      node_t *n;
      disk_block_t disk_old_root = t->root;

      t->root = t->callbacks.alloc_node(t, context);
      n = edit_node(t, t->root, NULL, context);
      assert(n);
      n->num_elems = 0;
      n->leaf = 0;
      n->children[0] = disk_old_root;

      r = edit_node(t, disk_old_root, n, context);
      assert(r);
      split_child(t, n, 0, r, context);

      put_node(t, r, context);
      put_node(t, n, context);
   }

   insert_non_full(t, t->root, NULL, e, context);

   put_node(t, old_root, context);
}

void tree_delete(btree_t *t, elem_t * e, void *context)
{
   const node_t *r;

   IN;

   delete_from(t, t->root, NULL, e, context);
   r = get_node(t, t->root, context);

   if (r->num_elems == 0) {
      if (!r->leaf) {
         free_node(t, r);
         t->root = r->children[0];
      }
   }
   put_node(t, r, context);
   OUT;
}

const elem_t *tree_find_ref(btree_t *t, elem_t * e, void *context)
{
   const elem_t *r;
   const node_t *n = get_node(t, t->root, context);

   for (;;) {
      int pos = lower_bound(t, n, e);

      /* TODO fix this: */
      if (compare(t, elem_key(t, nth_elem(t, n, pos)), elem_key(t, e)) == 0
          || n->leaf) {
         if (compare(t, elem_key(t, nth_elem(t, n, pos)), elem_key(t, e)) == 0
             && n->num_elems > 0) {
            /* XXX we need a iter_free to put n later */
            r = nth_elem(t, n, pos);
            goto out;
         } else {
            r = NULL;
            goto out;
         }
      } else {
         const node_t *n2 = get_child(t, n, pos, context);
         put_node(t, n, context);
         n = n2;
      }
   }
 out:
   put_node(t, n, context);
   return r;
}

int tree_find(btree_t *t, elem_t * e, void *context)
{
   const elem_t *found = tree_find_ref(t, e, context);

   if (found) {
      memcpy(elem_value(t, e), elem_value(t, found), value_size(t));
      return 1;
   } else
      return 0;
}

/* return 0 if end, 1 if actual value */

tree_result_t tree_lower_bound(btree_t *t, btree_iter_t *it, elem_t *e,
                               void *context)
{
   int i;
   int r;
   int pos;
   const node_t *n;
   int stop;
   disk_block_t root = t->root;
   disk_block_t disk_node = root;
   int last_non_right_idx = -1;
   int num_elems;

   it->tree = t;
   it->depth = 0;

   for (i = 0;; i++) {
#ifdef VMKERNEL
      if (it->depth == TREE_MAX_DEPTH) {
         Panic("max depth reached at node %u, root %u\n", disk_node, root);
      }
#endif
      n = get_node(t, disk_node, context);

      if (n == NULL) {
         return tree_result_node_fault;
      }

      pos = lower_bound(t, n, e);
      if (pos < n->num_elems) {
         last_non_right_idx = i;
      }

      it->stack[it->depth++] = pos;

      stop = n->leaf;
      disk_node = n->children[pos];

      num_elems = n->num_elems;
      put_node(t, n, context);

      if (stop)
         break;
   }

   /* if we reached the end of a subtree, we have to backtrack
    * to the last seen node where we recursed through the not-last
    * element */
   if (pos == num_elems) {
      if (last_non_right_idx >= 0) {
         /* we can't get further to the right, so we have to
          * backtrack until we find a non-rightmost pointer,
          * and then iterate all the way to its leftmost child */

         it->depth = last_non_right_idx + 1;

         r = tree_result_found;
      } else {
         r = tree_result_end;
      }
   } else
      r = tree_result_found;

   return r;
}

#if 0
static void clear_subtree(btree_t *t, disk_block_t disk_node, void *context)
{
   int i;
   node_t *n = edit_node(t, disk_node, context);

   if (!n->leaf)
      for (i = 0; i < n->num_elems + 1; i++) {
         clear_subtree(t, n->children[i], context);
      }
   put_node(t, n, context);

   /* the first node is inlined in the btree struct */
   //if(n != &t->first_node) free(n);
}
#endif

tree_result_t tree_begin(btree_t *t, btree_iter_t *it, void *context)
{
   tree_result_t r = tree_result_found;

   disk_block_t disk_node = t->root;

   it->tree = t;
   it->depth = 0;

   for (;;) {
      const node_t *n = get_node(t, disk_node, context);
      int stop = n->leaf;
      disk_node = n->children[0];

      if (n->num_elems == 0)
         r = tree_result_end;
      it->stack[it->depth++] = 0;
      put_node(t, n, context);

      if (stop)
         break;
   }
   return r;
}

tree_result_t tree_end(btree_t *t, btree_iter_t *it, void *context)
{
   tree_result_t r = tree_result_found;
   disk_block_t disk_node = t->root;

   it->tree = t;
   it->depth = 0;

   for (;;) {
      const node_t *n = get_node(t, disk_node, context);
      int stop = n->leaf;
      disk_node = n->children[n->num_elems];

      if (n->num_elems == 0)
         r = tree_result_end;

      it->stack[it->depth++] = n->num_elems;
      put_node(t, n, context);

      if (stop)
         break;
   }
   it->stack[it->depth - 1]--;

   return r;
}

tree_result_t tree_iter_read(void *dst, btree_iter_t *it, void *context)
{
   btree_t *t = it->tree;
   const node_t *n;
   disk_block_t disk_node;
   int i;
   int keyidx;

   /* Loop down the tree guided by the iterator stack.  
    * Finish with n pointing to the node pointed to by the iterator. */

   for (i=0, disk_node=t->root ;; ) {

      n = get_node(t, disk_node, context);

      if(n==NULL) {
         return tree_result_node_fault;
      }

      disk_node = n->children[it->stack[i]];

      if(++i == it->depth) {
         break;
      }

      put_node(t, n, context);
   }

   keyidx = it->stack[it->depth - 1];
   if (keyidx > n->num_elems - 1) {
      keyidx = n->num_elems - 1;
   }
   memcpy(dst, nth_elem(t, n, keyidx), elem_size(t));
   put_node(t, n, context);

   return tree_result_found;
}

tree_result_t tree_iter_write(btree_iter_t *it, void *src, void *context)
{
   btree_t *t = it->tree;
   int i;
   int keyidx;

   node_t *n = edit_node(t, t->root, NULL, context);

   if (n==NULL) {
      return tree_result_node_fault;
   }

   for (i=0; i<it->depth-1; i++) {

      node_t *p = n;
      n = edit_node(t, p->children[it->stack[i]], p, context);
      put_node(t, p, context);

      if (n==NULL) {
         return tree_result_node_fault;
      }
   }

   keyidx = it->stack[it->depth - 1];
   if (keyidx > n->num_elems - 1) {
      keyidx = n->num_elems - 1;
   }
   memcpy(nth_elem_l(t, n, keyidx), src, elem_size(t));
   put_node(t, n, context);

   return tree_result_ok;
}

tree_result_t tree_iter_inc(btree_iter_t *it, void *context)
{
   btree_t *t = it->tree;
   disk_block_t disk_node = t->root;
   tree_result_t r;
   const node_t *n;
   int idx;
   int leaf;
   int num_elems;

   int last_non_right_idx = -1;

   int i;

   for (i = 0; i < it->depth - 1; i++) {
      const node_t *n = get_node(t, disk_node, context);

      if (n == NULL) {
         return tree_result_node_fault;
      }

      idx = it->stack[i];
      if (idx < n->num_elems) {
         last_non_right_idx = i;
      }

      disk_node = n->children[idx];
      put_node(t, n, context);
   }

   n = get_node(t, disk_node, context);

   if (n == NULL) {
      return tree_result_node_fault;
   }

   idx = it->stack[it->depth - 1];
   leaf = n->leaf;
   num_elems = n->num_elems;
   put_node(t, n, context);

   if (leaf) {
      if (idx + 1 < num_elems) {
         it->stack[it->depth - 1]++;
         r = tree_result_found;
      }
      /* at the root already? */
      else if (it->depth == 1) {
         r = tree_result_end;
      } else {
         if (last_non_right_idx >= 0) {
            /* we can't get further to the right, so we have to
             * backtrack until we find a non-rightmost pointer,
             * and then iterate all the way to its leftmost child */

            it->depth = last_non_right_idx + 1;

            r = tree_result_found;
         } else {
            r = tree_result_end;
         }

      }
   } else {
      /* address leftmost child of current node */

      it->stack[it->depth - 1]++;

      for (;;) {
         n = get_node(t, disk_node, context);

         if (n == NULL) {
            return tree_result_node_fault;
         }

         leaf = n->leaf;
         disk_node = n->children[0];
         put_node(t, n, context);

         if (leaf)
            break;

         it->stack[it->depth++] = 0;
      }

      r = tree_result_found;
   }

   return r;
}

tree_result_t tree_iter_dec(btree_iter_t *it, void *context)
{
   btree_t *t = it->tree;
   int last_non_left_idx = -1;
   tree_result_t r;
   const node_t *n;
   int idx;
   int i;
   int leaf;
   disk_block_t disk_node = t->root;

   for (i = 0; i < it->depth - 1; i++) {
      n = get_node(t, disk_node, context);
      if (n == NULL) {
         r = tree_result_node_fault;
         goto out;
      }

      idx = it->stack[i];
      if (idx > 0) {
         last_non_left_idx = i;
      }

      disk_node = n->children[idx];
      put_node(t, n, context);
   }

   n = get_node(t, disk_node, context);
   if (n == NULL) {
      r = tree_result_node_fault;
      goto out;
   }

   leaf = n->leaf;
   put_node(t, n, context);

   idx = it->stack[it->depth - 1];
   if (leaf) {
      if (idx > 0) {
         it->stack[it->depth - 1]--;
         r = tree_result_found;
      }
      /* at the root already? */
      else if (it->depth == 1) {
         r = tree_result_end;
      } else {
         if (last_non_left_idx >= 0) {
            /* we can't get further to the right, so we have to
             * backtrack until we find a non-rightmost pointer,
             * and then iterate all the way to its leftmost child */

            it->depth = last_non_left_idx + 1;
            it->stack[it->depth - 1]--;

            r = tree_result_found;
         } else {
            r = tree_result_end;
         }

      }
   } else {
      /* address rightmost child left of current node */

      n = get_node(t, disk_node, context);
      if (n == NULL) {
         r = tree_result_node_fault;
         goto out;
      }

      disk_node = n->children[idx];
      put_node(t, n, context);

      while (!leaf) {
         n = get_node(t, disk_node, context);
         if (n == NULL) {
            r = tree_result_node_fault;
            goto out;
         }

         disk_node = n->children[n->num_elems];
         leaf = n->leaf;

         it->stack[it->depth++] = n->num_elems - leaf;
         put_node(t, n, context);
      }

      r = tree_result_found;
   }

 out:
   return r;
}


#ifndef VMKERNEL
#include <stdio.h>
void print_node(btree_t *t, disk_block_t disk_node, FILE * f)
{
   int i;
   const node_t *n = get_node(t, disk_node, NULL);

   fprintf(f, "node%x [shape=record label=\"", disk_node);

   //unsigned long long* key0 = (unsigned long long*) elem_key(t,nth_elem(t,n,0));
   //unsigned long long* keyN = (unsigned long long*) elem_key(t,nth_elem(t,n,n->num_elems-1));
   //fprintf(f,"%lu: ",disk_node);
   for (i = 0; i < n->num_elems; i++) {

      unsigned long long *keyN =
          (unsigned long long *)elem_key(t, nth_elem(t, n, i));
      fprintf(f, "%lld, ", *keyN);
   }

   fprintf(f, "\"];\n");

   //const node_t* parent = n->user_data;
   //fprintf(f,"node%x->node%x [color=red]\n", disk_node,parent? parent->nodeIdx : parent);

   if (!n->leaf) {
      for (i = 0; i < n->num_elems + 1; i++) {
         disk_block_t disk_child = n->children[i];
         if (disk_child)
            print_node(t, disk_child, f);
         fprintf(f, "node%x->node%x\n", disk_node, n->children[i]);
      }
   }

   put_node(t, n, NULL);

}
#endif


#ifndef VMKERNEL
void print_tree(btree_t *t, FILE * f)
{
   fputs("digraph G { ", f);
   print_node(t, t->root, f);
   fputs("}", f);
}
#endif


static inline void tree_set_constants(btree_t *t, int key_size, int value_size,
                                      int node_size)
{
   t->key_size = key_size;
   t->value_size = value_size;
   t->real_node_size = node_size;
   t->branch = calc_branch_factor(t, node_size);
}

void tree_create(btree_t *t, btree_callbacks_t *callbacks,
                     int key_size, int value_size, int node_size,
                     void *user_data, void *context)
{
   node_t *r;

   memset(t, 0, sizeof(btree_t));
   t->callbacks = *callbacks;
   t->user_data = user_data;

   tree_set_constants(t, key_size, value_size, node_size);

   /* Create and initialize root node */
   t->root = t->callbacks.alloc_node(t, context);
   r = edit_node(t, t->root, NULL, context);
   r->num_elems = 0;
   r->leaf = 1;
   put_node(t, r, context);
}

void tree_reopen(btree_t *t, 
      btree_callbacks_t *callbacks, disk_block_t root,
      int key_size, int value_size, int node_size,
      void *user_data, void *context)
{
   memset(t, 0, sizeof(btree_t));

   t->callbacks = *callbacks;
   t->user_data = user_data;
   t->root = root;

   tree_set_constants(t, key_size, value_size, node_size);
}
