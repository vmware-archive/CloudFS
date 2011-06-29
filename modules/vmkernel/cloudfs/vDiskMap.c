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
#include "logtypes.h"
#include "logfsHash.h"
#include "vDisk.h"
#include "vDiskMap.h"

LogFS_VDisk *vdisks[1024];

/* XXX we need locking for all of this, and need to adhere to code conventions */

Atomic_uint32 numDisks;

void LogFS_DiskMapInit(void)
{
   Atomic_Write(&numDisks, 0);
}

LogFS_VDisk *LogFS_DiskMapLookupDisk(Hash disk)
{
   int i;
   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];
      if (LogFS_HashEquals(disk, LogFS_VDiskGetBaseId(vd))) {
         return vd;
      }
   }
   return NULL;
}

LogFS_VDisk *LogFS_DiskMapLookupDiskForVersion(Hash id)
{
   int i;
   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];
      Hash cur = LogFS_VDiskGetCurrentId(vd);
      if (LogFS_HashEquals(id, cur)) {
         return vd;
      }
   }
   return NULL;
}

/* XXX we should allow multiple orphans with same parent */

LogFS_VDisk *LogFS_DiskMapLookupOrphan(Hash id)
{
   int i;

   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];
      if (LogFS_VDiskIsOrphaned(vd)
          && LogFS_HashEquals(id, LogFS_VDiskGetParentDiskId(vd))) ;
      {
         return vd;
      }
   }
   return NULL;
}

void LogFS_DiskMapInsert(LogFS_VDisk *vd)
{
   vdisks[Atomic_FetchAndInc(&numDisks)] = vd;
}

size_t LogFS_DiskMapListDisks(char *s)
{
   int i;
   char *s0 = s;

   for (i = 0; i < Atomic_Read(&numDisks); i++) {
      LogFS_VDisk *vd = vdisks[i];
      LogFS_Hash disk = LogFS_VDiskGetBaseId(vd);
      LogFS_Hash id = LogFS_VDiskGetCurrentId(vd);

#if 0
      LogFS_Hash id;
      if (VDISK_INTERFACE(vd)->type == VDI_VDISK)
         id = ((LogFS_VDisk *)vd)->view;
      else
         id = VDISK_INTERFACE(vd)->GetCurrentId(vd);
#endif

      LogFS_HashPrint(s, &disk);
      s += SHA1_HEXED_SIZE - 1;
      *s++ = ':';

      LogFS_HashPrint(s, &id);
      s += SHA1_HEXED_SIZE - 1;

      *s++ = '\n';
   }

   return s - s0;
}

#if 0
#include "btree.h"
#include "vdisk.h"

/* B-tree listing all virtual disks */

btree_t disk_tree;

int disk_cmp(LogFS_VDisk *a, LogFS_VDisk *b)
{
   Hash *ha = (Hash *) a;
   Hash *hb = (Hash *) b;
   return memcmp(ha->raw, hb->raw, SHA1_DIGEST_SIZE);
}

static disk_block_t alloc_node_mem(btree_t *t)
{
   return (disk_block_t) malloc(t->real_node_size);
}

static const node_t *get_node_mem(btree_t *t, disk_block_t block)
{
   return (const node_t *)block;
}

static node_t *edit_node_mem(btree_t *t, disk_block_t block)
{
   return (node_t *)block;
}

struct disk_tree_elem {
   char hash[20];
   LogFS_VDiskInterface *vd;
} __attribute__ ((__packed__));

LogFS_VDiskInterface *find_vdisk(Hash disk)
{
   struct disk_tree_elem e;
   memcpy(e.hash, disk.raw, SHA1_DIGEST_SIZE);

   if (tree_find(&disk_tree, (elem_t *) & disk)) {
      printf("disk found %p\n", e.vd);
      return e.vd;

   }
}

void insert_vdisk(Hash disk, LogFS_VDiskInterface * vd)
{
   struct disk_tree_elem e;
   memcpy(e.hash, disk.raw, SHA1_DIGEST_SIZE);
   e.vd = vd;

   tree_insert(&disk_tree, (elem_t *) & e);
}

void init_vdisk_tree(void)
{
   btree_callbacks_t callbacks;
   callbacks.cmp = disk_cmp;
   callbacks.alloc_node = alloc_node_mem;
   callbacks.edit_node = edit_node_mem;
   callbacks.get_node = get_node_mem;

   LogFS_VDisk *base = malloc(0x4000); // XXX fixed sized memory of vdisk tree

   tree_create(&disk_tree, &callbacks, SHA1_DIGEST_SIZE,
               sizeof(LogFS_VDiskInterface *), 0x40, base);
}

int main()
{
   init_vdisk_tree();

   Hash disk;
   LogFS_HashRandomize(&disk);

   LogFS_VDiskInterface vd;
   LogFS_VDiskInterfaceInit(&vd, disk, NULL);

}
#endif
