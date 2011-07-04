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
/* *****************************************************************************
* ****************************************************************************/

/*
 * module.c --
 *
 *      Logfs based LSOM initialization and cleanup 
 *
 */
#include "thinDom.h"
#include "vm_types.h"
#include "vm_libc.h"
#include "vmkernel.h"
#include "mod_loader_public.h"
#include "world.h"
#include "system.h"
#include "common.h"

/*
 *-----------------------------------------------------------------------------
 *
 * LogfsInit --
 *    Entry point for logfs initialization.
 *
 * Results:
 *    0 on success, -1 on error.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static int
InitLogFS()
{
   VMK_ReturnStatus status;

   // FixME: VMK_CheckVMKernelID is obsolete. Use VMK_GetHostID instead
   if (!VMK_CheckVMKernelID()) { 
      Warning("Invalid vmkernel ID %#x. Can't load LogFS driver",
              VMK_GetVMKernelID());
      return VMK_FAILURE;
   }

   status = LogFS_CommonInit();
   if (status != VMK_OK) {
      Warning("Logfs_CommonInit failed with %s", 
               VMK_ReturnStatusToString(status));
      return -1;
   }

   status = Dom_Init(World_GetLastModID());
   if (status != VMK_OK) {
      Warning("Logfs init failed with %s", VMK_ReturnStatusToString(status));
      LogFS_CommonCleanup();
      return -1;
   }

   return 0;
}


/*
 *-----------------------------------------------------------------------------
 *
 * cleanup_module --
 *    Called at module unload time. Cleanup module local data structures.
 *
 * Results:
 *    None.
 *
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */

static void
CleanupLogFS()
{
   DEBUG_ONLY(Log("cleanup!\n"));
   Dom_Cleanup();
   LogFS_CommonCleanup();
}


#ifdef STATIC_LINK_VMLOGFS

/* Statically linked, expose unique init/cleanup functions */
int 
LogFS_init_module(void)
{
   return InitLogFS();
}

void 
LogFS_cleanup_module(void)
{
   CleanupLogFS();
}

#else

/* Compiled as part of module expose standard init/cleanup functions */
int 
init_module(void)
{
   return InitLogFS();
}
void cleanup_module(void)
{
   CleanupLogFS();
}
VMK_VERSION_INFO("Version" VMK_STRINGIFY(CBRCFILTER_MAJOR_VERSION) "."
                       VMK_STRINGIFY(CBRCFILTER_MINOR_VERSION)
                                        ", Built on: " __DATE__);
VMK_LICENSE_INFO(VMK_MODULE_LICENSE_VMWARE);
#endif
