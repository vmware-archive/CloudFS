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
/* **********************************************************
 * **********************************************************/

#include "vm_types.h"
#include "vm_libc.h"
#include "vmkernel.h"
#include "splock.h"
#include "libc.h"
#include "mod_loader_public.h"

#include "fss_int.h"
#include "logfs_int.h"
#include "fs_common.h"
#include "fsSwitch.h"

#include "devfs.h"
#include "deviceLib.h"
#include "vmkapi.h"
#include "module_ns.h"

#define LOGLEVEL_MODULE FS3
#define LOGLEVEL_MODULE_LEN 3
#include "log.h"

VMK_VERSION_INFO("Version" VMK_STRINGIFY(CBRCFILTER_MAJOR_VERSION) "."
                       VMK_STRINGIFY(CBRCFILTER_MINOR_VERSION)
                                        ", Built on: " __DATE__);
VMK_LICENSE_INFO(VMK_MODULE_LICENSE_VMWARE);
VMK_NAMESPACE_REQUIRED(MOD_VMKERNEL_NAMESPACE, MOD_VMKERNEL_NAMESPACE_VERSION);


#ifdef STATIC_LINK_VMLOGFS
/*
 * Linking the module in directly with vmkernel. This means we need
 * unique names for the init and cleanup routines. (cleanup won't be called).
 *
 * The "LOGFS" prefix is also used by bootUser to find these functions.
 */
#define INIT_FUNC_NAME          LogFS_init_module
#define CLEANUP_FUNC_NAME       LogFS_cleanup_module
#else
/*
 * Default dynamic module init/cleanup function names
 */
#define INIT_FUNC_NAME          init_module
#define CLEANUP_FUNC_NAME       cleanup_module
#endif

extern FSS_FSOps logfsOps;

// Registration ID obtained from FSS
// Module ID for this module
vmk_ModuleID logfsModuleID;

/*
 *-----------------------------------------------------------------------------
 *
 * init_module --
 *    Register this FS implementation with the VMKernel FS switch.
 *
 * Results:
 *    0 on success, -1 on error.
 *
 * Side effects:
 *    None.
 *
 *-----------------------------------------------------------------------------
 */
int INIT_FUNC_NAME(void)
{
   VMK_ReturnStatus status;

   logfsModuleID = World_GetLastModID();
   Log("logfsModuleID %d\n", logfsModuleID);
   status = LogFS_Init();
   if (status != VMK_OK) {
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
void CLEANUP_FUNC_NAME(void)
{
   Log("cleanup!\n");
   LogFS_Cleanup();
}
