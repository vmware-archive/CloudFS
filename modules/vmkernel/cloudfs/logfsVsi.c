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

#include "globals.h"
#include "vmkernel.h"
#include "log.h"

#include "vm_basic_types.h"
#include "vm_basic_defs.h"
#include "vm_assert.h"
#include "vm_libc.h"

#include "vsiDefs.h"
#include "logfs_vsi.h"
#include "parse.h"

VMK_ReturnStatus LogFS_AddPhysicalDevice(const char *deviceName);

VMK_ReturnStatus
LogFS_VSIDeviceGet(VSI_NodeID nodeID,
                   VSI_ParamList * instanceArgs, VSI_LogDeviceStruct * data)
{
   return VMK_OK;
}

VMK_ReturnStatus
LogFS_VSIFastDeviceGet(VSI_NodeID nodeID,
                       VSI_ParamList * instanceArgs, VSI_LogDeviceStruct * data)
{
   return VMK_OK;
}

VMK_ReturnStatus
LogFS_VSIDeviceSet(VSI_NodeID nodeID,
                   VSI_ParamList * instanceArgs,
                   VSI_ParamList * inputArgs, VSI_Empty_Output * data)
{
   if (VSI_ParamListUsedCount(inputArgs) != 1)
      return VMK_BAD_PARAM_TYPE;

   VSI_Param *param = VSI_ParamListGetParam(inputArgs, 0);

   return LogFS_AddPhysicalDevice(VSI_ParamGetString(param));

}

VMK_ReturnStatus
LogFS_VSIFastDeviceSet(VSI_NodeID nodeID,
                       VSI_ParamList * instanceArgs,
                       VSI_ParamList * inputArgs, VSI_Empty_Output * data)
{
   if (VSI_ParamListUsedCount(inputArgs) != 1)
      return VMK_BAD_PARAM_TYPE;

   VSI_Param *param = VSI_ParamListGetParam(inputArgs, 0);

   return LogFS_AddPhysicalDevice(VSI_ParamGetString(param));

}
