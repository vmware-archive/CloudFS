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
#include "logtypes.h"
#include "logfsIO.h"

VMK_ReturnStatus LogFS_DeviceRead(LogFS_Device *device,
                                  Async_Token * token,
                                  void *buf, log_size_t length,
                                  log_offset_t offset,
                                  LogFS_DiskSegmentType type)
{
   VMK_ReturnStatus status;
   FDS_Handle *fdsHandleArray[1];
   Async_Token *newToken = NULL;

   log_offset_t diskOffset =
       LogFS_DiskLayoutGetOffset(&device->diskLayout, type) + offset;
   //if(type != LogFS_LogSegmentsSection) ASSERT(diskOffset < LogFS_DiskLayoutGetOffset(&device->diskLayout,type+1));

   if (token == NULL) {
      newToken = token = Async_AllocToken(0);
   }

   SG_Array sg;
   SG_SingletonSGArray(&sg, diskOffset, (VA) buf, length, SG_VIRT_ADDR);
   fdsHandleArray[0] = device->fd;

   do {
      status = FDS_AsyncIO(fdsHandleArray, &sg, FS_READ_OP, token);
   } while (status == VMK_STORAGE_RETRY_OPERATION);

   if (newToken != NULL && status == VMK_OK) {
      Async_WaitForIO(token);
      Async_ReleaseToken(token);
   }

   return status;
}

VMK_ReturnStatus LogFS_DeviceWrite(LogFS_Device *device,
                                   Async_Token * token,
                                   SG_Array *sgArr,
                                   LogFS_DiskSegmentType type)
{
   VMK_ReturnStatus status;
   int i;
   log_offset_t diskOffset =
       LogFS_DiskLayoutGetOffset(&device->diskLayout, type);

   ASSERT(token);
   FDS_Handle **fdsHandleArray = malloc(sgArr->length*sizeof(FDS_Handle*));

   for(i=0;i<sgArr->length;++i) {
      sgArr->sg[i].offset += diskOffset;
      fdsHandleArray[i] = device->fd;
   }

   do {
      status = FDS_AsyncIO(fdsHandleArray, sgArr, FS_WRITE_OP, token);
   } while (status == VMK_STORAGE_RETRY_OPERATION);

   free(fdsHandleArray);

   return status;
}

VMK_ReturnStatus LogFS_DeviceWriteSimple(LogFS_Device *device,
                                   Async_Token * token,
                                   const void *buf, log_size_t length,
                                   log_offset_t offset,
                                   LogFS_DiskSegmentType type)
{
   VMK_ReturnStatus status;

   ASSERT(device);
   log_offset_t diskOffset =
       LogFS_DiskLayoutGetOffset(&device->diskLayout, type) + offset;
   FDS_Handle *fdsHandleArray[1];
   Async_Token *newToken = NULL;

   if (token == NULL) {
      newToken = token = Async_AllocToken(0);
   }

   SG_Array sg;

   SG_SingletonSGArray(&sg, diskOffset, (VA) buf, length, SG_VIRT_ADDR);

   fdsHandleArray[0] = device->fd;
   do {
      status = FDS_AsyncIO(fdsHandleArray, &sg, FS_WRITE_OP, token);
   } while (status == VMK_STORAGE_RETRY_OPERATION);

   if (newToken != NULL && status == VMK_OK) {
      Async_WaitForIO(token);
      Async_ReleaseToken(token);
   }


   return status;
}
