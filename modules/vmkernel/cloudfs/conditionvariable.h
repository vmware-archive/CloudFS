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
#ifndef __CONDITIONVARIABLE_H__
#define __CONDITIONVARIABLE_H__

#ifndef VMKERNEL
#include <pthread.h>

/* XXX this is some old and very bad code that is rarely used
 * and will be phased out soon. */

typedef struct {
   pthread_cond_t cv;
   pthread_mutex_t mutex;
   int counter;
} LogFS_ConditionVariable;

static inline void LogFS_ConditionVariableInit(LogFS_ConditionVariable *cv,
                                               unsigned rank)
{
   pthread_mutex_init(&cv->mutex, NULL);
   pthread_cond_init(&cv->cv, NULL);
   cv->counter = 0;
}

static inline void LogFS_ConditionVariableWait(LogFS_ConditionVariable *cv)
{
   pthread_mutex_lock(&cv->mutex);
   int oldcounter = cv->counter;
   while (cv->counter == oldcounter)
      pthread_cond_wait(&cv->cv, &cv->mutex);
   pthread_mutex_unlock(&cv->mutex);
}

static inline void LogFS_ConditionVariableSignal(LogFS_ConditionVariable *cv)
{
   pthread_mutex_lock(&cv->mutex);
   ++(cv->counter);
   pthread_cond_signal(&cv->cv);
   pthread_mutex_unlock(&cv->mutex);
}

#else

typedef struct {
   List_Links queue;
   SP_SpinLock lock;
} LogFS_ConditionVariable;

static inline void LogFS_ConditionVariableInit(LogFS_ConditionVariable *cv,
                                               unsigned rank)
{
   List_Init(&cv->queue);
   SP_InitLock("CVLock", &cv->lock, rank);
}

static inline void LogFS_ConditionVariableWait(LogFS_ConditionVariable *cv)
{
   SP_Lock(&cv->lock);
   CpuSched_Wait(&cv->queue, CPUSCHED_WAIT_SCSI, &cv->lock);
}

static inline void LogFS_ConditionVariableSignal(LogFS_ConditionVariable *cv)
{
   CpuSched_Wakeup(&cv->queue);

}

#endif
#endif                          /* __CONDITIONVARIABLE_H__ */
