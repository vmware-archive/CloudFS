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
struct viewchange {
   uint8_t view[20];
   uint8_t invalidates_view[20];

   uint16_t num_replicas;
   uint8_t hosts[5][20];

} __attribute__ ((__packed__));

Hash getHostId(void);
Hash getCurrentId(Hash diskId);
Hash getEntropy(Hash diskId);
Hash getSecret(Hash diskId);
uint64 getLsn(Hash diskId);

int appendHead(int fd, Hash parent, Hash id);
int appendBranch(int fd, Hash parent, Hash host0, Hash host1, Hash host2,
                 Hash secretView, Hash * retSecret);
int forceSecret(int fd, Hash diskId, Hash secretView, Hash hostId, Hash newView,
                Hash * retSecret);
int setSecret(Hash diskId, Hash secret, Hash secretView);

int forwardBranch(struct log_head *head, int numHosts, int threshold,
                  char **hostNames, Hash secret, Hash secretView);

int isHostInReplicaSet(Hash hostId, struct viewchange *v);
