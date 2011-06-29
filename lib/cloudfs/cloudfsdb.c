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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <ctype.h>
#include <str.h>

#include <sqlite3.h>

#include "logfsHash.h"
#include "cloudfsdb.h"

static pthread_mutex_t dbMutex;
static sqlite3 *db;

static char peerTableDef[] =
    "create table peer (hostName string, hostId char(40), unique(hostId));";
static char diskTableDef[] =
    "create table disk (diskId char(40), unique(diskId));";

static char secretViewTableDef[] =
    "create table secretView ("
    " diskId char(40), "
    " secretView char(40), " " publicView char(40), " " unique(diskId));";

static char invViewTableDef[] =
    "create table invalidView (publicView char(40), secretView char(40), unique(publicView));";
static char viewMemberTableDef[] =
    "create table viewMember (publicView char(40), hostId char(40), unique(publicView,hostId));";

static char *tableDefs[] = {
   peerTableDef,
   diskTableDef,
   secretViewTableDef,
   invViewTableDef,
   viewMemberTableDef,
};

int initDatabase(void)
{
   int i;
   int r;

   pthread_mutex_init(&dbMutex, NULL);

   r = sqlite3_open("/vmfs/volumes/local/testdb", &db);

   if (r != SQLITE_OK) {
      r = sqlite3_open("/local/testdb", &db);
   }

   else if (r != SQLITE_OK) {
      r = sqlite3_open("/testdb", &db);
   }

   assert(r == SQLITE_OK);

   for (i = 0; i < sizeof(tableDefs) / sizeof(tableDefs[0]); i++) {
      do {
         r = sqlite3_exec(db, tableDefs[i], NULL, NULL, NULL);
      } while (r == SQLITE_BUSY);

      //if(r!=SQLITE_OK) fprintf(stderr,"sql err %d\n",r);

      //assert(r==SQLITE_OK);
   }
   return r;
}

int SQL(int (*callback) (void *, int, char **, char **), void *data,
        char *format, ...)
{

   int r;
   char sql[512];
   va_list args;
   va_start(args, format);
   Str_Vsnprintf(sql, sizeof(sql), format, args);
   va_end(args);

   pthread_mutex_lock(&dbMutex);
   do {
      r = sqlite3_exec(db, sql, callback, data, NULL);
   } while (r == SQLITE_BUSY);
   pthread_mutex_unlock(&dbMutex);

   if (r != SQLITE_OK) {
      //printf("WARNING %s fails\n",sql);
   }
   return r;
}

int SQL2(int (*callback) (void *, int, char **, char **), void *data,
         char *format, ...)
{

   int r;
   char sql[512];
   va_list args;
   va_start(args, format);
   Str_Vsnprintf(sql, sizeof(sql), format, args);
   va_end(args);

   puts(sql);

   pthread_mutex_lock(&dbMutex);
   do {
      r = sqlite3_exec(db, sql, callback, data, NULL);
   } while (r == SQLITE_BUSY);
   pthread_mutex_unlock(&dbMutex);

   if (r != SQLITE_OK) {
      fprintf(stderr, "WARNING %s fails\n", sql);
      fflush(stderr);
   }
   return r;
}

int insertDisk(Hash diskId)
{
   char sDiskId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sDiskId, &diskId);

   return SQL(NULL, NULL, "insert into disk values('%s');", sDiskId);
}

int insertSecretView(Hash diskId, Hash secretView)
{
   int r;
   char sDiskId[SHA1_HEXED_SIZE];
   char sSecretView[SHA1_HEXED_SIZE];
   char sPublicView[SHA1_HEXED_SIZE];

   Hash publicView = LogFS_HashApply(secretView);
   LogFS_HashPrint(sDiskId, &diskId);
   LogFS_HashPrint(sSecretView, &secretView);
   LogFS_HashPrint(sPublicView, &publicView);

   r = SQL(NULL, NULL, "insert into secretView values('%s','%s','%s');",
               sDiskId, sSecretView, sPublicView);

   if (r != 0) {
      r = SQL(NULL, NULL,
              "update secretView set secretView='%s',publicView='%s' "
              "where diskId='%s';", sSecretView, sPublicView, sDiskId);
   }

   return r;
}

int insertPeer(const char *hostName, Hash hostId)
{
   char sHostId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sHostId, &hostId);

   return SQL(NULL, NULL, "insert into peer values('%s','%s');", hostName,
           sHostId);
}

int invalidateView(Hash secretView)
{
   int r;
   Hash publicView = LogFS_HashApply(secretView);

   char sPublicView[SHA1_HEXED_SIZE];
   char sSecretView[SHA1_HEXED_SIZE];

   LogFS_HashPrint(sPublicView, &publicView);
   LogFS_HashPrint(sSecretView, &secretView);

   r = SQL(NULL, NULL, "insert into invalidView values('%s','%s');",
           sPublicView, sSecretView);

   SQL(NULL, NULL, "delete from viewMember where publicview='%s';",
       sPublicView);

   return r;
}

int insertViewMember(Hash publicView, Hash hostId)
{
   char sPublicView[SHA1_HEXED_SIZE];
   char sHostId[SHA1_HEXED_SIZE];

   LogFS_HashPrint(sPublicView, &publicView);
   LogFS_HashPrint(sHostId, &hostId);

   return SQL2(NULL, NULL, "insert into viewMember values('%s','%s');",
               sPublicView, sHostId);
}

static int getSingleValueCallback(void *pArg, int nArg, char **azArg,
                                  char **azCol)
{
   Str_Strcpy(pArg, azArg[0],100);
   return 0;
}

Hash getSecretView(Hash diskId)
{
   int r;
   char sDiskId[SHA1_HEXED_SIZE];
   char sSecretView[SHA1_HEXED_SIZE];
   Hash secretView;

   LogFS_HashClear(&secretView);
   LogFS_HashPrint(sDiskId, &diskId);

   r = SQL(getSingleValueCallback, sSecretView,
               "select secretView from secretView where diskId='%s';", sDiskId);

   if (r == SQLITE_OK) {
      LogFS_HashSetString(&secretView, sSecretView);
   }
   return secretView;
}

static int getPeerForHostCallback(void *pArg, int nArg, char **azArg,
                                  char **azCol)
{
   Str_Strcpy(pArg, azArg[0],100);
   return 0;
}

int getPeerForHost(char *result, Hash hostId)
{
   char sHostId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sHostId, &hostId);

   *result = 0;

   return SQL(getPeerForHostCallback, result,
              "select hostName from peer where hostId='%s';", sHostId);
}

int getNextPeer(char *result, Hash hostId)
{
   char sHostId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sHostId, &hostId);

   *result = 0;

   return SQL(getPeerForHostCallback, result,
              "select hostName from peer where hostId="
              "(select max(hostId) from "
              " (select min(hostId) as hostId from peer union "
              "  select min(hostId) as hostId from peer where hostId>'%s'));",
              sHostId);
}

int getRandomPeer(char *result, Hash hostId)
{
   char sHostId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sHostId, &hostId);

   *result = 0;


   return SQL(getPeerForHostCallback, result,
           "select hostName from peer where hostId!='%s' order by random() limit 1;",
           sHostId);
}

int getRandomViewMember(char *result, Hash diskId)
{
   char sDiskId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sDiskId, &diskId);

   *result = 0;

   return SQL2(getPeerForHostCallback, result,
                "select peer.hostName "
                " from peer,viewMember,secretView "
                "  where secretView.diskId='%s' and "
                "   secretView.publicView=viewMember.publicView and "
                "   peer.hostId=viewMember.hostId order by random() limit 1;",
                sDiskId);
}

static int getHostPairCallback(void *pArg, int nArg, char **azArg, char **azCol)
{
   HostIdRecord *hr = pArg;
   int numIds = hr->numIds++;
   LogFS_HashSetString(&hr->hostIds[numIds], azArg[0]);
   hr->hostNames[numIds] = malloc(256);
   Str_Strcpy(hr->hostNames[numIds], azArg[1],100);
   return 0;
}

int selectNRandomPeers(HostIdRecord *hir, int n)
{
   int r = SQL(getHostPairCallback, hir,
               "select hostId,hostName from peer order by random() limit %d;",
               n);

   assert(r == SQLITE_OK);
   return r;
}

int selectHostsInView(HostIdRecord *hir, Hash publicView, Hash exclude)
{
   char sPublicView[SHA1_HEXED_SIZE];
   char sExclude[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sPublicView, &publicView);
   LogFS_HashPrint(sExclude, &exclude);

   return SQL(getHostPairCallback, hir,
               "select peer.hostId,peer.hostName from viewMember,peer "
               " where publicView='%s' and viewMember.hostId!='%s' and peer.hostId=viewMember.hostId "
               "  order by peer.hostId;", sPublicView, sExclude);
}

int selectHostsNotInView(HostIdRecord *hir, Hash publicView, int howMany)
{
   char sPublicView[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sPublicView, &publicView);

   return SQL(getHostPairCallback, hir,
               "select hostId,hostName from peer where hostId not in "
               "(select peer.hostId as hostId from viewMember,peer where "
               "publicView='%s' and peer.hostId=viewMember.hostId) "
               "order by random() limit %d;", sPublicView, howMany);
}

static int selectPeersCallback(void *pArg, int nArg, char **azArg, char **azCol)
{
   char **result = pArg;
   Str_Sprintf(*result,100,"%s @ %s\n", azArg[0], azArg[1]);
   *result += strlen(azArg[0]) + 3 + strlen(azArg[1]) + 1;
   **result = '\0';
   return 0;
}

int selectRandomPeers(char **result, Hash hostId, int howMany)
{
   char sHostId[SHA1_HEXED_SIZE];
   LogFS_HashPrint(sHostId, &hostId);

   return SQL(selectPeersCallback, result,
           "select hostId,hostName from peer where hostId!='%s' order by random() limit %d;",
           sHostId, howMany);
}
