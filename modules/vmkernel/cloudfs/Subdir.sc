files += Split('''

   shalib/sha1-compress.c  shalib/sha1-meta.c  shalib/sha1.c

   binHeap.c
   btree.c
   bTreeRange.c
   common.c
   fingerPrint.c
   graph.c
   hashDb.c
   httplib/parseHttp.c
   log.c
   logCompactor.c
   logfs.c
   logfsCheckPoint.c
   logfsHttpClient.c
   logfsHttpd.c
   logfsIO.c
   logfsNet.c
   logfsPosix.c
   logfsVsi.c
   logModule.c
   metaLog.c
   obsoleted.c
   pagedTree.c
   rangemap.c
   remoteLog.c
   vDisk.c
   vDiskMap.c
   vebTree.c
               ''')

vsi += [(['logfs_vsi.h'], 'cloudfs')]

includes += [ '#bora/vmkernel/sched',
              '#bora/vmkernel/filesystems',
              '#bora/modules/vmkernel/cloudfs/httplib',
              ] + env()['VMKERNEL_TCPIP_INCLUDES']

