#env().Append(CPPDEFINES = {'USERLEVEL': None,
#                           'LINUX': None,
#                           '__VMK_DAEMON__':None,
#                           '_GNU_SOURCE':None,
#                           'THREADSAFE=0':None,
#                           'HAVE_INTPTR_T':None,
#                           'SQLITE_OMIT_LOAD_EXTENSION':None,
#                           'SQLITE_OMIT_FAULTINJECTOR':None,
#                           'SQLITE_ENABLE_LOCKING_STYLE':None,
#                           'SQLITE_FIXED_LOCKING_STYLE=dotlockLockingStyle':None}
#            )

files += ['cloudfslib.c', 'cloudfsdb.c', 'sqlite3.c',
   '../../modules/vmkernel/cloudfs/httplib/parseHttp.c',
   '../../modules/vmkernel/cloudfs/shalib/sha1-compress.c',
   '../../modules/vmkernel/cloudfs/shalib/sha1-meta.c',
   '../../modules/vmkernel/cloudfs/shalib/sha1.c']

includes += ['#bora/lib/cloudfs', '#bora/modules/vmkernel/cloudfs', '#bora/modules/vmkernel/cloudfs/httplib']

