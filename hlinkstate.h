#pragma once

#include "allocator.h"
#include "filejob.h"
#include "gtl/phmap.hpp"
#include "lmdb.h"
#include "spinlock.h"

typedef gtl::flat_hash_map<uint64_t, AllocRef> pairmap;

struct HLinkInfo {
    spinlock lock;

    std::atomic_bool transferred;
    uint64_t destInode;

    StringPartRef remote;
    LinkEntryRef links;
    size_t nlinkRC;
};

struct HLinkState {
    pthread_rwlock_t lock;

    MDB_env *env;

    size_t maxTableCacheSize;

    MDB_dbi forwardTable;
    pairmap forwardTableCache;

    MDB_dbi revTable;
    pairmap revTableCache;

    MDB_txn *wrTxn;
    gtl::flat_hash_map<int, MDB_txn *>
        rdTxns; // thread -> txn, this is real lazy
    spinlock rdTxnAquireLock;
};

struct HLinkInfoRev {
    spinlock lock;
    uint64_t srcInode;
    size_t nlinkRC;
};

void hlinkStateRegisterLinkRoot(HLinkState *state, Allocator *alloc,
                                const char *remote, StringPartRef remoteRef,
                                uint64_t srcInode, uint64_t destInode,
                                bool destExists, size_t srcNLinksExpected,
                                size_t destNLinksExpected, int destRootFD,
                                bool *shouldTransfer, bool *isFirstLook,
                                bool *isPendingHardlink);

void hlinkStateHandleTransfer(const FileCopyJob &fileJob, HLinkState *state,
                              Allocator *alloc, const char *remote,
                              uint64_t srcInode, uint64_t destInode,
                              int srcRootFD, int destRootFD,
                              size_t *nFinishedInc);

void createHLinkState(HLinkState *state, const char *path,
                      size_t maxTableCacheSize, size_t maxEnvSize);
