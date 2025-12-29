#include "hlinkstate.h"
#include "allocator.h"
#include "filejob.h"
#include "gtl/phmap.hpp"
#include "logging.h"
#include <mutex>
#include <omp.h>
#include <pthread.h>

inline void resetRDOnlyTxns(HLinkState *state) {
    for (auto &[_, txn] : state->rdTxns) {
        mdb_txn_reset(txn);
        mdb_txn_renew(txn);
    }
}

static bool readPair(MDB_txn *txn, MDB_env *env, const pairmap &cache,
                     MDB_dbi dbi, uint64_t k, AllocRef *v) {
    int err = 0;

    MDB_val mdbv = {}, mdbk = {};

    bool ret = false;
    auto it = cache.find(k);
    if (it != cache.end()) {
        *v = it->second;
        ret = true;
        goto found;
    }

    mdbv.mv_data = &v;
    mdbv.mv_size = sizeof(uint64_t);
    mdbk.mv_data = &k;
    mdbk.mv_size = sizeof(uint64_t);

    if (!(err = mdb_get(txn, dbi, &mdbk, &mdbv))) {
        *v = *((AllocRef *)mdbv.mv_data);
        ret = true;
    } else if (err == MDB_NOTFOUND) {
        ret = false;
    } else {
        fprintf(stderr,
                "ERROR: failed to mdb_get: "
                "%s\n",
                mdb_strerror(err));
        ret = false;
        exit(1);
    }

found:
    return ret;
}

static void flushCache(HLinkState *state, MDB_env *env, MDB_txn **txn,
                       pairmap &cache, MDB_dbi dbi) {
    for (const auto &[k, v] : cache) {
        MDB_val mdbv = {}, mdbk = {};
        mdbv.mv_data = (void *)&v;
        mdbv.mv_size = sizeof(AllocRef);
        mdbk.mv_data = (void *)&k;
        mdbk.mv_size = sizeof(AllocRef);

        int err;
        if ((err = mdb_put(*txn, dbi, &mdbk, &mdbv, 0))) {
            fprintf(stderr,
                    "ERROR: failed to mdb_put: "
                    "%s\n",
                    mdb_strerror(err));
            exit(1);
        }
    }
    int err = 0;

    mdb_txn_commit(*txn);
    txn = NULL;
    if ((err = mdb_txn_begin(env, NULL, 0, txn))) {
        fprintf(stderr,
                "ERROR: failed to begin mdb read transaction: "
                "error %s\n",
                mdb_strerror(err));
        exit(1);
    }

    resetRDOnlyTxns(state);

    cache.clear();
}

static void writePair(HLinkState *state, MDB_txn **txn, MDB_env *env,
                      MDB_dbi dbi, pairmap &cache, size_t maxCacheSize,
                      uint64_t k, AllocRef v) {
    cache[k] = v;

    if (cache.size() >= maxCacheSize) {
        flushCache(state, env, txn, cache, dbi);
    }

    return;
}

static void deletePair(HLinkState *state, MDB_txn *txn, MDB_env *env,
                       MDB_dbi dbi, pairmap &cache, uint64_t k) {
    int err = 0;

    cache.erase(k);

    MDB_val mdbk = {};
    mdbk.mv_data = &k;
    mdbk.mv_size = sizeof(uint64_t);

    if ((err = mdb_del(txn, dbi, &mdbk, NULL)) && err != MDB_NOTFOUND) {
        fprintf(stderr,
                "ERROR: failed to mdb_del: "
                "%s",
                mdb_strerror(err));
        exit(1);
    }

    resetRDOnlyTxns(state);

    return;
}

MDB_txn *getRDOnlyTxn(HLinkState *state) {
    state->rdTxnAquireLock.lock();

    auto it = state->rdTxns.find(omp_get_thread_num());
    if (it == state->rdTxns.end()) {
        int err = 0;
        if ((err = mdb_txn_begin(state->env, NULL, 0,
                                 &state->rdTxns[omp_get_thread_num()]))) {
            fprintf(stderr,
                    "ERROR: failed to begin mdb read transaction: "
                    "error %s\n",
                    mdb_strerror(err));
            exit(1);
        }

        auto val = state->rdTxns[omp_get_thread_num()];
        state->rdTxnAquireLock.unlock();

        return val;
    }

    auto ret = it->second;
    state->rdTxnAquireLock.unlock();

    return ret;
}

void hlinkStateRegisterLinkRoot(HLinkState *state, Allocator *alloc,
                                const char *remote, StringPartRef remoteRef,
                                uint64_t srcInode, uint64_t destInode,
                                bool destExists, size_t srcNLinksExpected,
                                size_t destNLinksExpected, int destRootFD,
                                bool *shouldTransfer, bool *isFirstLook,
                                bool *isPendingHardlink) {

    bool origShouldTransfer = *shouldTransfer;
reroll:
    pthread_rwlock_rdlock(&state->lock);

    MDB_txn *rdTxn = getRDOnlyTxn(state);
    AllocRef hinfoRef;
    HLinkInfo *hinfo;
    bool haveSrcState = readPair(rdTxn, state->env, state->forwardTableCache,
                                 state->forwardTable, srcInode, &hinfoRef);

    HLinkInfoRev *revinfo;
    AllocRef revinfoRef;
    bool haveDstState =
        destExists && readPair(rdTxn, state->env, state->revTableCache,
                               state->revTable, destInode, &revinfoRef);

    *isPendingHardlink = false;
    *isFirstLook = false;

    bool haveWrlock = false;

    *shouldTransfer = origShouldTransfer;

    if (haveSrcState) {
        hinfo = (HLinkInfo *)allocDeref(alloc, hinfoRef, sizeof(HLinkInfo));
        bool wrongInode = destInode != hinfo->destInode;
        if (!destExists || wrongInode) {
            thread_local size_t linkRootBufSize = 4096;
            thread_local AllocRef linkRootBuf =
                AllocatorAllocate(alloc, linkRootBufSize);

            int ok = (!stringrefMemcpyWithRealloc(
                alloc, hinfo->remote, &linkRootBuf, &linkRootBufSize));
            assert(ok);
            const char *linkRoot =
                (const char *)allocDeref(alloc, linkRootBuf, linkRootBufSize);

            *shouldTransfer = false;

            if (wrongInode) {
                spdlog::debug("{} has incorrect inode on destination. "
                              "Relinking to root {}",
                              remote, linkRoot);
            }

            hinfo->lock.lock();
            if (hinfo->transferred) {
                spdlog::debug("root {} has already been transferred. "
                              "Hardlinking to {}",
                              linkRoot, remote);
                if (unlinkat(destRootFD, remote, 0) < 0 && errno != ENOENT) {
                    spdlog::error("Failed to unlink hardlink dest {}: {}",
                                  remote, strerror(errno));
                } else if (linkat(destRootFD, linkRoot, destRootFD, remote, 0) <
                           0) {
                    spdlog::error("Failed to hardlink root {} to {}: {}",
                                  linkRoot, remote, strerror(errno));
                    exit(1);
                }

                hinfo->nlinkRC--; // this can underflow, however it's fine
                                  // because once all readers release their
                                  // locks, the thread who observes the refcount
                                  // go to 0 deletes and frees

                if (!hinfo->nlinkRC) {
                    hinfo->lock.unlock();
                    pthread_rwlock_unlock(&state->lock);
                    pthread_rwlock_wrlock(&state->lock); // promote lock
                    haveWrlock = true;
                    if (readPair(state->wrTxn, state->env,
                                 state->forwardTableCache, state->forwardTable,
                                 srcInode, &hinfoRef)) {
                        hinfo->lock.lock();
                        deletePair(state, state->wrTxn, state->env,
                                   state->forwardTable,
                                   state->forwardTableCache, srcInode);
                        // AllocatorFree(alloc, hinfoRef);
                    }
                    hinfoRef = {};
                    hinfo = NULL;
                }
            } else {
                spdlog::debug("{} has not been transferred. Queueing "
                              "hardlinkage to {}",
                              linkRoot, remote);
                LinkEntryRef entryRef =
                    AllocatorAllocate(alloc, sizeof(LinkEntry));

                ((LinkEntry *)allocDeref(alloc, entryRef, sizeof(LinkEntry)))
                    ->link = STRING_PART_RC_INC(alloc, remoteRef);

                hinfo->links = linkEntryAppend(alloc, hinfo->links, entryRef);

                *isPendingHardlink = true;
            }
            *shouldTransfer = false;

            if (!isNullRef(hinfoRef)) {
                hinfo->lock.unlock();
            }
        }
    } else {
        pthread_rwlock_unlock(&state->lock);
        pthread_rwlock_wrlock(&state->lock); // promote lock
        haveWrlock = true;

        // srcHLIt could have been invalidated in the promotion and thus the
        // below could be true
        bool haveSrcState =
            readPair(rdTxn, state->env, state->forwardTableCache,
                     state->forwardTable, srcInode, &hinfoRef);

        if (haveSrcState) {
            pthread_rwlock_unlock(&state->lock);
            goto reroll;
        }

        *isFirstLook = true;

        hinfoRef = AllocatorAllocate(alloc, sizeof(HLinkInfo));
        hinfo = (HLinkInfo *)allocDeref(alloc, hinfoRef, sizeof(HLinkInfo));
        hinfo->links = {};
        hinfo->remote = STRING_PART_RC_INC(alloc, remoteRef);
        hinfo->destInode = destInode;
        hinfo->transferred = false;
        hinfo->nlinkRC = srcNLinksExpected - 1;
        assert(srcNLinksExpected > 1);

        writePair(state, &state->wrTxn, state->env, state->forwardTable,
                  state->forwardTableCache, state->maxTableCacheSize, srcInode,
                  hinfoRef);

        if (destExists && !origShouldTransfer) {
            if (haveDstState) {
                revinfo = (HLinkInfoRev *)allocDeref(alloc, revinfoRef,
                                                     sizeof(HLinkInfoRev));
                if (revinfo->srcInode != srcInode) {
                    spdlog::debug("Destination to source inode mapping {}->{} "
                                  "for {} doesn't match "
                                  "expected {}->{}. Transferring",
                                  destInode, srcInode, remote, remote,
                                  destInode, revinfo->srcInode);
                    *shouldTransfer = true;
                }
            }
        }
    }

reroll_dststate:
    if (destExists && !haveDstState && destNLinksExpected > 1) {
        if (!haveWrlock) {
            pthread_rwlock_unlock(&state->lock);
            pthread_rwlock_wrlock(&state->lock); // promote lock
            haveWrlock = true;

            haveDstState = readPair(rdTxn, state->env, state->revTableCache,
                                    state->revTable, srcInode, &revinfoRef);
            goto reroll_dststate;
        }

        revinfoRef = AllocatorAllocate(alloc, sizeof(HLinkInfoRev));
        revinfo =
            (HLinkInfoRev *)allocDeref(alloc, revinfoRef, sizeof(HLinkInfoRev));
        assert(destNLinksExpected > 0);
        revinfo->nlinkRC = destNLinksExpected - 1;
        revinfo->srcInode = *shouldTransfer ? UINT64_MAX : srcInode;

        writePair(state, &state->wrTxn, state->env, state->revTable,
                  state->revTableCache, state->maxTableCacheSize, destInode,
                  revinfoRef);
    } else if (destExists && haveDstState) {
        revinfo =
            (HLinkInfoRev *)allocDeref(alloc, revinfoRef, sizeof(HLinkInfoRev));
        revinfo->lock.lock();
        revinfo->nlinkRC--;

        if (!revinfo->nlinkRC) {
            if (!haveWrlock) {
                revinfo->lock.unlock();
                pthread_rwlock_unlock(&state->lock);
                pthread_rwlock_wrlock(&state->lock); // promote lock
                revinfo->lock.lock();
                haveWrlock = true;
            }

            if (readPair(state->wrTxn, state->env, state->revTableCache,
                         state->revTable, destInode, &revinfoRef)) {
                deletePair(state, state->wrTxn, state->env, state->revTable,
                           state->revTableCache, destInode);
                AllocatorFree(alloc, revinfoRef);
                revinfoRef = {};
                revinfo = NULL;
            }
        } else {
            revinfo->lock.unlock();
        }
    }

    pthread_rwlock_unlock(&state->lock);
}

void createHLinkState(HLinkState *state, const char *path,
                      size_t maxTableCacheSize, size_t maxEnvSize) {
    static MDB_env *mdbEnv = NULL;
    int err = 0;
    if ((err = mdb_env_create(&mdbEnv))) {
        fprintf(stderr, "ERROR: failed to create mdb env: error %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }
    mdb_env_set_maxdbs(mdbEnv, 2);
    mdb_env_set_mapsize(mdbEnv, maxEnvSize);
    if ((err = mdb_env_open(mdbEnv, path,
                            MDB_NOTLS | MDB_NOLOCK | MDB_CREATE | MDB_NOSUBDIR,
                            0777))) {
        fprintf(stderr, "ERROR: failed to open mdb environment: error %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }

    // MDB_txn *txn;
    if ((err = mdb_txn_begin(mdbEnv, NULL, 0, &state->wrTxn))) {
        fprintf(stderr,
                "ERROR: failed to begin mdb dbi aquisition transaction: "
                "error %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }

    MDB_dbi forwardTable, revTable;
    if ((err = mdb_dbi_open(state->wrTxn, "FORWARD",
                            MDB_CREATE | MDB_INTEGERKEY, &forwardTable))) {
        fprintf(stderr, "ERROR: failed to get/create INODES table: %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }

    if ((err = mdb_dbi_open(state->wrTxn, "REV", MDB_CREATE | MDB_INTEGERKEY,
                            &revTable))) {
        fprintf(stderr, "ERROR: failed to get/create INODES table: %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }

    if ((err = mdb_txn_commit(state->wrTxn))) {
        fprintf(stderr,
                "ERROR: failed to commit mdb dbi aquisition transaction: "
                "error %s\n",
                strerror(err));
        exit(EXIT_FAILURE);
    }

    if ((err = mdb_txn_begin(mdbEnv, NULL, 0, &state->wrTxn))) {
        fprintf(stderr,
                "ERROR: failed to begin write transaction: "
                "error %s\n",
                mdb_strerror(err));
        exit(EXIT_FAILURE);
    }

    state->revTable = revTable;
    state->forwardTable = forwardTable;
    state->env = mdbEnv;
    state->maxTableCacheSize = maxTableCacheSize;

    pthread_rwlock_init(&state->lock, 0);
}

void hlinkStateHandleTransfer(const FileCopyJob &fileJob, HLinkState *state,
                              Allocator *alloc, const char *remote,
                              uint64_t srcInode, uint64_t destInode,
                              int srcRootFD, int destRootFD,
                              size_t *nFinishedInc) {
    *nFinishedInc = 0;
    pthread_rwlock_rdlock(&state->lock);
    AllocRef hlinkinfoRef;
    bool haveHLinkState =
        readPair(getRDOnlyTxn(state), state->env, state->forwardTableCache,
                 state->forwardTable, srcInode, &hlinkinfoRef);
    HLinkInfo *hlinkinfo;

    if (haveHLinkState) {
        hlinkinfo =
            (HLinkInfo *)allocDeref(alloc, hlinkinfoRef, sizeof(HLinkInfo));
        hlinkinfo->lock.lock();
        assert(!hlinkinfo->transferred);
        hlinkinfo->transferred = true;
        hlinkinfo->destInode = destInode;

        LinkEntryRef curRef = hlinkinfo->links;
        while (!isNullRef(curRef)) {
            LinkEntry *entry =
                (LinkEntry *)allocDeref(alloc, curRef, sizeof(LinkEntry));

            thread_local size_t linkRootBufSize = 4096;
            thread_local AllocRef linkRootBuf =
                AllocatorAllocate(alloc, linkRootBufSize);

            int ok = (!stringrefMemcpyWithRealloc(
                alloc, entry->link, &linkRootBuf, &linkRootBufSize));
            assert(ok);

            const char *linkRoot =
                (const char *)allocDeref(alloc, linkRootBuf, linkRootBufSize);

            spdlog::trace("{} finish queue performing "
                          "pending link {} -> {}",
                          fileJob, remote, linkRoot);

            if (unlinkat(destRootFD, linkRoot, 0) < 0 && errno != ENOENT) {
                spdlog::error("{} failed to unlink "
                              "{} to apply "
                              "pending: {}",
                              fileJob, linkRoot, strerror(errno));
            }

            if (linkat(destRootFD, remote, destRootFD, linkRoot, 0) < 0) {
                spdlog::error("{} failed to perform pending"
                              " link {} -> {} : {}",
                              fileJob, remote, linkRoot, strerror(errno));
            }
            (*nFinishedInc)++;

            LinkEntryRef prevRef = entry->prev;
            stringPartRelease(alloc, entry->link);
            AllocatorFree(alloc, curRef);

            curRef = prevRef;
        }

        hlinkinfo->nlinkRC--;
        if (!hlinkinfo->nlinkRC) {
            hlinkinfo->lock.unlock();
            pthread_rwlock_unlock(&state->lock);
            pthread_rwlock_wrlock(&state->lock); // promote lock
            if (readPair(state->wrTxn, state->env, state->forwardTableCache,
                         state->forwardTable, destInode, &hlinkinfoRef)) {
                hlinkinfo->lock.lock();
                deletePair(state, state->wrTxn, state->env, state->revTable,
                           state->revTableCache, srcInode);
                AllocatorFree(alloc, hlinkinfoRef);
                hlinkinfoRef = {};
                hlinkinfo = NULL;
            }
        }

        if (!isNullRef(hlinkinfoRef)) {
            hlinkinfo->lock.unlock();
        }
    }

    pthread_rwlock_unlock(&state->lock);
}
