#include <chrono>
#include <thread>
#define CLI11_ENABLE_EXTRA_VALIDATORS 1
#include "CLI/CLI.hpp"
#include "allocator.h"
#include "blockingconcurrentqueue.h"
#include "bytesize.hh"
#include "concurrentqueue.h"
#include "filejob.h"
#include "hlinkstate.h"
#include "lightweightsemaphore.h"
#include "logging.h"
#include "queue.h"
#include "sharedstate.h"
#include "spdlog/common.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "syscalls.h"
#include "xxh_x86dispatch.h"
#include <algorithm>
#include <asm-generic/errno.h>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <gtl/phmap.hpp>
#include <libaio.h>
#include <linux/stat.h>
#include <memory>
#include <omp.h>
#include <optional>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <string>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#include <unistd.h>
#include <utility>

#include "fdrcguard.h"

bool isZero(const char *const data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        if (data[i])
            return false;
    }

    return true;
}

#define INITIAL_READLINK_BUF_SIZE 512

#define ISDOT(a) (a[0] == '.' && (!a[1] || (a[1] == '.' && !a[2])))

/**
 * Arbitrary length readlink
 * @param path Path to symlink
 * relative to that. Otherwise, set to -1
 * @return path (or NULL if error), stored in thread local buffer.
 */
char *readSymlink(SharedState *sharedState, Allocator *alloc, int srcFD,
                  const char *path) {
    thread_local size_t size = INITIAL_READLINK_BUF_SIZE;
    thread_local AllocRef readlinkBuf = AllocatorAllocate(alloc, size);

    char *bufPtr = (char *)allocDeref(alloc, readlinkBuf, size);
    ssize_t nread = 0;
    while (1) {
        nread = readlinkat_wrapper(sharedState, srcFD, path, bufPtr, size);

        if (nread < 0) {
            spdlog::error("readlink failed to read symlink {}"
                          "({})\n",
                          path, strerror(errno));
            return nullptr;
        } else if (nread == 0) {
            bufPtr[0] = '\0';
            break;
        }

        if ((size_t)nread >= size) {
            size += INITIAL_READLINK_BUF_SIZE;
            AllocatorFree(alloc, readlinkBuf);
            readlinkBuf = AllocatorAllocate(alloc, size);
            bufPtr = (char *)allocDeref(alloc, readlinkBuf, size);
        } else {
            bufPtr[nread] = '\0';
            break;
        }
    }

    return bufPtr;
}

static void wr_done(io_context_t ctx, struct iocb *iocb, long res, long res2) {
    // The devil himself
    BlockCopyJob *job =
        (BlockCopyJob *)((uint8_t *)iocb - offsetof(BlockCopyJob, cb));

    spdlog::debug("wr_done {}", *job);

    job->status = 0;
    if (res <= 0)
        job->status = -res;
    else if ((unsigned long)res != iocb->u.c.nbytes) {
        fprintf(stderr, "ERROR: write missed bytes expect %lu got %ld\n",
                iocb->u.c.nbytes, res);
        job->status = EIO;
    }
}

static void rd_done(io_context_t ctx, struct iocb *iocb, long res, long res2) {
    // The devil himself
    BlockCopyJob *job =
        (BlockCopyJob *)((uint8_t *)iocb - offsetof(BlockCopyJob, cb));

    spdlog::debug("rd_done {}", *job);

    job->status = 0;
    if (res <= 0)
        job->status = -res;
    else if ((unsigned long)res != iocb->u.c.nbytes) {
        spdlog::error("{}: read missed bytes expected {} got {}", *job,
                      iocb->u.c.nbytes, res);
        job->status = EIO;
    }
}

struct CopyScheduler {
    bool done;

    size_t nFileCopyJobs;
    size_t nBlockCopyJobsPerFileCopyJob;
    FileCopyJob *fileCopyJobs;
    BlockCopyJob *blockCopyJobs;

    iocb **cbBatch;
    size_t cbBatchSize;

    io_context_t ctx;
};

int createScheduler(CopyScheduler *sched, size_t nFileCopyJobs,
                    size_t nBlockCopyJobsPerFileCopyJob) {
    *sched = {
        .done = false,
        .nFileCopyJobs = nFileCopyJobs,
        .nBlockCopyJobsPerFileCopyJob = nBlockCopyJobsPerFileCopyJob,
        .fileCopyJobs = (FileCopyJob *)allocDeref(
            globalAllocator,
            AllocatorAllocate(globalAllocator,
                              nFileCopyJobs * sizeof(FileCopyJob))),
        .blockCopyJobs = (BlockCopyJob *)allocDeref(
            globalAllocator,
            AllocatorAllocate(globalAllocator, nBlockCopyJobsPerFileCopyJob *
                                                   nFileCopyJobs *
                                                   sizeof(BlockCopyJob))),
        .cbBatch = (iocb **)allocDeref(
            globalAllocator,
            AllocatorAllocate(globalAllocator,
                              nFileCopyJobs * nBlockCopyJobsPerFileCopyJob *
                                  sizeof(iocb *))),
        .cbBatchSize = 0,
    };

    spdlog::info("attempting to create {} jobs",
                 nFileCopyJobs * nBlockCopyJobsPerFileCopyJob);
    int ret;
    if ((ret = io_queue_init(nFileCopyJobs * nBlockCopyJobsPerFileCopyJob,
                             &sched->ctx))) {
        spdlog::error("Failed to create aio queue: {}", strerror(-ret));
        return 1;
    }

    return 0;
}

inline void schedulerClearBlockCopyJob(CopyScheduler *sched,
                                       SharedState *sharedState,
                                       FileCopyJob *fileJob,
                                       BlockCopyJob *blockJob,
                                       bool nofree = false) {
    spdlog::trace("clearing {}", *blockJob);
    if (!nofree) {
        sharedState->allocLock.lock();
        if (blockJob->data) {
            buddy_free(sharedState->copybufferAllocator, blockJob->data);
            sharedState->totalMemAllocated -= fileJob->blockSize;
        }
        sharedState->allocLock.unlock();

        blockJob->data = nullptr;
    }

    *blockJob = {.data = blockJob->data};
}

void schedIOSubmitDeferred(CopyScheduler *sched, iocb *cb) {
    assert(sched->cbBatchSize <
           (sched->nFileCopyJobs * sched->nBlockCopyJobsPerFileCopyJob));
    sched->cbBatch[sched->cbBatchSize++] = cb;
}

int schedIOSubmitBatch(CopyScheduler *sched, SharedState *sharedState) {
    if (sched->cbBatchSize) {
        if (io_submit(sched->ctx, sched->cbBatchSize, sched->cbBatch) < 0) {
            for (size_t i = 0; i < sched->cbBatchSize; i++) {
                BlockCopyJob *blockJob =
                    (BlockCopyJob *)((uint8_t *)sched->cbBatch[i] -
                                     offsetof(BlockCopyJob, cb));
                FileCopyJob *fileJob = blockJob->parent;

                spdlog::error("{} failed to queue aio request {}", *blockJob,
                              strerror(errno));

                schedulerClearBlockCopyJob(sched, sharedState, fileJob,
                                           blockJob);
                fileJob->nBlockJobsScheduled--;
                fileJob->nBlockJobsFinished++;
                fileJob->nErrors++;
            }
            sched->cbBatchSize = 0;

            return -1;
        }
    }

    sched->cbBatchSize = 0;

    return 0;
}

inline void schedulerScheduleBlockCopyJob(CopyScheduler *sched,
                                          SharedState *sharedState,
                                          FileCopyJob *fileJob,
                                          BlockCopyJob *blockJob,
                                          bool dataPreallocated = false) {
    blockJob->jobno =
        fileJob->nBlockJobsScheduled + fileJob->nBlockJobsFinished;
    blockJob->parent = fileJob;
    blockJob->cb = {};
    blockJob->type = READ;
    blockJob->status = EINPROGRESS;
    blockJob->offset = fileJob->blockSize * blockJob->jobno;

    if (blockJob->offset >= fileJob->stx.stx_size) {
        schedulerClearBlockCopyJob(sched, sharedState, fileJob, blockJob);

        return;
    }

    blockJob->nbytes = std::min(
        fileJob->blockSize, (size_t)fileJob->stx.stx_size - blockJob->offset);
    if (!dataPreallocated) {
        sharedState->allocLock.lock();
        void *data =
            buddy_malloc(sharedState->copybufferAllocator, fileJob->blockSize);
        sharedState->allocLock.unlock();

        sharedState->allocationAttempts++;
        if (!data) {
            sharedState->allocationFailures++;
            spdlog::trace("{} failed to allocate {}", *fileJob,
                          fileJob->blockSize);
            schedulerClearBlockCopyJob(sched, sharedState, fileJob, blockJob);
            return;
        }
        sharedState->totalMemAllocated += fileJob->blockSize;

        blockJob->data = data;
    }
    blockJob->inUse = true;

    spdlog::trace("scheduling {}", *blockJob);
    io_prep_pread(&blockJob->cb, fileJob->srcFd, blockJob->data,
                  blockJob->nbytes, blockJob->offset);

    // iocb *cbp = &blockJob->cb;
    io_set_callback(&blockJob->cb, rd_done);

    schedIOSubmitDeferred(sched, &blockJob->cb);

    blockJob->jobno = fileJob->nBlockJobsScheduled++;
}

inline void schedulerUpdateBlockCopyJob(CopyScheduler *sched,
                                        SharedState *sharedState,
                                        FileCopyJob *fileJob,
                                        BlockCopyJob *blockJob) {
    if (blockJob->status == 0) {
        if (blockJob->type == READ) {
            if (sharedState->opts.sparse &&
                isZero((const char *const)blockJob->data, blockJob->nbytes)) {
                blockJob->type = WRITE;
                blockJob->status = 0;
                sharedState->bytesRead += blockJob->nbytes;
                return;
            }

            sharedState->bytesRead += blockJob->nbytes;

            io_prep_pwrite(&blockJob->cb, blockJob->parent->dstFd,
                           blockJob->data, blockJob->nbytes, blockJob->offset);
            // iocb *cbp = &blockJob->cb;
            blockJob->status = EINPROGRESS;
            blockJob->type = WRITE;
            io_set_callback(&blockJob->cb, wr_done);

            blockJob->srcHash = 0;
            if (sharedState->opts.readback) {
                blockJob->srcHash =
                    XXH3_64bits(blockJob->data, blockJob->nbytes);
                spdlog::trace("{} hash is {:x}", *blockJob, blockJob->srcHash);
            }

            spdlog::trace("{} submitting write request corresponding "
                          "to finished read request",
                          *blockJob);

            schedIOSubmitDeferred(sched, &blockJob->cb);
        } else if (blockJob->type == WRITE) {
            spdlog::trace("{} write request finished", *blockJob);

            sharedState->bytesWritten += blockJob->nbytes;

            if (sharedState->opts.readback) {
                memset(blockJob->data, 0, blockJob->nbytes);
                io_prep_pread(&blockJob->cb, blockJob->parent->dstFd,
                              blockJob->data, blockJob->nbytes,
                              blockJob->offset);
                // iocb *cbp = &blockJob->cb;
                blockJob->status = EINPROGRESS;
                blockJob->type = READDST;
                io_set_callback(&blockJob->cb, rd_done);

                spdlog::trace(
                    "{} submitting destination readback request corresponding "
                    "to finished write request",
                    *blockJob);

                schedIOSubmitDeferred(sched, &blockJob->cb);
            } else {
                blockJob->type = FINISH;
                blockJob->status = 0;
            }
        } else {
            if (blockJob->type == READDST) {
                spdlog::trace("{} readdst request finished", *blockJob);

                auto dstHash = XXH3_64bits(blockJob->data, blockJob->nbytes);
                spdlog::trace("{} dst hash is {:x}", *blockJob, dstHash);
                if (dstHash != blockJob->srcHash) {
                    spdlog::error("{} src vs dst hash mismatch {:x} vs {:x}",
                                  *blockJob, blockJob->srcHash, dstHash,
                                  dstHash);
                    fileJob->nErrors++;
                }
            }

            fileJob->nBlockJobsScheduled--;
            fileJob->nBlockJobsFinished++;

            schedulerClearBlockCopyJob(sched, sharedState, fileJob, blockJob,
                                       true);
            schedulerScheduleBlockCopyJob(sched, sharedState, fileJob, blockJob,
                                          true);
        }
    } else if (blockJob->status != 0 && blockJob->status != EINPROGRESS) {

        spdlog::error("{} aio request failed: {}", *blockJob,
                      strerror(blockJob->status));
        fileJob->nBlockJobsFinished++;
        fileJob->nBlockJobsScheduled--;
        schedulerClearBlockCopyJob(sched, sharedState, fileJob, blockJob);
    } else {
    }
}

inline void schedulerShallowDupFileCopyJob(FileCopyJob *src, FileCopyJob *dst) {
    *dst = *src; // profound
}

// ensure the associated block jobs are cleared and the entry itself is cleared,
// but file path and file descriptors are not freed/closed
inline void schedulerClearFileCopyJob(CopyScheduler *sched,
                                      SharedState *sharedState,
                                      FileCopyJob *fileJob) {
    BlockCopyJob *blockJobBase =
        sched->blockCopyJobs +
        (fileJob - sched->fileCopyJobs) * sched->nBlockCopyJobsPerFileCopyJob;

    for (BlockCopyJob *it = blockJobBase;
         it != blockJobBase + sched->nBlockCopyJobsPerFileCopyJob; it++) {
        if (it->inUse) {
            schedulerClearBlockCopyJob(sched, sharedState, fileJob, it);
        }
    }

    *fileJob = {};
}

int io_queue_run_timeout(io_context_t ctx, struct timespec timeout) {

    // static struct timespec timeout = {0, 0};

    struct io_event event;

    int ret;

    /* FIXME: batch requests? */

    while (1 == (ret = io_getevents(ctx, 0, 1, &event, &timeout))) {

        io_callback_t cb = (io_callback_t)event.data;

        struct iocb *iocb = event.obj;

        cb(ctx, iocb, event.res, event.res2);
    }

    return ret;
}

int schedulerTick(CopyScheduler *sched, SharedState *sharedState) {
    int rc = 0;
    rc = schedIOSubmitBatch(sched, sharedState);

    if (rc) {
        return rc;
    }

    // int rc = io_queue_run(sched->ctx);
    rc = io_queue_run_timeout(sched->ctx, {.tv_sec = 1, .tv_nsec = 0});
    if (rc < 0) {
        spdlog::error("aio queue error: {}", strerror(-rc));
        return -rc;
    } else {
        return 0;
    }
}

int schedulerUpdate(CopyScheduler *sched, SharedState *sharedState,
                    Queue &jobQueue) {
    // update current block jobs
    for (size_t i = 0; i < sched->nFileCopyJobs; i++) {
        FileCopyJob *fileJob = &sched->fileCopyJobs[i];
        if (!fileJob->active) {
            continue;
        }

        for (size_t j = 0; j < sched->nBlockCopyJobsPerFileCopyJob; j++) {
            BlockCopyJob *blockJob =
                &sched->blockCopyJobs[i * sched->nBlockCopyJobsPerFileCopyJob +
                                      j];
            if (blockJob->inUse) {
                schedulerUpdateBlockCopyJob(sched, sharedState, fileJob,
                                            blockJob);
            }
        }
    }

    // check for file job completion
    // schedule new block jobs
    for (size_t i = 0; i < sched->nFileCopyJobs; i++) {
        FileCopyJob *fileJob = &sched->fileCopyJobs[i];
        if (!fileJob->active) {
            continue;
        }

        if (fileJob->nBlockJobsFinished >=
            blockCount(fileJob->stx.stx_size, fileJob->blockSize)) {
            spdlog::debug("{} finished", *fileJob);

            FileCopyJob dupJob = {};
            schedulerShallowDupFileCopyJob(fileJob, &dupJob);
            dupJob.actuallyTransferred = true;
            sharedState->finishQueue.enqueue(dupJob);

            schedulerClearFileCopyJob(sched, sharedState, fileJob);

            continue;
        }

        for (size_t j = 0;
             j < sched->nBlockCopyJobsPerFileCopyJob &&
             (fileJob->nBlockJobsFinished + fileJob->nBlockJobsScheduled) <
                 blockCount(fileJob->stx.stx_size, fileJob->blockSize);
             j++) {
            BlockCopyJob *blockJob =
                &sched->blockCopyJobs[i * sched->nBlockCopyJobsPerFileCopyJob +
                                      j];
            if (!blockJob->inUse) {
                spdlog::debug("{} scheduling block job", *fileJob);
                schedulerScheduleBlockCopyJob(sched, sharedState, fileJob,
                                              blockJob);
                break;
                // only schedule on job per file job per pass to
                // encourage more even distribution across fds
            }
        }
    }

    // schedule new jobs
    size_t activeJobs = 0; // this is not an accurate counter
    for (size_t i = 0; i < sched->nFileCopyJobs; i++) {
        FileCopyJob newJob;
        if (!sched->fileCopyJobs[i].active) {
            if (jobQueue.wait_dequeue_timed(newJob, 100000)) {
                sched->fileCopyJobs[i] = newJob;
                sched->fileCopyJobs[i].nBlockJobsScheduled = 0;
                sched->fileCopyJobs[i].nBlockJobsFinished = 0;
                sched->fileCopyJobs[i].nErrors = 0;
                sched->fileCopyJobs[i].active = true;
                // activeJobs++;

                // break;
            } else {
                break;
            }
        } else {
            // activeJobs++;
        }
    }

    for (size_t i = 0; i < sched->nFileCopyJobs; i++) {
        activeJobs += (size_t)(sched->fileCopyJobs[i].active);
    }

    sharedState->schedulerIterations++;

    if (!activeJobs && sharedState->crawlDone) {
        return 0;
    }

    schedulerTick(sched, sharedState);

    return EINPROGRESS;
}
int recursiveRemoveNonDir(int dstfd, StringPartRef path,
                          SharedState *sharedState) {

    StringPartGuard pathGuard{globalAllocator, path};
    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);

    stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                               &remoteBufSize);

    const char *remote =
        (const char *)allocDeref(globalAllocator, remoteBuf, remoteBufSize);

    spdlog::trace("recursiveRemoveNonDir: remove {}", remote);
    if (unlinkat_wrapper(sharedState, dstfd, remote, 0) < 0) {
        spdlog::error("recursiveRemoveNonDir failed to remove file {}: {}",
                      remote, strerror(errno));
        return -1;
    }

    return 0;
}

int recursiveRemove(int dstfd, StringPartRef path, SharedState *sharedState,
                    int depth = 0) {
    StringPartGuard pathGuard{globalAllocator, path};
    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);

    stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                               &remoteBufSize);

    const char *remote =
        (const char *)allocDeref(globalAllocator, remoteBuf, remoteBufSize);

    if (depth == 0 && unlinkat_wrapper(sharedState, dstfd, remote, 0) == 0) {
        return 0;
    } else if (depth == 0 && errno == ENOENT) {
        return 0;
    } else if (depth == 0 && errno != EISDIR) {
        spdlog::error(
            "recursiveRemove failed initial unlink pass on {} with error {}",
            remote, strerror(errno));
        return -1;
    }

    int dstDirFd =
        openat_wrapper(sharedState, dstfd, remote, O_DIRECTORY | O_RDONLY, 0);
    if (dstDirFd < 0) {
        spdlog::error("recursiveRemove failed to open directory {}: {}", remote,
                      strerror(errno));
        return -1;
    }
    DIR *dstdir = fdopendir_wrapper(sharedState, dstDirFd);

    if (!dstdir) {
        spdlog::error("recursiveRemove failed to fdopnedir {}: {}", remote,
                      strerror(errno));
        return -1;
    }

#pragma omp taskgroup
    {
        struct dirent *d;

        int x = 0;

        while ((d = readdir(dstdir))) {
            if (ISDOT(d->d_name))
                continue;

            StringPartRef newPathRef = toStringPart(
                globalAllocator, path, "/", 1, d->d_name, strlen(d->d_name));

            if (d->d_type == DT_DIR) {
#pragma omp task depend(in : x)
                {
                    recursiveRemove(dstfd, newPathRef, sharedState, depth + 1);
                }
            } else {
#pragma omp task depend(in : x)
                {
                    recursiveRemoveNonDir(dstfd, newPathRef, sharedState);
                }
            }
        }

        // we use a data dependency to ensure the directory handle is closed
        // before the above tasks run so we play nice with nfs
#pragma omp task depend(out : x) if (false)
        {
            closedir_wrapper(
                sharedState,
                dstdir); // Barrier 2: Release the handle BEFORE unlinking.
            x++;
        }
    }
    // Barrier 1: All files and sub-sub-dirs are definitely gone here.

    // child tasks could have overwritten the thread local buffer
    // this is terrible behavior, but such is the price we pay
    stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                               &remoteBufSize);
    spdlog::trace("recursiveRemove: removing {}", remote);
    if (unlinkat_wrapper(sharedState, dstfd, remote, AT_REMOVEDIR) < 0) {
        spdlog::error("recursiveRemove failed to remove directory {}: {}",
                      remote, strerror(errno));
    }

    return 0;
}

int processNonDir(StringPartRef path, std::optional<dev_t> dev,
                  FDRCRef srcParentFDRef, FDRCRef dstParentFDRef,
                  SharedState *sharedState) {
    StringPartGuard pathGuard{globalAllocator, path};
    FDRCGuard srcFDGuard(sharedState, srcParentFDRef);
    FDRCGuard dstFDGuard(sharedState, dstParentFDRef);

    assert(!isNullRef(path));

    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);

    thread_local size_t partialBufSize = PATH_MAX;
    thread_local AllocRef partialBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);

    int ok = (!stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                                          &remoteBufSize));
    assert(ok);

#define SUFFIX_BUF_LEN (16 + std::strlen(".partial") + 1)
    const size_t suffixBufLen = SUFFIX_BUF_LEN;
    char suffixBuf[SUFFIX_BUF_LEN]; // if your compiler turns this into a vla,
                                    // reconsider life
#undef SUFFIX_BUF_LEN

    const char *remote =
        (const char *)allocDeref(globalAllocator, remoteBuf, remoteBufSize);
    const char *name = strrchr(remote, '/');
    if (!name) {
        name = remote;
    } else {
        name++;
    }

    snprintf(suffixBuf, suffixBufLen, "%016lX.partial",
             XXH3_64bits_withSeed(remote, strlen(remote), 0));

    StringPart *pathPart = stringPartDeref(globalAllocator, path);
    StringPartRef partialRef =
        toStringPart(globalAllocator, pathPart->prev, pathPart->data,
                     pathPart->len, suffixBuf, suffixBufLen - 1);
    StringPartGuard partialRefGuard{globalAllocator, partialRef};
    stringrefMemcpyWithRealloc(globalAllocator, partialRef, &partialBuf,
                               &partialBufSize);

    const char *partial =
        (const char *)allocDeref(globalAllocator, partialBuf, partialBufSize);

    bool shouldTransfer = false;
    bool exists = true;

    struct statx stx;
    if (statx_wrapper(sharedState, srcFDGuard.fd(), name, AT_SYMLINK_NOFOLLOW,
                      STATX_BASIC_STATS, &stx) < 0) {
        spdlog::error("Failed to statx src {}: {}\n", remote, strerror(errno));
        return 1;
    }

    struct statx destStx = {};
    if (statx_wrapper(sharedState, dstFDGuard.fd(), name, AT_SYMLINK_NOFOLLOW,
                      STATX_BASIC_STATS, &destStx) < 0) {

        if (errno == ENOENT) {
            shouldTransfer = true;
            exists = false;
        } else {
            spdlog::error("Failed to statx dest {}: {}\n", remote,
                          strerror(errno));
            return 1;
        }
    }

    sharedState->filesSeen++;

    shouldTransfer = !exists;

    if (shouldTransfer) {
        spdlog::trace("transferring {} as it doesn't exist on the destination",
                      remote);
    }

    shouldTransfer |= stx.stx_mtime.tv_sec != destStx.stx_mtime.tv_sec ||
                      stx.stx_size != destStx.stx_size;

    if (exists && shouldTransfer) {
        spdlog::trace("transferring {} mtime {} vs {}, size {} vs {}", remote,
                      stx.stx_mtime.tv_sec, destStx.stx_mtime.tv_sec,
                      stx.stx_size, destStx.stx_size);
    }

    bool isPendingHardlink = false;
    bool isFirstLook = false;

    if (stx.stx_nlink > 1 || destStx.stx_nlink > 1) {
        hlinkStateRegisterLinkRoot(
            sharedState, &sharedState->hlinkState, globalAllocator, remote,
            path, stx.stx_ino, destStx.stx_ino, exists, stx.stx_nlink,
            destStx.stx_nlink, sharedState->destRootFD, &shouldTransfer,
            &isFirstLook, &isPendingHardlink, dstFDGuard.fdrcRef);
        if (isPendingHardlink) {
            shouldTransfer = false;
        }
    } else {
        isFirstLook = true;
    }

    if (S_ISREG(stx.stx_mode)) {
        sharedState->totalBytesSeen += stx.stx_size;
    }

    const char *partialName = strrchr(partial, '/');
    if (partialName) {
        partialName++;
    } else {
        partialName = partial;
    }

    if (shouldTransfer) {
        // TODO: check return value to determine if mtime on the directory is
        // necessary
        unlinkat_wrapper(sharedState, dstFDGuard.fd(), partialName, 0);

        if (S_ISREG(stx.stx_mode)) {

            int srcFd = openat_wrapper(sharedState, srcFDGuard.fd(), name,
                                       O_RDONLY | O_DIRECT | O_NOATIME, 0);
            if (srcFd < 0) {
                spdlog::error("failed to open src {}: {}\n", remote,
                              strerror(errno));
                return 1;
            }

            int dstFd = openat_wrapper(
                sharedState, dstFDGuard.fd(), partialName,
                O_RDWR | O_CREAT | O_DIRECT | O_NOATIME, stx.stx_mode);

            if (dstFd < 0) {
                spdlog::error("failed to open dest {}: {}", remote,
                              strerror(errno));
                close_wrapper(sharedState, srcFd);
                return 1;
            }

#define CLEAN                                                                  \
    close_wrapper(sharedState, srcFd);                                         \
    close_wrapper(sharedState, dstFd)

            if (ftruncate_wrapper(sharedState, dstFd, stx.stx_size) < 0) {
                spdlog::error("failed to ftruncate {}: {}", remote,
                              strerror(errno));
                CLEAN;
                return 1;
            }

            sharedState->totalBytesSeenAndWillTransfer += stx.stx_size;

            // spdlog::info("blockSize({}, {}, {}, {})", stx.stx_size,
            //              stx.stx_blksize, sharedState->opts.minBlockSize,
            //              sharedState->opts.maxBlockSize);
            sharedState->jobQueue.enqueue({
                .firstLook = isFirstLook,
                .remote = STRING_PART_RC_INC(globalAllocator, path),
                .partial = STRING_PART_RC_INC(globalAllocator, partialRef),
                .stx = stx,
                .srcFd = srcFd,
                .dstFd = dstFd,
                .blockSize = blockSize(stx.stx_size, 64ULL * (1ULL << 20),
                                       sharedState->opts.minBlockSize,
                                       sharedState->opts.maxBlockSize),
                .dstDirFD = dstFDGuard.incref(),
            });

#undef CLEAN
        } else if (S_ISLNK(stx.stx_mode)) {

            // symlinkat(, int tofd, const char *to)
            char *contents = readSymlink(sharedState, globalAllocator,
                                         srcFDGuard.fd(), name);

            if (!contents) {
                spdlog::error("failed to read symlink {}", remote);
                return 1;
            }

            spdlog::trace("performing symlink {}->{}", remote, contents);
            if (symlinkat_wrapper(sharedState, contents, dstFDGuard.fd(),
                                  partialName) < 0) {
                spdlog::error("failed to symlinkat {}: {}", remote,
                              strerror(errno));
                return 1;
            }

            if (!isPendingHardlink && isFirstLook) {
                sharedState->finishQueue.enqueue({
                    .firstLook = isFirstLook,
                    .remote = STRING_PART_RC_INC(globalAllocator, path),
                    .partial = STRING_PART_RC_INC(globalAllocator, partialRef),
                    .stx = stx,
                    .srcFd = -1,
                    .dstFd = -1,
                    .dstDirFD = dstFDGuard.incref(),
                });
            }
        }
    } else {
        if (!isPendingHardlink && isFirstLook) {
            sharedState->finishQueue.enqueue({
                .firstLook = isFirstLook,
                .remote = STRING_PART_RC_INC(globalAllocator, path),
                .partial = {},
                .stx = stx,
                .srcFd = -1,
                .dstFd = -1,
                .dstDirFD = dstFDGuard.incref(),
            });
        } else if (!isPendingHardlink && !isFirstLook) {
            sharedState->nFinished++;
        }
    }

    return 0;
}

int processDir(StringPartRef path, std::optional<dev_t> dev,
               FDRCRef srcParentFDRef, FDRCRef dstParentFDRef,
               SharedState *sharedState) {
    assert(!isNullRef(path));
    StringPartGuard pathGuard{globalAllocator, path};

    FDRCGuard srcFDGuard(sharedState, srcParentFDRef);
    FDRCGuard dstFDGuard(sharedState, dstParentFDRef);

    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);

    stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                               &remoteBufSize);

    const char *remote =
        (const char *)allocDeref(globalAllocator, remoteBuf, remoteBufSize);
    const char *name = strrchr(remote, '/');
    if (!name) {
        name = remote;
    } else {
        name++;
    }

    int fd = openat_wrapper(sharedState, srcFDGuard.fd(), name,
                            O_RDONLY | O_DIRECTORY | O_NOATIME, 0);
    if (fd < 0) {
        spdlog::error("failed to open directory {}: {}({})", remote, errno,
                      strerror(errno));
        return -1;
    }

    struct statx stx;
    int ret = statx_wrapper(sharedState, srcFDGuard.fd(), name,
                            AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &stx);

    if (ret < 0) {
        spdlog::error("failed to stat {}: {} ({})\n", remote, errno,
                      strerror(errno));
        return -1;
    }

    dev_t thisdev = makedev(stx.stx_dev_major, stx.stx_dev_minor);
    if (!dev) {
        dev = thisdev;
    } else if (thisdev != dev.value()) {
        return 0;
    }

    if (remote[0] &&
        mkdirat_wrapper(sharedState, dstFDGuard.fd(), name, stx.stx_mode) < 0 &&
        errno != EEXIST) {
        spdlog::error("failed to mkdir {}: {}", remote, strerror(errno));
        return -1;
    }

    sem_wait(&sharedState->fdSem);
    int newSrcFD = dup(fd);
    if (newSrcFD < 0) {
        spdlog::error("failed to duplicate src fd for {}: {}({})", remote,
                      errno, strerror(errno));
        return -1;
    }

    if (sharedState->opts.sync) {
        int dstFdDup = openat_wrapper(sharedState, dstFDGuard.fd(), name,
                                      O_DIRECTORY | O_NOATIME, 0);
        if (dstFdDup < 0) {
            spdlog::error("processDir sync failed to open dst dir {}: {}({})",
                          remote, errno, strerror(errno));
            return -1;
        }

        if (dstFdDup < 0) {
            spdlog::error("failed to duplicate dst fd for {}: {}", remote,
                          strerror(errno));
            return -1;
        }

        DIR *dir = fdopendir_wrapper(sharedState, dstFdDup);
        if (!dir) {
            spdlog::error("failed to open dst directory {}: {} ({})\n", remote,
                          errno, strerror(errno));
            return -1;
        }

#pragma omp taskgroup
        {
            struct dirent *d;

            int x = 0;

            while ((d = readdir_wrapper(sharedState, dir))) {
                if (faccessat(newSrcFD, d->d_name, F_OK, AT_SYMLINK_NOFOLLOW) <
                    0) {
                    StringPartRef newPathRef =
                        toStringPart(globalAllocator, path, "/", 1, d->d_name,
                                     strlen(d->d_name));
#pragma omp task depend(in : x)
                    recursiveRemove(sharedState->destRootFD, newPathRef,
                                    sharedState);
                }
            }

            // The data dependency hack, again to ensure directory modification
            // happens after the dst dir fd closes
#pragma omp task depend(out : x) if (false)
            {
                closedir_wrapper(sharedState, dir);
                x++;
            }
        }
    }

    int newDstFD = openat_wrapper(sharedState, dstFDGuard.fd(), name,
                                  O_DIRECTORY | O_NOATIME, 0);
    if (newDstFD < 0) {
        spdlog::error("failed to open dst dir {}: {}({})", remote, errno,
                      strerror(errno));
        return -1;
    }

    FDRCRef newSrcFDRef = AllocatorAllocate(globalAllocator, sizeof(FDRC));
    FDRCRef newDstFDRef = AllocatorAllocate(globalAllocator, sizeof(FDRC));

    FDRCGuard newSrcFDRefGuard(sharedState, newSrcFDRef);
    newSrcFDRefGuard.deref()->fd = newSrcFD;
    newSrcFDRefGuard.deref()->rc = 1;
    newSrcFDRefGuard.deref()->path = STRING_PART_RC_INC(globalAllocator, path);
    newSrcFDRefGuard.deref()->dst = 0;

    FDRCGuard newDstFDRefGuard(sharedState, newDstFDRef);
    newDstFDRefGuard.deref()->fd = newDstFD;
    newDstFDRefGuard.deref()->rc = 1;
    newDstFDRefGuard.deref()->path = STRING_PART_RC_INC(globalAllocator, path);
    newDstFDRefGuard.deref()->dst = true;
    newDstFDRefGuard.deref()->srcStx = stx;

    sharedState->filesSeen++;

    sharedState->ndirs++;

    DIR *dir = fdopendir_wrapper(sharedState, fd);
    if (!dir) {
        spdlog::error("failed to open directory {}: {} ({})\n", remote, errno,
                      strerror(errno));
        return -1;
    }

#pragma omp taskgroup
    {
        struct dirent *d;
        while ((d = readdir_wrapper(sharedState, dir))) {
            if (ISDOT(d->d_name)) {
                continue;
            }

            newDstFDRefGuard.incref();
            newSrcFDRefGuard.incref();

            if (d->d_type == DT_DIR) {
                StringPartRef newPathRef =
                    toStringPart(globalAllocator, path, "/", 1, d->d_name,
                                 strlen(d->d_name));
#pragma omp task
                processDir(newPathRef, dev, newSrcFDRef, newDstFDRef,
                           sharedState);
            } else {
                StringPartRef newPathRef =
                    toStringPart(globalAllocator, path, "/", 1, d->d_name,
                                 strlen(d->d_name));
#pragma omp task
                processNonDir(newPathRef, dev, newSrcFDRef, newDstFDRef,
                              sharedState);
            }
        }
    }

    closedir_wrapper(sharedState, dir);

    // sharedState->finishQueue.enqueue({
    //     .firstLook = true,
    //     .remote = STRING_PART_RC_INC(globalAllocator, path),
    //     .partial = {},
    //     .stx = stx,
    //     .srcFd = -1,
    //     .dstFd = -1,
    //     .dstDirFD = dstFDGuard.incref(),
    // });

    return 0;
}

void finishProcessorOne(SharedState *sharedState, FileCopyJob &fileJob) {
    if (fileJob.srcFd >= 0) {
        close_wrapper(sharedState, fileJob.srcFd);
    }

    if (fileJob.dstFd >= 0) {
        close_wrapper(sharedState, fileJob.dstFd);
    }

    assert(!isNullRef(fileJob.remote));

    StringPartGuard remotePartGuard{globalAllocator, fileJob.remote};
    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);
    int ok = (!stringrefMemcpyWithRealloc(globalAllocator, fileJob.remote,
                                          &remoteBuf, &remoteBufSize));
    assert(ok);
    const char *remote =
        (const char *)allocDeref(globalAllocator, remoteBuf, remoteBufSize);

    StringPartGuard partialPartGuard{globalAllocator, fileJob.partial};
    thread_local size_t partialBufSize = PATH_MAX;
    thread_local AllocRef partialBuf =
        AllocatorAllocate(globalAllocator, partialBufSize);
    if (!isNullRef(fileJob.partial)) {
        ok = (!stringrefMemcpyWithRealloc(globalAllocator, fileJob.partial,
                                          &partialBuf, &partialBufSize));
        assert(ok);
    }

    FDRCGuard dstFDGuard(sharedState, fileJob.dstDirFD);

    const char *partial =
        (const char *)allocDeref(globalAllocator, partialBuf, partialBufSize);

    const char *dstFileName = !isNullRef(fileJob.partial)
                                  ? strrchr(partial, '/')
                                  : strrchr(remote, '/');
    if (!dstFileName) {
        dstFileName = !isNullRef(fileJob.partial) ? partial : remote;
    } else {
        dstFileName++;
    }

    if (fileJob.isDot) {
        assert(isNullRef(fileJob.partial));
        dstFileName = ".";
    }

    const char *dstRemoteName = strrchr(remote, '/');
    if (!dstRemoteName) {
        dstRemoteName = remote;
    } else {
        dstRemoteName++;
    }

    if (fileJob.isDot) {
        dstRemoteName = ".";
    }

    bool fileJobSuccess = true;
    bool partialDeleted = true;

    struct statx destStx = {};
    if (statx_wrapper(sharedState, dstFDGuard.fd(), dstFileName,
                      AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &destStx) < 0) {
        spdlog::error("{} failed to stat destination: {}, name={} ", fileJob,
                      strerror(errno), dstFileName);
        partialDeleted = fileJobSuccess = false;
    } else if (S_ISREG(fileJob.stx.stx_mode) &&
               destStx.stx_size != fileJob.stx.stx_size) {
        spdlog::error("{} src vs dst size mismatch: {} vs {} ", fileJob,
                      fileJob.stx.stx_size, destStx.stx_size);
        partialDeleted = fileJobSuccess = false;
    } else if (fileJob.nErrors) {
        spdlog::error("{} failed with {} errors", fileJob, fileJob.nErrors);
        partialDeleted = fileJobSuccess = false;
    } else {
        if (!isNullRef(fileJob.partial)) {
            recursiveRemove(sharedState->destRootFD,
                            STRING_PART_RC_INC(globalAllocator, fileJob.remote),
                            sharedState);
        }

        struct timespec times[] = {
            {.tv_nsec = UTIME_OMIT},
            {.tv_sec = fileJob.stx.stx_mtime.tv_sec,
             .tv_nsec = fileJob.stx.stx_mtime.tv_nsec},
        };

        if (!isNullRef(fileJob.partial) &&
            (spdlog::trace("{} finish queue linking "
                           "partial to remote {} -> {}",
                           fileJob, partial, remote),
             renameat_wrapper(sharedState, dstFDGuard.fd(), dstFileName,
                              dstFDGuard.fd(), dstRemoteName) < 0)) {
            spdlog::error("{} failed to perform partial to "
                          "remote link {} -> {}: {} ",
                          fileJob, partial, remote, strerror(errno));
            fileJobSuccess = false;
            partialDeleted = false;
        } else if ((destStx.stx_mtime.tv_sec != fileJob.stx.stx_mtime.tv_sec) &&
                   utimensat_wrapper(sharedState, dstFDGuard.fd(),
                                     dstRemoteName, times,
                                     AT_SYMLINK_NOFOLLOW) < 0) {
            spdlog::error("{} failed to set modification times : {} ", fileJob,
                          strerror(errno));
            fileJobSuccess = false;
        } else if ((destStx.stx_uid != fileJob.stx.stx_uid ||
                    destStx.stx_gid != fileJob.stx.stx_gid) &&
                   fchownat_wrapper(sharedState, dstFDGuard.fd(), dstRemoteName,
                                    fileJob.stx.stx_uid, fileJob.stx.stx_gid,
                                    AT_SYMLINK_NOFOLLOW) < 0) {
            spdlog::error("{} failed to chown: {} ", fileJob, strerror(errno));
            fileJobSuccess = false;

        } else if (!S_ISLNK(fileJob.stx.stx_mode) &&
                   (destStx.stx_mode != fileJob.stx.stx_mode) &&
                   fchmodat_wrapper(sharedState, dstFDGuard.fd(), dstRemoteName,
                                    fileJob.stx.stx_mode, 0) < 0) {
            spdlog::error("{} failed to set mode: {} ", fileJob,
                          strerror(errno));
            fileJobSuccess = false;
        } else if (fileJob.stx.stx_nlink > 1) {
            size_t nFinishedInc = 0;
            hlinkStateHandleTransfer(
                sharedState, fileJob, &sharedState->hlinkState, globalAllocator,
                remote, fileJob.stx.stx_ino, destStx.stx_ino,
                sharedState->srcRootFD, sharedState->destRootFD, &nFinishedInc);

            sharedState->nFinished += nFinishedInc;
        }

        if (S_ISREG(fileJob.stx.stx_mode) && fileJobSuccess &&
            fileJob.firstLook) {
            sharedState->totalBytesTransferred += fileJob.stx.stx_size;

            if (fileJob.actuallyTransferred) {
                sharedState->totalBytesActuallyTransferred +=
                    fileJob.stx.stx_size;
            }
        }
    }

    if (!partialDeleted && !isNullRef(fileJob.partial)) {
        unlinkat_wrapper(sharedState, dstFDGuard.fd(), dstFileName, 0);
    }

    sharedState->nFinished++;
}

void finishProcessor(SharedState *sharedState) {
    while (true) {
        bool done = true;
        for (size_t i = 0; i < sharedState->nscheds; i++) {
            if (!sharedState->scheds[i].done) {
                done = false;
            }
        }

        FileCopyJob fileJob;
        while (sharedState->finishQueue.wait_dequeue_timed(fileJob, 100000)) {
            finishProcessorOne(sharedState, fileJob);
        }

        if (done)
            break;
    }

    sharedState->nFinishProcessorsDone++;
}

void incrementalLogger(SharedState *sharedState) {
    double lastPrint = omp_get_wtime();
    while (true) {
        bool done = sharedState->nFinishProcessorsDone ==
                    sharedState->opts.nFinishProcessors;

        double now = omp_get_wtime();
        double delta = now - lastPrint;
        if (delta > 3.0 || done) {
            spdlog::info(
                "transferred {}/{} files, {}({})/{}({}) (read {}/s, "
                "write {}/s)",
                sharedState->nFinished.load(), sharedState->filesSeen.load(),
                bytesize::bytesize{sharedState->totalBytesTransferred.load()},
                bytesize::bytesize{
                    sharedState->totalBytesActuallyTransferred.load()},
                bytesize::bytesize{sharedState->totalBytesSeen.load()},
                bytesize::bytesize{
                    sharedState->totalBytesSeenAndWillTransfer.load()},
                bytesize::bytesize{
                    delta < 0.5
                        ? 0
                        : (size_t)((double)sharedState->bytesRead.load() /
                                   delta)},
                bytesize::bytesize{
                    delta < 0.5
                        ? 0
                        : (size_t)((double)sharedState->bytesWritten.load() /
                                   delta)});

            if (sharedState->opts.extraStats) {
                spdlog::info("\tjobqueue: size={}, enqueuesem={}",
                             sharedState->jobQueue.size_approx(),
                             sharedState->jobQueue.enqueueSem->get_value());
                spdlog::info("\tfinishQueue: size={}, enqueuesem={}",
                             sharedState->finishQueue.size_approx(),
                             sharedState->finishQueue.enqueueSem->get_value());

                spdlog::info("\tallocation failures {}/{}",
                             sharedState->allocationFailures.load(),
                             sharedState->allocationAttempts.load());
                spdlog::info("\tqueue left {}",
                             sharedState->jobQueue.size_approx());
                spdlog::info("\ttotal allocated {}KB",
                             sharedState->totalMemAllocated.load() / 1024);
                spdlog::info("\tscheduler iterations {}",
                             sharedState->schedulerIterations.load());
                int val;
                sem_getvalue(&sharedState->fdSem, &val);
                spdlog::info("\tfdsem: {}", val);
                spdlog::info("\tndirs: {}", sharedState->ndirs.load());
                spdlog::info("\tstat time: {}ms", sharedState->statms.load());
            }

            // for (int i = 0; i < nScheds; i++) {
            //     size_t nFActive = 0;
            //     size_t nBActive = 0;
            //     for (int j = 0; j < scheds[i].nFileCopyJobs; j++) {
            //         nFActive +=
            //             (size_t)(scheds[i].fileCopyJobs[j].active);
            //     }
            //
            //     for (int j = 0;
            //          j < scheds[i].nBlockCopyJobsPerFileCopyJob *
            //                  scheds[i].nFileCopyJobs;
            //          j++) {
            //         nBActive +=
            //             (size_t)(scheds[i].blockCopyJobs[j].inUse);
            //     }
            //
            //     spdlog::info("\tsched{}: {}/{}f, {}/{}b", i,
            //     nFActive,
            //                  scheds[i].nFileCopyJobs, nBActive,
            //                  scheds[i].nBlockCopyJobsPerFileCopyJob *
            //                      scheds[i].nFileCopyJobs);
            // }

            sharedState->bytesWritten = 0;
            sharedState->bytesRead = 0;
            sharedState->allocationAttempts = 0;
            sharedState->allocationFailures = 0;
            sharedState->schedulerIterations = 0;
            sharedState->ndirs = 0;
            sharedState->statms = 0;
            lastPrint = omp_get_wtime();

            spdlog::default_logger()->flush();
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        if (done) {
            break;
        }
    }
}

int main(int argc, char *argv[]) {
    auto cli = CLI::App("file transfer tool");
    argv = cli.ensure_utf8(argv);

#define MB (1ULL << 20)
#define GB (1ULL << 30)
#define TB (1ULL << 40)

    Opts opts = {};
    cli.add_option("--data-dir", opts.dataDir,
                   "place to put files used by copy2 internally")
        ->check(CLI::ExistingDirectory);
    cli.add_option("--allocator-mem-size", opts.allocatorMemSize,
                   "amount of RAM the allocator is given")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("4GB");
    cli.add_option("--allocator-disk-size", opts.allocatorDiskSize,
                   "the size of the allocator's disk backed file")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("0B");
    cli.add_option("--crawlers", opts.nCrawlers, "number of crawler threads")
        ->default_val(16);
    cli.add_option("--transfers", opts.nTransfers, "number of transfer threads")
        ->default_val(8);
    cli.add_option("--file-copy-jobs", opts.nFileCopyJobs,
                   "number of file copy jobs per thread")
        ->default_val(128);
    cli.add_option("--block-copy-jobs", opts.nBlockCopyJobs,
                   "number of block copy jobs per thread")
        ->default_val(32);
    cli.add_option("--finish-processors", opts.nFinishProcessors,
                   "number of finish processor threads")
        ->default_val(16);
    cli.add_option("--copy-buffer-size", opts.copyBufferSize,
                   "size of staging buffer used to transfer files (from which "
                   "staging buffers are allocated for individual files)")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("2GB");
    cli.add_option("--min-block-size", opts.minBlockSize,
                   "minimum staging buffer size")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("4KB");
    cli.add_option("--max-block-size", opts.maxBlockSize,
                   "maximum staging buffer size")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("128MB");
    cli.add_option("--mem-per-thread", opts.threadMemSize, "Memory per thread")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("256KB");
    cli.add_option("--readback", opts.readback,
                   "should we read transferred blocks back and run checksums?")
        ->default_val(false);
    cli.add_option("--hlink-cache-members", opts.hlinkMemCacheMembers,
                   "number of hardlink entries to store in ram before "
                   "writing to disk")
        ->default_val(100000);
    cli.add_option("--hlink-disk-size", opts.hlinkDiskSize,
                   "maximum hlink store disk size")
        ->transform(CLI::AsSizeValue(false))
        ->default_val("1TB");
    cli.add_option("--max-FDs", opts.maxFDs,
                   "maximum number of open file descriptors allowed")
        ->default_val(60000);
#ifndef NDEBUG
    cli.add_option("--tests", opts.runTests, "should we run tests?")
        ->default_val(false);
#endif
    cli.add_option("src", opts.src, "path to transfer files from")
        ->required()
        ->check(CLI::ExistingDirectory);
    cli.add_option("dest", opts.dest, "path to transfer files to")
        ->required()
        ->check(CLI::ExistingDirectory);
    cli.add_flag("--sync", opts.sync, "delete extra files on the destination");
    cli.add_flag(
        "--sparse", opts.sparse,
        "don't write sparse blocks to destination to maintain sparseness");
    cli.add_flag("--extra-stats", opts.extraStats,
                 "enable the printing of internal statistics");
    // cli.validate_positionals();

    CLI11_PARSE(cli, argc, argv);

    mkdir(opts.dataDir.c_str(), 0755);

    auto err_logger = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_logger->set_level(spdlog::level::info);
    // spdlog::cfg::load_env_levels();

    double starttime = omp_get_wtime();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
        fmt::format("{}/copy2-{}.log", opts.dataDir, starttime), true);
    file_sink->set_level(spdlog::level::trace);

    auto logger = std::make_shared<spdlog::logger>(
        spdlog::logger("copy2", {err_logger, file_sink}));
    logger->set_level(spdlog::level::trace);
    spdlog::register_logger(logger);
    spdlog::set_default_logger(logger);

    if (argc < 3) {
        spdlog::error("please specify a source and a destination path");
        return 1;
    }

    logger->flush_on(spdlog::level::trace);

    const char *root = opts.src.c_str();
    const char *dest = opts.dest.c_str();

    int destRootFD = open(dest, O_DIRECTORY | O_RDONLY | O_NOATIME);
    if (destRootFD < 0) {
        spdlog::error("failed to open destination {}: {}", dest,
                      strerror(errno));
        return 1;
    }

    int srcRootFD = open(root, O_DIRECTORY | O_RDONLY | O_NOATIME);
    if (srcRootFD < 0) {
        spdlog::error("failed to open source: {}", strerror(errno));
        return 1;
    }

    spdlog::info("copying files from {} to {}", opts.src, opts.dest);

    size_t arena_size = opts.copyBufferSize;
    globalAllocator = createAllocator(
        opts.allocatorMemSize,
        opts.nFinishProcessors + opts.nCrawlers + opts.nTransfers,
        opts.threadMemSize, opts.allocatorDiskSize, arena_size,
        fmt::format("{}/copy2-{}.mem", opts.dataDir, starttime).c_str());
    if (!globalAllocator) {
        spdlog::error("failed to create allocator");
        return 1;
    }

    SharedState *sharedState = new SharedState;
    sharedState->opts = opts;
    sharedState->logger = logger;

    sharedState->jobQueue =
        Queue(std::min((size_t)60000, sharedState->opts.maxFDs));
    sharedState->finishQueue =
        Queue(std::min((size_t)60000, sharedState->opts.maxFDs));

    // pthread_rwlock_init(&sharedState->hlinkState.lock, NULL);
    createHLinkState(
        &sharedState->hlinkState,
        fmt::format("{}/hlstate-{}.db", sharedState->opts.dataDir, starttime)
            .c_str(),
        sharedState->opts.hlinkMemCacheMembers,
        sharedState->opts.hlinkDiskSize);
    sem_init(&sharedState->fdSem, 0, opts.maxFDs);

    if (!globalAllocator->copyBuffer) {
        spdlog::error("failed to allocate copy buffer!");
        return 1;
    }

    sharedState->copybufferAllocator = buddy_embed_alignment(
        (unsigned char *)globalAllocator->copyBuffer, arena_size, 4096);

    if (!sharedState->copybufferAllocator) {
        spdlog::error("failed to create copy buffer allocator!");
        return 1;
    }

    omp_set_nested(1);
    omp_set_max_active_levels(INT32_MAX);

    const int nScheds = sharedState->opts.nTransfers; // omp_get_max_threads();
    CopyScheduler *scheds = new CopyScheduler[nScheds];

    for (size_t i = 0; i < nScheds; i++) {
        CopyScheduler &sched = scheds[i];
        if (createScheduler(&sched, opts.nFileCopyJobs, opts.nBlockCopyJobs)) {
            exit(1);
        }
        // if (createScheduler(&sched, 512, 128)) {
        //     exit(1);
        // }
    }

    sharedState->scheds = scheds;
    sharedState->nscheds = nScheds;

    sharedState->destRootFD = destRootFD;
    sharedState->srcRootFD = srcRootFD;

    StringPartRef rootRef =
        toStringPart(globalAllocator, {}, ".", 1, nullptr, 0);

    FDRCRef srcRootFDRef = AllocatorAllocate(globalAllocator, sizeof(FDRC));
    FDRCRef dstRootFDRef = AllocatorAllocate(globalAllocator, sizeof(FDRC));

    FDRCGuard srcRootFDRefGuard(sharedState, srcRootFDRef);
    srcRootFDRefGuard.deref()->fd = srcRootFD;
    srcRootFDRefGuard.deref()->rc = 1;
    srcRootFDRefGuard.deref()->path =
        STRING_PART_RC_INC(globalAllocator, rootRef);
    srcRootFDRefGuard.deref()->dst = false;

    FDRCGuard dstRootFDRefGuard(sharedState, dstRootFDRef);
    dstRootFDRefGuard.deref()->fd = destRootFD;
    dstRootFDRefGuard.deref()->rc = 1;
    dstRootFDRefGuard.deref()->path =
        STRING_PART_RC_INC(globalAllocator, rootRef);
    dstRootFDRefGuard.deref()->dst = false;

    // spdlog::info("logger is {}", (void *)spdlog::default_logger_raw());

    int ret = 0;
#pragma omp parallel sections
    {
#pragma omp section
        {
#pragma omp parallel num_threads(sharedState->opts.nCrawlers)
#pragma omp single
#pragma omp taskgroup
            ret = processDir(STRING_PART_RC_INC(globalAllocator, rootRef),
                             std::nullopt, srcRootFDRefGuard.incref(),
                             dstRootFDRefGuard.incref(), sharedState);
            sharedState->crawlDone = 1;
        }

#pragma omp section
        {
            incrementalLogger(sharedState);
        }

#pragma omp section
        {
#pragma omp parallel num_threads(sharedState->opts.nFinishProcessors)
            {
                finishProcessor(sharedState);
            }

            spdlog::info("done with {} files", sharedState->nFinished.load());
        }

#pragma omp section
        {

            sharedState->bytesWritten = 0;
            sharedState->bytesRead = 0;

#pragma omp parallel num_threads(sharedState->nscheds)
            {
                int i = omp_get_thread_num();
                while (true) {
                    if (schedulerUpdate(sharedState->scheds + i, sharedState,
                                        sharedState->jobQueue) != EINPROGRESS) {
                        scheds[i].done = true;
                        break;
                    }
                }
            }
        }
    }

    spdlog::info("copying finished in {}s",
                 (size_t)(omp_get_wtime() - starttime));

    spdlog::shutdown();

    return ret;
}
