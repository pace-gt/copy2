#include "allocator.h"
#include "blockingconcurrentqueue.h"
#include "bytesize.hh"
#include "concurrentqueue.h"
#include "filejob.h"
#include "gtl/phmap_fwd_decl.hpp"
#include "hlinkstate.h"
#include "logging.h"
#include "spdlog/common.h"
#include "spdlog/fmt/bundled/format.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
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

#define BUDDY_ALLOC_IMPLEMENTATION
#include "buddy_alloc.h"

#define JOBS_PER_FILE 4
#define BUF_SIZE (1024UL * 1024 * 128)
#define QUEUE_SIZE 64

#define MAX_FILES 30000
size_t done = 0;

#define INITIAL_READLINK_BUF_SIZE 512

#define ISDOT(a) (a[0] == '.' && (!a[1] || (a[1] == '.' && !a[2])))

struct QueueTraits : public moodycamel::ConcurrentQueueDefaultTraits {
    static const size_t BLOCK_SIZE = 8192; // Use bigger blocks
};

// AT_EMPTY_PATH and AT_SYMLINK_NOFOLLOW on fchmodat is only supported on kernel
// versions >= 6.6 So here's a shim. I too am a kernel hacker
static inline int fchmodat_shim(int dirfd, const char *pathname, mode_t mode) {
    if (!pathname[0])
        return fchmodat(dirfd, ".", mode, 0);

    if (S_ISLNK(mode)) {
        // The libc implementation calls openat to figure out if pathname is a
        // symlink. We already have this data
        return fchmodat(dirfd, pathname, mode, AT_SYMLINK_NOFOLLOW);
    }

    return fchmodat(dirfd, pathname, mode, 0);
}

struct SharedState {
    moodycamel::BlockingConcurrentQueue<FileCopyJob, QueueTraits> jobQueue;
    moodycamel::BlockingConcurrentQueue<FileCopyJob, QueueTraits> finishQueue;

    HLinkState hlinkState;
    sem_t fdSem;

    std::atomic_size_t nFinished;
    std::atomic_size_t filesSeen;
    std::atomic_size_t totalBytesTransferred; // actually transferred + existing
    std::atomic_size_t totalBytesActuallyTransferred;
    std::atomic_size_t totalBytesSeen;
    std::atomic_size_t bytesRead;
    std::atomic_size_t bytesWritten;

    std::atomic_size_t allocationAttempts;
    std::atomic_size_t allocationFailures;

    std::atomic_size_t totalMemAllocated = 0;

    std::atomic_size_t schedulerIterations;

    std::atomic<int> crawlDone;

    spinlock allocLock;
    buddy *copybufferAllocator;

    size_t nscheds;
    struct CopyScheduler *scheds;

    int destRootFD;
    int srcRootFD;
};

struct FileDescriptor {
    int fd;

    ~FileDescriptor() { close(fd); }
};

/**
 * Arbitrary length readlink
 * @param path Path to symlink
 * relative to that. Otherwise, set to -1
 * @return path (or NULL if error), stored in thread local buffer.
 */
char *readSymlink(Allocator *alloc, int srcFD, const char *path) {
    thread_local size_t size = INITIAL_READLINK_BUF_SIZE;
    thread_local AllocRef readlinkBuf = AllocatorAllocate(alloc, size);

    char *bufPtr = (char *)allocDeref(alloc, readlinkBuf);
    ssize_t nread = 0;
    while (1) {
        nread = readlinkat(srcFD, path, bufPtr, size);

        if (nread < 0) {
            fprintf(stderr,
                    "ERROR: readlink failed to read symlink \"%s\" "
                    "(errorcode %ld)\n",
                    path, errno);
            return nullptr;
        } else if (nread == 0) {
            bufPtr[0] = '\0';
            break;
        }

        if (nread >= size) {
            size += INITIAL_READLINK_BUF_SIZE;
            AllocatorFree(alloc, readlinkBuf);
            readlinkBuf = AllocatorAllocate(alloc, size);
            char *bufPtr = (char *)allocDeref(alloc, readlinkBuf);
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
    else if (res != iocb->u.c.nbytes) {
        fprintf(stderr, "ERROR: write missed bytes expect %lu got %ld\n",
                iocb->u.c.nbytes, res);
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
    else if (res != iocb->u.c.nbytes) {
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
        .fileCopyJobs =
            (FileCopyJob *)calloc(nFileCopyJobs, sizeof(FileCopyJob)),
        .blockCopyJobs = (BlockCopyJob *)calloc(
            nBlockCopyJobsPerFileCopyJob * nFileCopyJobs, sizeof(BlockCopyJob)),
        .cbBatch = (iocb **)calloc(nFileCopyJobs * nBlockCopyJobsPerFileCopyJob,
                                   sizeof(iocb *)),
        .cbBatchSize = 0,
    };

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

    iocb *cbp = &blockJob->cb;
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
            sharedState->bytesRead += blockJob->nbytes;

            io_prep_pwrite(&blockJob->cb, blockJob->parent->dstFd,
                           blockJob->data, blockJob->nbytes, blockJob->offset);
            iocb *cbp = &blockJob->cb;
            blockJob->status = EINPROGRESS;
            blockJob->type = WRITE;
            io_set_callback(&blockJob->cb, wr_done);

            // blockJob->srcHash = XXH3_64bits(blockJob->data,
            // blockJob->nbytes);
            blockJob->srcHash = 0;
            spdlog::trace("{} hash is {:x}", *blockJob, blockJob->srcHash);

            spdlog::trace("{} submitting write request corresponding "
                          "to finished read request",
                          *blockJob);

            schedIOSubmitDeferred(sched, &blockJob->cb);
        } else if (blockJob->type == WRITE) {
            spdlog::trace("{} write request finished", *blockJob);

            sharedState->bytesWritten += blockJob->nbytes;

            memset(blockJob->data, 0, blockJob->nbytes);
            io_prep_pread(&blockJob->cb, blockJob->parent->dstFd,
                          blockJob->data, blockJob->nbytes, blockJob->offset);
            iocb *cbp = &blockJob->cb;
            blockJob->status = EINPROGRESS;
            blockJob->type = READDST;
            io_set_callback(&blockJob->cb, rd_done);

            spdlog::trace(
                "{} submitting destination readback request corresponding "
                "to finished write request",
                *blockJob);

            schedIOSubmitDeferred(sched, &blockJob->cb);
        } else {
            spdlog::trace("{} readdst request finished", *blockJob);

            // auto dstHash = XXH3_64bits(blockJob->data, blockJob->nbytes);
            XXH64_hash_t dstHash = 0;
            spdlog::trace("{} dst hash is {:x}", *blockJob, dstHash);
            if (dstHash != blockJob->srcHash) {
                spdlog::error("{} src vs dst hash mismatch {:x} vs {:x}",
                              *blockJob, blockJob->srcHash, dstHash, dstHash);
                fileJob->nErrors++;
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
    schedIOSubmitBatch(sched, sharedState);

    // int rc = io_queue_run(sched->ctx);
    int rc = io_queue_run_timeout(sched->ctx, {.tv_sec = 1, .tv_nsec = 0});
    if (rc < 0) {
        spdlog::error("aio queue error: {}", strerror(-rc));
        return -rc;
    } else {
        return 0;
    }
}

int schedulerUpdate(
    CopyScheduler *sched, SharedState *sharedState,
    moodycamel::BlockingConcurrentQueue<FileCopyJob, QueueTraits> &jobQueue) {
    // update current block jobs
    for (int i = 0; i < sched->nFileCopyJobs; i++) {
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
    for (int i = 0; i < sched->nFileCopyJobs; i++) {
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
                           // for (int j = 0; j < 64; j++) {
    for (int i = 0; i < sched->nFileCopyJobs; i++) {
        FileCopyJob newJob;
        if (!sched->fileCopyJobs[i].active) {
            if (jobQueue.try_dequeue(newJob)) {
                sched->fileCopyJobs[i] = newJob;
                sched->fileCopyJobs[i].nBlockJobsScheduled = 0;
                sched->fileCopyJobs[i].nBlockJobsFinished = 0;
                sched->fileCopyJobs[i].nErrors = 0;
                sched->fileCopyJobs[i].active = true;
                // activeJobs++;

                break;
            }
        } else {
            // activeJobs++;
        }
    }
    // }
    for (int i = 0; i < sched->nFileCopyJobs; i++) {
        activeJobs += (size_t)(sched->fileCopyJobs[i].active);
    }

    sharedState->schedulerIterations++;

    if (!activeJobs && sharedState->crawlDone) {
        return 0;
    }

    schedulerTick(sched, sharedState);

    return EINPROGRESS;
}

int processNonDir(StringPartRef path, std::optional<dev_t> dev,
                  size_t srcRootLen, int srcRootFD, int destRootFD,
                  SharedState *sharedState) {
    StringPartGuard pathGuard{globalAllocator, path};

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

    size_t suffixBufLen = 16 + std::strlen(".partial");
    char suffixBuf[suffixBufLen]; // if your compiler turns this into a vla,
                                  // resconsider life

    const char *remote = (const char *)allocDeref(globalAllocator, remoteBuf);

    snprintf(suffixBuf, suffixBufLen, "%016lX.partial",
             XXH3_64bits_withSeed(remote, strlen(remote), 0));

    StringPart *pathPart = (StringPart *)allocDeref(globalAllocator, path);
    StringPartRef partialRef =
        toStringPart(globalAllocator, pathPart->prev, pathPart->data,
                     pathPart->len, suffixBuf, suffixBufLen);
    StringPartGuard partialRefGuard{globalAllocator, partialRef};
    ok = (!stringrefMemcpyWithRealloc(globalAllocator, partialRef, &partialBuf,
                                      &partialBufSize));
    assert(ok);

    const char *partial = (const char *)allocDeref(globalAllocator, partialBuf);

    bool shouldTransfer = false;
    bool exists = true;

    struct statx stx;
    if (statx(srcRootFD, remote, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &stx) <
        0) {
        spdlog::error("Failed to statx src {}: {}\n", remote, strerror(errno));
        assert(false);
        return 1;
    }

    struct statx destStx = {};
    if (statx(destRootFD, remote, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
              &destStx) < 0) {

        if (errno == ENOENT) {
            shouldTransfer = true;
            exists = false;
        } else {
            assert(false);
            spdlog::error("Failed to statx dest {}: {}\n", remote,
                          strerror(errno));
            return 1;
        }
    }

    sharedState->filesSeen++;

reroll:
    shouldTransfer = !exists;
    shouldTransfer |= memcmp(&stx.stx_mtime, &destStx.stx_mtime,
                             sizeof(stx.stx_mtime)) != 0 ||
                      stx.stx_size != destStx.stx_size;

    bool isPendingHardlink = false;
    bool isFirstLook = false;

    if (stx.stx_nlink > 1) {
        hlinkStateRegisterLinkRoot(&sharedState->hlinkState, globalAllocator,
                                   remote, path, stx.stx_ino, destStx.stx_ino,
                                   exists, stx.stx_nlink, destStx.stx_nlink,
                                   destRootFD, &shouldTransfer, &isFirstLook,
                                   &isPendingHardlink);
        if (isPendingHardlink) {
            shouldTransfer = false;
        }
    } else {
        isFirstLook = true;
    }

    if (shouldTransfer) {
        unlinkat(destRootFD, partial, 0);

        if (S_ISREG(stx.stx_mode)) {

            sem_wait(&sharedState->fdSem);
            int srcFd =
                openat(srcRootFD, remote, O_RDONLY | O_DIRECT | O_NOATIME);
            if (srcFd < 0) {
                spdlog::error("failed to open src {}: {}\n", remote,
                              strerror(errno));
                sem_post(&sharedState->fdSem);
                return 1;
            }

            int dstFd =
                openat(destRootFD, partial,
                       O_RDWR | O_CREAT | O_DIRECT | O_NOATIME, stx.stx_mode);

            if (dstFd < 0) {
                spdlog::error("failed to open dest {}: {}", remote,
                              strerror(errno));
                sem_post(&sharedState->fdSem);
                close(srcFd);
                return 1;
            }

#define CLEAN                                                                  \
    sem_post(&sharedState->fdSem);                                             \
    close(srcFd);                                                              \
    close(dstFd)

            if (fchownat(destRootFD, partial, stx.stx_uid, stx.stx_gid,
                         AT_SYMLINK_NOFOLLOW) < 0) {
                spdlog::error("failed to fchownat {}: {}", remote,
                              strerror(errno));
                CLEAN;
                return 1;
            }

            if (ftruncate(dstFd, stx.stx_size)) {
                spdlog::error("failed to ftruncate {}: {}", remote,
                              strerror(errno));
                CLEAN;
                return 1;
            }

            sharedState->totalBytesSeen += stx.stx_size;

            sharedState->jobQueue.enqueue({
                .firstLook = isFirstLook,
                .remote = STRING_PART_RC_INC(globalAllocator, path),
                .partial = STRING_PART_RC_INC(globalAllocator, partialRef),
                .stx = stx,
                .srcFd = srcFd,
                .dstFd = dstFd,
                .blockSize = blockSize(stx.stx_size),
            });

#undef CLEAN
        } else if (S_ISLNK(stx.stx_mode)) {

            // symlinkat(, int tofd, const char *to)
            char *contents = readSymlink(globalAllocator, srcRootFD, remote);

            if (!contents) {
                spdlog::error("failed to read symlink {}", remote);
                return 1;
            }

            spdlog::trace("performing symlink {}->{}", remote, contents);
            if (symlinkat(contents, destRootFD, partial) < 0) {
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
            });
        } else if (!isPendingHardlink && !isFirstLook) {
            sharedState->nFinished++;
        }
    }

    return 0;
}

int processDir(StringPartRef path, std::optional<dev_t> dev, size_t srcRootLen,
               int srcRootFD, int destRootFD, SharedState *sharedState) {
    assert(!isNullRef(path));

    thread_local size_t remoteBufSize = PATH_MAX;
    thread_local AllocRef remoteBuf =
        AllocatorAllocate(globalAllocator, remoteBufSize);
    int ok = (!stringrefMemcpyWithRealloc(globalAllocator, path, &remoteBuf,
                                          &remoteBufSize));
    assert(ok);

    const char *remote = (const char *)allocDeref(globalAllocator, remoteBuf);

    sem_wait(&sharedState->fdSem);
    int fd = openat(srcRootFD, remote, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        assert(false);
        spdlog::error("failed to open directory {}: {}({})", remote, errno,
                      strerror(errno));
        return -1;
    }

    struct statx stx;
    int ret =
        statx(srcRootFD, remote, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS, &stx);

    if (ret < 0) {
        assert(false);
        fprintf(stderr, "ERROR: failed to stat %s: %d (%s)\n", remote, errno,
                strerror(errno));
        return -1;
    }

    // fprintf(stderr, "%s size: %llu\n", path.c_str(), lmd.lmd_stx.stx_size);

    dev_t thisdev = makedev(stx.stx_dev_major, stx.stx_dev_minor);
    if (!dev) {
        dev = thisdev;
    } else if (thisdev != dev.value()) {
        return 0;
    }

    // const char *remote = path.c_str() + srcRootLen;
    // if (remote[0] == '/') {
    //     remote++;
    // }

    if (remote[0] && mkdirat(destRootFD, remote, stx.stx_mode) < 0 &&
        errno != EEXIST) {
        fprintf(stderr, "ERROR: failed to mkdir %s: %s\n", remote,
                strerror(errno));
        return -1;
    }

    DIR *dir = fdopendir(fd);
    if (!dir) {
        fprintf(stderr, "ERROR: failed to open directory %s: %d (%s)\n", remote,
                errno, strerror(errno));
        sem_post(&sharedState->fdSem);
        return -1;
    }

    sharedState->filesSeen++;

    struct dirent *d;
    while ((d = readdir(dir))) {
        if (ISDOT(d->d_name)) {
            continue;
        }

        StringPartRef newPathRef = toStringPart(globalAllocator, path, "/", 1,
                                                d->d_name, strlen(d->d_name));

        if (d->d_type == DT_DIR) {
#pragma omp task
            processDir(newPathRef, dev, srcRootLen, srcRootFD, destRootFD,
                       sharedState);
        } else {
#pragma omp task
            processNonDir(newPathRef, dev, srcRootLen, srcRootFD, destRootFD,
                          sharedState);
        }
    }

    closedir(dir);
    sem_post(&sharedState->fdSem);

    sharedState->finishQueue.enqueue({
        .firstLook = true,
        .remote = STRING_PART_RC_INC(globalAllocator, path),
        .partial = {},
        .stx = stx,
        .srcFd = -1,
        .dstFd = -1,
    });

    return 0;
}

void finishProcessor(SharedState *sharedState) {
    while (true) {
        bool done = true;
        for (int i = 0; i < sharedState->nscheds; i++) {
            if (!sharedState->scheds[i].done) {
                done = false;
            }
        }

        FileCopyJob fileJob;
        while (sharedState->finishQueue.wait_dequeue_timed(fileJob, 100000)) {
            if (fileJob.srcFd >= 0) {
                close(fileJob.srcFd);
            }

            if (fileJob.dstFd >= 0) {
                close(fileJob.dstFd);
            }

            if (fileJob.srcFd >= 0 && fileJob.dstFd >= 0) {
                sem_post(&sharedState->fdSem);
            }

            assert(!isNullRef(fileJob.remote));

            StringPartGuard remotePartGuard{globalAllocator, fileJob.remote};
            thread_local size_t remoteBufSize = PATH_MAX;
            thread_local AllocRef remoteBuf =
                AllocatorAllocate(globalAllocator, remoteBufSize);
            int ok = (!stringrefMemcpyWithRealloc(
                globalAllocator, fileJob.remote, &remoteBuf, &remoteBufSize));
            assert(ok);
            const char *remote =
                (const char *)allocDeref(globalAllocator, remoteBuf);

            StringPartGuard partialPartGuard{globalAllocator, fileJob.partial};
            thread_local size_t partialBufSize = PATH_MAX;
            thread_local AllocRef partialBuf =
                AllocatorAllocate(globalAllocator, partialBufSize);
            if (!isNullRef(fileJob.partial)) {
                ok = (!stringrefMemcpyWithRealloc(globalAllocator,
                                                  fileJob.partial, &partialBuf,
                                                  &partialBufSize));
                assert(ok);
            }

            const char *partial =
                (const char *)allocDeref(globalAllocator, partialBuf);

            struct statx destStx = {};
            if (statx(sharedState->destRootFD,
                      !isNullRef(fileJob.partial) ? partial : remote,
                      AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH, STATX_BASIC_STATS,
                      &destStx) < 0) {
                spdlog::error("{} failed to stat destination: {} ", fileJob,
                              strerror(errno));
            } else if (S_ISREG(fileJob.stx.stx_mode) &&
                       destStx.stx_size != fileJob.stx.stx_size) {
                spdlog::error("{} src vs dst size mismatch: {} vs {} ", fileJob,
                              fileJob.stx.stx_size, destStx.stx_size);
            } else if (fileJob.nErrors) {
                spdlog::error("{} failed with {} errors", fileJob,
                              fileJob.nErrors);
            } else {
                if (!isNullRef(fileJob.partial) &&
                    (spdlog::trace("{} finish queue unlinking {}", fileJob,
                                   remote),
                     unlinkat(sharedState->destRootFD, remote, 0)) < 0 &&
                    errno != ENOENT) {
                    spdlog::warn("{} failed to delete existing remote {} ",
                                 fileJob, strerror(errno));
                }

                struct timespec times[] = {
                    {.tv_nsec = UTIME_OMIT},
                    {.tv_sec = fileJob.stx.stx_mtime.tv_sec,
                     .tv_nsec = fileJob.stx.stx_mtime.tv_nsec},
                };

                bool fileJobSuccess = true;

                if (!isNullRef(fileJob.partial) &&
                    (spdlog::trace("{} finish queue linking "
                                   "partial to remote {} -> {}",
                                   fileJob, partial, remote),
                     renameat(sharedState->destRootFD, partial,
                              sharedState->destRootFD, remote) < 0)) {
                    spdlog::error("{} failed to perform partial to "
                                  "remote link {} -> {}: {} ",
                                  fileJob, partial, remote, strerror(errno));
                    fileJobSuccess = false;
                } else if ((destStx.stx_mtime.tv_sec !=
                            fileJob.stx.stx_mtime.tv_sec) &&
                           utimensat(sharedState->destRootFD, remote, times,
                                     AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH) < 0) {
                    spdlog::error("{} failed to set modification times : {} ",
                                  fileJob, strerror(errno));
                    fileJobSuccess = false;
                } else if ((destStx.stx_mode != fileJob.stx.stx_mode) &&
                           fchmodat_shim(sharedState->destRootFD, remote,
                                         fileJob.stx.stx_mode) < 0) {
                    spdlog::error("{} failed to set mode: {} ", fileJob,
                                  strerror(errno));
                    fileJobSuccess = false;
                } else if ((destStx.stx_uid != fileJob.stx.stx_uid ||
                            destStx.stx_gid != fileJob.stx.stx_gid) &&
                           fchownat(sharedState->destRootFD, remote,
                                    fileJob.stx.stx_uid, fileJob.stx.stx_gid,
                                    AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH) < 0) {
                    spdlog::error("{} failed to chown: {} ", fileJob,
                                  strerror(errno));
                    fileJobSuccess = false;

                } else if (fileJob.stx.stx_nlink > 1) {
                    size_t nFinishedInc = 0;
                    hlinkStateHandleTransfer(
                        fileJob, &sharedState->hlinkState, globalAllocator,
                        remote, fileJob.stx.stx_ino, destStx.stx_ino,
                        sharedState->srcRootFD, sharedState->destRootFD,
                        &nFinishedInc);

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

            sharedState->nFinished++;

            // if (fileJob.remote) {
            //     free((void *)fileJob.remote);
            // }
            //
            // if (fileJob.partial) {
            //     free((void *)fileJob.partial);
            // }
        }

        if (done)
            break;
    }
}

void incrementalLogger(SharedState *sharedState) {
    double lastPrint = omp_get_wtime();
    bool quit = false;
    while (true) {
        bool done = true;
        for (int i = 0; i < sharedState->nscheds; i++) {
            if (!sharedState->scheds[i].done) {
                done = false;
            }
        }

        if (done)
            break;

        double now = omp_get_wtime();
        double delta = now - lastPrint;
        if (delta > 3.0 || (quit && (size_t)(delta * 1000) > 1.0)) {
            spdlog::info(
                "transferred {}/{} files, {}({})/{} (read {}/s, "
                "write {}/s)",
                sharedState->nFinished.load(), sharedState->filesSeen.load(),
                bytesize::bytesize{sharedState->totalBytesTransferred.load()},
                bytesize::bytesize{
                    sharedState->totalBytesActuallyTransferred.load()},
                bytesize::bytesize{sharedState->totalBytesSeen.load()},
                bytesize::bytesize{
                    (size_t)((double)sharedState->bytesRead.load() / delta)},
                bytesize::bytesize{(
                    size_t)((double)sharedState->bytesWritten.load() / delta)});

            spdlog::info("\tallocation failures {}/{}",
                         sharedState->allocationFailures.load(),
                         sharedState->allocationAttempts.load());
            spdlog::info("\tqueue left {}",
                         sharedState->jobQueue.size_approx());
            spdlog::info("\ttotal allocated {}KB",
                         sharedState->totalMemAllocated.load() / 1024);
            spdlog::info("\tscheduler iterations {}",
                         sharedState->schedulerIterations.load());

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
            lastPrint = omp_get_wtime();
        }

        if (quit) {
            spdlog::info("done: {}", delta);
            break;
        }
    }
}

int main(int argc, char *argv[]) {
    auto err_logger = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_logger->set_level(spdlog::level::info);
    // spdlog::cfg::load_env_levels();

    auto file_sink =
        std::make_shared<spdlog::sinks::basic_file_sink_mt>("copy2.log", true);
    file_sink->set_level(spdlog::level::trace);

    auto logger = std::make_shared<spdlog::logger>(
        spdlog::logger("copy2", {err_logger, file_sink}));
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(logger);

    if (argc < 3) {
        spdlog::error("please specify a source and a destination path");
        return 1;
    }

    const char *root = argv[1];
    const char *dest = argv[2];

    int destRootFD = open(dest, O_DIRECTORY | O_RDONLY | O_NOATIME);
    if (destRootFD < 0) {
        spdlog::error("failed to open destination: {}", strerror(errno));
        return 1;
    }

    int srcRootFD = open(root, O_DIRECTORY | O_RDONLY | O_NOATIME);
    if (srcRootFD < 0) {
        spdlog::error("failed to open source: {}", strerror(errno));
        return 1;
    }

    spdlog::info("copying files from {} to {}", argv[1], argv[2]);
    double starttime = omp_get_wtime();

    SharedState *sharedState = new SharedState;
    // pthread_rwlock_init(&sharedState->hlinkState.lock, NULL);
    createHLinkState(&sharedState->hlinkState, "hlstate.db", 1000000,
                     1ULL << 30);
    sem_init(&sharedState->fdSem, 0, MAX_FILES);

    size_t arena_size = 80UL * (1UL << 30);
    void *buddy_metadata = malloc(buddy_sizeof_alignment(arena_size, 4096));
    void *buddy_arena;
    posix_memalign(&buddy_arena, 4096, arena_size);

    sharedState->copybufferAllocator =
        buddy_init_alignment((unsigned char *)buddy_metadata,
                             (unsigned char *)buddy_arena, arena_size, 4096);

    omp_set_nested(1);
    omp_set_max_active_levels(INT32_MAX);

    const int nScheds = 8; // omp_get_max_threads();
    CopyScheduler *scheds = new CopyScheduler[nScheds];

    for (int i = 0; i < nScheds; i++) {
        CopyScheduler &sched = scheds[i];
        if (createScheduler(&sched, 2048, 16)) {
            exit(1);
        }
    }

    sharedState->scheds = scheds;
    sharedState->nscheds = nScheds;

    sharedState->destRootFD = destRootFD;
    sharedState->srcRootFD = srcRootFD;

    globalAllocator = createAllocator(8, 1ULL << 40, "copy2.mem");
    if (!globalAllocator) {
        spdlog::error("failed to create allocator");
        return 1;
    }

    StringPartRef rootRef =
        toStringPart(globalAllocator, {}, ".", 1, nullptr, 0);

    int ret = 0;
#pragma omp parallel sections
    {
#pragma omp section
        {
#pragma omp taskgroup
            ret = processDir(STRING_PART_RC_INC(globalAllocator, rootRef),
                             std::nullopt, strlen(root), srcRootFD, destRootFD,
                             sharedState);
            sharedState->crawlDone = 1;
        }

#pragma omp section
        {
            incrementalLogger(sharedState);
        }

#pragma omp section
        {
#pragma omp parallel num_threads(128)
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

    return ret;
}
