#include "concurrentqueue.h"
#include "gtl/phmap_fwd_decl.hpp"
#include "spdlog/cfg/env.h"
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
#include <chrono>
#include <complex>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <gtl/phmap.hpp>
#include <iostream>
#include <libaio.h>
#include <linux/stat.h>
#include <memory>
#include <omp.h>
#include <optional>
#include <sched.h>
#include <semaphore.h>
#include <string>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#include <thread>
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

// https://rigtorp.se/spinlock
struct spinlock {
    std::atomic<bool> lock_ = {0};

    void lock() noexcept {
        for (;;) {
            // Optimistically assume the lock is free on the first try
            if (!lock_.exchange(true, std::memory_order_acquire)) {
                return;
            }
            // Wait for lock to be released without generating cache misses
            while (lock_.load(std::memory_order_relaxed)) {
                // Issue X86 PAUSE or ARM YIELD instruction to reduce contention
                // between hyper-threads
                __builtin_ia32_pause();
            }
        }
    }

    bool try_lock() noexcept {
        // First do a relaxed load to check if lock is free in order to prevent
        // unnecessary cache misses if someone does while(!try_lock())
        return !lock_.load(std::memory_order_relaxed) &&
               !lock_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept { lock_.store(false, std::memory_order_release); }
};

// AT_EMPTY_PATH on fchmodat is only supported on kernel versions >= 6.6
// So here's a shim. I too am a kernel hacker
static inline int fchmodat_shim(int dirfd, const char *pathname, mode_t mode,
                                int flags) {
    if (flags & AT_EMPTY_PATH)
        return fchmodat(dirfd, pathname[0] ? pathname : ".", mode,
                        flags & ~AT_EMPTY_PATH);

    return fchmodat(dirfd, pathname, mode, flags);
}

struct FileCopyJob {
    bool active;

    const char *remote;
    const char *partial;

    struct statx stx;

    int srcFd;
    int dstFd;

    size_t blockSize;

    size_t nBlockJobsScheduled;
    size_t nBlockJobsFinished;
    size_t nErrors;
};

enum RW {
    READ = 0,
    WRITE = 1,
    READDST = 2,
};

const char *rwStrings[] = {"READ", "WRITE", "READDST"};

struct BlockCopyJob {
    int jobno; // ID within parent

    FileCopyJob *parent;

    struct iocb cb;

    enum RW type;
    int status;

    size_t offset;
    size_t nbytes;

    void *data;

    bool inUse;

    XXH64_hash_t srcHash;
};

struct HLinkInfo {
    spinlock lock;

    bool transferred;
    uint64_t destInode;
    std::string remote;
    std::vector<std::string> links;
};

struct HLinkState {
    spinlock lock;

    gtl::flat_hash_map<uint64_t, HLinkInfo *> srcToDst;
    gtl::flat_hash_map<uint64_t, uint64_t> dstToSrc;
};

struct SharedState {
    moodycamel::ConcurrentQueue<FileCopyJob> jobQueue;
    moodycamel::ConcurrentQueue<FileCopyJob> finishQueue;

    HLinkState hlinkState;
    sem_t fdSem;

    std::atomic<size_t> nFinished;
    std::atomic_size_t bytesRead;
    std::atomic_size_t bytesWritten;

    std::atomic<int> crawlDone;

    spinlock allocLock;
    buddy *allocator;

    size_t nscheds;
    struct CopyScheduler *scheds;
};

inline size_t blockSize(size_t fsize) {
    return std::max(
        4096UL, std::min(32UL * (1 << 20), ceiling_power_of_two(fsize / 2)));
}

inline size_t blockCount(size_t fsize, size_t blockSize) {
    if (blockSize == 0)
        return 0;
    return (fsize / blockSize) + (size_t)((fsize % blockSize) > 0);
}

template <> struct fmt::formatter<FileCopyJob> : fmt::formatter<std::string> {
    auto format(const FileCopyJob &my, format_context &ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(
            ctx.out(),
            "[FileCopyJob remote={}, partial={}, "
            "fileSize={}, blockSize={}, nScheduled={}, nFinished={}/{}]",
            my.remote ? my.remote : "NULL", my.partial ? my.partial : "NULL",
            my.stx.stx_size, my.blockSize, my.nBlockJobsScheduled,
            my.nBlockJobsFinished, blockCount(my.stx.stx_size, my.blockSize));
    }
};

template <> struct fmt::formatter<BlockCopyJob> : fmt::formatter<std::string> {
    auto format(const BlockCopyJob &my, format_context &ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "[BlockCopyJob parent={}, type={}, status={}, "
                              "offset={}, nbytes={}, buf={}]",
                              *my.parent, rwStrings[my.type], my.status,
                              my.offset, my.nbytes, (void *)my.data);
    }
};

struct FileDescriptor {
    int fd;

    ~FileDescriptor() { close(fd); }
};

/**
 * Arbitrary length readlink
 * @param path Path to symlink
 * relative to that. Otherwise, set to -1
 * @return path (or NULL if error). Please free when done
 */
char *readSymlink(const char *path) {
    size_t size = INITIAL_READLINK_BUF_SIZE;
    char *buf = NULL;
    ssize_t nread = 0;
    while (1) {
        buf = (char *)reallocarray(buf, size, 1);

        nread = readlink(path, buf, size);

        if (nread < 0) {
            fprintf(stderr,
                    "ERROR: readlink failed to read symlink \"%s\" "
                    "(errorcode %ld)\n",
                    path, errno);
            free(buf);
            return nullptr;
        } else if (nread == 0) {
            buf[0] = '\0';
            break;
        }

        if (nread >= size) {
            size += INITIAL_READLINK_BUF_SIZE;
        } else {
            buf[nread] = '\0';
            break;
        }
    }

    return buf;
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
        buddy_free(sharedState->allocator, blockJob->data);
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
        void *data = buddy_malloc(sharedState->allocator, fileJob->blockSize);
        sharedState->allocLock.unlock();

        if (!data) {
            spdlog::trace("{} failed to allocate {}", *fileJob,
                          fileJob->blockSize);
            schedulerClearBlockCopyJob(sched, sharedState, fileJob, blockJob);
            return;
        }

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

            blockJob->srcHash = XXH3_64bits(blockJob->data, blockJob->nbytes);
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

            auto dstHash = XXH3_64bits(blockJob->data, blockJob->nbytes);
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

    // close(fileJob->srcFd);
    // close(fileJob->dstFd);
    // sem_post(&sharedState->fdSem);

    *fileJob = {};
}

int schedulerTick(CopyScheduler *sched, SharedState *sharedState) {
    schedIOSubmitBatch(sched, sharedState);

    int rc = io_queue_run(sched->ctx);
    if (rc < 0) {
        spdlog::error("aio queue error: {}", strerror(-rc));
        return -rc;
    } else {
        return 0;
    }
}

int schedulerUpdate(CopyScheduler *sched, SharedState *sharedState,
                    moodycamel::ConcurrentQueue<FileCopyJob> &jobQueue) {
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
                // break;
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

                // break;
            } else {
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

    if (!activeJobs && sharedState->crawlDone) {
        return 0;
    }

    schedulerTick(sched, sharedState);

    return EINPROGRESS;
}

int processNonDir(const std::string path,
                  std::shared_ptr<FileDescriptor> parentFD,
                  std::optional<dev_t> dev, size_t srcRootLen, int srcRootFD,
                  int destRootFD, SharedState *sharedState) {

    const char *remote = path.c_str() + srcRootLen;
    if (remote[0] == '/') {
        remote++;
    }

    std::string partial =
        fmt::format("{}{:016X}.partial", remote,
                    XXH3_64bits_withSeed(remote, strlen(remote), 0));

    bool shouldTransfer = false;
    bool exists = true;

    struct statx stx;
    if (statx(AT_FDCWD, path.c_str(), AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
              &stx) < 0) {
        spdlog::error("Failed to statx src {}: {}\n", remote, strerror(errno));
        return 1;
    }

    struct statx destStx = {};
    if (statx(destRootFD, remote, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
              &destStx) < 0) {

        if (errno == ENOENT) {
            shouldTransfer = true;
            exists = false;
        } else {
            spdlog::error("Failed to statx dest {}: {}\n", remote,
                          strerror(errno));
            return 1;
        }
    }

    shouldTransfer |= memcmp(&stx.stx_mtime, &destStx.stx_mtime,
                             sizeof(stx.stx_mtime)) != 0 ||
                      stx.stx_size != destStx.stx_size;

    bool isPendingHardlink = false;

    sharedState->hlinkState.lock.lock();
    auto srcHLIt = sharedState->hlinkState.srcToDst.find(stx.stx_ino);
    if (srcHLIt != sharedState->hlinkState.srcToDst.end()) {
        shouldTransfer = false;
        bool wrongInode = destStx.stx_ino != srcHLIt->second->destInode;
        if (!exists || wrongInode) {
            if (wrongInode) {
                spdlog::debug("{} has incorrect inode on destination. "
                              "Relinking to root {}",
                              remote, srcHLIt->second->remote.c_str());
            }

            if (srcHLIt->second->transferred) {
                isPendingHardlink = true;
                const char *linkRoot = srcHLIt->second->remote.c_str();
                spdlog::debug(
                    "root {} has already been transferred. Hardlinking to {}",
                    linkRoot, remote);
                if (unlinkat(destRootFD, remote, 0) < 0 && errno != ENOENT) {
                    spdlog::error("Failed to unlink hardlink dest {}: {}",
                                  remote, strerror(errno));
                }

                if (linkat(destRootFD, linkRoot, destRootFD, remote, 0) < 0) {
                    raise(SIGTRAP);
                    spdlog::error("Failed to hardlink root {} to {}: {}",
                                  linkRoot, remote, strerror(errno));
                }
            } else {
                spdlog::debug(
                    "{} has not been transferred. Queueing hardlinkage to {}",
                    srcHLIt->second->remote.c_str(), remote);
                srcHLIt->second->links.push_back(std::string(remote));
                isPendingHardlink = true;
            }
        }
    } else {
        HLinkInfo *phinfo = new HLinkInfo;
        sharedState->hlinkState.srcToDst[stx.stx_ino] = phinfo;
        phinfo->remote = std::string(remote);
        phinfo->destInode = destStx.stx_ino;
        phinfo->transferred = false;

        if (exists) {
            auto destHLState =
                sharedState->hlinkState.dstToSrc.find(destStx.stx_ino);

            if (destHLState != sharedState->hlinkState.dstToSrc.end()) {
                if (destHLState->second != stx.stx_ino) {
                    spdlog::debug("Destination to source inode mapping {}->{} "
                                  "for {} doesn't match "
                                  "expected {}->{}. Transferring",
                                  destStx.stx_ino, stx.stx_ino, destStx.stx_ino,
                                  remote, destHLState->second);
                    shouldTransfer = true;
                }
            } else {
                sharedState->hlinkState.dstToSrc[destStx.stx_ino] = stx.stx_ino;
            }
        }
    }
    sharedState->hlinkState.lock.unlock();

    if (shouldTransfer) {
        unlinkat(destRootFD, partial.c_str(), 0);

        if (S_ISREG(stx.stx_mode)) {

            sem_wait(&sharedState->fdSem);
            int srcFd = openat(srcRootFD, remote, O_RDONLY | O_DIRECT);
            if (srcFd < 0) {
                spdlog::error("failed to open src {}: {}\n", remote,
                              strerror(errno));
                sem_post(&sharedState->fdSem);
                return 1;
            }

            int dstFd = openat(destRootFD, partial.c_str(),
                               O_RDWR | O_CREAT | O_DIRECT, stx.stx_mode);

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

            if (fchownat(destRootFD, partial.c_str(), stx.stx_uid, stx.stx_gid,
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

            struct stat destStat;
            if (fstat(dstFd, &destStat) < 0) {
                spdlog::error("failed to stat destfd {} ({}): {}", dstFd,
                              remote, strerror(errno));
                CLEAN;
                return 1;
            }

            sharedState->hlinkState.lock.lock();
            sharedState->hlinkState.srcToDst[stx.stx_ino]->destInode =
                destStat.st_ino;
            sharedState->hlinkState.dstToSrc[destStat.st_ino] = stx.stx_ino;
            sharedState->hlinkState.lock.unlock();

            sharedState->jobQueue.enqueue({
                .remote = strdup(remote),
                .partial = strdup(partial.c_str()),
                .stx = stx,
                .srcFd = srcFd,
                .dstFd = dstFd,
                .blockSize = blockSize(stx.stx_size),
            });
        } else if (S_ISLNK(stx.stx_mode)) {
            // symlinkat(, int tofd, const char *to)
            char *contents = readSymlink(path.c_str());

            if (!contents) {
                spdlog::error("failed to read symlink {}", path);
                return 1;
            }

            spdlog::trace("performing symlink {}->{}", remote, contents);
            if (symlinkat(contents, destRootFD, partial.c_str()) < 0) {
                spdlog::error("failed to symlinkat {}: {}", remote,
                              strerror(errno));
                free(contents);
                sem_post(&sharedState->fdSem);
                return 1;
            }

            free(contents);

            if (!isPendingHardlink) {
                sharedState->finishQueue.enqueue({
                    .remote = strdup(remote),
                    .partial = strdup(partial.c_str()),
                    .stx = stx,
                    .srcFd = -1,
                    .dstFd = -1,
                });
            }
        }
    } else {
        if (!isPendingHardlink) {
            sharedState->finishQueue.enqueue({
                .remote = strdup(remote),
                .partial = NULL,
                .stx = stx,
                .srcFd = -1,
                .dstFd = -1,
            });
        }
    }

    return 0;
}

int processDir(const std::string &path, std::optional<dev_t> dev,
               size_t srcRootLen, int srcRootFD, int destRootFD,
               SharedState *sharedState) {
    int fd = open(path.c_str(), O_RDONLY);
    auto sharedFD = std::make_shared<FileDescriptor>();
    sharedFD->fd = fd;

    struct statx stx;
    int ret = statx(AT_FDCWD, path.c_str(), AT_SYMLINK_NOFOLLOW,
                    STATX_BASIC_STATS, &stx);

    if (ret < 0) {
        fprintf(stderr, "ERROR: failed to stat %s: %d (%s)\n", path.c_str(),
                errno, strerror(errno));
        return -1;
    }

    // fprintf(stderr, "%s size: %llu\n", path.c_str(), lmd.lmd_stx.stx_size);

    dev_t thisdev = makedev(stx.stx_dev_major, stx.stx_dev_minor);
    if (!dev) {
        dev = thisdev;
    } else if (thisdev != dev.value()) {
        return 0;
    }

    const char *remote = path.c_str() + srcRootLen;
    if (remote[0] == '/') {
        remote++;
    }

    if (remote[0] && mkdirat(destRootFD, remote, stx.stx_mode) < 0 &&
        errno != EEXIST) {
        fprintf(stderr, "ERROR: failed to mkdir %s: %s\n", remote,
                strerror(errno));
        return -1;
    }

    sharedState->finishQueue.enqueue({
        .remote = strdup(remote),
        .partial = NULL,
        .stx = stx,
        .srcFd = -1,
        .dstFd = -1,
    });

    sem_wait(&sharedState->fdSem);
    DIR *dir = opendir(path.c_str());
    if (!dir) {
        fprintf(stderr, "ERROR: failed to open directory %s: %d (%s)\n",
                path.c_str(), errno, strerror(errno));
        sem_post(&sharedState->fdSem);
        return -1;
    }

    struct dirent *d;
    while ((d = readdir(dir))) {
        if (ISDOT(d->d_name)) {
            continue;
        }

        // fprintf(stderr, "name: %s\n", d->d_name);

        std::string newPath =
            path + std::string("/") + std::string((const char *)d->d_name);
        if (d->d_type == DT_DIR) {
#pragma omp task
            processDir(newPath, dev, srcRootLen, srcRootFD, destRootFD,
                       sharedState);
        } else {
#pragma omp task
            processNonDir(newPath, sharedFD, dev, srcRootLen, srcRootFD,
                          destRootFD, sharedState);
        }
    }
    // #pragma taskwait
    // close(fd);

    closedir(dir);
    sem_post(&sharedState->fdSem);

    return 0;
}

int main(int argc, char *argv[]) {
    auto err_logger = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    err_logger->set_level(spdlog::level::info);
    // spdlog::cfg::load_env_levels();

    auto file_sink =
        std::make_shared<spdlog::sinks::basic_file_sink_mt>("copy2.log", true);
    file_sink->set_level(spdlog::level::trace);

    auto logger = std::make_shared<spdlog::logger>(
        spdlog::logger("multi_sink", {err_logger, file_sink}));
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(logger);

    if (argc < 3) {
        spdlog::error("please specify a source and a destination path");
        return 1;
    }

    const char *root = argv[1];
    const char *dest = argv[2];

    int destRootFD = open(dest, O_DIRECTORY | O_RDONLY);
    if (destRootFD < 0) {
        spdlog::error("failed to open destination: {}", strerror(errno));
        return 1;
    }

    int srcRootFD = open(root, O_DIRECTORY | O_RDONLY);
    if (srcRootFD < 0) {
        spdlog::error("failed to open source: {}", strerror(errno));
        return 1;
    }

    spdlog::info("copying files from {} to {}", argv[1], argv[2]);

    SharedState *sharedState = new SharedState;
    sem_init(&sharedState->fdSem, 0, MAX_FILES);

    size_t arena_size = 80UL * (1UL << 30);
    void *buddy_metadata = malloc(buddy_sizeof(arena_size));
    void *buddy_arena;
    posix_memalign(&buddy_arena, 4096, arena_size);
    // malloc(arena_size);
    sharedState->allocator =
        buddy_init_alignment((unsigned char *)buddy_metadata,
                             (unsigned char *)buddy_arena, arena_size, 4096);

    omp_set_nested(1);
    omp_set_max_active_levels(1024);

    const int nScheds = 16; // omp_get_max_threads();
    CopyScheduler *scheds = new CopyScheduler[nScheds];

    for (int i = 0; i < nScheds; i++) {
        CopyScheduler &sched = scheds[i];
        if (createScheduler(&sched, 2048, 8)) {
            exit(1);
        }
    }

    sharedState->scheds = scheds;
    sharedState->nscheds = nScheds;

    int ret = 0;
#pragma omp parallel sections
    {
#pragma omp section
        {
#pragma omp taskgroup
            ret = processDir(root, std::nullopt, strlen(root), srcRootFD,
                             destRootFD, sharedState);
            sharedState->crawlDone = 1;
        }

#pragma omp section
        {
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
                        "transfered {} files (read {} kbytes/sec, write {} "
                        "kbytes/sec))",
                        sharedState->nFinished.load(),
                        ((sharedState->bytesRead.load() / 1024) * 1000) /
                            (size_t)(delta * 1000),
                        ((sharedState->bytesWritten.load() / 1024) * 1000) /
                            (size_t)(delta * 1000));

                    for (int i = 0; i < nScheds; i++) {
                        size_t nFActive = 0;
                        size_t nBActive = 0;
                        for (int j = 0; j < scheds[i].nFileCopyJobs; j++) {
                            nFActive +=
                                (size_t)(scheds[i].fileCopyJobs[j].active);
                        }

                        for (int j = 0;
                             j < scheds[i].nBlockCopyJobsPerFileCopyJob *
                                     scheds[i].nFileCopyJobs;
                             j++) {
                            nBActive +=
                                (size_t)(scheds[i].blockCopyJobs[j].inUse);
                        }

                        spdlog::info("\tsched{}: {}/{}f, {}/{}b", i, nFActive,
                                     scheds[i].nFileCopyJobs, nBActive,
                                     scheds[i].nBlockCopyJobsPerFileCopyJob *
                                         scheds[i].nFileCopyJobs);
                    }

                    sharedState->bytesWritten = 0;
                    sharedState->bytesRead = 0;
                    lastPrint = omp_get_wtime();
                }

                if (quit) {
                    spdlog::info("done: {}", delta);
                    break;
                }
            }
        }

#pragma omp section
        {
#pragma omp parallel
            {
                while (true) {
                    bool done = true;
                    for (int i = 0; i < sharedState->nscheds; i++) {
                        if (!sharedState->scheds[i].done) {
                            done = false;
                        }
                    }

                    FileCopyJob fileJob;
                    while (sharedState->finishQueue.try_dequeue(fileJob)) {
                        if (fileJob.srcFd >= 0) {
                            close(fileJob.srcFd);
                        }

                        if (fileJob.dstFd >= 0) {
                            close(fileJob.dstFd);
                        }

                        if (fileJob.srcFd >= 0 && fileJob.dstFd >= 0) {
                            sem_post(&sharedState->fdSem);
                        }

                        struct statx destStx = {};
                        if (statx(destRootFD,
                                  fileJob.partial ? fileJob.partial
                                                  : fileJob.remote,
                                  AT_SYMLINK_NOFOLLOW | AT_EMPTY_PATH,
                                  STATX_BASIC_STATS, &destStx) < 0) {
                            spdlog::error("{} failed to stat destination: {} ",
                                          fileJob, strerror(errno));
                        } else if (S_ISREG(fileJob.stx.stx_mode) &&
                                   destStx.stx_size != fileJob.stx.stx_size) {
                            spdlog::error(
                                "{} src vs dst size mismatch: {} vs {} ",
                                fileJob, fileJob.stx.stx_size,
                                destStx.stx_size);
                        } else if (fileJob.nErrors) {
                            spdlog::error("{} failed with {} errors", fileJob,
                                          fileJob.nErrors);
                        } else {
                            if (fileJob.partial &&
                                (spdlog::trace("{} finish queue unlinking {}",
                                               fileJob, fileJob.remote),
                                 unlinkat(destRootFD, fileJob.remote, 0)) < 0 &&
                                errno != ENOENT) {
                                spdlog::warn(
                                    "{} failed to delete existing remote {} ",
                                    fileJob, strerror(errno));
                            }

                            struct timespec times[] = {
                                {.tv_nsec = UTIME_OMIT},
                                {.tv_sec = fileJob.stx.stx_mtime.tv_sec,
                                 .tv_nsec = fileJob.stx.stx_mtime.tv_nsec},
                            };

                            if (fileJob.partial &&
                                (spdlog::trace("{} finish queue linking "
                                               "partial to remote {} -> {}",
                                               fileJob, fileJob.partial,
                                               fileJob.remote),
                                 linkat(destRootFD, fileJob.partial, destRootFD,
                                        fileJob.remote, 0) < 0)) {
                                spdlog::error("{} failed to perform partial to "
                                              "remote link {} -> {}: {} ",
                                              fileJob, fileJob.partial,
                                              fileJob.remote, strerror(errno));
                            } else if ((destStx.stx_mtime.tv_sec !=
                                        fileJob.stx.stx_mtime.tv_sec) &&
                                       utimensat(destRootFD, fileJob.remote,
                                                 times,
                                                 AT_SYMLINK_NOFOLLOW |
                                                     AT_EMPTY_PATH) < 0) {
                                spdlog::error(
                                    "{} failed to set modification times : {} ",
                                    fileJob, strerror(errno));
                            } else if ((destStx.stx_mode !=
                                        fileJob.stx.stx_mode) &&
                                       fchmodat_shim(destRootFD, fileJob.remote,
                                                     fileJob.stx.stx_mode,
                                                     AT_SYMLINK_NOFOLLOW |
                                                         AT_EMPTY_PATH) < 0) {
                                spdlog::error("{} failed to set mode: {} ",
                                              fileJob, strerror(errno));
                            } else if ((destStx.stx_uid !=
                                            fileJob.stx.stx_uid ||
                                        destStx.stx_gid !=
                                            fileJob.stx.stx_gid) &&
                                       fchownat(destRootFD, fileJob.remote,
                                                fileJob.stx.stx_gid,
                                                fileJob.stx.stx_uid,
                                                AT_SYMLINK_NOFOLLOW |
                                                    AT_EMPTY_PATH) < 0) {
                                spdlog::error("{} failed to chown: {} ",
                                              fileJob, strerror(errno));

                            } else {
                                sharedState->hlinkState.lock.lock();
                                auto it = sharedState->hlinkState.srcToDst.find(
                                    fileJob.stx.stx_ino);

                                bool notend = false;
                                HLinkInfo *phlstate = NULL;
                                if (it !=
                                    sharedState->hlinkState.srcToDst.end()) {
                                    it->second->transferred = true;
                                    it->second->destInode = destStx.stx_ino;

                                    sharedState->hlinkState
                                        .dstToSrc[destStx.stx_ino] =
                                        fileJob.stx.stx_ino;

                                    notend = true;
                                    phlstate = it->second;
                                }

                                sharedState->hlinkState.lock.unlock();

                                if (notend) {
                                    for (auto &tgt : phlstate->links) {
                                        spdlog::trace(
                                            "{} finish queue performing "
                                            "pending link {} -> {}",
                                            fileJob, fileJob.remote,
                                            tgt.c_str());

                                        if (unlinkat(destRootFD, tgt.c_str(),
                                                     0) < 0 &&
                                            errno != ENOENT) {
                                            spdlog::error("{} failed to unlink "
                                                          "{} to apply "
                                                          "pending: {}",
                                                          fileJob, tgt.c_str(),
                                                          strerror(errno));
                                        }

                                        if (linkat(destRootFD, fileJob.remote,
                                                   destRootFD, tgt.c_str(),
                                                   0) < 0) {
                                            spdlog::error(
                                                "{} failed to perform pending"
                                                " link {} -> {} : {} ",
                                                fileJob, fileJob.remote,
                                                tgt.c_str(), strerror(errno));
                                        }
                                        sharedState->nFinished++;
                                    }
                                }
                            }
                        }

                        if (fileJob.partial &&
                            unlinkat(destRootFD, fileJob.partial, 0) < 0) {
                            spdlog::warn(
                                "{} failed to delete existing partial {}",
                                fileJob, strerror(errno));
                        }

                        sharedState->nFinished++;

                        if (fileJob.remote) {
                            free((void *)fileJob.remote);
                        }

                        if (fileJob.partial) {
                            free((void *)fileJob.partial);
                        }
                    }

                    if (done)
                        break;
                }
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

    return ret;
}
