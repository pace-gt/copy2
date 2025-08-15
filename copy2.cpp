#include "concurrentqueue.h"
#include "gtl/phmap_fwd_decl.hpp"
#include "spdlog/cfg/env.h"
#include "spdlog/fmt/bundled/format.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"
#include "xxh_x86dispatch.h"
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
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
#include <sys/sysmacros.h>
#include <unistd.h>

#define BUF_SIZE (1024UL * 1024 * 128)
#define QUEUE_SIZE 64

#define MAX_FILES 30000
sem_t globalFileSem;
size_t done = 0;

#define INITIAL_READLINK_BUF_SIZE 512

#define ISDOT(a) (a[0] == '.' && (!a[1] || (a[1] == '.' && !a[2])))

struct FileCopyJob {
    std::string remote;
    std::string partial;

    struct statx stx;

    int srcFd;
    int dstFd;

    // uint8_t srcHash[8];
};

enum RW {
    READ = 0,
    WRITE = 1,
};

const char *rwStrings[] = {"READ", "WRITE"};

struct BlockCopyJob {
    FileCopyJob parent;

    struct iocb cb;

    enum RW type;
    int status;

    size_t offset;
    size_t nbytes;

    void *data;

    bool inUse;

    XXH3_state_t *srcHashState;
    XXH3_state_t *dstHashState;
};

struct HLinkInfo {
    omp_lock_t mut;

    bool transferred;
    uint64_t destInode;
    std::string remote;
    std::vector<std::string> links;
};

struct HLinkState {
    omp_lock_t mut;
    gtl::flat_hash_map<uint64_t, HLinkInfo> srcToDst;
    gtl::flat_hash_map<uint64_t, uint64_t> dstToSrc;
};

template <> struct fmt::formatter<BlockCopyJob> : fmt::formatter<std::string> {
    auto format(BlockCopyJob my, format_context &ctx) const
        -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(),
                              "[BlockCopyJob remote={}, type={}, status={}, "
                              "offset={}, nbytes={}, fileSize={}]",
                              my.parent.remote, rwStrings[my.type], my.status,
                              my.offset, my.nbytes, my.parent.stx.stx_size);
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

    job->status = 0;
    if (res <= 0)
        job->status = -res;
    else if (res != iocb->u.c.nbytes) {
        fprintf(stderr, "ERROR: read missed bytes expect %lu got %ld\n",
                iocb->u.c.nbytes, res);
    }
}

int processNonDir(const std::string path,
                  std::shared_ptr<FileDescriptor> parentFD,
                  std::optional<dev_t> dev,
                  moodycamel::ConcurrentQueue<FileCopyJob> &jobQueue,
                  size_t srcRootLen, int srcRootFD, int destRootFD) {

    const char *remote = path.c_str() + srcRootLen;
    if (remote[0] == '/') {
        remote++;
    }

    std::string partial =
        fmt::format("{}{:016X}.partial", remote,
                    XXH3_64bits_withSeed(remote, strlen(remote), 0));

    bool shouldTransfer = false;

    struct statx stx;
    if (statx(AT_FDCWD, path.c_str(), AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
              &stx) < 0) {
        spdlog::error("Failed to statx src {}: {}\n", remote, strerror(errno));
        return 1;
    }

    struct statx destStx;
    if (statx(destRootFD, remote, AT_SYMLINK_NOFOLLOW, STATX_BASIC_STATS,
              &destStx) < 0) {

        if (errno == ENOENT) {
            shouldTransfer = true;
        } else {
            spdlog::error("Failed to statx dest {}: {}\n", remote,
                          strerror(errno));
            return 1;
        }
    }

    shouldTransfer |= memcmp(&stx.stx_mtime, &destStx.stx_mtime,
                             sizeof(stx.stx_mtime)) != 0 ||
                      stx.stx_size != destStx.stx_size;

    if (shouldTransfer) {
        unlinkat(destRootFD, partial.c_str(), AT_SYMLINK_NOFOLLOW);

        if (S_ISREG(stx.stx_mode)) {

            sem_wait(&globalFileSem);
            int srcFd = openat(srcRootFD, remote, O_RDONLY | O_DIRECT);
            if (srcFd < 0) {
                spdlog::error("failed to open src {}: {}\n", remote,
                              strerror(errno));
                return 1;
            }

            int dstFd = openat(destRootFD, partial.c_str(),
                               O_RDWR | O_CREAT | O_DIRECT, stx.stx_mode);

            if (dstFd < 0) {
                spdlog::error("failed to open dest {}: {}", remote,
                              strerror(errno));
                return 1;
            }

            if (fchownat(destRootFD, remote, stx.stx_uid, stx.stx_gid,
                         AT_SYMLINK_NOFOLLOW) < 0) {
                spdlog::error("failed to fchownat {}: {}", remote,
                              strerror(errno));
                return 1;
            }

            if (ftruncate(dstFd, stx.stx_size)) {
                spdlog::error("failed to ftruncate {}: {}", remote,
                              strerror(errno));
                return 1;
            }

            jobQueue.enqueue({
                .remote = std::move(remote),
                .partial = std::move(partial),
                .stx = stx,
                .srcFd = srcFd,
                .dstFd = dstFd,
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
                return 1;
            }

            free(contents);

            if (linkat(destRootFD, partial.c_str(), destRootFD, remote,
                       AT_SYMLINK_NOFOLLOW) < 0) {
                spdlog::error("failed to move (linkat)) {}->{}: {}",
                              partial.c_str(), remote, strerror(errno));
                return 1;
            }

            if (unlinkat(destRootFD, partial.c_str(), AT_SYMLINK_NOFOLLOW) <
                0) {
                spdlog::error(
                    "failed to move (unlinkat) {}->{}: failed to unlink {}: {}",
                    partial.c_str(), remote, partial.c_str(), strerror(errno));
                return 1;
            }
        }
    }

    return 0;
}

int processDir(const std::string &path, std::optional<dev_t> dev,
               moodycamel::ConcurrentQueue<FileCopyJob> &jobQueue,
               size_t srcRootLen, int srcRootFD, int destRootFD) {

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

    sem_wait(&globalFileSem);
    DIR *dir = opendir(path.c_str());
    if (!dir) {
        fprintf(stderr, "ERROR: failed to open directory %s: %d (%s)\n",
                path.c_str(), errno, strerror(errno));
        sem_post(&globalFileSem);
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
#pragma omp task shared(jobQueue)
            processDir(newPath, dev, jobQueue, srcRootLen, srcRootFD,
                       destRootFD);
        } else {
#pragma omp task shared(jobQueue)
            processNonDir(newPath, sharedFD, dev, jobQueue, srcRootLen,
                          srcRootFD, destRootFD);
        }
    }
    // #pragma taskwait
    // close(fd);

    closedir(dir);
    sem_post(&globalFileSem);

    return 0;
}

int main(int argc, char *argv[]) {
    auto err_logger = spdlog::stderr_color_mt("stderr");
    spdlog::cfg::load_env_levels();
    spdlog::set_default_logger(err_logger);

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

    sem_init(&globalFileSem, 0, MAX_FILES);

    omp_set_nested(1);
    omp_set_max_active_levels(1024);

    moodycamel::ConcurrentQueue<FileCopyJob> fileCopyQueue;

    int ret = 0;
#pragma omp parallel sections shared(fileCopyQueue, done, globalFileSem)
    {
#pragma omp section
        {
#pragma omp taskgroup
            ret = processDir(root, std::nullopt, fileCopyQueue, strlen(root),
                             srcRootFD, destRootFD);
            // fprintf(stderr, "HERE, %p\n", &done);
            done = 1;
        }

#pragma omp section
        {
            std::atomic<size_t> counter = 0;
            std::atomic<size_t> bytesWritten = 0;
#pragma omp parallel for shared(done)
            for (int thr = 0; thr < 2; thr++) {
                BlockCopyJob *requests = new BlockCopyJob[QUEUE_SIZE];
                // (BlockCopyJob *)calloc(QUEUE_SIZE, sizeof(BlockCopyJob));
                for (int i = 0; i < QUEUE_SIZE; i++) {
                    requests[i] = {};
                    requests[i].data = calloc(1, BUF_SIZE);
                    posix_memalign(reinterpret_cast<void **>(&requests[i].data),
                                   4096, BUF_SIZE);
                    requests[i].srcHashState = XXH3_createState();
                    requests[i].dstHashState = XXH3_createState();
                    XXH3_128bits_reset(requests[i].srcHashState);
                    XXH3_128bits_reset(requests[i].dstHashState);
                }

#define RESET_REQ                                                              \
    close(requests[i].parent.srcFd);                                           \
    close(requests[i].parent.dstFd);                                           \
    XXH3_128bits_reset(requests[i].srcHashState);                              \
    XXH3_128bits_reset(requests[i].dstHashState);                              \
    requests[i] = {                                                            \
        .data = requests[i].data,                                              \
        .srcHashState = requests[i].srcHashState,                              \
        .dstHashState = requests[i].dstHashState,                              \
    };                                                                         \
    sem_post(&globalFileSem);

                io_context_t ctx = {};
                io_queue_init(QUEUE_SIZE, &ctx);

                int sval;
                int full = true;
                while (!done || sem_getvalue(&globalFileSem, &sval) ||
                       (sval < MAX_FILES)) {
                    for (int i = 0; i < QUEUE_SIZE; i++) {
                        if (!requests[i].inUse) {
                            FileCopyJob job;
                            if (fileCopyQueue.try_dequeue(job)) {
                                requests[i].parent = job;
                                requests[i].inUse = true;

                                requests[i].offset = 0;
                                requests[i].nbytes = std::min(
                                    (size_t)job.stx.stx_size, BUF_SIZE);
                                // requests[i].cb.fildes = job.srcFd;
                                io_prep_pread(&requests[i].cb, job.srcFd,
                                              requests[i].data,
                                              requests[i].nbytes,
                                              requests[i].offset);

                                iocb *cbp = &requests[i].cb;
                                requests[i].status = EINPROGRESS;
                                io_set_callback(&requests[i].cb, rd_done);

                                spdlog::trace(
                                    "{} submitting read request on new slot",
                                    requests[i]);

                                if (io_submit(ctx, 1, &cbp) < 0) {
                                    spdlog::error(
                                        "{} failed to enqueue initial aio read "
                                        "request: {}",
                                        requests[i], strerror(errno));
                                    RESET_REQ
                                } else {
                                    requests[i].type = READ;
                                }
                            } else {
                                full = false;
                            }
                        } else if (requests[i].inUse &&
                                   requests[i].status == 0) {
                            if (requests[i].type == READ) {
                                XXH3_128bits_update(requests[i].srcHashState,
                                                    requests[i].data,
                                                    requests[i].nbytes);

                                io_prep_pwrite(
                                    &requests[i].cb, requests[i].parent.dstFd,
                                    requests[i].data, requests[i].nbytes,
                                    requests[i].offset);
                                iocb *cbp = &requests[i].cb;
                                requests[i].status = EINPROGRESS;
                                io_set_callback(&requests[i].cb, wr_done);

                                spdlog::trace(
                                    "{} submitting write request corresponding "
                                    "to finished read request",
                                    requests[i]);

                                if (io_submit(ctx, 1, &cbp) < 0) {
                                    spdlog::error(
                                        "{} failed to enqueue aio write: {}",
                                        requests[i], strerror(errno));
                                    RESET_REQ
                                } else {
                                    requests[i].type = WRITE;
                                }
                            } else if (requests[i].offset +
                                           requests[i].nbytes >=
                                       requests[i].parent.stx.stx_size) {
                                spdlog::trace("{} copy finished...performing "
                                              "final validation",
                                              requests[i]);
                                XXH3_128bits_update(requests[i].dstHashState,
                                                    requests[i].data,
                                                    requests[i].nbytes);

                                XXH128_hash_t srcHash = XXH3_128bits_digest(
                                    requests[i].srcHashState);
                                XXH128_hash_t dstHash = XXH3_128bits_digest(
                                    requests[i].dstHashState);

                                if (memcmp(&srcHash, &dstHash,
                                           sizeof(XXH128_hash_t)) != 0) {
                                    spdlog::error("{} hashes don't match",
                                                  requests[i]);
                                }

                                bytesWritten += requests[i].nbytes;

                                counter++;
                                if ((counter % 1000) == 0) {
                                    spdlog::info(
                                        "copied {} ({}G)", counter.load(),
                                        bytesWritten / 1024 / 1024 / 1024);
                                }
                                RESET_REQ
                            } else {
                                XXH3_128bits_update(requests[i].dstHashState,
                                                    requests[i].data,
                                                    requests[i].nbytes);
                                bytesWritten += requests[i].nbytes;
                                requests[i].offset += requests[i].nbytes;
                                assert(requests[i].parent.stx.stx_size >
                                       requests[i].offset);
                                requests[i].nbytes = std::min(
                                    (size_t)requests[i].parent.stx.stx_size -
                                        requests[i].offset,
                                    (size_t)BUF_SIZE);
                                // requests[i].cb.aio_fildes =
                                //     requests[i].parent.srcFd;
                                requests[i].type = READ;
                                io_prep_pread(
                                    &requests[i].cb, requests[i].parent.srcFd,
                                    requests[i].data, requests[i].nbytes,
                                    requests[i].offset);
                                iocb *cbp = &requests[i].cb;
                                requests[i].status = EINPROGRESS;
                                io_set_callback(&requests[i].cb, rd_done);

                                spdlog::trace("{} submitting read request",
                                              requests[i]);
                                if (io_submit(ctx, 1, &cbp) < 0) {
                                    spdlog::error(
                                        "{} failed to queue aio read {}",
                                        requests[i], strerror(errno));
                                    RESET_REQ
                                }
                            }
                        } else if (requests[i].inUse &&
                                   requests[i].status != 0 &&
                                   requests[i].status != EINPROGRESS) {

                            spdlog::error("{} aio request failed: {}",
                                          requests[i],
                                          strerror(requests[i].status));
                            RESET_REQ
                        } else {
                        }
                    }

                    // Handle IO's that have completed
                    int rc = io_queue_run(ctx);
                    if (rc < 0)
                        spdlog::error("aio queue error: %s\n", strerror(-rc));

                    // if we have maximum number of i/o's in flight
                    // then wait for one to complete
                    if (full) {
                        rc = io_getevents(ctx, 0, 0, NULL, NULL);
                        if (rc < 0)
                            spdlog::error("io wait error: {}", strerror(-rc));
                    }
                }

                // std::cout << "DONE" << std::endl;
            }
        }
    }

    // std::cout << "size: " << totalSize << " count: " << totalCount <<
    // std::endl;

    return ret;
}
