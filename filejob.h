#pragma once

#include "allocator.h"
#include "xxh_x86dispatch.h"
#include <cmath>
#include <libaio.h>
#include <linux/stat.h>

inline size_t blockSize(size_t fsize) {
    return std::max(4096UL,
                    std::min(32UL * (1 << 20), std::bit_ceil(fsize / 2)));
}

inline size_t blockCount(size_t fsize, size_t blockSize) {
    if (blockSize == 0)
        return 0;
    return (fsize / blockSize) + (size_t)((fsize % blockSize) > 0);
}

struct FileCopyJob {
    bool active;
    bool firstLook;
    bool actuallyTransferred;

    StringPartRef remote;
    StringPartRef partial;

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

static const char *rwStrings[] = {"READ", "WRITE", "READDST"};

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
