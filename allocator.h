#pragma once

#include "buddy_alloc.h"
#include "spdlog/spdlog.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>

#define ALLOC_MAX_ARENA_SIZE UINT32_MAX
#define ALLOC_ARENA_SIZE ALLOC_MAX_ARENA_SIZE

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

static_assert(ALLOC_ARENA_SIZE <= ALLOC_MAX_ARENA_SIZE,
              "allocator arena size cannot exceed UINT32_MAX for "
              "indexing purposes");

struct AllocRef {
    uint32_t arenaID;
    uint32_t offset;
};

enum ArenaType {
    ARENA_MEMORY = 0,
    ARENA_DISK,
};

struct AllocatorArena {
    ArenaType type;
    void *_metadata;
    void *_data;
    buddy *arena;
};

struct AllocatorDiskState {
    int fd;
    size_t offset;
    size_t maxSize;
    void *data;
};

struct Allocator {
    spinlock lock;

    AllocatorDiskState diskState;

    size_t maxMemArenas;
    size_t nMemArenas;

    uint32_t nArenas;
    AllocatorArena *arenas;
};

inline AllocatorArena __AllocatorCreateMemoryArena() {
    const size_t alignment =
        128; // I assume here that for non-trivial projects the path length is
             // going to be more than 64B in 90% of cases
    void *arenaData = malloc(ALLOC_ARENA_SIZE);
    void *arenaMetadata =
        malloc(buddy_sizeof_alignment(ALLOC_ARENA_SIZE, alignment));

    return {
        .type = ARENA_MEMORY,
        ._metadata = arenaMetadata,
        ._data = arenaData,
        .arena = buddy_init_alignment((unsigned char *)arenaMetadata,
                                      (unsigned char *)arenaData,
                                      ALLOC_ARENA_SIZE, alignment),
    };
}

inline AllocatorArena __AllocatorCreateDiskArena(Allocator *alloc) {
    const size_t alignment =
        128; // I assume here that for non-trivial projects the path length is
             // going to be more than 64B in 90% of cases
    if (alloc->diskState.offset + ALLOC_ARENA_SIZE > alloc->diskState.maxSize) {
        spdlog::error("Allocator is out of disk space!");
        exit(1);
    }

    void *arenaData =
        (uint8_t *)alloc->diskState.data + alloc->diskState.offset;
    alloc->diskState.offset += ALLOC_ARENA_SIZE;
    void *arenaMetadata =
        malloc(buddy_sizeof_alignment(ALLOC_ARENA_SIZE, alignment));

    spdlog::info("arenaData {}, arenaMetadata {}", arenaData, arenaMetadata);

    return {
        .type = ARENA_DISK,
        ._metadata = arenaMetadata,
        ._data = arenaData,
        .arena = buddy_init_alignment((unsigned char *)arenaMetadata,
                                      (unsigned char *)arenaData,
                                      ALLOC_ARENA_SIZE, alignment),
    };
}

// inline void __AllocatorDestroyMemoryArena(AllocatorArena *arena) {
//     free(arena->arena);
//     free(arena->_metadata);
//     free(arena->_data);
//     *arena = {};
// }

inline Allocator *createAllocator(size_t maxMemArenas, size_t diskSize,
                                  const char *diskfilename) {
    Allocator *alloc = (Allocator *)calloc(1, sizeof(Allocator));

    // alloc->nArenas = 1;
    // alloc->nMemArenas = 1;
    // alloc->arenas = (AllocatorArena *)realloc(
    //     alloc->arenas, sizeof(AllocatorArena) * alloc->nArenas);
    //
    // alloc->arenas[0] = __AllocatorCreateMemoryArena();

    int fd = open(diskfilename, O_RDWR | O_CREAT);
    if (fd < 0) {
        spdlog::error("Failed to open allocator disk backing file {}: {}",
                      diskfilename, strerror(errno));
        exit(1);
    }

    if (ftruncate(fd, diskSize) < 0) {
        spdlog::error(
            "Failed to ftruncate allocator disk backing file {} to size {}: {}",
            diskfilename, diskSize, strerror(errno));
        exit(1);
    }

    alloc->maxMemArenas = maxMemArenas;

    alloc->diskState = {};
    alloc->diskState.fd = fd;
    alloc->diskState.offset = 0;
    alloc->diskState.maxSize = diskSize;

    struct rlimit rlim = {};
    getrlimit(RLIMIT_DATA, &rlim);

    spdlog::info("diskSize: {}", diskSize);
    spdlog::info("RLIMIT_DATA: {}, {}", rlim.rlim_cur, rlim.rlim_max);

    alloc->diskState.data =
        mmap(NULL, diskSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (alloc->diskState.data == (void *)-1) {
        spdlog::error("Failed to mmap allocator disk backing file: {}({})",
                      errno, strerror(errno));
        exit(1);
    }

    return alloc;
}

// Allocate a string of len
// Note that len includes the null terminator
inline AllocRef AllocatorAllocate(Allocator *alloc, size_t len) {
    assert(len);
    assert(alloc);
    assert(len <= UINT32_MAX);

    alloc->lock.lock();

    void *allocated = NULL;
    uint32_t arena = 0;
    // I assume that the number of arenas for this application will be
    // relatively low so we can iterate through all of them to prioritize
    // compactness
    for (; arena < alloc->nArenas; arena++) {
        allocated = buddy_calloc(alloc->arenas[arena].arena, 1, len);

        if (allocated) {
            goto alloc_success;
        }
    }

    arena = alloc->nArenas++;
    alloc->arenas = (AllocatorArena *)realloc(
        alloc->arenas, sizeof(AllocatorArena) * alloc->nArenas);
    if (alloc->nMemArenas < alloc->maxMemArenas) {
        alloc->arenas[alloc->nArenas - 1] = __AllocatorCreateMemoryArena();
    } else {
        spdlog::info("creating disk allocator...");
        alloc->arenas[alloc->nArenas - 1] = __AllocatorCreateDiskArena(alloc);
    }

    spdlog::info("arena is {}",
                 (void *)alloc->arenas[alloc->nArenas - 1].arena);
    allocated = buddy_calloc(alloc->arenas[alloc->nArenas - 1].arena, 1, len);

alloc_success:
    assert(allocated);
    int64_t offset =
        (uint8_t *)allocated - (uint8_t *)alloc->arenas[arena]._data;
    assert(offset >= 0);
    assert(offset <= UINT32_MAX);

    alloc->lock.unlock();

    return {
        .arenaID = arena,
        .offset = (uint32_t)offset,
    };
}

inline void *allocDeref(const Allocator *alloc, AllocRef ref) {
    assert(ref.arenaID < alloc->nArenas);
    assert(ref.offset <= ALLOC_MAX_ARENA_SIZE);

    return (uint8_t *)alloc->arenas[ref.arenaID]._data + ref.offset;
}

inline void AllocatorFree(Allocator *alloc, AllocRef ref) {
    alloc->lock.lock();
    void *ptr = allocDeref(alloc, ref);

    assert(ref.arenaID < alloc->nArenas);

    buddy_free(alloc->arenas[ref.arenaID].arena, ptr);
    alloc->lock.unlock();
}

inline bool isNullRef(AllocRef ref) {
    AllocRef zeroRef = {};
    return !memcmp(&zeroRef, &ref, sizeof(AllocRef));
}
