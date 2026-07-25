#pragma once

#include "buddy_alloc.h"

#include "spdlog/spdlog.h"
#include "spinlock.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <linux/stat.h>
#include <sys/mman.h>
#include <sys/resource.h>

#define ALLOC_MAX_ARENA_SIZE UINT32_MAX
#define ALLOC_ARENA_SIZE ALLOC_MAX_ARENA_SIZE

#ifndef NDEBUG
#define ALLOC_DEBUG
#endif

static_assert(ALLOC_ARENA_SIZE <= ALLOC_MAX_ARENA_SIZE,
              "allocator arena size cannot exceed UINT32_MAX for "
              "indexing purposes");

#ifdef ALLOC_DEBUG
struct AllocDebug {
    uint32_t canary; // = 0xDEADBEEF
    uint32_t magic;
    size_t size;
};
#endif

struct AllocRef {
    void *ptr;

#ifdef ALLOC_DEBUG
    AllocDebug dbg;
#endif
};

struct AllocatorDiskState {
    int fd;
    size_t offset;
    size_t maxSize;
    void *data;
};

struct Allocator {
    spinlock lock;

    buddy *diskAlloc;
    buddy *memAlloc;
    unsigned char *memBase;

    void *copyBuffer;

    size_t nSubAllocators;
    size_t subAllocatorSize;
    spinlock *subAllocatorLocks;
};

inline void *allocDeref(const Allocator *alloc, AllocRef ref,
                        size_t expectedSize = 0) {
    return ref.ptr;
#ifdef ALLOC_DEBUG
    // assert that the allocation ref thinks it's allocated
    assert(ref.ptr);
    assert(ref.dbg.size);
    assert(ref.dbg.magic);
    assert(ref.dbg.canary);

    AllocDebug *dbg = (AllocDebug *)((uint8_t *)ref.ptr + ref.dbg.size);

    // assert this is most likely allocated
    assert(dbg->canary);
    assert(dbg->magic);
    assert(dbg->size);

    // sanity
    assert(ref.dbg.canary == 0xDEADBEEF);
    assert(ref.dbg.magic > 0);
    assert(ref.dbg.magic <= (uint32_t)RAND_MAX + 1);
    assert(dbg->magic > 0);
    assert(dbg->magic <= (uint32_t)RAND_MAX + 1);

    // do they match?
    assert(dbg->canary == ref.dbg.canary);
    assert(dbg->magic == ref.dbg.magic);
    assert(dbg->size == ref.dbg.size);

    if (expectedSize) {
        assert(dbg->size == expectedSize);
    }
#endif

    return ref.ptr;
}

inline bool isNullRef(AllocRef ref) {
    const AllocRef zeroRef = {};
#ifdef ALLOC_DEBUG
    assert((0 != ref.ptr) ==
           (0 != memcmp(&ref.dbg, &zeroRef.dbg, sizeof(AllocDebug))));
#endif

    return !ref.ptr;
}

Allocator *createAllocator(size_t memSize, size_t nSubAllocators,
                           size_t subAllocatorSize, size_t diskSize,
                           size_t copyBufferSize, const char *diskfilename);
AllocRef AllocatorAllocate(Allocator *alloc, size_t len);
AllocRef AllocatorAllocateRange(Allocator *alloc, size_t start, size_t len);
void AllocatorFree(Allocator *alloc, AllocRef ref);
void allocatorReportLeaks();

// const size_t STRING_PART_MAX_LEN = 64 - sizeof(AllocRef) - 1;
typedef AllocRef StringPartRef;
struct StringPart {
    std::atomic_size_t rc;
    size_t len;
    StringPartRef prev;
    char data[];
};

inline StringPart *stringPartDeref(const Allocator *alloc, StringPartRef ref) {
    StringPart *ptr = (StringPart *)allocDeref(alloc, ref);
    ptr =
        (StringPart *)allocDeref(alloc, ref, 1 + ptr->len + sizeof(StringPart));

    return ptr;
}

#define STRING_PART_RC_INC(alloc, ref)                                         \
    (((StringPart *)allocDeref(alloc, ref))->rc++, ref)

const char *stringrefImmediateToCString(Allocator *alloc, StringPartRef ref);
inline void stringPartRelease(Allocator *alloc, StringPartRef s) {
    StringPart *part;
    StringPartRef curRef = s;
    size_t i = 0;
    while (!isNullRef(curRef) && (part = stringPartDeref(alloc, curRef))) {
        i++;
        size_t rc = part->rc--;
        if (!rc) {
            spdlog::error("rc is zero: {}, it {}",
                          stringrefImmediateToCString(alloc, s), i);
        }
        assert(rc);

        AllocRef sTemp = curRef;
        curRef = part->prev;
        if (rc <= 1) {
            AllocatorFree(alloc, sTemp);
        } else {
            break;
        }
    }
}

struct StringPartGuard {
    Allocator *alloc;
    StringPartRef ref;

    ~StringPartGuard() {
        if (!isNullRef(ref))
            stringPartRelease(alloc, ref);
    }
};

size_t stringRefMemcpy(const Allocator *alloc, StringPartRef ref, char *buf,
                       size_t bufsize);

int stringrefMemcpyWithRealloc(Allocator *alloc, StringPartRef ref,
                               AllocRef *buf, size_t *bufsize);

StringPartRef toStringPart(Allocator *alloc, StringPartRef currentTip,
                           const char *s1, size_t len1, const char *s2,
                           size_t len2);

struct FDRC {
    std::atomic_size_t rc;
    int fd;

    StringPartRef path;
    bool dst;

    statx srcStx; // only relevant if dst is true
};

typedef AllocRef FDRCRef;
typedef AllocRef LinkEntryRef;
struct LinkEntry {
    StringPartRef link;
    LinkEntryRef prev;
    FDRCRef dstFDRef;
};

inline LinkEntryRef linkEntryAppend(Allocator *alloc, LinkEntryRef tip,
                                    LinkEntryRef next) {
    assert(!isNullRef(next));
    if (isNullRef(tip)) {
        ((LinkEntry *)allocDeref(alloc, next, sizeof(LinkEntry)))->prev = {};
        return next;
    }

    ((LinkEntry *)allocDeref(alloc, next, sizeof(LinkEntry)))->prev = tip;

    return next;
}

void *buddy_base_ptr(buddy *bdy);

extern Allocator *globalAllocator;
