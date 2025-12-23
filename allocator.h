#pragma once

#include "buddy_alloc.h"
#include "spdlog/spdlog.h"
#include "spinlock.h"
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>

#define ALLOC_MAX_ARENA_SIZE UINT32_MAX
#define ALLOC_ARENA_SIZE ALLOC_MAX_ARENA_SIZE

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

inline void *allocDeref(const Allocator *alloc, AllocRef ref) {
    return *(void **)&ref;

    assert(ref.arenaID <= alloc->nArenas);
    assert(ref.arenaID != 0);
    assert(ref.offset <= ALLOC_MAX_ARENA_SIZE);

    return (uint8_t *)alloc->arenas[ref.arenaID - 1]._data + ref.offset;
}

inline bool isNullRef(AllocRef ref) {
    AllocRef zeroRef = {};
    return !memcmp(&zeroRef, &ref, sizeof(AllocRef));
}

Allocator *createAllocator(size_t maxMemArenas, size_t diskSize,
                           const char *diskfilename);
AllocRef AllocatorAllocate(Allocator *alloc, size_t len);
void AllocatorFree(Allocator *alloc, AllocRef ref);

// const size_t STRING_PART_MAX_LEN = 64 - sizeof(AllocRef) - 1;
typedef AllocRef StringPartRef;
struct StringPart {
    std::atomic_size_t rc;
    size_t len;
    StringPartRef prev;
    char data[];
};

#define STRING_PART_RC_INC(alloc, ref)                                         \
    (((StringPart *)allocDeref(alloc, ref))->rc++, ref)

const char *stringrefImmediateToCString(Allocator *alloc, StringPartRef ref);
inline void stringPartRelease(Allocator *alloc, StringPartRef s) {
    StringPart *part;
    StringPartRef curRef = s;
    size_t i = 0;
    while (!isNullRef(s) && (part = (StringPart *)allocDeref(alloc, curRef))) {
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

typedef AllocRef LinkEntryRef;
struct LinkEntry {
    StringPartRef link;
    LinkEntryRef prev;
};

inline LinkEntryRef linkEntryAppend(Allocator *alloc, LinkEntryRef tip,
                                    LinkEntryRef next) {
    assert(!isNullRef(next));
    if (isNullRef(tip)) {
        ((LinkEntry *)allocDeref(alloc, next))->prev = {};
        return next;
    }

    ((LinkEntry *)allocDeref(alloc, next))->prev = tip;

    return next;
}

extern Allocator *globalAllocator;
