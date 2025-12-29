#include <ranges>
#define BUDDY_ALLOC_IMPLEMENTATION
#include "allocator.h"
#include <sys/mman.h>

Allocator *globalAllocator = NULL;

Allocator *createAllocator(size_t memSize, size_t diskSize,
                           const char *diskfilename) {

    Allocator *alloc = NULL;
    buddy *memAlloc = NULL;
    if (memSize) {
        void *mem = mmap(NULL, memSize, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

        if (mem == (void *)-1) {
            spdlog::error("Failed to mmap memory buffer: {}", strerror(errno));
            exit(1);
        }

        memAlloc = buddy_embed_alignment((uint8_t *)mem, memSize, 128);

        if (!memAlloc) {
            spdlog::error("Failed to embed memory allocator");
            exit(1);
        }

        alloc = (Allocator *)buddy_calloc(memAlloc, 1, sizeof(Allocator));
    }

    if (diskSize) {
        int fd = open(diskfilename, O_RDWR | O_CREAT);
        if (fd < 0) {
            spdlog::error("Failed to open allocator disk backing file {}: {}",
                          diskfilename, strerror(errno));
            exit(1);
        }

        if (ftruncate(fd, diskSize) < 0) {
            spdlog::error("Failed to ftruncate allocator disk backing file {} "
                          "to size {}: {}",
                          diskfilename, diskSize, strerror(errno));
            exit(1);
        }

        // struct rlimit rlim = {};
        // getrlimit(RLIMIT_DATA, &rlim);

        // spdlog::info("diskSize: {}", diskSize);
        // spdlog::info("RLIMIT_DATA: {}, {}", rlim.rlim_cur, rlim.rlim_max);

        void *data =
            mmap(NULL, diskSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (data == (void *)-1) {
            spdlog::error("Failed to mmap allocator disk backing file: {}({})",
                          errno, strerror(errno));
            exit(1);
        }

        buddy *diskAlloc = buddy_embed((uint8_t *)data, diskSize);

        if (!diskAlloc) {
            spdlog::error("Failed to embed disk allocator");
            exit(1);
        }

        if (!alloc) {
            alloc = (Allocator *)buddy_calloc(diskAlloc, 1, sizeof(Allocator));
        }

        if (alloc) {
            alloc->diskAlloc = diskAlloc;
        }
    }

    return alloc;
}

AllocRef AllocatorAllocate(Allocator *alloc, size_t len) {
    // void *val = calloc(len, 1);
    // return *(AllocRef *)&val;

    assert(len);
    assert(alloc);

    alloc->lock.lock();

    void *allocation = NULL;

#ifdef ALLOC_DEBUG
    AllocDebug dbg = {
        .canary = 0xDEADBEEF,
        .magic = (uint32_t)rand(),
        .size = len,
    };
    len += sizeof(AllocDebug);
#endif

    if (alloc->memAlloc) {
        allocation = buddy_calloc(alloc->memAlloc, 1, len);
    }

    if (!allocation && alloc->diskAlloc) {
        allocation = buddy_calloc(alloc->diskAlloc, 1, len);
    }

#ifdef ALLOC_DEBUG
    if (allocation) {
        *(AllocDebug *)((uint8_t *)allocation + len - sizeof(AllocDebug)) = dbg;
    }
#endif

    alloc->lock.unlock();

    assert(allocation);
    return {
        .ptr = allocation,
#ifdef ALLOC_DEBUG
        .dbg = dbg,
#endif
    };
}

AllocRef AllocatorAllocateRange(Allocator *alloc, size_t offset, size_t len) {
    // void *val = calloc(len, 1);
    // return *(AllocRef *)&val;

    assert(len);
    assert(alloc);

    alloc->lock.lock();

    void *allocation = NULL;

#ifdef ALLOC_DEBUG
    AllocDebug dbg = {
        .canary = 0xDEADBEEF,
        .magic = (uint32_t)rand(),
        .size = len,
    };
    len += sizeof(AllocDebug);
#endif

    if (alloc->memAlloc) {
        char *base = (char *)buddy_base_ptr(alloc->memAlloc);
        char *allocBase = base + offset;
        if (!(allocBase < base) &&
            !((allocBase + len) > (base + alloc->memAlloc->memory_size))) {
            buddy_reserve_range(alloc->memAlloc, allocBase, len);
            allocation = allocBase;
        }
    }

    if (!allocation && alloc->diskAlloc) {
        char *base = (char *)buddy_base_ptr(alloc->diskAlloc);
        char *allocBase = base + offset;
        if (!(allocBase < base) &&
            !((allocBase + len) > (base + alloc->diskAlloc->memory_size))) {
            buddy_reserve_range(alloc->diskAlloc, allocBase, len);
            allocation = allocBase;
        }
    }

#ifdef ALLOC_DEBUG
    if (allocation) {
        *(AllocDebug *)((uint8_t *)allocation + len - sizeof(AllocDebug)) = dbg;
    }
#endif

    alloc->lock.unlock();

    assert(allocation);
    return {
        .ptr = allocation,
#ifdef ALLOC_DEBUG
        .dbg = dbg,
#endif
    };
}

void AllocatorFree(Allocator *alloc, AllocRef ref) {
    alloc->lock.lock();
    void *ptr = allocDeref(alloc, ref);

#ifdef ALLOC_DEBUG
    buddy_safe_free_status status = buddy_safe_free(
        alloc->memAlloc, ptr, ref.dbg.size + sizeof(AllocDebug));

    if (status == BUDDY_SAFE_FREE_INVALID_ADDRESS) {
        status = buddy_safe_free(alloc->diskAlloc, ptr,
                                 ref.dbg.size = sizeof(AllocDebug));
    }

    assert(status != BUDDY_SAFE_FREE_ALREADY_FREE);
    assert(status != BUDDY_SAFE_FREE_INVALID_ADDRESS);
    assert(status != BUDDY_SAFE_FREE_SIZE_MISMATCH);
    assert(status != BUDDY_SAFE_FREE_BUDDY_IS_NULL);
    assert(status == BUDDY_SAFE_FREE_SUCCESS);

    memset(ptr, 0, ref.dbg.size + sizeof(AllocDebug));
#else
    buddy_free(alloc->memAlloc, ptr);
    buddy_free(alloc->diskAlloc, ptr);
#endif
    alloc->lock.unlock();
}

int stringrefMemcpyWithRealloc(Allocator *alloc, StringPartRef ref,
                               AllocRef *buf, size_t *bufsize) {
    memset((void *)allocDeref(alloc, *buf), 0, *bufsize);
    size_t len = stringRefMemcpy(
        alloc, ref, (char *)allocDeref(alloc, *buf, *bufsize), *bufsize);
    if (len >= *bufsize) {
        *bufsize = len;
        AllocatorFree(alloc, *buf);
        *buf = AllocatorAllocate(alloc, *bufsize);

        if (len != stringRefMemcpy(alloc, ref,
                                   (char *)allocDeref(alloc, *buf, *bufsize),
                                   len)) {
            return 1;
        }
    }

    return 0;
}

StringPartRef toStringPart(Allocator *alloc, StringPartRef currentTip,
                           const char *s1, size_t len1, const char *s2,
                           size_t len2) {
    if (!isNullRef(currentTip)) {
        StringPart *prevPart = stringPartDeref(alloc, currentTip);
        STRING_PART_RC_INC(alloc, currentTip);
    }

    StringPartRef cur =
        AllocatorAllocate(alloc, sizeof(StringPart) + len1 + len2);

    StringPart *part = stringPartDeref(alloc, cur);
    part->len = len1 + len2;

#ifdef ALLOC_DEBUG
    char *bp = (char *)part;
    char *maxp = (char *)part + cur.dbg.size;

    assert((part->data) >= bp);
    assert((part->data + len1) <= maxp);
#endif
    memcpy(part->data, s1, len1);

    if (len2) {
#ifdef ALLOC_DEBUG
        assert((part->data + len1 + len2) <= maxp);
        assert((part->data + len1) >= bp);
#endif
        memcpy(part->data + len1, s2, len2);
    }
    part->rc = 1;
    part->prev = currentTip;

    return cur;
}

const char *stringrefImmediateToCString(Allocator *alloc, StringPartRef ref) {
    thread_local size_t bufSize = PATH_MAX;
    thread_local AllocRef remoteBuf = AllocatorAllocate(alloc, bufSize);

    stringrefMemcpyWithRealloc(alloc, ref, &remoteBuf, &bufSize);

    return (const char *)allocDeref(alloc, remoteBuf, bufSize);
}

size_t stringRefMemcpy(const Allocator *alloc, StringPartRef ref, char *buf,
                       size_t bufsize) {
    size_t size = 0;

    StringPartRef cur = ref;

    if (isNullRef(cur)) {
        size++;
        if (bufsize) {
            buf[0] = '\0';
        }

        return 1;
    }

    while (!isNullRef(cur)) {
        StringPart *part = stringPartDeref(alloc, cur);
        size += part->len;

        cur = part->prev;
    }
    size++;

    if (bufsize < size) {
        return size;
    }

    size_t it = 0;
    cur = ref;
    if (buf) {
        while (!isNullRef(cur)) {
            StringPart *part = stringPartDeref(alloc, cur);
            it += part->len;

            assert((size - it - 1) >= 0);
            assert((size - it - 1 + part->len) <= bufsize);

            memcpy(buf + size - it - 1, part->data, part->len);

            cur = part->prev;
        }

        assert(size <= bufsize);
        buf[size - 1] = '\0';
    }

    // spdlog::info("size is {}", size);

    return size;
}

void *buddy_base_ptr(buddy *bdy) { return buddy_main(bdy); }
