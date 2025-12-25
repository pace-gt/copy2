#include "allocator.h"

Allocator *globalAllocator = NULL;

static AllocatorArena __AllocatorCreateMemoryArena() {
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

static AllocatorArena __AllocatorCreateDiskArena(Allocator *alloc) {
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

Allocator *createAllocator(size_t maxMemArenas, size_t diskSize,
                           const char *diskfilename) {
    Allocator *alloc = (Allocator *)calloc(1, sizeof(Allocator));

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
AllocRef AllocatorAllocate(Allocator *alloc, size_t len) {
    // void *val = calloc(len, 1);
    // return *(AllocRef *)&val;

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
        .arenaID = arena + 1,
        .offset = (uint32_t)offset,
    };
}

void AllocatorFree(Allocator *alloc, AllocRef ref) {
    // free(*(void **)&ref);
    // return;

    alloc->lock.lock();
    void *ptr = allocDeref(alloc, ref);

    assert(ref.arenaID <= alloc->nArenas);
    assert(ref.arenaID != 0);

    buddy_free(alloc->arenas[ref.arenaID - 1].arena, ptr);
    alloc->lock.unlock();
}

int stringrefMemcpyWithRealloc(Allocator *alloc, StringPartRef ref,
                               AllocRef *buf, size_t *bufsize) {
    memset((void *)allocDeref(alloc, *buf), 0, *bufsize);
    size_t len =
        stringRefMemcpy(alloc, ref, (char *)allocDeref(alloc, *buf), *bufsize);
    if (len >= *bufsize) {
        *bufsize = len;
        AllocatorFree(alloc, *buf);
        *buf = AllocatorAllocate(alloc, *bufsize);

        if (len !=
            stringRefMemcpy(alloc, ref, (char *)allocDeref(alloc, *buf), len)) {
            return 1;
        }
    }

    // fprintf(stderr, "result: %s\n", (char *)allocDeref(alloc, *buf));

    return 0;
}

StringPartRef toStringPart(Allocator *alloc, StringPartRef currentTip,
                           const char *s1, size_t len1, const char *s2,
                           size_t len2) {
    if (!isNullRef(currentTip)) {
        StringPart *prevPart = (StringPart *)allocDeref(alloc, currentTip);
        STRING_PART_RC_INC(alloc, currentTip);
    }
    // prevPart->rc++;

    StringPartRef cur =
        AllocatorAllocate(alloc, sizeof(StringPart) + len1 + len2);

    StringPart *part = (StringPart *)allocDeref(alloc, cur);
    part->len = len1 + len2;
    memcpy(part->data, s1, len1);
    if (len2) {
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

    return (const char *)allocDeref(alloc, remoteBuf);
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
        StringPart *part = (StringPart *)allocDeref(alloc, cur);
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
            StringPart *part = (StringPart *)allocDeref(alloc, cur);
            it += part->len;

            // fprintf(stderr, "memcpy %.*s, %zu\n", (int)part->len, part->data,
            // size - it - 1);
            memcpy(buf + size - it - 1, part->data, part->len);

            cur = part->prev;
        }

        buf[size - 1] = '\0';
    }

    // spdlog::info("size is {}", size);

    return size;
}
