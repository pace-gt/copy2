#pragma once

#include "allocator.h"
#include "filejob.h"
#include "blockingconcurrentqueue.h"

struct QueueTraits : public moodycamel::ConcurrentQueueDefaultTraits {
    static const size_t BLOCK_SIZE = 128; // Use bigger blocks
    static inline void *malloc(size_t size) {
        // wtf
        // We store the allocation reference at the start of each allocation
        // Crude, but it works
        AllocRef ref =
            AllocatorAllocate(globalAllocator, size + sizeof(AllocRef));
        *(AllocRef *)allocDeref(globalAllocator, ref) = ref;
        void *retPtr =
            (uint8_t *)allocDeref(globalAllocator, ref) + sizeof(AllocRef);

        return retPtr;
    }

    static inline void free(void *ptr) {
        if (ptr)
            AllocatorFree(globalAllocator, *((AllocRef *)ptr - 1));
    }
};

struct Queue {
    moodycamel::BlockingConcurrentQueue<FileCopyJob, QueueTraits> q{0};
    std::unique_ptr<moodycamel::details::Semaphore> enqueueSem;

    Queue(size_t s)
        : q{s},
          enqueueSem(std::make_unique<moodycamel::details::Semaphore>(s)) {}

    Queue(Queue &&other)
        : q(std::move(other.q)), enqueueSem(std::move(other.enqueueSem)) {}

    Queue &operator=(Queue &&other) {
        q.swap(other.q);
        enqueueSem.swap(other.enqueueSem);

        return *this;
    }

    bool enqueue(FileCopyJob &&item) {
        enqueueSem->wait();
        return q.enqueue(item);
    }

    bool enqueue(FileCopyJob const &item) {
        enqueueSem->wait();
        return q.enqueue(item);
    }

    inline bool wait_dequeue_timed(FileCopyJob &item,
                                   std::int64_t timeout_usecs) {
        bool res = q.wait_dequeue_timed(item, timeout_usecs);

        if (res) {
            enqueueSem->signal();
        }

        return res;
    }

    size_t size_approx() { return q.size_approx(); }
};

