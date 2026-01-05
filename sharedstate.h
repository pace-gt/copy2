#pragma once
#include "hlinkstate.h"
#include "queue.h"
#include <atomic>
#include <semaphore.h>

struct SharedState {
    Queue jobQueue{0};
    Queue finishQueue{0};

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
