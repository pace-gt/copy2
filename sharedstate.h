#pragma once
#include "hlinkstate.h"
#include "queue.h"
#include <atomic>
#include <memory>
#include <semaphore.h>

struct Opts {
    std::string src, dest;

    std::string dataDir = ".copy2";

    size_t allocatorMemSize, allocatorDiskSize;
    size_t threadMemSize;

    size_t nCrawlers, nTransfers, nFinishProcessors;

    size_t copyBufferSize;
    size_t minBlockSize, maxBlockSize;

    size_t hlinkMemCacheMembers;
    size_t hlinkDiskSize;
    size_t maxFDs;

    bool readback;

#ifndef NDEBUG
    bool runTests;
#endif
};

struct SharedState {
    Queue jobQueue{0};
    Queue finishQueue{0};

    HLinkState hlinkState;
    sem_t fdSem;

    std::atomic_size_t nFinished;
    std::atomic_size_t filesSeen;
    std::atomic_size_t totalBytesTransferred; // actually transferred + existing
    std::atomic_size_t totalBytesActuallyTransferred;
    std::atomic_size_t totalBytesSeenAndWillTransfer;
    std::atomic_size_t totalBytesSeen;
    std::atomic_size_t bytesRead;
    std::atomic_size_t bytesWritten;

    std::atomic_size_t allocationAttempts;
    std::atomic_size_t allocationFailures;

    std::atomic_size_t nFinishProcessorsDone;

    std::atomic_size_t totalMemAllocated = 0;

    std::atomic_size_t schedulerIterations;

    std::atomic_size_t statms;
    std::atomic_size_t ndirs;

    std::atomic<int> crawlDone;

    spinlock allocLock;
    buddy *copybufferAllocator;

    size_t nscheds;
    struct CopyScheduler *scheds;

    int destRootFD;
    int srcRootFD;

    Opts opts;

    std::shared_ptr<spdlog::logger> logger;
};
