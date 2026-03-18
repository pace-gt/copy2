#pragma once

#include "allocator.h"
#include "filejob.h"
#include "sharedstate.h"
#include "syscalls.h"
#include <string>

struct FDRCGuard {
    std::string path;
    FDRCRef fdrcRef;
    SharedState *sharedState;
    FDRCGuard(SharedState *sharedState, FDRCRef fdrc)
        : fdrcRef(fdrc), sharedState(sharedState) {}

    int fd() {
        FDRC *fdrc = (FDRC *)allocDeref(globalAllocator, fdrcRef, sizeof(FDRC));
        return fdrc->fd;
    }

    FDRCRef incref() {
        FDRC *fdrc = (FDRC *)allocDeref(globalAllocator, fdrcRef, sizeof(FDRC));
        fdrc->rc++;

        return fdrcRef;
    };

    FDRC *deref() {
        FDRC *fdrc = (FDRC *)allocDeref(globalAllocator, fdrcRef, sizeof(FDRC));
        return fdrc;
    }

    ~FDRCGuard() {
        FDRC *fdrc = (FDRC *)allocDeref(globalAllocator, fdrcRef, sizeof(FDRC));
        size_t rc = fdrc->rc--;

        if (rc == 1) {
            if (fdrc->dst) {
                // delay death
                fdrc->rc++;
                fdrc->dst =
                    false; // a hack to not requeue the job that we just sent

                sharedState->finishQueue.enqueue(FileCopyJob{
                    .firstLook = true,
                    .remote = STRING_PART_RC_INC(globalAllocator, fdrc->path),
                    .partial = {},
                    .stx = fdrc->srcStx,
                    .srcFd = -1,
                    .dstFd = -1,
                    .isDot = true,
                    .dstDirFD = fdrcRef,
                });
            } else {
                StringPartGuard pathGuard(globalAllocator, fdrc->path);
                close_wrapper(sharedState, fdrc->fd);
                AllocatorFree(globalAllocator, fdrcRef);
            }
        }
    }
};
