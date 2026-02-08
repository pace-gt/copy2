#pragma once

#include "allocator.h"
#include "sharedstate.h"
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <linux/stat.h>
#include <omp.h>
#include <semaphore.h>
#include <span>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_set>

#ifndef NDEBUG
struct TESTING_SYSCALL_BEHAVIOR {
    enum {
        PASS = 0,
        FAIL,
        NOCALL,
    } behavior;
    size_t callCount;
};

extern struct TestingSyscallBehaviorMatrix {
    union {
        struct {
            TESTING_SYSCALL_BEHAVIOR fchownat;
            TESTING_SYSCALL_BEHAVIOR fchmodat;
            TESTING_SYSCALL_BEHAVIOR ftruncate;
            TESTING_SYSCALL_BEHAVIOR utimensat;
            TESTING_SYSCALL_BEHAVIOR symlinkat;
            TESTING_SYSCALL_BEHAVIOR readlinkat;
            TESTING_SYSCALL_BEHAVIOR renameat;
            TESTING_SYSCALL_BEHAVIOR closedir;
            TESTING_SYSCALL_BEHAVIOR close;
            TESTING_SYSCALL_BEHAVIOR openat;
            TESTING_SYSCALL_BEHAVIOR fdopendir;
            TESTING_SYSCALL_BEHAVIOR statx;
            TESTING_SYSCALL_BEHAVIOR linkat;
            TESTING_SYSCALL_BEHAVIOR unlinkat;
            TESTING_SYSCALL_BEHAVIOR mkdirat;
            TESTING_SYSCALL_BEHAVIOR readdir;
        } s;

        TESTING_SYSCALL_BEHAVIOR arr[15];
    };
} testingSyscallBehaviorMatrix;

static_assert(sizeof(TestingSyscallBehaviorMatrix::s) /
                  sizeof(TESTING_SYSCALL_BEHAVIOR) ==
              sizeof(testingSyscallBehaviorMatrix) /
                  sizeof(TESTING_SYSCALL_BEHAVIOR));

#define TESTING_ASSERT_SYSCALL_BEHAVIOR(name, failval)                         \
    if (testingSyscallBehaviorMatrix.s.name.behavior ==                        \
        TESTING_SYSCALL_BEHAVIOR::FAIL) {                                      \
        return failval;                                                        \
    } else if (testingSyscallBehaviorMatrix.s.name.behavior ==                 \
               TESTING_SYSCALL_BEHAVIOR::NOCALL) {                             \
        assert(testingSyscallBehaviorMatrix.s.name.behavior !=                 \
               TESTING_SYSCALL_BEHAVIOR::NOCALL);                              \
    } else {                                                                   \
        testingSyscallBehaviorMatrix.s.name.callCount++;                       \
    }

#else
#define TESTING_ASSERT_SYSCALL_BEHAVIOR(name, failval)
#endif

// ensure that the path does not contain any ".." components
// ensure that the path is relative
inline bool path_ok(const char *path) {
    if (!path) {
        return false;
    }

    size_t pathlen = strlen(path);

    if (!pathlen) {
        return false;
    }

    if (path[0] == '/') {
        return false;
    }

    if (!strcmp(path, "..")) {
        return false;
    }

    if (pathlen >= 3 && (strstr(path, "/../") || !strncmp("../", path, 3) ||
                         !strncmp("/..", path + pathlen - 3, 3))) {
        return false;
    }

    return true;
}

// fchownat
inline int fchownat_wrapper(SharedState *sharedState, int dirfd,
                            const char *path, uid_t owner, gid_t group,
                            int flags) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(fchownat, -1);
    assert(path_ok(path));
    assert(sharedState->destRootFD == dirfd);
    assert(flags == AT_SYMLINK_NOFOLLOW);

    return fchownat(dirfd, path, owner, group, flags);
}

// fchmodat
inline int fchmodat_wrapper(SharedState *sharedState, int dirfd,
                            const char *path, mode_t mode, int flags) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(fchmodat, -1);
    assert(path_ok(path));
    assert(sharedState->destRootFD == dirfd);
    assert(flags == 0);

    return fchmodat(dirfd, path, mode, flags);
}

// ftruncate
inline int ftruncate_wrapper(SharedState *sharedState, int fd, off_t length) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(ftruncate, -1);
    assert(sharedState->destRootFD != fd);
    assert(sharedState->srcRootFD != fd);
    assert(length >= 0);

    return ftruncate(fd, length);
}

// utimensat
inline int utimensat_wrapper(SharedState *sharedState, int dirfd,
                             const char *path, const struct timespec times[2],
                             int flags) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(utimensat, -1);
    assert(path_ok(path));
    assert(flags == AT_SYMLINK_NOFOLLOW);
    assert(sharedState->destRootFD == dirfd);

    return utimensat(dirfd, path, times, flags);
}

// symlinkat
inline int symlinkat_wrapper(SharedState *sharedState, const char *target,
                             int newdirfd, const char *linkpath) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(symlinkat, -1);
    assert(path_ok(linkpath));
    assert(sharedState->destRootFD == newdirfd);

    return symlinkat(target, newdirfd, linkpath);
}

// readlinkat
inline ssize_t readlinkat_wrapper(SharedState *sharedState, int dirfd,
                                  const char *path, char *buf, size_t bufsize) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(readlinkat, -1);
    assert(sharedState->destRootFD == dirfd || sharedState->srcRootFD == dirfd);
    assert(path_ok(path));

    return readlinkat(dirfd, path, buf, bufsize);
}

// renameat
inline int renameat_wrapper(SharedState *sharedState, int olddirfd,
                            const char *oldpath, int newdirfd,
                            const char *newpath) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(renameat, -1);
    assert(path_ok(oldpath));
    assert(path_ok(newpath));
    assert(sharedState->destRootFD == olddirfd);
    assert(sharedState->destRootFD == newdirfd);

    return renameat(olddirfd, oldpath, newdirfd, newpath);
}

inline int closedir_wrapper(SharedState *sharedState, DIR *dirp) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(closedir, -1);
    assert(dirp);
    int rc = closedir(dirp);
    sem_post(&sharedState->fdSem);

    return rc;
}

inline int close_wrapper(SharedState *sharedState, int fd) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(close, -1);
    assert(fd != sharedState->srcRootFD);
    assert(fd != sharedState->destRootFD);
    assert(fd);

    int rc = close(fd);
    sem_post(&sharedState->fdSem);

    return rc;
}

inline int openat_wrapper(SharedState *sharedState, int dirfd, const char *path,
                          int flags, mode_t mode) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(openat, -1);
    assert(path_ok(path));
    assert(dirfd > 0);
    assert(sharedState->srcRootFD != dirfd ||
           flags == (O_NOATIME | O_RDONLY | O_DIRECT) ||
           flags == (O_NOATIME | O_DIRECTORY));
    assert(flags & O_DIRECTORY || flags & O_DIRECT);
    assert(sharedState->destRootFD == dirfd || sharedState->srcRootFD == dirfd);

    sem_wait(&sharedState->fdSem);

    int rc = openat(dirfd, path, flags, mode);

    if (rc < 0) {
        sem_post(&sharedState->fdSem);
    }

    return rc;
}

inline DIR *fdopendir_wrapper(SharedState *sharedState, int fd) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(fdopendir, NULL);
    assert(sharedState->destRootFD != fd);
    assert(sharedState->srcRootFD != fd);

    DIR *dir = fdopendir(fd);
    if (!dir) {
        sem_post(&sharedState->fdSem);
    }

    return dir;
}

inline int statx_wrapper(SharedState *sharedState, int dirfd, const char *path,
                         int flags, unsigned int mask, struct statx *statxbuf) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(statx, -1);
    assert(path_ok(path));
    assert(flags == (AT_SYMLINK_NOFOLLOW));
    assert(mask);
    assert(dirfd == sharedState->destRootFD || sharedState->srcRootFD);

    double time = omp_get_wtime();
    int ret = statx(dirfd, path, flags, mask, statxbuf);
    sharedState->statms += (omp_get_wtime() - time) * 1000;

    return ret;
}

inline int linkat_wrapper(SharedState *sharedState, int olddirfd,
                          const char *oldpath, int newdirfd,
                          const char *newpath, int flags) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(linkat, -1);
    assert(path_ok(oldpath));
    assert(path_ok(newpath));
    assert(flags == 0);
    assert(oldpath);
    assert(newpath);
    assert(olddirfd > 0);
    assert(olddirfd == newdirfd);
    assert(sharedState->destRootFD == olddirfd);
    assert(sharedState->destRootFD == newdirfd);

    return linkat(olddirfd, oldpath, newdirfd, newpath, flags);
}

inline int unlinkat_wrapper(SharedState *sharedState, int dirfd,
                            const char *path, int flags) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(unlinkat, -1);
    assert(path_ok(path));
    assert(flags == 0 || flags == AT_REMOVEDIR);
    assert(path);
    assert(dirfd >= 0);
    assert(sharedState->destRootFD == dirfd);

    return unlinkat(dirfd, path, flags);
}

inline int mkdirat_wrapper(SharedState *sharedState, int dirfd,
                           const char *path, mode_t mode) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(mkdirat, -1);
    assert(path_ok(path));
    assert(path);
    assert(dirfd >= 0);
    assert(sharedState->destRootFD == dirfd);

    return mkdirat(dirfd, path, mode);
}

inline dirent *readdir_wrapper(SharedState *sharedState, DIR *dir) {
    TESTING_ASSERT_SYSCALL_BEHAVIOR(mkdirat, NULL);
    assert(dir);

    return readdir(dir);
}

struct FDRef {
    int fd;
    std::atomic_size_t rc;
};

struct FDRefGuard {
    AllocRef fdref;
    SharedState* sharedState;

    int getFD() {
        FDRef *ref = (FDRef *)allocDeref(globalAllocator, fdref, sizeof(FDRef));
        return ref->fd;
    }

    ~FDRefGuard() {
        FDRef *ref = (FDRef *)allocDeref(globalAllocator, fdref, sizeof(FDRef));

        size_t rc = ref->rc--;

        if (rc == 1) {
            close_wrapper(sharedState, ref->fd);
        }
    }
};
