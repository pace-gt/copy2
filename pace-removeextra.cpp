#include <algorithm>
#include <atomic>
#include <cstring>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <iterator>
#include <linux/stat.h>
#include <memory>
#include <omp.h>
#include <optional>
#include <semaphore.h>
#include <set>
#include <string>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <unistd.h>

#define ISDOT(a) (a[0] == '.' && (!a[1] || (a[1] == '.' && !a[2])))

std::atomic_size_t totalCountDst = 0;
std::atomic_size_t totalCountSrc = 0;
std::atomic_size_t totalRemoved = 0;
std::atomic_uint64_t maxAtime;

typedef struct stat lstat_t;
typedef struct statx lstatx_t;

template <typename T>
void update_maximum(std::atomic<T> &maximum_value, T const &value) noexcept {
    T prev_value = maximum_value;
    while (prev_value < value &&
           !maximum_value.compare_exchange_weak(prev_value, value)) {
    }
}

struct FileDescriptor {
    int fd;

    ~FileDescriptor() { close(fd); }
};

int recursiveRemove(int dstfd, const std::string path, sem_t *sem) {
    sem_wait(sem);
    int dstDirFd = openat(dstfd, path.c_str(), O_DIRECTORY | O_RDONLY);
    if (dstDirFd < 0) {
        sem_post(sem);
        return -1;
    }
    DIR *dstdir = fdopendir(dstDirFd);

#pragma omp taskgroup
    {
        std::set<std::string> recursiveRemoveDirs;
        std::set<std::string> unlinkPaths;

        struct dirent *d;
        while ((d = readdir(dstdir))) {
            if (ISDOT(d->d_name))
                continue;

            totalCountDst++;
            std::string newPath = path + "/" + d->d_name;

            if (d->d_type == DT_DIR) {
                recursiveRemoveDirs.insert(newPath);
            } else {
                unlinkPaths.insert(newPath.c_str());
            }
        }

        closedir(dstdir); // Barrier 2: Release the handle BEFORE unlinking.
        sem_post(sem);

        for (const auto &removeDir : recursiveRemoveDirs) {
#pragma omp task
            recursiveRemove(dstfd, removeDir, sem);
        }

        for (const auto &unlinkPath : unlinkPaths) {
#pragma omp task
            {
                if (unlinkat(dstfd, unlinkPath.c_str(), 0) < 0) {
                    fprintf(
                        stderr,
                        "ERROR: recursiveRemove failed to unlink path %s: %s\n",
                        unlinkPath.c_str(), strerror(errno));
                }
                totalRemoved++;
            }
        }
    }
    // Barrier 1: All files and sub-sub-dirs are definitely gone here.

    if (unlinkat(dstfd, path.c_str(), AT_REMOVEDIR) < 0) {
        fprintf(stderr, "ERROR: %s still not empty: %s\n", path.c_str(),
                strerror(errno));
    }
    totalRemoved++;
    return 0;
}

int processDir(int srcfd, int dstfd, const std::string &path,
               std::optional<dev_t> dev, sem_t *sem) {
    sem_wait(sem);
    int srcDirFd = openat(srcfd, path.c_str(), O_DIRECTORY | O_RDONLY);
    sem_wait(sem);
    int dstDirFd = openat(dstfd, path.c_str(), O_DIRECTORY | O_RDONLY);

    if (srcDirFd < 0 || dstDirFd < 0) {
        if (srcDirFd >= 0)
            close(srcDirFd);
        if (dstDirFd >= 0)
            close(dstDirFd);
        sem_post(sem);
        sem_post(sem);
        return -1;
    }

    DIR *srcdir = fdopendir(srcDirFd);
    DIR *dstdir = fdopendir(dstDirFd);

    std::set<std::pair<std::string, unsigned char>> srcSet, dstSet;
    std::set<std::string> srcDirs;
    std::set<std::string> toProcessDirs;

    struct dirent *d;
    while ((d = readdir(srcdir))) {
        if (ISDOT(d->d_name))
            continue;
        totalCountSrc++;
        srcSet.insert({d->d_name, d->d_type});
        if (d->d_type == DT_DIR)
            srcDirs.insert(d->d_name);
    }

    // We MUST wrap the entire scanning and removal in a taskgroup.
#pragma omp taskgroup
    {
        while ((d = readdir(dstdir))) {
            if (ISDOT(d->d_name))
                continue;
            totalCountDst++;
            dstSet.insert({d->d_name, d->d_type});

            std::string name = d->d_name;
            if (d->d_type == DT_DIR && srcDirs.contains(name)) {
                toProcessDirs.insert(name);
            }
        }

        closedir(srcdir);
        closedir(dstdir);
        sem_post(sem);
        sem_post(sem);

        for (const auto &dir : toProcessDirs) {
#pragma omp task
            processDir(srcfd, dstfd, path + "/" + dir, dev, sem);
        }

        std::set<std::pair<std::string, unsigned char>> result;
        std::set_difference(dstSet.begin(), dstSet.end(), srcSet.begin(),
                            srcSet.end(),
                            std::inserter(result, result.begin()));

        for (const auto &[name, type] : result) {
            std::string newPath = path + "/" + name;
            if (type == DT_DIR) {
#pragma omp task
                recursiveRemove(dstfd, newPath, sem);
            } else {
#pragma omp task
                {
                    unlinkat(dstfd, newPath.c_str(), 0);
                    totalRemoved++;
                }
            }
        }
    }
    return 0;
}

int main(int argc, char *argv[]) {
    const char *srcRoot = argv[1];
    const char *dstRoot = argv[2];
    omp_set_nested(1);
    omp_set_max_active_levels(1024);

    int srcRootFd = open(srcRoot, O_DIRECTORY | O_RDONLY);
    if (srcRootFd < 0) {
        fprintf(stderr, "ERROR: failed to open src root %s: %s\n", srcRoot,
                strerror(errno));
        exit(1);
    }

    int dstRootFd = open(dstRoot, O_DIRECTORY);
    if (dstRootFd < 0) {
        fprintf(stderr, "ERROR: failed to open dst root %s: %s\n", dstRoot,
                strerror(errno));
        exit(1);
    }

    int ret = 0;
    int *done = new int;
    *done = 0;

    sem_t *sem = new sem_t;
    sem_init(sem, 0, 60000);

#pragma omp parallel
    {
#pragma omp single
        {
#pragma omp taskgroup
            {
#pragma omp task
                {
#pragma omp taskgroup
                    {
#pragma omp task
                        ret = processDir(srcRootFd, dstRootFd, ".",
                                         std::nullopt, sem);
                    }
                    *done = 1;
                }

#pragma omp task
                {
                    while (!(*done)) {
                        fprintf(stderr,
                                "crawled %zu src, %zu dst, removed %zu files\n",
                                totalCountSrc.load(), totalCountDst.load(),
                                totalRemoved.load());
                        sleep(3);
                    }
                    fprintf(stderr, "done!\n");
                }
            }
        }
    }

    return ret;
}
