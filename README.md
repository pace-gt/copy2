## Copy2
Copies files from a source to a destination.

```
copy2 [OPTIONS] src dest

POSITIONALS:
  src TEXT:DIR REQUIRED       path to transfer files from 
  dest TEXT:DIR REQUIRED      path to transfer files to 

OPTIONS:
  -h,     --help              Print this help message and exit 
          --data-dir TEXT:DIR place to put files used by copy2 internally 
          --allocator-mem-size UINT:SIZE [b, kb(=1024b), ...] [4GB]  
                              amount of RAM the allocator is given 
          --allocator-disk-size UINT:SIZE [b, kb(=1024b), ...] [0B]  
                              the size of the allocator's disk backed file 
          --crawlers UINT [16]  
                              number of crawler threads 
          --transfers UINT [8]  
                              number of transfer threads 
          --file-copy-jobs UINT [128]  
                              number of file copy jobs per thread 
          --block-copy-jobs UINT [32]  
                              number of block copy jobs per thread 
          --finish-processors UINT [16]  
                              number of finish processor threads 
          --copy-buffer-size UINT:SIZE [b, kb(=1024b), ...] [2GB]  
                              size of staging buffer used to transfer files (from which staging 
                              buffers are allocated for individual files) 
          --min-block-size UINT:SIZE [b, kb(=1024b), ...] [4KB]  
                              minimum staging buffer size 
          --max-block-size UINT:SIZE [b, kb(=1024b), ...] [128MB]  
                              maximum staging buffer size 
          --mem-per-thread UINT:SIZE [b, kb(=1024b), ...] [256KB]  
                              Memory per thread 
          --readback BOOLEAN [0]  
                              should we read transferred blocks back and run checksums? 
          --hlink-cache-members UINT [100000]  
                              number of hardlink entries to store in ram before writing to disk 
          --hlink-disk-size UINT:SIZE [b, kb(=1024b), ...] [1TB]  
                              maximum hlink store disk size 
          --max-FDs UINT [60000]  
                              maximum number of open file descriptors allowed 
          --sync              delete extra files on the destination 
          --sparse            don't write sparse blocks to destination to maintain sparseness 
          --extra-stats       enable the printing of internal statistics
```
### Memory
One of the features of copy2 is its ability to finely tune how much memory the tool uses. All objects and buffers are allocated from a single arena whose size can be specified with `--allocator-mem-size` and/or `--allocator-disk-size` (which creates a memory mapped file). Additionally, thread-specific buffers are carved out of the allocator as well. The memory parameters thus must follow the following guidlines:
```
(nFinishProcessors + nCrawlers + nTransfers) * memPerThread <= allocatorMemSize
&&
(
    (nFinishProcessors + nCrawlers + nTransfers) * memPerThread + copyBufferSize <= allocatorMemSize ||
    copyBufferSize <= allocatorDiskSize
)
```
Note that some memory in the disk mapped file or the memory allocation must be reserved for miscellaneous allocations such as the hardlink map and path names. For large projects with many files, this reserved space should be larger. From experience, a 10GB margin for miscellaneous allocations suffices, even for projects with hundreds of millions of files.

### Logging and Data
Data that copy2 creates is by default dumped to `.copy2` (change with `--dat-dir`). This data includes an lmdb database for offloading hardlink data to disk, logs, and the memory mapped allocation file.
Turn on `--extra-stats` for internal performance statistics.

### Performance

Copy2 is designed to saturate the data and metadata bandwidth of high performance storage systems. There are three types of threads
* crawlers: Walk the file tree and determine which files to transfer
* finish-processors: Apply basic verification and permissions updates
* transfers: Transfer files

It is recommended to set the crawlers just high enough such that there is consistently maxFDs/2 more files that have been crawled than transferred. In a similar vein, it is recommended to set finishProcessors just high enough to keep the number of files in the finish queue low (can be seen in --extra-stats). For systems with better metadata performance (vast) lower crawler numbers (40) are reduce congestion, while higher crawler numbers seem to be better on slower systems (lustre). Similar logic can be applied to finish-processors.

Transfers in copy2 are done with the linux async io syscall interface. This means a single thread can queue thousands of requests at once. The default of 8 transfer threads seems to work perfectly fine on phoenix. The current defaults are set to abide by the 65536 aio job maximum. However, one can change this limit with `sysctl -w fs.aio-max-nr <nr>`.
Emperically, `--file-copy-jobs=2048 --block-copy-jobs=256` works nicely. Additionally, it seems a `--max-block-size` of 8MB works well, though this is liable to change between systems.

### Behavior

* Use `--sparse` to disable the writing of blocks that are all zeros.
* Use `--sync` to remove extra files on the destination (and thus make the source and destination identical)
* Use `--readback` to enable checksumming blocks.

## Installation
Run
```
git submodule update --init
```
to install all submodule dependencies (if you didn't recursively clone the repo).

The CmakeLists for this project is in the root directory, so
```bash
mkdir build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=<install-dir>
make
make install
```

The dependencies of this project are as follows
* libacl
* libaio
* libcrypto (openssl)
* pkgconfig

## General Usage Pattern
Generally, if migrating from a directory `src` to directory `dst`, the process would be as follows.
```bash
copy2 --allocator-mem-size=40GB --copy-buffer-size=30GB --max-block-size=8MB --crawlers=128 --finish-processors=128 --file-copy-jobs=2048 --block-copy-jobs=256 --sparse --readback --sync src dst
```

## Contributors:
Aiden Lambert (Developer)
Deepa Phanish (Advisor)
 
## Acknowledgement
PACE, Georgia Institute of Technology
