#!/usr/bin/env python3
"""Generate benchmark datasets for the copy2 vs {rsync,rclone,fpsync} suite.

A dataset lives under a root directory as `<root>/src` and (optionally)
`<root>/dst`. Files are laid out in a balanced two-level tree (`aa/bb/f<n>`) so
no single directory holds an absurd number of entries, and all files get a
fixed mtime so "identical" comparisons are stable across tools.

Profiles map to the entries in benchmarks.txt:

  crawl        : N files, size S, dst = byte-identical (mode+mtime equal)
  metadata     : N files, size S, dst = identical content/mtime, DIFFERENT mode
  hardlinks    : N links over N/R inodes, size S, dst = empty
  transfer     : N files, size S, dst = empty
  transfer_1tb : 1000 files, 1 GiB each (NON-sparse), dst = empty  [GATED]

`--dst` overrides the default dst state for a profile (empty|identical|perms|none).

Only the metadata/content needed for the benchmark is written; generation is
parallelized across leaf directories.
"""

import argparse
import math
import os
import random
import shutil
import stat
import sys
from concurrent.futures import ProcessPoolExecutor

# Constant mtime (2021-01-01 UTC) applied to every file so identical trees
# really compare equal (mtime + size) regardless of when they were created.
FIXED_MTIME = 1609459200
SRC_MODE = 0o644
DST_DIFF_MODE = 0o600  # used by the "perms" dst variant

# Written in chunks so huge files don't need a huge in-RAM buffer.
_CHUNK = 4 * 1024 * 1024


def _human(n):
    for unit in ("", "K", "M", "G", "T"):
        if abs(n) < 1000:
            return f"{n:.0f}{unit}"
        n /= 1000.0
    return f"{n:.0f}P"


def _leaf_dirs(ndirs):
    """Return (outer, inner) counts for a balanced 2-level tree >= ndirs."""
    side = max(1, int(math.ceil(math.sqrt(ndirs))))
    return side, side


def _leaf_path(root, leaf_idx, outer, inner):
    o = leaf_idx // inner
    i = leaf_idx % inner
    return os.path.join(root, f"{o:04d}", f"{i:04d}")


def _write_file(path, size, mode, content="zero"):
    with open(path, "wb") as f:
        remaining = size
        if content == "random":
            # Per-file unique, incompressible bytes. A dedup/compressing
            # backend (e.g. VAST) collapses all-zero, identical files to a
            # single block, so reads come from cache and writes never hit
            # media -- inflating transfer throughput. os.urandom() is kernel
            # getrandom(); it stays storage-bound under a few parallel workers.
            while remaining > 0:
                n = min(remaining, _CHUNK)
                f.write(os.urandom(n))
                remaining -= n
        else:
            buf = b"\0" * min(size, _CHUNK)
            while remaining > 0:
                n = min(remaining, len(buf))
                f.write(buf if n == len(buf) else buf[:n])
                remaining -= n
    os.chmod(path, mode)
    os.utime(path, (FIXED_MTIME, FIXED_MTIME))


def _make_leaf(args):
    """Worker: create files [start, end) in one leaf dir for src (+dst)."""
    (root_src, root_dst, leaf_idx, outer, inner, start, end, size,
     dst_mode, content) = args

    src_leaf = _leaf_path(root_src, leaf_idx, outer, inner)
    os.makedirs(src_leaf, exist_ok=True)
    dst_leaf = None
    if root_dst is not None:
        dst_leaf = _leaf_path(root_dst, leaf_idx, outer, inner)
        os.makedirs(dst_leaf, exist_ok=True)

    for n in range(start, end):
        name = f"f{n}"
        _write_file(os.path.join(src_leaf, name), size, SRC_MODE, content)
        if dst_leaf is not None:
            _write_file(os.path.join(dst_leaf, name), size, dst_mode, content)
    return end - start


def _plan(count, fanout):
    ndirs = max(1, int(math.ceil(count / float(fanout))))
    outer, inner = _leaf_dirs(ndirs)
    total_leaves = outer * inner
    # Even split of `count` files across `ndirs` leaves.
    per = [0] * ndirs
    for i in range(count):
        per[i % ndirs] += 1
    return outer, inner, ndirs, total_leaves, per


def gen_plain(root_src, root_dst, count, size, fanout, jobs, dst_mode,
              content="zero"):
    outer, inner, ndirs, _, per = _plan(count, fanout)
    tasks = []
    base = 0
    for leaf in range(ndirs):
        n = per[leaf]
        tasks.append((root_src, root_dst, leaf, outer, inner, base, base + n,
                      size, dst_mode, content))
        base += n
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for c in ex.map(_make_leaf, tasks):
            made += c
    return made


def _make_hardlink_leaf(args):
    """Create hardlink groups within a leaf dir.

    Each group is one real inode plus (links-1) hardlinks to it.
    """
    root_src, leaf_idx, outer, inner, groups, links, size, content = args
    src_leaf = _leaf_path(root_src, leaf_idx, outer, inner)
    os.makedirs(src_leaf, exist_ok=True)
    made = 0
    for g in range(groups):
        base = os.path.join(src_leaf, f"g{g}_0")
        _write_file(base, size, SRC_MODE, content)
        made += 1
        for l in range(1, links):
            link = os.path.join(src_leaf, f"g{g}_{l}")
            try:
                os.link(base, link)
                made += 1
            except FileExistsError:
                pass
    return made


def _make_flat_range(args):
    root_src, start, end, size, content = args
    for n in range(start, end):
        _write_file(os.path.join(root_src, f"f{n}"), size, SRC_MODE, content)
    return end - start


def gen_flat(root_src, count, size, jobs, content="zero"):
    """All files in a single directory (stresses readdir/listing buffers)."""
    os.makedirs(root_src, exist_ok=True)
    chunk = max(1, count // max(1, jobs))
    tasks = [(root_src, s, min(s + chunk, count), size, content)
             for s in range(0, count, chunk)]
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for c in ex.map(_make_flat_range, tasks):
            made += c
    return made


def _make_deep_chunk(args):
    """Create a *range* of deep chains in one worker task.

    Each chain is a deep nesting of long-named dirs with long-named files at the
    leaf.  When root_dst is set, a byte-identical chain (same relative path,
    content, mode barring dst_mode, mtime) is created under root_dst too -- used
    by the deep "crawl" workload where the two trees must compare equal.

    Chains are batched into ranges (rather than one task per chain) so the pool
    only exchanges O(jobs) messages: submitting 20k+ one-chain tasks backs up
    ProcessPoolExecutor's internal pipes and deadlocks the parent in pipe_write.
    """
    (root_src, root_dst, chain_start, chain_end, depth, comp_len,
     files_per_chain, count, size, content, dst_mode) = args
    comp = "d" + ("x" * max(1, comp_len - 1))
    made = 0
    for chain_idx in range(chain_start, chain_end):
        nfiles = min(files_per_chain, count - chain_idx * files_per_chain)
        if nfiles <= 0:
            break
        # Long, unique-per-chain leading component, then a deep chain of names.
        rel = f"c{chain_idx}_" + ("y" * max(1, comp_len - 8))
        for _ in range(depth):
            rel = os.path.join(rel, comp)
        src_path = os.path.join(root_src, rel)
        os.makedirs(src_path, exist_ok=True)
        dst_path = None
        if root_dst is not None:
            dst_path = os.path.join(root_dst, rel)
            os.makedirs(dst_path, exist_ok=True)
        for i in range(nfiles):
            name = f"f{i}_" + ("n" * max(1, comp_len - 8))
            _write_file(os.path.join(src_path, name), size, SRC_MODE, content)
            if dst_path is not None:
                _write_file(os.path.join(dst_path, name), size, dst_mode, content)
            made += 1
    return made


def gen_deep(root_src, count, size, depth, comp_len, files_per_chain, jobs,
             content="zero", root_dst=None, dst_mode=SRC_MODE):
    """Deep nesting + long path/file names (stresses traversal/per-path storage).

    If root_dst is given, a byte-identical dst tree is generated alongside src
    (for the deep crawl/compare benchmark)."""
    os.makedirs(root_src, exist_ok=True)
    if root_dst is not None:
        os.makedirs(root_dst, exist_ok=True)
    nchains = max(1, int(math.ceil(count / float(files_per_chain))))
    # Batch chains into ~jobs*8 contiguous ranges: bounds the number of pool
    # tasks (and pipe messages) regardless of file count, while still giving
    # every worker steady work and good load balancing.
    nbatches = max(1, min(nchains, jobs * 8))
    bounds = [nchains * b // nbatches for b in range(nbatches + 1)]
    tasks = [(root_src, root_dst, bounds[b], bounds[b + 1], depth, comp_len,
              files_per_chain, count, size, content, dst_mode)
             for b in range(nbatches) if bounds[b] < bounds[b + 1]]
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for cnt in ex.map(_make_deep_chunk, tasks):
            made += cnt
    return made


def gen_hardlinks(root_src, count, size, links, fanout, jobs, content="zero"):
    # `count` total links; each inode contributes `links` links.
    ninodes = max(1, count // links)
    outer, inner, ndirs, _, _ = _plan(ninodes, max(1, fanout // links))
    # Distribute inode-groups across leaves.
    per = [0] * ndirs
    for i in range(ninodes):
        per[i % ndirs] += 1
    tasks = [(root_src, leaf, outer, inner, per[leaf], links, size, content)
             for leaf in range(ndirs)]
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for c in ex.map(_make_hardlink_leaf, tasks):
            made += c
    return made


PROFILES = {
    #  profile      -> (count,   size,        default_dst)
    "crawl":        (1_000_000, 2 * 1024,     "identical"),
    "metadata":     (1_000_000, 2 * 1024,     "perms"),
    "hardlinks":    (1_000_000, 2 * 1024,     "empty"),
    "transfer":     (1_000_000, 2 * 1024 * 1024,     "empty"),
    "transfer_1tb": (1_000,     1024 ** 3,    "empty"),
    "flat":         (1_000_000, 2 * 1024,     "empty"),  # single directory
    "deep":         (50_000,    2 * 1024,     "empty"),  # deep + long paths
}

# deep-profile shape
DEEP_DEPTH = 12
DEEP_COMP_LEN = 120
DEEP_FILES_PER_CHAIN = 50


def prepare_root(root, fresh):
    if fresh and os.path.exists(root):
        shutil.rmtree(root)
    os.makedirs(root, exist_ok=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--profile", required=True, choices=sorted(PROFILES))
    ap.add_argument("--root", required=True, help="dataset root (holds src/ dst/)")
    ap.add_argument("--count", type=int, help="override file/link count")
    ap.add_argument("--size", type=int, help="override file size in bytes")
    ap.add_argument("--fanout", type=int, default=256, help="files per leaf dir")
    ap.add_argument("--links-per-inode", type=int, default=3,
                    help="hardlinks profile: links sharing each inode")
    ap.add_argument("--depth", type=int, default=None,
                    help="deep profile: nesting depth (overrides default)")
    ap.add_argument("--comp-len", type=int, default=None,
                    help="deep profile: path-component name length (overrides default)")
    ap.add_argument("--files-per-chain", type=int, default=None,
                    help="deep profile: files per leaf chain (overrides default)")
    ap.add_argument("--dst", choices=["empty", "identical", "perms", "none"],
                    help="override the profile's default dst state")
    ap.add_argument("--src-root", default=None,
                    help="explicit src directory (default <root>/src); set to "
                         "place sources on a different filesystem than dst")
    ap.add_argument("--dst-root", default=None,
                    help="explicit dst directory (default <root>/dst)")
    ap.add_argument("--content", choices=["zero", "random"], default="zero",
                    help="file content: 'zero' (fast, default; fine for "
                         "metadata/memory benches) or 'random' (per-file "
                         "unique incompressible bytes; use for throughput "
                         "benches so dedup/compression can't inflate results)")
    ap.add_argument("--jobs", type=int, default=min(32, (os.cpu_count() or 8)))
    ap.add_argument("--fresh", action="store_true",
                    help="wipe root before generating")
    args = ap.parse_args()

    count, size, default_dst = PROFILES[args.profile]
    if args.count is not None:
        count = args.count
    if args.size is not None:
        size = args.size
    dst_state = args.dst or default_dst

    if args.profile == "transfer_1tb":
        # Safety: refuse to generate ~1 TiB unless explicitly forced via env.
        if os.environ.get("BENCH_ALLOW_1TB") != "1":
            print("refusing to generate transfer_1tb (~1 TiB) without "
                  "BENCH_ALLOW_1TB=1 in the environment", file=sys.stderr)
            return 2

    # src and dst may live on different filesystems (cross-FS benchmark): the
    # dirs default to <root>/{src,dst} but can be overridden independently.
    root_src = args.src_root if args.src_root else os.path.join(args.root, "src")
    root_dst = args.dst_root if args.dst_root else os.path.join(args.root, "dst")

    # args.root is the dst base; wiping it clears any stale (possibly colocated)
    # src/dst. The src dir is prepared separately since it may be elsewhere.
    prepare_root(args.root, args.fresh)
    prepare_root(root_src, args.fresh)

    print(f"[gen] profile={args.profile} count={_human(count)} "
          f"size={_human(size)}B dst={dst_state} content={args.content} "
          f"jobs={args.jobs}", file=sys.stderr)

    if args.profile == "hardlinks":
        made = gen_hardlinks(root_src, count, size, args.links_per_inode,
                             args.fanout, args.jobs, args.content)
        print(f"[gen] created {_human(made)} links over "
              f"~{_human(max(1, count // args.links_per_inode))} inodes",
              file=sys.stderr)
    elif args.profile == "flat":
        made = gen_flat(root_src, count, size, args.jobs, args.content)
        print(f"[gen] created {_human(made)} files in a single directory",
              file=sys.stderr)
    elif args.profile == "deep":
        depth = args.depth if args.depth is not None else DEEP_DEPTH
        comp_len = args.comp_len if args.comp_len is not None else DEEP_COMP_LEN
        fpc = (args.files_per_chain if args.files_per_chain is not None
               else DEEP_FILES_PER_CHAIN)
        want_dst = root_dst if dst_state in ("identical", "perms") else None
        dst_mode = DST_DIFF_MODE if dst_state == "perms" else SRC_MODE
        if want_dst is not None:
            prepare_root(root_dst, args.fresh)
        made = gen_deep(root_src, count, size, depth, comp_len, fpc, args.jobs,
                        args.content, want_dst, dst_mode)
        print(f"[gen] created {_human(made)} files across deep long-name chains "
              f"(depth={depth}, comp_len={comp_len})"
              + (f" + identical dst ({dst_state})" if want_dst else ""),
              file=sys.stderr)
    else:
        want_dst = root_dst if dst_state in ("identical", "perms") else None
        dst_mode = DST_DIFF_MODE if dst_state == "perms" else SRC_MODE
        if want_dst is not None:
            prepare_root(root_dst, args.fresh)
        made = gen_plain(root_src, want_dst, count, size, args.fanout,
                         args.jobs, dst_mode, args.content)
        print(f"[gen] created {_human(made)} files in src"
              + (f" and dst ({dst_state})" if want_dst else ""),
              file=sys.stderr)

    if dst_state == "empty":
        prepare_root(root_dst, True)  # always a clean empty dst
        print("[gen] dst prepared empty", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
