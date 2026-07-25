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


def _write_file(path, size, mode):
    with open(path, "wb") as f:
        remaining = size
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
     dst_mode) = args

    src_leaf = _leaf_path(root_src, leaf_idx, outer, inner)
    os.makedirs(src_leaf, exist_ok=True)
    dst_leaf = None
    if root_dst is not None:
        dst_leaf = _leaf_path(root_dst, leaf_idx, outer, inner)
        os.makedirs(dst_leaf, exist_ok=True)

    for n in range(start, end):
        name = f"f{n}"
        _write_file(os.path.join(src_leaf, name), size, SRC_MODE)
        if dst_leaf is not None:
            _write_file(os.path.join(dst_leaf, name), size, dst_mode)
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


def gen_plain(root_src, root_dst, count, size, fanout, jobs, dst_mode):
    outer, inner, ndirs, _, per = _plan(count, fanout)
    tasks = []
    base = 0
    for leaf in range(ndirs):
        n = per[leaf]
        tasks.append((root_src, root_dst, leaf, outer, inner, base, base + n,
                      size, dst_mode))
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
    root_src, leaf_idx, outer, inner, groups, links, size = args
    src_leaf = _leaf_path(root_src, leaf_idx, outer, inner)
    os.makedirs(src_leaf, exist_ok=True)
    made = 0
    for g in range(groups):
        base = os.path.join(src_leaf, f"g{g}_0")
        _write_file(base, size, SRC_MODE)
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
    root_src, start, end, size = args
    for n in range(start, end):
        _write_file(os.path.join(root_src, f"f{n}"), size, SRC_MODE)
    return end - start


def gen_flat(root_src, count, size, jobs):
    """All files in a single directory (stresses readdir/listing buffers)."""
    os.makedirs(root_src, exist_ok=True)
    chunk = max(1, count // max(1, jobs))
    tasks = [(root_src, s, min(s + chunk, count), size)
             for s in range(0, count, chunk)]
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for c in ex.map(_make_flat_range, tasks):
            made += c
    return made


def _make_deep_chain(args):
    """One deep chain of long-named dirs with long-named files at the leaf."""
    root_src, chain_idx, depth, comp_len, nfiles, size = args
    # Long, unique-per-chain leading component, then a deep chain of long names.
    comp = "d" + ("x" * max(1, comp_len - 1))
    path = os.path.join(root_src, f"c{chain_idx}_" + ("y" * max(1, comp_len - 8)))
    for _ in range(depth):
        path = os.path.join(path, comp)
    os.makedirs(path, exist_ok=True)
    made = 0
    for i in range(nfiles):
        name = f"f{i}_" + ("n" * max(1, comp_len - 8))
        _write_file(os.path.join(path, name), size, SRC_MODE)
        made += 1
    return made


def gen_deep(root_src, count, size, depth, comp_len, files_per_chain, jobs):
    """Deep nesting + long path/file names (stresses per-path storage)."""
    os.makedirs(root_src, exist_ok=True)
    nchains = max(1, int(math.ceil(count / float(files_per_chain))))
    tasks = [(root_src, c, depth, comp_len,
              min(files_per_chain, count - c * files_per_chain), size)
             for c in range(nchains)]
    made = 0
    with ProcessPoolExecutor(max_workers=jobs) as ex:
        for cnt in ex.map(_make_deep_chain, tasks):
            made += cnt
    return made


def gen_hardlinks(root_src, count, size, links, fanout, jobs):
    # `count` total links; each inode contributes `links` links.
    ninodes = max(1, count // links)
    outer, inner, ndirs, _, _ = _plan(ninodes, max(1, fanout // links))
    # Distribute inode-groups across leaves.
    per = [0] * ndirs
    for i in range(ninodes):
        per[i % ndirs] += 1
    tasks = [(root_src, leaf, outer, inner, per[leaf], links, size)
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
    ap.add_argument("--dst", choices=["empty", "identical", "perms", "none"],
                    help="override the profile's default dst state")
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

    root_src = os.path.join(args.root, "src")
    root_dst = os.path.join(args.root, "dst")

    prepare_root(args.root, args.fresh)
    prepare_root(root_src, args.fresh)

    print(f"[gen] profile={args.profile} count={_human(count)} "
          f"size={_human(size)}B dst={dst_state} jobs={args.jobs}",
          file=sys.stderr)

    if args.profile == "hardlinks":
        made = gen_hardlinks(root_src, count, size, args.links_per_inode,
                             args.fanout, args.jobs)
        print(f"[gen] created {_human(made)} links over "
              f"~{_human(max(1, count // args.links_per_inode))} inodes",
              file=sys.stderr)
    elif args.profile == "flat":
        made = gen_flat(root_src, count, size, args.jobs)
        print(f"[gen] created {_human(made)} files in a single directory",
              file=sys.stderr)
    elif args.profile == "deep":
        made = gen_deep(root_src, count, size, DEEP_DEPTH, DEEP_COMP_LEN,
                        DEEP_FILES_PER_CHAIN, args.jobs)
        print(f"[gen] created {_human(made)} files across deep long-name chains "
              f"(depth={DEEP_DEPTH}, comp_len={DEEP_COMP_LEN})", file=sys.stderr)
    else:
        want_dst = root_dst if dst_state in ("identical", "perms") else None
        dst_mode = DST_DIFF_MODE if dst_state == "perms" else SRC_MODE
        if want_dst is not None:
            prepare_root(root_dst, args.fresh)
        made = gen_plain(root_src, want_dst, count, size, args.fanout,
                         args.jobs, dst_mode)
        print(f"[gen] created {_human(made)} files in src"
              + (f" and dst ({dst_state})" if want_dst else ""),
              file=sys.stderr)

    if dst_state == "empty":
        prepare_root(root_dst, True)  # always a clean empty dst
        print("[gen] dst prepared empty", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
