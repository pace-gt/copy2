#!/usr/bin/env python3
"""Deterministic tree generator for the copy2 test suite.

This script's ONLY job is to materialize directory trees on disk. It does not
run copy2 and it does not verify anything -- orchestration lives in CMake/CTest
(see run_scenario.cmake) and verification lives in verify.py.

It produces, under --out:
    <out>/src    the source tree (the oracle copy2 must reproduce)
    <out>/dest   the initial destination tree, in one of three modes:
                   empty      -- empty directory (full transfer)
                   identical  -- faithful copy of src (copy2 should no-op)
                   modified   -- faithful copy with deterministic corruptions
                                 (missing/size/mtime/mode/symlink/hardlink, and
                                  optionally extra entries with --extras)

Everything is derived from --seed, so a given (seed, profile, mode) always
produces byte-identical trees.

Usage:
    gentrees.py --mode {empty,identical,modified} --out DIR
                [--extras] [--seed N] [--profile small|medium]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import shutil
import stat
import sys
from dataclasses import dataclass

# A fixed reference mtime (seconds) so generated trees are reproducible.
BASE_MTIME = 1_600_000_000  # 2020-09-13T12:26:40Z
# An obviously-different mtime used to mark corrupted destination entries so
# copy2 is forced to re-transfer them (it only fixes metadata on files it
# actually transfers, i.e. those whose size or mtime-seconds differ).
WRONG_MTIME_NS = (BASE_MTIME - 5_000_000) * 1_000_000_000 + 123

PROFILES = {
    "small": dict(
        depth=2,
        dirs_per_level=2,
        files_per_dir=4,
        n_symlinks=3,
        n_hardlink_groups=2,
        hardlink_group_size=3,
        sizes=[0, 1, 511, 512, 513, 4096, 4097, 100_000, 1 << 20, (1 << 20) + 123],
        # (name, block-pattern); 'D' = 1MiB data block, 'H' = 1MiB hole block.
        sparse_specs=[
            ("zero",  "HHHH"),   # entirely holes
            ("end",   "DDHH"),   # hole at the end
            ("begin", "HHDD"),   # hole at the beginning
            ("multi", "DHDHD"),  # multiple interior holes
        ],
    ),
    "medium": dict(
        depth=3,
        dirs_per_level=3,
        files_per_dir=5,
        n_symlinks=8,
        n_hardlink_groups=4,
        hardlink_group_size=3,
        sizes=[
            0, 1, 511, 512, 513, 1023, 4096, 4097, 65_536, 100_000,
            1 << 20, (1 << 20) + 123, 5 << 20, (9 << 20) + 777,
        ],
        sparse_specs=[
            ("zero",   "HHHH"),       # entirely holes
            ("end",    "DDDHHH"),     # hole at the end
            ("begin",  "HHHDDD"),     # hole at the beginning
            ("multi",  "DHDHDHD"),    # multiple interior holes
            ("mixed",  "HDHHDDH"),    # holes at both ends + interior
        ],
    ),
}

# ---------------------------------------------------------------------------
# Deterministic content helpers
# ---------------------------------------------------------------------------


def _derive_seed(global_seed: int, *parts: object) -> int:
    h = hashlib.sha256()
    h.update(str(global_seed).encode())
    for p in parts:
        h.update(b"\x00")
        h.update(str(p).encode())
    return int.from_bytes(h.digest()[:8], "little")


def _det_bytes(global_seed: int, key: str, n: int) -> bytes:
    if n == 0:
        return b""
    return random.Random(_derive_seed(global_seed, "content", key)).randbytes(n)


# Block size for sparse pattern files. The sparse scenarios run copy2 with
# --max-block-size 1MB so holes land exactly on copy2's block boundaries and
# are therefore individually skippable (and preserved on the destination).
_SPARSE_BLOCK = 1 << 20


def _write_file(path: str, size: int, global_seed: int, key: str) -> None:
    """Create a fully-allocated regular file with deterministic content."""
    with open(path, "wb") as f:
        f.write(_det_bytes(global_seed, key, size))


def _write_pattern_file(path: str, pattern: str, global_seed: int,
                        key: str) -> None:
    """Create a genuinely sparse file from a block pattern.

    ``pattern`` is a string of 'D' (data) and 'H' (hole) characters; each
    character describes one _SPARSE_BLOCK-sized block. Data blocks get
    deterministic content; hole blocks are left unwritten (real holes). This
    lets us place holes at the beginning, the end, or interleaved.
    """
    block = _SPARSE_BLOCK
    with open(path, "wb") as f:
        for i, ch in enumerate(pattern):
            if ch == "D":
                f.seek(i * block)
                f.write(_det_bytes(global_seed, f"{key}:blk{i}", block))
            elif ch != "H":
                raise ValueError(f"bad pattern char {ch!r} in {pattern!r}")
        f.truncate(len(pattern) * block)


def _det_mode(global_seed: int, key: str, is_dir: bool) -> int:
    pool = [0o755, 0o750, 0o700, 0o775] if is_dir else \
           [0o644, 0o640, 0o600, 0o664, 0o755, 0o700]
    return pool[_derive_seed(global_seed, "mode", key) % len(pool)]


def _mtime_ns(global_seed: int, key: str) -> int:
    return (BASE_MTIME + (_derive_seed(global_seed, "mtime", key) % 5_000_000)) \
        * 1_000_000_000


# ---------------------------------------------------------------------------
# Tree walking
# ---------------------------------------------------------------------------


def _walk_rel(root: str):
    """Yield every entry path relative to root (excluding root itself)."""
    stack = [""]
    while stack:
        rel = stack.pop()
        abspath = os.path.join(root, rel) if rel else root
        try:
            entries = sorted(os.listdir(abspath))
        except NotADirectoryError:
            continue
        for name in entries:
            crel = os.path.join(rel, name) if rel else name
            yield crel
            if stat.S_ISDIR(os.lstat(os.path.join(root, crel)).st_mode):
                stack.append(crel)


# ---------------------------------------------------------------------------
# Source tree generation
# ---------------------------------------------------------------------------


def generate_src(root: str, global_seed: int, profile: dict) -> None:
    os.makedirs(root, exist_ok=True)
    sizes = profile["sizes"]
    created_files: list[str] = []
    all_dirs: list[str] = [""]
    size_idx = [0]

    def make_dir(rel: str, depth: int) -> None:
        abspath = os.path.join(root, rel) if rel else root
        os.makedirs(abspath, exist_ok=True)
        all_dirs.append(rel)
        for fi in range(profile["files_per_dir"]):
            frel = os.path.join(rel, f"f{fi}.bin") if rel else f"f{fi}.bin"
            size = sizes[size_idx[0] % len(sizes)]
            size_idx[0] += 1
            _write_file(os.path.join(root, frel), size, global_seed, frel)
            created_files.append(frel)
        if depth > 0:
            for di in range(profile["dirs_per_level"]):
                drel = os.path.join(rel, f"d{di}") if rel else f"d{di}"
                make_dir(drel, depth - 1)

    make_dir("", profile["depth"])

    sorted_files = sorted(created_files)
    for gi in range(profile["n_hardlink_groups"]):
        if not sorted_files:
            break
        base = sorted_files[_derive_seed(global_seed, "hlbase", gi) % len(sorted_files)]
        base_abs = os.path.join(root, base)
        for li in range(1, profile["hardlink_group_size"]):
            link_abs = os.path.join(root, f"{base}.hl{gi}_{li}")
            if not os.path.exists(link_abs):
                os.link(base_abs, link_abs)

    for si in range(profile["n_symlinks"]):
        d = all_dirs[_derive_seed(global_seed, "lnkdir", si) % len(all_dirs)]
        link_abs = os.path.join(root, os.path.join(d, f"link{si}") if d else f"link{si}")
        if os.path.lexists(link_abs):
            continue
        kind = si % 3
        if kind == 0 and created_files:
            target = "./" + os.path.basename(
                created_files[_derive_seed(global_seed, "lnktgt", si) % len(created_files)])
        elif kind == 1:
            target = f"/nonexistent/abs/target_{si}"
        else:
            target = f"../dangling_{si}_relative"
        os.symlink(target, link_abs)

    # Genuinely sparse files (holes preserved on disk) to exercise --sparse,
    # covering holes at the beginning, the end, and multiple interior holes.
    for name, pattern in profile.get("sparse_specs", []):
        rel = f"sparse_{name}.bin"
        _write_pattern_file(os.path.join(root, rel), pattern, global_seed, rel)

    _stamp_tree(root, global_seed)


def _stamp_tree(root: str, global_seed: int) -> None:
    """Apply deterministic metadata; directories deepest-last so their mtimes
    are not perturbed by child creation."""
    files, links, dirs = [], [], []
    for rel in _walk_rel(root):
        st = os.lstat(os.path.join(root, rel))
        (links if stat.S_ISLNK(st.st_mode) else
         dirs if stat.S_ISDIR(st.st_mode) else files).append(rel)

    for rel in files:
        os.chmod(os.path.join(root, rel), _det_mode(global_seed, rel, False))
        mt = _mtime_ns(global_seed, rel)
        os.utime(os.path.join(root, rel), ns=(mt, mt))
    for rel in links:
        mt = _mtime_ns(global_seed, rel)
        os.utime(os.path.join(root, rel), ns=(mt, mt), follow_symlinks=False)
    for rel in sorted(dirs, key=lambda r: r.count(os.sep), reverse=True):
        key = rel or "."
        os.chmod(os.path.join(root, rel) if rel else root, _det_mode(global_seed, key, True))
        mt = _mtime_ns(global_seed, key)
        os.utime(os.path.join(root, rel) if rel else root, ns=(mt, mt))


# ---------------------------------------------------------------------------
# Destination construction
# ---------------------------------------------------------------------------


def faithful_copy(src: str, dest: str) -> None:
    """Replicate src into dest preserving content, symlinks, hardlinks and
    metadata (an unmodified copy must be a true no-op for copy2)."""
    os.makedirs(dest, exist_ok=True)
    ino_to_dest: dict[int, str] = {}
    entries = list(_walk_rel(src))

    for rel in entries:
        sabs, dabs = os.path.join(src, rel), os.path.join(dest, rel)
        st = os.lstat(sabs)
        if stat.S_ISDIR(st.st_mode):
            os.makedirs(dabs, exist_ok=True)
        elif stat.S_ISLNK(st.st_mode):
            os.symlink(os.readlink(sabs), dabs)
        elif st.st_nlink > 1 and st.st_ino in ino_to_dest:
            os.link(ino_to_dest[st.st_ino], dabs)
        else:
            shutil.copyfile(sabs, dabs)
            if st.st_nlink > 1:
                ino_to_dest[st.st_ino] = dabs

    def is_dir(r: str) -> bool:
        return stat.S_ISDIR(os.lstat(os.path.join(src, r)).st_mode)

    for rel in sorted(entries, key=lambda r: (is_dir(r), r.count(os.sep)), reverse=True):
        sabs, dabs = os.path.join(src, rel), os.path.join(dest, rel)
        st = os.lstat(sabs)
        if stat.S_ISLNK(st.st_mode):
            os.utime(dabs, ns=(st.st_mtime_ns, st.st_mtime_ns), follow_symlinks=False)
        else:
            os.chmod(dabs, stat.S_IMODE(st.st_mode))
            os.utime(dabs, ns=(st.st_mtime_ns, st.st_mtime_ns))


@dataclass
class _SrcInfo:
    rel: str
    kind: str
    size: int
    mode: int
    ino: int
    nlink: int


def _scan_src(root: str) -> dict[str, _SrcInfo]:
    out: dict[str, _SrcInfo] = {}
    for rel in _walk_rel(root):
        st = os.lstat(os.path.join(root, rel))
        kind = ("dir" if stat.S_ISDIR(st.st_mode)
                else "link" if stat.S_ISLNK(st.st_mode) else "file")
        out[rel] = _SrcInfo(rel, kind, st.st_size, stat.S_IMODE(st.st_mode),
                            st.st_ino, st.st_nlink)
    return out


def apply_corruptions(src: str, dest: str, global_seed: int,
                      include_extras: bool) -> list[str]:
    """Corrupt the (already faithful) dest. Every corruption changes size or
    mtime (or removes/adds entries) so copy2 is guaranteed to act on it.
    Returns human-readable descriptions of what was done."""
    rng = random.Random(_derive_seed(global_seed, "corrupt"))
    src_entries = _scan_src(src)

    reg_files = sorted(e.rel for e in src_entries.values()
                       if e.kind == "file" and e.nlink == 1)
    links = sorted(e.rel for e in src_entries.values() if e.kind == "link")
    subdirs = sorted((e.rel for e in src_entries.values() if e.kind == "dir"),
                     key=lambda r: r.count(os.sep), reverse=True)
    groups: dict[int, list[str]] = {}
    for e in src_entries.values():
        if e.kind == "file" and e.nlink > 1:
            groups.setdefault(e.ino, []).append(e.rel)
    group_list = [sorted(v) for v in groups.values()]

    rng.shuffle(reg_files)
    rng.shuffle(links)
    rng.shuffle(group_list)

    log: list[str] = []
    pool = list(reg_files)

    def take(n: int) -> list[str]:
        out = pool[:n]
        del pool[:n]
        return out

    for rel in take(2):  # missing files
        os.remove(os.path.join(dest, rel))
        log.append(f"delete_file {rel}")

    for rel in take(2):  # wrong size
        with open(os.path.join(dest, rel), "wb") as f:
            f.write(b"\xab" * (src_entries[rel].size + 7))
        log.append(f"wrong_size {rel}")

    for rel in take(2):  # wrong content + bumped mtime
        size = src_entries[rel].size
        with open(os.path.join(dest, rel), "wb") as f:
            f.write(b"\x00" * size)
        os.utime(os.path.join(dest, rel), ns=(WRONG_MTIME_NS, WRONG_MTIME_NS))
        log.append(f"wrong_content_mtime {rel}")

    for rel in take(2):  # wrong mode + bumped mtime
        dabs = os.path.join(dest, rel)
        os.chmod(dabs, 0o600 if src_entries[rel].mode != 0o600 else 0o644)
        os.utime(dabs, ns=(WRONG_MTIME_NS, WRONG_MTIME_NS))
        log.append(f"wrong_mode_mtime {rel}")

    for rel in links[:2]:  # retargeted symlinks (different length)
        dabs = os.path.join(dest, rel)
        os.remove(dabs)
        os.symlink("/some/totally/different/target/that/differs/in/length", dabs)
        os.utime(dabs, ns=(WRONG_MTIME_NS, WRONG_MTIME_NS), follow_symlinks=False)
        log.append(f"retarget_symlink {rel}")

    if group_list:  # delete an entire hardlink group
        for rel in group_list[0]:
            p = os.path.join(dest, rel)
            if os.path.exists(p):
                os.remove(p)
        log.append(f"delete_hardlink_group {group_list[0][0]} ({len(group_list[0])})")
    if len(group_list) > 1:  # break a group into independent stale files
        for rel in group_list[1]:
            p = os.path.join(dest, rel)
            if os.path.exists(p):
                os.remove(p)
            with open(p, "wb") as f:
                f.write(b"\x11" * (src_entries[rel].size + 3))
            os.utime(p, ns=(WRONG_MTIME_NS, WRONG_MTIME_NS))
        log.append(f"unlink_hardlink_group {group_list[1][0]} ({len(group_list[1])})")

    for rel in subdirs:  # delete a subtree
        if rel:
            shutil.rmtree(os.path.join(dest, rel))
            log.append(f"delete_subtree {rel}")
            break

    if include_extras:
        os.makedirs(os.path.join(dest, "EXTRA_dir", "nested"), exist_ok=True)
        with open(os.path.join(dest, "EXTRA_dir", "nested", "junk.bin"), "wb") as f:
            f.write(b"junk-should-be-removed")
        with open(os.path.join(dest, "EXTRA_file.bin"), "wb") as f:
            f.write(b"orphan-should-be-removed")
        log.append("extra_subtree EXTRA_dir")
        log.append("extra_file EXTRA_file.bin")

    return log


def build_dest(src: str, dest: str, mode: str, global_seed: int,
               include_extras: bool) -> list[str]:
    if os.path.exists(dest):
        shutil.rmtree(dest)
    if mode == "empty":
        os.makedirs(dest, exist_ok=True)
        return []
    faithful_copy(src, dest)
    if mode == "identical":
        return []
    if mode == "modified":
        return apply_corruptions(src, dest, global_seed, include_extras)
    raise ValueError(f"unknown mode {mode!r}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Deterministic copy2 tree generator")
    p.add_argument("--mode", required=True, choices=["empty", "identical", "modified"])
    p.add_argument("--out", required=True, help="output dir (creates <out>/src and <out>/dest)")
    p.add_argument("--extras", action="store_true",
                   help="add extra dest entries (only meaningful for --mode modified)")
    p.add_argument("--seed", type=int, default=1234)
    p.add_argument("--profile", choices=list(PROFILES), default="small")
    args = p.parse_args(argv)

    profile = PROFILES[args.profile]
    src = os.path.join(args.out, "src")
    dest = os.path.join(args.out, "dest")

    os.makedirs(args.out, exist_ok=True)
    if os.path.exists(src):
        shutil.rmtree(src)
    generate_src(src, args.seed, profile)
    log = build_dest(src, dest, args.mode, args.seed, include_extras=args.extras)

    print(f"generated src+dest at {args.out} "
          f"(mode={args.mode} seed={args.seed} profile={args.profile})")
    for line in log:
        print(f"  corrupt: {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
