#!/usr/bin/env python3
"""Standalone verifier for the copy2 test suite.

Treats the source tree as the oracle and asserts that, after `copy2 src dest`,
the destination reproduces the source under copy2's semantics: content, size,
mode, ownership, mtime, symlink targets, and hardlink topology.

Usage:
    verify.py --src DIR --dest DIR [--allow-extra] [--strict-mtime]
              [--no-dir-mtime] [--check-owner]

Exit code is non-zero (and diffs are printed) if the destination does not match.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import stat
import sys
from dataclasses import dataclass

MAX_DIFFS = 60


@dataclass
class Entry:
    rel: str
    kind: str  # "dir" | "file" | "link"
    mode: int
    uid: int
    gid: int
    mtime_ns: int
    size: int
    ino: int
    nlink: int
    blocks: int = 0  # st_blocks (512-byte units); used for sparseness checks
    target: str | None = None


def _walk_rel(root: str):
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


def scan_tree(root: str) -> dict[str, Entry]:
    out: dict[str, Entry] = {}
    for rel in _walk_rel(root):
        abspath = os.path.join(root, rel)
        st = os.lstat(abspath)
        kind = ("dir" if stat.S_ISDIR(st.st_mode)
                else "link" if stat.S_ISLNK(st.st_mode) else "file")
        out[rel] = Entry(
            rel=rel, kind=kind, mode=stat.S_IMODE(st.st_mode),
            uid=st.st_uid, gid=st.st_gid, mtime_ns=st.st_mtime_ns,
            size=st.st_size, ino=st.st_ino, nlink=st.st_nlink,
            blocks=st.st_blocks,
            target=os.readlink(abspath) if kind == "link" else None,
        )
    return out


# A file is considered sparse if its allocated bytes are well under its apparent
# size. Only meaningful for files large enough to dwarf filesystem allocation
# granularity, so small files are never flagged.
SPARSE_MIN_SIZE = 1 << 20


def _allocated(e: Entry) -> int:
    return e.blocks * 512


def _is_sparse(e: Entry, ratio: float) -> bool:
    return e.size >= SPARSE_MIN_SIZE and _allocated(e) < e.size * ratio


# Holes smaller than this are ignored when comparing hole positions, to avoid
# noise from filesystem allocation/alignment quirks. Generated holes are >=1MiB.
MIN_HOLE = 1 << 18


def _hole_extents(path: str) -> list[tuple[int, int]]:
    """Return the [start, end) byte ranges that are holes, via SEEK_DATA/HOLE.

    On filesystems without hole support this conservatively reports no holes
    (everything looks like data), so callers must treat an empty result as
    'unknown' rather than 'fully allocated'."""
    holes: list[tuple[int, int]] = []
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return holes
    try:
        size = os.fstat(fd).st_size
        pos = 0
        while pos < size:
            try:
                data = os.lseek(fd, pos, os.SEEK_DATA)
            except OSError:
                holes.append((pos, size))  # ENXIO: rest of file is a hole
                break
            if data > pos:
                holes.append((pos, data))
            if data >= size:
                break
            pos = os.lseek(fd, data, os.SEEK_HOLE)
    finally:
        os.close(fd)
    return holes


def _overlap(interval: tuple[int, int], regions: list[tuple[int, int]]) -> int:
    s, e = interval
    total = 0
    for rs, re in regions:
        lo, hi = max(s, rs), min(e, re)
        if hi > lo:
            total += hi - lo
    return total


def file_hash(path: str) -> str:
    h = hashlib.blake2b(digest_size=16)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _mtime_match(a_ns: int, b_ns: int, strict: bool) -> bool:
    return a_ns == b_ns if strict else a_ns // 1_000_000_000 == b_ns // 1_000_000_000


def verify(src: str, dest: str, *, allow_extra: bool, strict_mtime: bool,
           check_dir_mtime: bool, check_owner: bool,
           check_sparse: bool = False) -> list[str]:
    diffs: list[str] = []
    src_entries = scan_tree(src)
    dest_entries = scan_tree(dest)

    def add(msg: str) -> None:
        if len(diffs) < MAX_DIFFS:
            diffs.append(msg)

    for rel, se in sorted(src_entries.items()):
        de = dest_entries.get(rel)
        if de is None:
            add(f"MISSING in dest: {rel} ({se.kind})")
            continue
        if de.kind != se.kind:
            add(f"TYPE mismatch {rel}: src={se.kind} dest={de.kind}")
            continue

        if se.kind == "link":
            if se.target != de.target:
                add(f"SYMLINK target {rel}: src={se.target!r} dest={de.target!r}")
            if not _mtime_match(se.mtime_ns, de.mtime_ns, strict_mtime):
                add(f"SYMLINK mtime {rel}: src={se.mtime_ns} dest={de.mtime_ns}")
            continue

        if se.kind == "dir":
            if se.mode != de.mode:
                add(f"DIR mode {rel}: src={oct(se.mode)} dest={oct(de.mode)}")
            if check_dir_mtime and not _mtime_match(se.mtime_ns, de.mtime_ns, strict_mtime):
                add(f"DIR mtime {rel}: src={se.mtime_ns} dest={de.mtime_ns}")
            if check_owner and (se.uid, se.gid) != (de.uid, de.gid):
                add(f"DIR owner {rel}: src={se.uid}:{se.gid} dest={de.uid}:{de.gid}")
            continue

        # regular file
        if se.size != de.size:
            add(f"SIZE {rel}: src={se.size} dest={de.size}")
        elif file_hash(os.path.join(src, rel)) != file_hash(os.path.join(dest, rel)):
            add(f"CONTENT {rel}: hash mismatch (size={se.size})")
        if se.mode != de.mode:
            add(f"MODE {rel}: src={oct(se.mode)} dest={oct(de.mode)}")
        if not _mtime_match(se.mtime_ns, de.mtime_ns, strict_mtime):
            add(f"MTIME {rel}: src={se.mtime_ns} dest={de.mtime_ns}")
        if check_owner and (se.uid, se.gid) != (de.uid, de.gid):
            add(f"OWNER {rel}: src={se.uid}:{se.gid} dest={de.uid}:{de.gid}")
        # If the source is sparse, copy2 --sparse must keep the dest sparse too,
        # both in aggregate and at the same hole positions.
        if check_sparse and _is_sparse(se, 0.9):
            if not _is_sparse(de, 0.95):
                add(f"SPARSE {rel}: src allocated={_allocated(se)} "
                    f"dest allocated={_allocated(de)} size={se.size}")
            dest_holes = _hole_extents(os.path.join(dest, rel))
            for hs, he in _hole_extents(os.path.join(src, rel)):
                length = he - hs
                if length < MIN_HOLE:
                    continue
                covered = _overlap((hs, he), dest_holes)
                if covered < length * 0.8:
                    add(f"SPARSE HOLE {rel}: src hole [{hs},{he}) "
                        f"({length} B) only {covered} B hole on dest")

    if not allow_extra:
        for rel in sorted(dest_entries):
            if rel not in src_entries:
                add(f"UNEXPECTED in dest (sync should remove): {rel}")

    _verify_hardlinks(src_entries, dest_entries, add)
    return diffs


def _verify_hardlinks(src_entries: dict[str, Entry], dest_entries: dict[str, Entry],
                      add) -> None:
    rels = [r for r, e in src_entries.items()
            if e.kind == "file" and r in dest_entries and dest_entries[r].kind == "file"]

    src_groups: dict[int, list[str]] = {}
    for r in rels:
        src_groups.setdefault(src_entries[r].ino, []).append(r)
    for members in src_groups.values():
        if len(members) < 2:
            continue
        dest_inos = {dest_entries[r].ino for r in members}
        if len(dest_inos) != 1:
            add(f"HARDLINK broken: src group {sorted(members)} maps to "
                f"{len(dest_inos)} dest inodes")

    dest_to_src: dict[int, set[int]] = {}
    for r in rels:
        dest_to_src.setdefault(dest_entries[r].ino, set()).add(src_entries[r].ino)
    for sinos in dest_to_src.values():
        if len(sinos) > 1:
            add(f"HARDLINK over-linked: a dest inode is shared across "
                f"{len(sinos)} distinct src files")


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="Verify copy2 destination against source")
    p.add_argument("--src", required=True)
    p.add_argument("--dest", required=True)
    p.add_argument("--allow-extra", action="store_true",
                   help="tolerate dest entries absent from src (no --sync runs)")
    p.add_argument("--strict-mtime", action="store_true",
                   help="compare mtime at nanosecond granularity")
    p.add_argument("--no-dir-mtime", action="store_true",
                   help="do not verify directory mtimes")
    p.add_argument("--check-owner", action="store_true",
                   help="verify uid/gid (default: auto-on when running as root)")
    p.add_argument("--check-sparse", action="store_true",
                   help="require sparse source files to stay sparse on dest")
    args = p.parse_args(argv)

    check_owner = args.check_owner or os.geteuid() == 0
    diffs = verify(
        args.src, args.dest,
        allow_extra=args.allow_extra,
        strict_mtime=args.strict_mtime,
        check_dir_mtime=not args.no_dir_mtime,
        check_owner=check_owner,
        check_sparse=args.check_sparse,
    )
    if diffs:
        print(f"VERIFY FAILED: {len(diffs)} difference(s)")
        for d in diffs:
            print(f"  {d}")
        return 1
    print("VERIFY OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
