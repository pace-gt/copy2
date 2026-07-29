#!/usr/bin/env python3
"""Run a command and record its peak memory + wall time (rootless).

This samples the *entire process subtree* of the launched command, so it works
for multi-process tools (fpsync spawns fpart + several rsync workers, local
rsync forks a sender/receiver, etc.) where /usr/bin/time -v -- which only sees
the direct child -- would badly undercount.

Peak memory is reported as PSS (proportional set size, from
/proc/<pid>/smaps_rollup) summed across the subtree. PSS divides shared pages
by the number of sharers, so summing across a process tree does not
double-count shared libraries -- it is the fairest single number for "how much
RAM did this tool need". VmRSS is also reported for reference.

Caveat vs. cgroup memory.peak: this is a *sampler*, so a spike shorter than the
sample interval can be missed. For these tools memory rises during listing and
then plateaus, so a small interval captures the peak well. Use the runner's
--mem-method cgroup (needs root/delegation here) if you need exact peaks.

Usage:
    memsample.py [--interval S] [--json PATH] [--label L] -- CMD [ARGS...]
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time


_PWR_RE_AVG = re.compile(rb"Average power reading over sample period:\s+(\d+)")
_PWR_RE_INST = re.compile(rb"Instantaneous power reading:\s+(\d+)")


def read_power_watts(retries=4, timeout=25):
    """Read node power (watts) from the BMC via DCMI, or None on failure.

    `ipmitool dcmi power reading` reports WATTS (there is no cumulative joule
    counter on this BMC), so energy is derived as watts*time by the caller. We
    prefer the BMC's own "Average power reading over sample period" (a ~5s
    rolling mean) over the instantaneous value to smooth out sub-second spikes.
    Runs via `sudo -n` (passwordless on the compute nodes).

    Under heavy I/O the BMC query can be slow or transiently fail, so we retry a
    few times (with a short backoff) and use a generous timeout before giving up
    and returning None (which degrades power accounting to blank for that run).
    """
    for attempt in range(retries):
        try:
            r = subprocess.run(
                ["sudo", "-n", "ipmitool", "dcmi", "power", "reading"],
                capture_output=True, timeout=timeout)
            m = _PWR_RE_AVG.search(r.stdout) or _PWR_RE_INST.search(r.stdout)
            if m:
                return float(m.group(1))
        except subprocess.TimeoutExpired:
            pass
        except (OSError, subprocess.SubprocessError):
            pass
        if attempt < retries - 1:
            time.sleep(1.0)
    return None


def _read_int_field(path, field):
    """Return the integer kB value of `field:` in a /proc file, or 0."""
    try:
        with open(path, "rb") as f:
            for line in f:
                if line.startswith(field):
                    # e.g. b"Pss:                1234 kB\n"
                    return int(line.split()[1])
    except (OSError, ValueError, IndexError):
        pass
    return 0


def _ppid_of(pid):
    try:
        with open(f"/proc/{pid}/stat", "rb") as f:
            data = f.read()
        # comm may contain spaces/parens; ppid is the field after ") X"
        rparen = data.rindex(b")")
        fields = data[rparen + 2:].split()
        return int(fields[1])  # state is fields[0], ppid is fields[1]
    except (OSError, ValueError, IndexError):
        return -1


def _subtree_pids(root):
    """All live pids in the subtree rooted at `root` (inclusive)."""
    children = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        ppid = _ppid_of(pid)
        if ppid >= 0:
            children.setdefault(ppid, []).append(pid)

    out = []
    stack = [root]
    while stack:
        pid = stack.pop()
        out.append(pid)
        stack.extend(children.get(pid, ()))
    return out


def _sample(root):
    """Sum PSS and RSS (kB) over the subtree rooted at `root`."""
    pss = rss = 0
    for pid in _subtree_pids(root):
        p = _read_int_field(f"/proc/{pid}/smaps_rollup", b"Pss:")
        if p == 0:
            # smaps_rollup can be unreadable for a transient pid; fall back.
            p = _read_int_field(f"/proc/{pid}/status", b"VmRSS:")
        pss += p
        rss += _read_int_field(f"/proc/{pid}/status", b"VmRSS:")
    return pss, rss


def _categorize(path, size_kb):
    """Bucket a mapping into an attribution category.

    copy2's whole allocator is a single large anonymous mmap, so its resident
    footprint shows up as one >=1G anonymous mapping -- this cleanly separates
    "copy2 arena resident" from glibc arenas, thread stacks, and libraries.
    """
    if path:
        if path.startswith("[stack"):
            return "main-stack"
        if path == "[heap]":
            return "glibc-heap"
        if path.startswith("[anon"):
            return f"named-anon {path}"
        if path.endswith(".so") or ".so." in path or "/lib" in path:
            return "shared-libs"
        if path.endswith("/copy2") or path.endswith("copy2"):
            return "binary"
        if path.endswith(".db") or "hlstate" in path or path.endswith("lock"):
            return "lmdb"
        return f"file {os.path.basename(path)}"
    # anonymous
    if size_kb >= 1024 * 1024:
        return "copy2-arena (anon>=1G)"
    if size_kb >= 16 * 1024:
        return "big-anon 16M-1G (glibc arena?)"
    if 8 * 1024 - 128 <= size_kb <= 8 * 1024 + 128:
        return "thread-stack (~8M)"
    return "small-anon (<16M)"


def _smaps_breakdown(pids):
    """Return {category: rss_kb} summed over the given pids' /proc/*/smaps."""
    cats = {}
    hdr = None  # (path, size_kb)
    for pid in pids:
        try:
            with open(f"/proc/{pid}/smaps", "rb") as f:
                path, size_kb = None, 0
                for line in f:
                    # mapping header: "addr-addr perms off dev inode  path"
                    if b"-" in line[:20] and b" " in line and line[0:1] != b" ":
                        parts = line.split()
                        if len(parts) >= 5 and b"-" in parts[0]:
                            lo, hi = parts[0].split(b"-")
                            size_kb = (int(hi, 16) - int(lo, 16)) // 1024
                            path = (parts[5].decode("utf-8", "replace")
                                    if len(parts) >= 6 else None)
                            continue
                    if line.startswith(b"Rss:"):
                        rss = int(line.split()[1])
                        if rss:
                            c = _categorize(path, size_kb)
                            cats[c] = cats.get(c, 0) + rss
        except (OSError, ValueError, IndexError):
            continue
    return cats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=0.05,
                    help="sample interval in seconds (default 0.05)")
    ap.add_argument("--json", help="write a JSON result record to this path")
    ap.add_argument("--label", default="", help="free-form label for the record")
    ap.add_argument("--smaps", action="store_true",
                    help="capture a per-mapping RSS attribution at peak RSS")
    ap.add_argument("--power", action="store_true",
                    help="record node energy via BMC power (watts) x wall time: "
                         "reads power at start and finish, energy = mean*wall")
    ap.add_argument("cmd", nargs=argparse.REMAINDER,
                    help="-- followed by the command to run")
    args = ap.parse_args()

    cmd = args.cmd
    if cmd and cmd[0] == "--":
        cmd = cmd[1:]
    if not cmd:
        ap.error("no command given (use: memsample.py [opts] -- CMD ...)")

    start = time.monotonic()
    # New session so a stray tree can be signalled as a group if needed.
    proc = subprocess.Popen(cmd, start_new_session=True)
    # Power at the start of the window (concurrent with the tool's warm-up).
    pwr_start = read_power_watts() if args.power else None

    peak_pss = peak_rss = 0
    peak_breakdown = {}
    try:
        while True:
            rc = proc.poll()
            pss, rss = _sample(proc.pid)
            peak_pss = max(peak_pss, pss)
            if rss > peak_rss:
                peak_rss = rss
                if args.smaps:
                    peak_breakdown = _smaps_breakdown(_subtree_pids(proc.pid))
            if rc is not None:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        proc.wait()
        raise

    # Power at the finish of the window (just after the tool exits).
    pwr_end = read_power_watts() if args.power else None
    wall = time.monotonic() - start
    rc = proc.returncode

    record = {
        "label": args.label,
        "cmd": cmd,
        "wall_s": round(wall, 3),
        "peak_pss_kb": peak_pss,
        "peak_rss_kb": peak_rss,
        "peak_pss_mb": round(peak_pss / 1024.0, 1),
        "peak_rss_mb": round(peak_rss / 1024.0, 1),
        "rc": rc,
        "interval_s": args.interval,
    }

    if args.power:
        # No cumulative-energy register on this BMC (DCMI reports watts), so
        # energy = mean(start,end power) * wall. Whichever endpoint is available
        # is used; energy stays blank only if both reads failed.
        vals = [w for w in (pwr_start, pwr_end) if w is not None]
        avg_w = (sum(vals) / len(vals)) if vals else None
        record["power_start_w"] = pwr_start
        record["power_end_w"] = pwr_end
        record["avg_watts"] = round(avg_w, 1) if avg_w is not None else None
        record["energy_j"] = round(avg_w * wall, 1) if avg_w is not None else None

    if args.json:
        with open(args.json, "w") as f:
            json.dump(record, f)

    pwr_msg = ""
    if args.power:
        pwr_msg = (f" energy={record.get('energy_j')}J "
                   f"avg_watts={record.get('avg_watts')}")
    print(f"[memsample] {args.label or ' '.join(cmd)}: "
          f"wall={record['wall_s']}s peak_pss={record['peak_pss_mb']}MB "
          f"peak_rss={record['peak_rss_mb']}MB{pwr_msg} rc={rc}", file=sys.stderr)

    if args.smaps and peak_breakdown:
        total = sum(peak_breakdown.values()) or 1
        print(f"[smaps] RSS attribution at peak ({peak_rss // 1024}MB total, "
              f"subtree):", file=sys.stderr)
        for cat, kb in sorted(peak_breakdown.items(), key=lambda kv: -kv[1]):
            print(f"[smaps]   {kb/1024.0:8.1f}MB {100.0*kb/total:5.1f}%  {cat}",
                  file=sys.stderr)

    return rc


if __name__ == "__main__":
    sys.exit(main())
