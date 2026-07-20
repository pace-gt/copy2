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
import signal
import subprocess
import sys
import time


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=float, default=0.05,
                    help="sample interval in seconds (default 0.05)")
    ap.add_argument("--json", help="write a JSON result record to this path")
    ap.add_argument("--label", default="", help="free-form label for the record")
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

    peak_pss = peak_rss = 0
    try:
        while True:
            rc = proc.poll()
            pss, rss = _sample(proc.pid)
            peak_pss = max(peak_pss, pss)
            peak_rss = max(peak_rss, rss)
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

    if args.json:
        with open(args.json, "w") as f:
            json.dump(record, f)

    print(f"[memsample] {args.label or ' '.join(cmd)}: "
          f"wall={record['wall_s']}s peak_pss={record['peak_pss_mb']}MB "
          f"peak_rss={record['peak_rss_mb']}MB rc={rc}", file=sys.stderr)

    return rc


if __name__ == "__main__":
    sys.exit(main())
