# copy2 benchmark suite

Compares `copy2` against `rsync`, `rclone`, and `fpsync` on the scenarios in
`../benchmarks.txt` (crawl / metadata / hardlinks / transfer / memory), recording
**wall time** and **peak memory** (PSS + RSS of the whole process tree) to a CSV,
plus plots.

## Files

| file | purpose |
|---|---|
| `run_benchmarks.sh` | orchestrator: generates datasets, runs each tool, writes CSV |
| `gen_dataset.py`    | deterministic dataset generator (called by the runner) |
| `memsample.py`      | rootless peak PSS/RSS sampler for a process subtree |
| `plot.py`           | turns a results CSV into PNG charts |
| `results_*.csv`, `plots_*/` | example results from the dev box (regenerate on the cluster) |

## Prerequisites

- `copy2` built (the runner auto-discovers `../../copy2`, or set `COPY2=/path`).
- `rsync`, `rclone`, and `fpsync` on `PATH` (fpsync also needs `fpart` on `PATH`
  **at run time**). Any tool whose binary is missing is skipped, not fatal.
- `python3` with `matplotlib` (only needed for `plot.py`).
- Root for cold-cache runs (`--drop-caches`). On the cluster sudo is passwordless,
  so this Just Works; otherwise it prompts.

## Quick start

```bash
cd utils/copy2/bench

# where datasets (huge) and scratch live -- put these on the fast target FS
export DATA_ROOT=/scratch/$USER/copy2-bench-data
export WORK=/scratch/$USER/copy2-bench-work

# 1. generate datasets once (scale = file count for the 1e6-class benches)
./run_benchmarks.sh --generate --scale 1000000
#    ...or only build what's missing (resume an interrupted suite without
#    rebuilding the million-file trees):
./run_benchmarks.sh --generate-missing --scale 1000000

# 2. run everything, cold caches, with the tuning we settled on
./run_benchmarks.sh --scale 1000000 --drop-caches 3 \
    --copy2-crawlers 16 --copy2-transfers 4 --copy2-finishers 8 \
    --rclone-checkers 128 --rclone-transfers 32 \
    --fpsync-conc 16 \
    --results results.csv

# 3. plot
python3 plot.py --csv results.csv --outdir plots
```

`--drop-caches N` flushes the filesystem containing the current destination,
then writes `N` to `/proc/sys/vm/drop_caches` before every run
(`1`=pagecache, `2`=dentries/inodes, `3`=both — use **3** for a truly cold
comparison). The filesystem-scoped flush avoids waiting for unrelated mounts
on a shared node. Omit the flag to run warm.

Empty destinations are reset by renaming the old tree aside (O(1) on the same
FS) and queuing it; the actual million-file `rm` is **deferred to the very end
of the suite** rather than run in the background during the next measured run,
so deletion IO/CPU never contends with a timed tool. The trade-off is that every
retired destination is kept on disk until the suite finishes — size `DATA_ROOT`
accordingly (see the cluster notes). Metadata resets still chmod in place, but
that walk is parallel. `--reset-jobs N` controls cleanup/chmod concurrency and
defaults to `--conc`.

## Selecting a subset

```bash
--only crawling,metadata,hardlinks,transfer   # skip the (slow) memory benches
--only mem,memstress                          # only the memory benches
--tools copy2,rsync                           # only some tools
--scale 100000                                # smaller/faster
```

Bench names for `--only`: `crawling metadata hardlinks transfer mem memstress
transfer_1tb size_scaling`. The 1 TiB transfer (`4b`) is gated — pass
`--enable-1tb` (and `--generate`) to create/run it.

Inside the memory groups, the defaults changed to keep the suite fast and the
results interpretable:

- `mem` (default) now runs only **`mem_hlinks`**. **`mem_scaling`** is opt-in via
  `--enable-mem-scaling`. When enabled it is a true file-*count* sweep at a fixed
  **2KB** at every point (10k/100k/1M); the old version reused the 2 MB
  `transfer` dataset for the top point, which conflated a 1000× file-*size* jump
  with the count sweep. **rsync is excluded** from `mem_scaling` (single-stream,
  ~hours on the top point, and it adds nothing to the memory story).
- `memstress` (default) now runs the **`mem_deep` depth sweep**. **`mem_flat`** is
  opt-in via `--enable-mem-flat`: its wall time is dominated by single-directory
  filesystem serialization (borne by all tools) and it yields no distinctive
  memory signal for copy2, so it is not run by default.

`mem_deep` (`5d`) holds file count (`MEM_DEEP_COUNT`, default 200k) and size
(2KB) fixed and sweeps directory **depth** (`MEM_DEEP_DEPTHS`, default 2/4/8/16;
path length ≈ depth × `MEM_DEEP_COMP_LEN`). It isolates per-path storage: copy2
shares path prefixes via its `StringPart` chain, so its per-path memory should
grow sublinearly in depth. Each depth is a row whose `variant` column carries
the depth (`depth2`…`depth16`); `plot.py` renders `mem_deep.png` (peak PSS vs
depth).

`size_scaling` (`4c`) holds the file count fixed at `SIZE_SWEEP_COUNT` (default
100k, independent of `--scale`) and sweeps the per-file size in 6 ~8x steps —
2KB, 16KB, 128KB, 1MB, 8MB, 64MB — to show how peak memory and throughput scale
with file size (the complement of `mem_scaling`). It is gated for the same
reason as `4b`: the 64MB step alone is 100k×64MiB ≈ 6.4 TiB of source data.
**rsync is excluded** here too. Pass `--enable-size-sweep` (and `--generate`) to
create/run it, e.g.:

```bash
run_benchmarks.sh --only size_scaling --enable-size-sweep --generate
```

Each step is a separate row with its byte size in the CSV `filesize_bytes`
column (`2048/16384/131072/1048576/8388608/67108864`). Set `SIZE_SWEEP_COUNT` in
the environment to change the fixed file count.

**File content (`--content`).** `gen_dataset.py` writes all-zero, byte-identical
files by default (`--content zero`) — cheap to generate and fine for the
metadata- and memory-bound benches (peak PSS/RSS is independent of byte
content). But a dedup/compressing backend (e.g. VAST) collapses zero/identical
files to a single block, so reads come from cache and writes never hit media,
**inflating throughput** on the bandwidth-bound benches. The runner therefore
generates the bandwidth-bound datasets — `transfer` (`4a`), `transfer_1tb`
(`4b`), and `size_scaling` (`4c`) — with `--content random` (per-file unique,
incompressible bytes via `os.urandom`). Metadata/memory datasets stay on zeros.
Note: `--generate-missing` skips datasets whose `src/` already exists, so
regenerating a previously zero-filled throughput dataset as random requires
`--generate` (force) or removing the old dataset first.

Per-bench tool exclusion is handled by an optional last argument to `run_one`
(e.g. `rsync`); the excluded tool still runs in every other bench unless you
also drop it from `--tools`.

## Concurrency knobs (this is the important part)

Each tool is configured independently, and each pipeline **phase** has its own
knob (a single global knob hides real behaviour — crawl/stat phases want high
concurrency, the write phase often wants low):

| tool | per-tool | per-phase |
|---|---|---|
| copy2  | `--copy2-conc`  | `--copy2-crawlers` / `--copy2-transfers` / `--copy2-finishers` |
| rclone | `--rclone-conc` | `--rclone-checkers` (crawl/compare) / `--rclone-transfers` (copy) |
| fpsync | `--fpsync-conc` | (fpart crawls serially; conc = rsync worker count) |
| rsync  | — (single-stream) | — |

Rules: `--conc N` sets the global default for all tools; `--<tool>-conc N`
overrides one tool; a `--<tool>-<phase>` flag overrides one phase. Anything left
unset inherits the level above it. Every phase value is recorded in the CSV
`conc` column, e.g. `cr16/tr4/fp8` (copy2) or `ck128/tr32` (rclone).

Other per-tool overrides: `--<tool>-bin PATH` (binary), `--<tool>-opts "..."`
(extra flags), block/buffer via `--block-mb` or `COPY2_BLOCK`/`RCLONE_BUFFER`.
Every flag has an env equivalent (`COPY2_CRAWLERS`, `RCLONE_CHECKERS`, `WORK`,
`DATA_ROOT`, ...); see the header comment in `run_benchmarks.sh` or `--help`.

## Output

CSV columns:
`ts,bench,variant,tool,nfiles,filesize_bytes,conc,wall_s,peak_pss_mb,peak_rss_mb,rc,mem_method`

`plot.py` writes `wall_by_bench.png`, `peak_by_bench.png`, and (if the memory
benches ran) `mem_scaling.png`.

## Notes for the cluster

- Point `DATA_ROOT`/`WORK` at the real target filesystem — that's what you're
  actually benchmarking. `WORK` needs room for copy2's data-dir and fpsync's
  part queue; `DATA_ROOT` holds src+dst plus every retired dst tree, since
  deletions are now deferred to the end of the suite (one retired dst per
  tool/bench beyond the first). Budget for the peak accordingly — with N tools
  it can approach src + N×dst for the largest bench before the final cleanup
  runs.
- The optimal concurrency is **hardware-specific**. On the single-NVMe dev box
  copy2 preferred *low* transfer concurrency (4) and high crawlers (16); on a
  parallel/networked FS it will want far more transfer concurrency. Re-sweep
  there rather than trusting these numbers.
- Memory is measured with the rootless PSS sampler by default (`memsample.py`),
  which correctly sums multi-process tools (fpsync/rsync). `--mem-method cgroup`
  uses `systemd-run` for exact `memory.peak` but needs cgroup memory delegation.
