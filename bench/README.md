# copy2 benchmark suite

Compares `copy2` against `rsync`, `rclone`, and `fpsync` on the scenarios in
`../benchmarks.txt` (crawl / metadata / hardlinks / transfer / memory), recording
**wall time**, **peak memory** (PSS + RSS of the whole process tree), and
optionally **energy** (per-run joules + average watts) to a CSV, plus plots.

This README is the reproduction reference for the paper. Every headline number
was produced by `run_benchmarks.sh` with a specific set of flags; the exact
invocations, dataset shapes, and tunings are documented below. (We also keep
thin per-scenario `sbatch` wrappers around these commands, but those are local
and not part of the repo — reproduce from the `run_benchmarks.sh` command lines
here.)

## Files

| file | purpose |
|---|---|
| `run_benchmarks.sh` | orchestrator: generates datasets, runs each tool, writes CSV |
| `gen_dataset.py`    | deterministic dataset generator (called by the runner) |
| `memsample.py`      | rootless peak PSS/RSS (+ optional power) sampler for a process subtree |
| `plot.py`           | turns a results CSV into PNG charts |
| `results_tuned.csv`, `results_phase.csv`, `plots_tuned/`, `plots_phase/` | example results/plots from the dev box (regenerate on your cluster) |

## Prerequisites

- **`copy2` built from current source** (`ninja copy2` in `../../build`). The
  paper numbers require two fixes that live in the source tree: the fixed-block
  scheduler decoupling and the non-blocking `try_dequeue` **tail-drain fix**
  (`queue.h` + `copy2.cpp`). An old binary will understate large-file
  throughput. The runner auto-discovers `../../copy2`, or set `COPY2=/path`.
- `rsync`, `rclone`, and `fpsync` on `PATH` (fpsync also needs `fpart` on `PATH`
  **at run time**). Any tool whose binary is missing is skipped, not fatal.
- `python3` with `matplotlib` (only needed for `plot.py`).
- **Root** (passwordless `sudo` on the cluster) for:
  - cold-cache runs (`--drop-caches`),
  - raising the libaio limit — copy2 needs `sudo sysctl -w fs.aio-max-nr=1000000000`
    before any large transfer, and a high `ulimit -n` (raise soft to hard) for
    large `--max-FDs`,
  - power sampling (`--power` shells out to `sudo -n ipmitool dcmi power reading`).

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

# 2. run everything, cold caches, with the CLUSTER/PAPER tuning
sudo sysctl -w fs.aio-max-nr=1000000000
./run_benchmarks.sh --scale 1000000 --drop-caches 3 \
    --copy2-crawlers 128 --copy2-transfers 8 --copy2-finishers 128 \
    --rclone-checkers 128 --rclone-transfers 128 \
    --fpsync-conc 128 \
    --results results.csv

# 3. plot
python3 plot.py --csv results.csv --outdir plots
```

The concurrency above (`cr128/tr8/fp128`, `ck128/tr128`, `fp128`) is the config
used for the paper on the VAST cluster. **Optimal concurrency is
hardware-specific** — on the single-NVMe dev box copy2 preferred *low* transfer
concurrency (`--copy2-transfers 4`) and high crawlers; re-sweep on your storage
rather than trusting these numbers.

`--drop-caches N` flushes the filesystem containing the current destination,
then writes `N` to `/proc/sys/vm/drop_caches` before every run
(`1`=pagecache, `2`=dentries/inodes, `3`=both — use **3** for a truly cold
comparison). The filesystem-scoped flush avoids waiting for unrelated mounts
on a shared node. Omit the flag to run warm.

Empty destinations are reset by renaming the old tree aside (O(1) on the same
FS) and queuing it; the million-file `rm` is **deferred to the end of the suite**
so deletion IO/CPU never contends with a timed tool. `--reset-immediate` instead
deletes each retired tree in place right away (parallel `find | xargs rm`) — use
it for the huge-file sweeps where keeping every retired dst on disk would blow
the budget. Metadata resets chmod in place (parallel walk). `--reset-jobs N`
controls cleanup/chmod/mtime concurrency and defaults to `--conc`.

## Benchmarks and gating

Bench names for `--only`: `crawling metadata hardlinks transfer mem memstress
transfer_1tb size_scaling big_sweep incr_sweep`.

```bash
--only crawling,metadata,hardlinks,transfer   # skip the (slow) memory benches
--only mem,memstress                          # only the memory benches
--tools copy2,rclone                          # only some tools
--scale 100000                                # smaller/faster
```

### Always-on core benches

- **`crawling`** — 1M files, 2KB, dst = **byte-identical mirror** (same content,
  mode, mtime): nothing to transfer, so wall time is pure parallel traversal +
  `statx` compare. Generated from the **`deep`** profile (deep nesting + long
  names), because a shallow uniform tree ties all tools while a deep/irregular
  tree is where copy2's parallel traversal pulls ahead (as on real home dirs).
  Tree shape is env-tunable: `CRAWL_FPC` (files per chain — smaller ⇒ many more
  directories ⇒ more parallel `readdir` pressure), `CRAWL_DEPTH`,
  `CRAWL_COMP_LEN` (component length; default 24 keeps paths ~350 chars).
- **`metadata`** — 1M files, 2KB, dst identical in content+mtime but **different
  mode** (`perms`), forcing a metadata-only update (chmod/utimensat) on every
  file with zero data movement.
- **`hardlinks`** — 1M-file hardlink-heavy tree (hardlink graph reconstruction).
- **`transfer`** — 1M files, 2MB each, empty dst — the bandwidth-bound copy.
  Generated with **`--content random`** (see *File content* below).

### Memory benches (`mem`, `memstress`)

Defaults are tuned to keep the suite fast and the results interpretable:

- **`mem`** runs only **`mem_hlinks`** by default. **`mem_scaling`** is opt-in
  (`--enable-mem-scaling`): a true file-*count* sweep at a fixed **2KB** at every
  point (10k/100k/1M). **rsync excluded** (single-stream, hours on the top point).
- **`memstress`** runs the **`mem_deep` depth sweep** by default. **`mem_flat`**
  (single-directory stress) is opt-in (`--enable-mem-flat`): its wall time is
  dominated by single-directory FS serialization borne by all tools and it gives
  no distinctive copy2 memory signal.

**`mem_deep`** holds file count (`MEM_DEEP_COUNT`, default 200k) and size (2KB)
fixed and sweeps directory **depth** (`MEM_DEEP_DEPTHS`, default 2/4/8/16; path
length ≈ depth × `MEM_DEEP_COMP_LEN`). It isolates per-path storage: copy2 shares
path prefixes via its `StringPart` chain, so its per-path memory grows
sublinearly in depth. Each depth is a row whose `variant` column carries the
depth (`depth2`…`depth16`); `plot.py` renders `mem_deep.png`.

### Throughput / size sweeps (opt-in)

- **`transfer_1tb`** — 1000 × 1 GiB files. Gated: `--enable-1tb` (+ `--generate`).
- **`size_scaling`** — holds the file count fixed at `SIZE_SWEEP_COUNT` (default
  100k, independent of `--scale`) and sweeps per-file size in 6 ~8× steps:
  **2KB, 16KB, 128KB, 1MB, 8MB, 64MB**. Gated (`--enable-size-sweep`) because the
  64MB step alone is 100k×64MiB ≈ 6.4 TiB. **rsync excluded.** Each step is a row
  with its `filesize_bytes`. `SIZE_SWEEP_COUNT` overridable via env.
- **`big_sweep`** — few huge files: `BIG_SWEEP_COUNT` (default 100) files at
  per-file **{1,2,4,8} GB** (total {0.1,0.2,0.4,0.8} TB). Gated
  (`--enable-big-sweep`). Each point is **gen→run→wipe in place** so peak disk is
  a single point. rsync excluded; `--content random`.
- **`incr_sweep`** — `size_scaling` **as an incremental sync**: dst is generated
  as a byte-identical mirror and before each tool ~10% of dst files have their
  mtime shifted (`reset_mtime10`), so only that 10% is re-transferred. Isolates
  crawl + compare + delta-detection cost. Gated (`--enable-incr-sweep`);
  gen→run→wipe per point. Size points default to the full size sweep but can be
  restricted for disk with env lists `INCR_SWEEP_BYTES_LIST` /
  `INCR_SWEEP_TAGS_LIST` (space-separated, 1:1); file count via
  `INCR_SWEEP_COUNT`. Note: with mtime-only perturbation the re-copied 10% is
  byte-identical to what's already on the target, so on a dedup backend those
  writes collapse — this measures **metadata/delta cost, not write bandwidth**.

Example:

```bash
run_benchmarks.sh --only size_scaling --enable-size-sweep --generate
```

### File content (`--content`)

`gen_dataset.py` writes all-zero, byte-identical files by default
(`--content zero`) — cheap and fine for metadata/memory benches (peak PSS/RSS is
independent of byte content). But a dedup/compressing backend (e.g. VAST)
collapses zero/identical files to a single block, so reads come from cache and
writes never hit media, **inflating throughput** on bandwidth-bound benches. The
runner therefore generates the bandwidth-bound datasets — `transfer`,
`transfer_1tb`, `size_scaling`, `big_sweep`, `incr_sweep` — with
**`--content random`** (per-file unique, incompressible `os.urandom`).
Metadata/memory datasets stay on zeros. `--generate-missing` skips datasets whose
`src/` already exists, so re-rolling a zero dataset as random needs `--generate`
(force) or removing it first.

Per-bench tool exclusion is an optional last argument to `run_one` (e.g.
`rsync`); the excluded tool still runs in every other bench unless you also drop
it from `--tools`.

## Concurrency knobs

Each tool is configured independently, and each pipeline **phase** has its own
knob (a single global knob hides real behaviour — crawl/stat phases want high
concurrency, the write phase often wants low):

| tool | per-tool | per-phase |
|---|---|---|
| copy2  | `--copy2-conc`  | `--copy2-crawlers` / `--copy2-transfers` / `--copy2-finishers` |
| rclone | `--rclone-conc` | `--rclone-checkers` (crawl/compare) / `--rclone-transfers` (copy) |
| fpsync | `--fpsync-conc` | (fpart crawls serially; conc = rsync worker count) |
| rsync  | — (single-stream) | — |

Rules: `--conc N` sets the global default; `--<tool>-conc N` overrides one tool;
a `--<tool>-<phase>` flag overrides one phase. Anything unset inherits the level
above. Every phase value is recorded in the CSV `conc` column, e.g. `cr128/tr8/fp128`
(copy2) or `ck128/tr128` (rclone).

Other per-tool overrides: `--<tool>-bin PATH`, `--<tool>-opts "..."`. Every flag
has an env equivalent (`COPY2_CRAWLERS`, `RCLONE_CHECKERS`, `WORK`, `DATA_ROOT`,
…); see the header comment in `run_benchmarks.sh`.

## Block / buffer size (copy2 vs rclone are deliberately decoupled)

copy2's `--max-block-size` is its **unit of parallel work** (files are split into
blocks driven concurrently via `O_DIRECT`+libaio), *not* a read buffer. Making it
large starves intra-file parallelism (a block sweep on 64MB files showed
throughput falling monotonically from ~10.7 GiB/s at 1MB to ~6.6 GiB/s at 128MB).
So copy2 always uses a **small fixed** block, `COPY2_BLOCK` (default 1MB).

rclone's `--buffer-size` *is* a read-ahead buffer and benefits from being large
on big files, so the runner **tiers it by per-file size** (`block_for_size`):
≤1 MiB → `BLOCK_MB` (default 1), >1 MiB & <10 MiB → 4, ≥10 MiB → 128 MiB.

Overrides: `--block-mb N` (sets both baselines), or env `COPY2_BLOCK`
(e.g. `2MB`) / `RCLONE_BUFFER`. Unifying the two — as an earlier version did —
kneecaps copy2, so keep them separate.

## Power / energy (`--power`)

Pass `--power` to annotate every run with energy. `memsample.py` reads node power
from the BMC (`sudo -n ipmitool dcmi power reading`, **watts** — there is no
cumulative-joule register) at the start and end of each run and records
`energy_j = mean(start,end) × wall_s` plus `avg_watts`. It prefers the BMC's
"Average power reading over sample period" and retries on transient failures.

When `--power` is set the CSV header gains `node,t0,t1,energy_j,avg_watts`.

Caveats (documented from our runs):
- The reading is **whole-node** chassis power and, for I/O-bound work, is
  dominated by the node baseline (~250 W here), roughly constant across tools —
  so it does **not** discriminate instantaneous wattage between tools. The
  meaningful metric is **energy per byte (J/GiB)**, which tracks wall time at
  ~constant power. Report *energy*, not a per-tool wattage claim.
- Some nodes ship with DCMI power reading **deactivated** (`ipmitool` returns
  0 W and `dcmi power activate` fails); on those nodes energy comes back 0. If
  you need energy, confirm on the node first, or add finer-grained RAPL
  (`/sys/class/powercap/intel-rapl`) package-energy sampling.

`memsample.py --smaps` additionally dumps a per-mapping RSS attribution at peak
RSS (categorised) — used to attribute copy2's memory floor (arena / copy buffer /
queues) and to verify the THP `MADV_NOHUGEPAGE` mitigation.

## Cross-filesystem runs (`--src-root`)

By default src and dst live under `DATA_ROOT/<dataset>/{src,dst}`. To benchmark a
**cross-FS** copy (e.g. Lustre source → VAST destination), set `--src-root
/other/fs/...` (or `SRC_ROOT`): the src tree is generated and read there while the
dst stays under `DATA_ROOT`.

## Output

CSV columns (base):
`ts,bench,variant,tool,nfiles,filesize_bytes,conc,wall_s,peak_pss_mb,peak_rss_mb,rc,mem_method`

with `--power`, five more are appended: `node,t0,t1,energy_j,avg_watts`.

`plot.py` writes `wall_by_bench.png`, `peak_by_bench.png`, `mem_deep.png`, and
(if applicable) `mem_scaling.png`. It creates the output dir if needed.

## Reproducing the paper numbers

Set `DATA_ROOT`/`WORK` (and `COPY2` if not auto-discovered), raise
`fs.aio-max-nr`, then run the tracked orchestrator with the flags below. All
measured runs use `--drop-caches 3` for a cold comparison and the cluster
concurrency (`cr128/tr8/fp128`, `ck128/tr128`, `fp128`). Point each at its own
`--results` file so runs don't clobber each other. Wrap in `sbatch` (whole node,
non-preemptible QOS) as you see fit.

```bash
export DATA_ROOT=/fast/fs/copy2-bench-data WORK=/fast/fs/copy2-bench-work
sudo sysctl -w fs.aio-max-nr=1000000000
CONC="--copy2-crawlers 128 --copy2-transfers 8 --copy2-finishers 128 \
      --rclone-checkers 128 --rclone-transfers 128 --fpsync-conc 128"

# Core suite: crawl + metadata + hardlinks + transfer (+ memory benches)
./run_benchmarks.sh --scale 1000000 --drop-caches 3 --generate-missing \
    $CONC --results results.core.csv

# Deep-tree crawl (copy2's traversal advantage). Cross-FS: add --src-root /lustre/...
CRAWL_FPC=2 ./run_benchmarks.sh --only crawling --scale 1000000 --drop-caches 3 \
    --tools copy2,rclone $CONC --generate-missing --results results.crawl.csv

# File-size throughput sweep (2KB..64MB, 100k files)
./run_benchmarks.sh --only size_scaling --enable-size-sweep --drop-caches 3 \
    $CONC --generate-missing --results results.size.csv

# Throughput + ENERGY over size sweep and big files ({1,2,4,8} GB)
./run_benchmarks.sh --only size_scaling,big_sweep \
    --enable-size-sweep --enable-big-sweep --power --reset-immediate \
    --drop-caches 3 $CONC --generate-missing --results results.power.csv

# Incremental 10%-delta sync, 1M files at {2KB,16KB,128KB}, energy-annotated
INCR_SWEEP_COUNT=1000000 INCR_SWEEP_BYTES_LIST="2048 16384 131072" \
INCR_SWEEP_TAGS_LIST="2k 16k 128k" \
./run_benchmarks.sh --only incr_sweep --enable-incr-sweep --power \
    --drop-caches 3 $CONC --results results.incr.csv
```

## Notes for the cluster

- Point `DATA_ROOT`/`WORK` at the real target filesystem — that's what you're
  actually benchmarking. `WORK` needs room for copy2's data-dir and fpsync's
  part queue. For the deferred-delete default, `DATA_ROOT` holds src+dst **plus
  every retired dst tree** until the suite ends — with N tools it approaches
  src + N×dst for the largest bench. Use `--reset-immediate` (and the
  gen→run→wipe sweeps) to bound peak disk for the huge-file cases.
- copy2's peak memory is a **fixed floor** (its bounded queues sized by
  `--max-FDs`, the reserved copy buffer, and the arena), largely independent of
  file count/size — ~600 MB–2.6 GB depending on config — whereas rclone's memory
  grows with in-flight file size (it can reach ~13 GB buffering GB-files ×
  transfers). This is the memory story to report, and it is not a leak (verified
  with the `ALLOC_DEBUG` live/total allocation counters).
- Memory is measured with the rootless PSS sampler by default (`memsample.py`),
  which correctly sums multi-process tools (fpsync/rsync). `--mem-method cgroup`
  uses `systemd-run` for exact `memory.peak` but needs cgroup memory delegation.
- SLURM: request a whole node (`--exclusive`) for clean memory/power numbers, use
  a non-preemptible QOS, and lower `--time` to improve backfill (a single pending
  job can't be reordered with `scontrol top`).
