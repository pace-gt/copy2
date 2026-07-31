#!/usr/bin/env bash
#
# copy2 benchmark harness: runs the benchmarks.txt scenarios against copy2 and
# the competitors (rsync, rclone, fpsync) and records wall time + peak memory
# to a CSV.
#
# Benchmarks (see ../benchmarks.txt):
#   1  crawling      identical DEEP trees, no data moved -> crawl/compare cost
#   2  metadata      identical except mode           -> metadata-fix cost
#   3  hardlinks     many hardlinks, empty dst
#   4a transfer      1e6 x 2kb, empty dst
#   4b transfer_1tb  1000 x 1GiB, empty dst          -> GATED (opt-in only)
#   4c size_scaling  100k files, 6 sizes 2KB->64MB   -> GATED (opt-in only)
#                    peak mem + wall vs per-file size at fixed file count
#   5a mem_scaling   transfer at scale/100, /10, /1  -> peak RSS vs file count
#                    GATED (--enable-mem-scaling); fixed 2KB at every point;
#                    rsync excluded (single-stream, hours on the top point)
#   5b mem_hardlinks hardlink tree                   -> hlink-table memory
#   5c mem_flat      1e6 files in ONE directory      -> GATED (--enable-mem-flat)
#                    (single-dir wall time is FS-bound and not a memory signal)
#   5d mem_deep      deep tree, depth SWEEP          -> per-path storage vs depth
#   (5c/5d are the "memstress" group; 5d always runs, 5c is opt-in)
#
# Per-tool tuning (env or flags): --conc / --block-mb set globals; override
# concurrency per tool with --copy2-conc/--rclone-conc/--fpsync-conc (or the
# COPY2_CONC/RCLONE_CONC/FPSYNC_CONC env vars) and block size with
# COPY2_BLOCK/RCLONE_BUFFER.
# NOTE: put --conc before any per-tool --*-conc flag, since --conc overwrites
# all three.
#
# Per-PHASE concurrency (finer than per-tool; each phase has its own pool):
#   copy2 :  --copy2-crawlers / --copy2-transfers / --copy2-finishers
#   rclone:  --rclone-checkers (crawl/compare) / --rclone-transfers (copy)
#   fpsync:  --fpsync-conc sets the rsync worker count (fpart crawls serially)
#   rsync :  single-stream, no phase knobs
# (env equivalents: COPY2_CRAWLERS/COPY2_TRANSFERS/COPY2_FINISHERS,
#  RCLONE_CHECKERS/RCLONE_TRANSFERS.) A phase left unset inherits its tool's
# conc; an explicit phase value overrides it. The CSV 'conc' column records the
# per-phase breakdown, e.g. copy2 "cr16/tr8/fp4", rclone "ck128/tr64".
#
# Per-tool command (fully configurable per tool):
#   binary:  --copy2-bin/--rsync-bin/--rclone-bin/--fpsync-bin  (or COPY2/RSYNC/
#            RCLONE/FPSYNC env vars) point at a specific executable.
#   options: --copy2-opts/--rsync-opts/--rclone-opts/--fpsync-opts  (or COPY2_OPTS/
#            RSYNC_OPTS/RCLONE_OPTS/FPSYNC_RSYNC_OPTS env vars) replace that tool's
#            extra option string (word-split). fpsync's opts are handed to its
#            inner rsync via -o. These stack on top of the harness-managed
#            conc/block flags and the src/dst args.
#
# Memory: default is a rootless PSS sampler (memsample.py) that sums the whole
# process tree (fair to multi-process fpsync/rsync). On this box the cgroup
# memory controller is NOT delegated to the user session, so --mem-method cgroup
# needs sudo.  See notes at the bottom.
#
# Fairness knobs are equalized where a tool exposes them (see CONC + *_OPTS).
# rsync has no parallelism knob, so it always runs single-stream (conc=1).
#
# Usage:
#   run_benchmarks.sh --generate                 # (re)build datasets, then run all
#   run_benchmarks.sh --generate-missing         # build only datasets not present, then run
#   run_benchmarks.sh --only transfer,mem        # run a subset
#   run_benchmarks.sh --tools copy2,rsync        # subset of tools
#   run_benchmarks.sh --scale 500 --generate     # tiny smoke run
#   run_benchmarks.sh --conc 8 --rclone-conc 24  # per-tool concurrency override
#   run_benchmarks.sh --copy2-crawlers 16 --copy2-transfers 8 --copy2-finishers 4
#   run_benchmarks.sh --rclone-checkers 128 --rclone-transfers 32  # phase knobs
#   run_benchmarks.sh --copy2-bin /path/copy2 --copy2-opts "--readback"  # per-tool cmd
#   run_benchmarks.sh --rclone-opts "--stats 0 --checksum" --rsync-opts "-aH --inplace"
#   run_benchmarks.sh --enable-1tb --generate    # include the 1TiB case
#   run_benchmarks.sh --only size_scaling --enable-size-sweep --generate  # 2KB..2MB sweep
#   run_benchmarks.sh --only mem --enable-mem-scaling --generate  # opt-in count sweep
#   run_benchmarks.sh --only memstress --enable-mem-flat --generate  # opt-in flat case
#   run_benchmarks.sh --mem-method cgroup        # exact peaks (needs sudo here)
#   run_benchmarks.sh --drop-caches              # cold caches (sync+drop 2) per run
#   run_benchmarks.sh --drop-caches 3            # drop pagecache+dentries+inodes
#   run_benchmarks.sh --reset-jobs 32             # parallel destination cleanup
#
# Empty destinations: never rm a million-file tree on the critical path between
# tools. Rename the old dst aside (O(1) on the same FS) and delete it in the
# background while the next measured run proceeds. Metadata resets still need a
# parallel chmod walk (modes must change in place).
#
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- defaults (override via flags or env) ---------------------------------
DATA_ROOT="${DATA_ROOT:-$HOME/copy2-bench-data}"
# Source datasets default to living under DATA_ROOT (src+dst colocated). Set
# SRC_ROOT (or --src-root) to place sources on a DIFFERENT filesystem than the
# destination, e.g. Lustre src -> VAST dst, to expose cross-FS behavior.
SRC_ROOT="${SRC_ROOT:-}"
WORK="${WORK:-$HOME/copy2-bench-work}"
RESULTS="${RESULTS:-$HERE/results.csv}"
COPY2="${COPY2:-$(find "$HERE/../.." -name copy2 -type f 2>/dev/null | head -1)}"
CONC="${CONC:-8}"                 # global concurrency default (per-tool below)
BLOCK_MB="${BLOCK_MB:-1}"        # global transfer block/buffer size in MiB
SCALE="${SCALE:-1000000}"        # file count for the 1e6-class datasets
INTERVAL="${INTERVAL:-0.05}"     # memsample interval (s)
MEM_METHOD="${MEM_METHOD:-sampler}"   # sampler | cgroup
DROP_CACHES="${DROP_CACHES:-}"   # if set (e.g. 2) sync+drop caches before each run
RESET_JOBS="${RESET_JOBS:-}"     # empty -> CONC after argument parsing
# Reclaim each retired dst synchronously (before the next measured run) instead
# of deferring its deletion to the end of the suite. Deferring accumulates every
# dst copy on disk; harmless for small files but fatal for large-file sweeps
# (e.g. 64MB x 100k), so enable it there. Setup gets slower; fairness is kept
# because the rm finishes before the next run is measured.
RESET_IMMEDIATE="${RESET_IMMEDIATE:-0}"
GENERATE=0
GENERATE_MISSING=0
ENABLE_1TB=0
ENABLE_SIZE_SWEEP=0
ENABLE_BIG_SWEEP=0
ENABLE_INCR_SWEEP=0
ENABLE_MEM_SCALING=0
ENABLE_MEM_FLAT=0
ONLY=""
# ---- power/energy accounting (optional, --power) --------------------------
# With --power, each measured run is annotated with the node's energy. There is
# no cumulative-joule register on this BMC (`ipmitool dcmi power reading` reports
# WATTS), so memsample reads power at the start and finish of the run window and
# reports energy = mean(start,end) * wall_s plus avg_watts. Requires passwordless
# `sudo ipmitool` on the node (true on the compute nodes). Only the sampler
# mem-method carries power; the cgroup path leaves the columns blank.
POWER_ENABLED="${POWER_ENABLED:-0}"
TOOLS="copy2,rsync,rclone,fpsync"

RSYNC="${RSYNC:-$(command -v rsync)}"
RCLONE="${RCLONE:-$(command -v rclone)}"
# fpsync must be on PATH (it also needs `fpart` on PATH at run time). If it
# lives elsewhere, set FPSYNC=/path/to/fpsync or pass --fpsync-bin; a tool whose
# binary is missing is skipped gracefully.
FPSYNC="${FPSYNC:-$(command -v fpsync)}"

# ---- per-tool concurrency (default to global CONC; rsync has no knob) -------
# Per-tool concurrency: each defaults to the global CONC but is independently
# settable via env (COPY2_CONC/RCLONE_CONC/FPSYNC_CONC) or the per-tool flags
# (--copy2-conc/--rclone-conc/--fpsync-conc). rsync has no parallelism knob.
COPY2_CONC="${COPY2_CONC:-$CONC}"
RCLONE_CONC="${RCLONE_CONC:-$CONC}"
FPSYNC_CONC="${FPSYNC_CONC:-$CONC}"

# ---- per-PHASE concurrency (finer than the per-tool conc above) -------------
# Real tools have separate pools per pipeline phase, so expose them:
#   copy2 :  --crawlers / --transfers / --finish-processors
#   rclone:  --checkers (crawl/compare) / --transfers (copy)
#   fpsync:  fpart crawls single-threaded; -n sets the rsync worker count
#   rsync :  single-stream, no phase knobs
# Each phase defaults to its tool's conc (set below, after arg parsing, so that
# --copy2-conc / --conc still propagate). Override via env
# (COPY2_CRAWLERS/COPY2_TRANSFERS/COPY2_FINISHERS, RCLONE_CHECKERS/RCLONE_TRANSFERS)
# or the matching --copy2-crawlers/--copy2-transfers/--copy2-finishers and
# --rclone-checkers/--rclone-transfers flags. An explicit phase value always
# wins over the tool-wide conc.
COPY2_CRAWLERS="${COPY2_CRAWLERS:-}"
COPY2_TRANSFERS="${COPY2_TRANSFERS:-}"
COPY2_FINISHERS="${COPY2_FINISHERS:-}"
RCLONE_CHECKERS="${RCLONE_CHECKERS:-}"
RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-}"

# ---- per-tool block/buffer size (rsync/fpsync have no transfer block) -------
# copy2:  --max-block-size ; rclone: --buffer-size
# rsync:  --block-size is delta-only and inert for local whole-file copies, so
#         it is only passed if RSYNC_BLOCK is explicitly set.
# fpsync: no transfer block; FPSYNC_CHUNK maps to fpart's -s partition size.
COPY2_BLOCK="${COPY2_BLOCK:-${BLOCK_MB}MB}"
COPY2_BUFFER="${COPY2_BUFFER:-}"        # empty -> copy2 default
RCLONE_BUFFER="${RCLONE_BUFFER:-${BLOCK_MB}Mi}"
RSYNC_BLOCK="${RSYNC_BLOCK:-}"          # empty -> not passed
FPSYNC_CHUNK="${FPSYNC_CHUNK:-}"        # empty -> not passed

# Extra, tool-specific opts (env-overridable).
# rclone: --ignore-checksum skips its default post-copy checksum verification
# (it otherwise hashes src+dst after every file). copy2 (no --readback) and
# rsync don't verify, so this keeps the comparison fair.
COPY2_OPTS="${COPY2_OPTS:-}"
RSYNC_OPTS="${RSYNC_OPTS:--a}"
RCLONE_OPTS="${RCLONE_OPTS:---stats 0 --ignore-checksum -M}"
FPSYNC_RSYNC_OPTS="${FPSYNC_RSYNC_OPTS:--lptgoD}"

# ---- arg parsing ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --generate)     GENERATE=1 ;;
        --generate-missing) GENERATE_MISSING=1 ;;
        --enable-1tb)   ENABLE_1TB=1 ;;
        --enable-size-sweep) ENABLE_SIZE_SWEEP=1 ;;
        --enable-big-sweep)  ENABLE_BIG_SWEEP=1 ;;
        --enable-incr-sweep) ENABLE_INCR_SWEEP=1 ;;
        --power)             POWER_ENABLED=1 ;;
        --enable-mem-scaling) ENABLE_MEM_SCALING=1 ;;
        --enable-mem-flat)    ENABLE_MEM_FLAT=1 ;;
        --only)         ONLY="$2"; shift ;;
        --tools)        TOOLS="$2"; shift ;;
        --scale)        SCALE="$2"; shift ;;
        --conc)         CONC="$2"; COPY2_CONC="$2"; RCLONE_CONC="$2"; FPSYNC_CONC="$2"; shift ;;
        --copy2-conc)   COPY2_CONC="$2"; shift ;;
        --rclone-conc)  RCLONE_CONC="$2"; shift ;;
        --fpsync-conc)  FPSYNC_CONC="$2"; shift ;;
        # per-phase concurrency (override the tool-wide conc for one phase)
        --copy2-crawlers)   COPY2_CRAWLERS="$2"; shift ;;
        --copy2-transfers)  COPY2_TRANSFERS="$2"; shift ;;
        --copy2-finishers)  COPY2_FINISHERS="$2"; shift ;;
        --rclone-checkers)  RCLONE_CHECKERS="$2"; shift ;;
        --rclone-transfers) RCLONE_TRANSFERS="$2"; shift ;;
        # per-tool binary override (full path to the executable)
        --copy2-bin)    COPY2="$2"; shift ;;
        --rsync-bin)    RSYNC="$2"; shift ;;
        --rclone-bin)   RCLONE="$2"; shift ;;
        --fpsync-bin)   FPSYNC="$2"; shift ;;
        # per-tool extra options (word-split; replaces that tool's opt string).
        # e.g. --copy2-opts "--readback --sparse", --rsync-opts "-aH --inplace",
        #      --rclone-opts "--stats 0 --ignore-checksum --checksum",
        #      --fpsync-opts "-lptgoD -H"  (passed to fpsync's inner rsync via -o)
        --copy2-opts)   COPY2_OPTS="$2"; shift ;;
        --rsync-opts)   RSYNC_OPTS="$2"; shift ;;
        --rclone-opts)  RCLONE_OPTS="$2"; shift ;;
        --fpsync-opts)  FPSYNC_RSYNC_OPTS="$2"; shift ;;
        --block-mb)     BLOCK_MB="$2"; COPY2_BLOCK="${2}MB"; RCLONE_BUFFER="${2}Mi"; shift ;;
        --data-root)    DATA_ROOT="$2"; shift ;;
        --src-root)     SRC_ROOT="$2"; shift ;;
        --results)      RESULTS="$2"; shift ;;
        --mem-method)   MEM_METHOD="$2"; shift ;;
        --drop-caches)  DROP_CACHES=2
                        [[ "${2:-}" =~ ^[123]$ ]] && { DROP_CACHES="$2"; shift; } ;;
        --reset-jobs)   RESET_JOBS="$2"; shift ;;
        --reset-immediate) RESET_IMMEDIATE=1 ;;
        --interval)     INTERVAL="$2"; shift ;;
        -h|--help)      grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "unknown arg: $1" >&2; exit 2 ;;
    esac
    shift
done

# Resolve per-phase concurrency now that all conc flags have been parsed: any
# phase not set explicitly (env or --*-<phase>) inherits its tool-wide conc.
COPY2_CRAWLERS="${COPY2_CRAWLERS:-$COPY2_CONC}"
COPY2_TRANSFERS="${COPY2_TRANSFERS:-$COPY2_CONC}"
COPY2_FINISHERS="${COPY2_FINISHERS:-$COPY2_CONC}"
RCLONE_CHECKERS="${RCLONE_CHECKERS:-$RCLONE_CONC}"
RCLONE_TRANSFERS="${RCLONE_TRANSFERS:-$RCLONE_CONC}"
RESET_JOBS="${RESET_JOBS:-$CONC}"
SRC_ROOT="${SRC_ROOT:-$DATA_ROOT}"   # empty -> colocate src with dst

[[ "$RESET_JOBS" =~ ^[1-9][0-9]*$ ]] ||
    { echo "--reset-jobs must be a positive integer" >&2; exit 2; }

[[ -z "$COPY2" || ! -x "$COPY2" ]] && { echo "copy2 binary not found (set COPY2=)" >&2; exit 1; }
mkdir -p "$DATA_ROOT" "$WORK"
[[ "$SRC_ROOT" != "$DATA_ROOT" ]] && { mkdir -p "$SRC_ROOT"; echo "== cross-FS: src=$SRC_ROOT  dst=$DATA_ROOT =="; }

# Map a dst-side dataset root ($DATA_ROOT/<name>) to its src dir, which lives
# under SRC_ROOT when running cross-FS (else colocated at <root>/src).
src_dir_for() {  # src_dir_for DATASET_ROOT -> echoes the src directory
    local root="$1"
    if [[ "$SRC_ROOT" == "$DATA_ROOT" ]]; then
        echo "$root/src"
    else
        echo "$SRC_ROOT/${root#$DATA_ROOT/}/src"
    fi
}

want_bench() { [[ -z "$ONLY" ]] || [[ ",$ONLY," == *",$1,"* ]]; }
have_tool()  { [[ ",$TOOLS," == *",$1,"* ]]; }

# ---- CSV ------------------------------------------------------------------
# Base schema is unchanged for backward compatibility. When power accounting is
# enabled we append extra columns (node,t0,t1,energy_j,avg_watts); existing CSVs
# written with the base schema are never touched (new runs use a fresh file).
CSV_HDR="ts,bench,variant,tool,nfiles,filesize_bytes,conc,wall_s,peak_pss_mb,peak_rss_mb,rc,mem_method"
[[ "$POWER_ENABLED" == "1" ]] && CSV_HDR="$CSV_HDR,node,t0,t1,energy_j,avg_watts"
if [[ ! -f "$RESULTS" ]]; then
    echo "$CSV_HDR" > "$RESULTS"
fi

# Globals set by measure()
M_WALL="" M_PSS="" M_RSS="" M_RC="" M_T0="" M_T1="" M_NODE="" M_ENERGY="" M_WATTS=""

measure() {  # measure LABEL -- CMD...
    local label="$1"; shift
    [[ "$1" == "--" ]] && shift
    local js; js="$(mktemp)"
    M_NODE="$(hostname -s)"; M_ENERGY=""; M_WATTS=""
    M_T0=$(date +%s)
    if [[ "$MEM_METHOD" == "cgroup" ]]; then
        # Exact peak via a transient systemd service (needs sudo on this box).
        local unit="copy2bench_$$_$RANDOM"
        local t0 t1
        t0=$(date +%s.%N)
        sudo systemd-run --wait --collect=no -q -p MemoryAccounting=yes \
             --unit="$unit" -- "$@"
        M_RC=$?
        t1=$(date +%s.%N)
        M_WALL=$(awk "BEGIN{printf \"%.3f\", $t1-$t0}")
        local peak_bytes
        peak_bytes=$(systemctl show "$unit" -p MemoryPeak --value 2>/dev/null)
        sudo systemctl reset-failed "$unit" 2>/dev/null
        [[ "$peak_bytes" =~ ^[0-9]+$ ]] || peak_bytes=0
        M_PSS=$(awk "BEGIN{printf \"%.1f\", $peak_bytes/1048576}")
        M_RSS="$M_PSS"
    else
        local pw=(); [[ "$POWER_ENABLED" == "1" ]] && pw=(--power)
        python3 "$HERE/memsample.py" --interval "$INTERVAL" --json "$js" \
                ${pw[@]+"${pw[@]}"} --label "$label" -- "$@"
        M_RC=$(python3 -c "import json;d=json.load(open('$js'));print(d['rc'])")
        M_WALL=$(python3 -c "import json;print(json.load(open('$js'))['wall_s'])")
        M_PSS=$(python3 -c "import json;print(json.load(open('$js'))['peak_pss_mb'])")
        M_RSS=$(python3 -c "import json;print(json.load(open('$js'))['peak_rss_mb'])")
        if [[ "$POWER_ENABLED" == "1" ]]; then
            M_ENERGY=$(python3 -c "import json;v=json.load(open('$js')).get('energy_j');print('' if v is None else v)")
            M_WATTS=$(python3 -c "import json;v=json.load(open('$js')).get('avg_watts');print('' if v is None else v)")
        fi
    fi
    M_T1=$(date +%s)
    rm -f "$js"
}

record() {  # record bench variant tool nfiles size conc
    local row="$(date -Is),$1,$2,$3,$4,$5,$6,$M_WALL,$M_PSS,$M_RSS,$M_RC,$MEM_METHOD"
    [[ "$POWER_ENABLED" == "1" ]] && row="$row,$M_NODE,$M_T0,$M_T1,$M_ENERGY,$M_WATTS"
    echo "$row" >> "$RESULTS"
    if [[ "$POWER_ENABLED" == "1" ]]; then
        printf "  %-12s %-8s %-8s files=%-9s wall=%-8s pss=%-8s energy=%-9s watts=%-7s rc=%s\n" \
            "$1" "$2" "$3" "$4" "${M_WALL}s" "${M_PSS}MB" "${M_ENERGY:-NA}J" "${M_WATTS:-NA}" "$M_RC"
    else
        printf "  %-12s %-8s %-8s files=%-9s wall=%-8s pss=%-8s rc=%s\n" \
            "$1" "$2" "$3" "$4" "${M_WALL}s" "${M_PSS}MB" "$M_RC"
    fi
}

# ---- dst reset helpers ----------------------------------------------------
# Retired destination/scratch trees are renamed aside (O(1) on the same FS) and
# their paths queued here; NOTHING is deleted until the whole suite is done.
# Deleting in the background used to overlap the next measured run, so a
# million-file `rm` stole IO/CPU from the tool being timed and skewed results.
# We now defer every delete to the very end (see cleanup_deferred_trash), at the
# cost of holding the retired trees on disk until then.
DEFERRED_TRASH=()

rm_tree_parallel() {  # rm_tree_parallel DIR  -- parallel shard delete, then rmdir
    local tree="$1"
    [[ -e "$tree" ]] || return 0
    # Delete directory shards independently. Batch non-directories so the flat
    # benchmark does not launch one rm process for each of its million files.
    find "$tree" -mindepth 1 -maxdepth 1 -type d -print0 |
        xargs -0 -r -n 1 -P "$RESET_JOBS" rm -rf --
    find "$tree" -mindepth 1 -maxdepth 1 ! -type d -print0 |
        xargs -0 -r -n 4096 -P "$RESET_JOBS" rm -f --
    rm -rf -- "$tree"
}

# Delete everything queued by defer_dst_trash/defer_misc_rm. Called once at the
# end of the suite (and from the EXIT trap so an interrupted run still cleans
# up). Trees are deleted concurrently since nothing is being measured anymore.
cleanup_deferred_trash() {
    [[ ${#DEFERRED_TRASH[@]} -gt 0 ]] || return 0
    local trees=("${DEFERRED_TRASH[@]}")
    DEFERRED_TRASH=()   # clear first so the EXIT trap is idempotent
    local t0 t1 pids=() tree
    t0=$(date +%s.%N)
    for tree in "${trees[@]}"; do
        [[ -e "$tree" ]] || continue
        rm_tree_parallel "$tree" &
        pids+=($!)
    done
    local pid
    for pid in "${pids[@]+"${pids[@]}"}"; do
        wait "$pid" 2>/dev/null || true
    done
    t1=$(date +%s.%N)
    printf "  [cleanup] deferred_rm trees=%s wall=%.3fs\n" "${#trees[@]}" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')"
}

defer_dst_trash() {  # defer_dst_trash DIR -- queue a retired dst for end-of-run rm
    local tree="$1"
    [[ -e "$tree" ]] || return 0
    DEFERRED_TRASH+=("$tree")
}

defer_misc_rm() {  # defer_misc_rm DIR -- queue scratch cleanup for end-of-run rm
    local tree="$1"
    [[ -e "$tree" ]] || return 0
    DEFERRED_TRASH+=("$tree")
}

# Sweep leftover dst.__trash.* from crashed runs (queued for end-of-run rm).
reap_stale_trash() {  # reap_stale_trash DATASET_ROOT
    local trash
    shopt -s nullglob
    for trash in "$1"/dst.__trash.*; do
        defer_misc_rm "$trash"
    done
    shopt -u nullglob
}

reset_empty() {  # reset_empty DSTDIR
    # Critical path must stay O(1): rename the old tree aside and mkdir a fresh
    # empty dst. The renamed tree is queued and its million-file delete is
    # deferred to the very end of the suite (cleanup_deferred_trash) so no `rm`
    # ever overlaps a measured run.
    local dst="$1" parent trash t0 t1
    parent="$(dirname "$dst")"
    mkdir -p "$parent"
    t0=$(date +%s.%N)
    if [[ -e "$dst" ]]; then
        trash="$dst.__trash.$$.$RANDOM"
        mv -- "$dst" "$trash"
        if [[ "$RESET_IMMEDIATE" == "1" ]]; then
            rm_tree_parallel "$trash"   # reclaim now (large-file sweeps)
        else
            defer_dst_trash "$trash"
        fi
    fi
    mkdir -p "$dst"
    t1=$(date +%s.%N)
    printf "  [setup] reset_empty (%s) wall=%.3fs\n" \
        "$([[ "$RESET_IMMEDIATE" == "1" ]] && echo rename+rm || echo rename+defer)" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')"
}

reset_perms() {  # reset_perms DSTDIR  -> re-diverge modes so metadata fix reruns
    local t0 t1
    # Modes must change in place, so this remains a walk — parallelize it.
    t0=$(date +%s.%N)
    find "$1" -type f -print0 |
        xargs -0 -r -n 4096 -P "$RESET_JOBS" chmod 600 --
    t1=$(date +%s.%N)
    printf "  [setup] reset_perms jobs=%s wall=%.3fs\n" "$RESET_JOBS" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')"
}

reset_mtime10() {  # reset_mtime10 DSTDIR -> shift ~10% of dst mtimes (incremental)
    # The incremental sweep starts from a byte-identical dst mirror. Before each
    # tool we shift the mtime of a deterministic ~10% of files (every 10th in
    # sorted order) to an epoch distinct from the generator's FIXED_MTIME, so a
    # size+mtime comparison re-transfers exactly that 10% each run. Selection is
    # stable across tools (sorted paths), so every tool faces the same 10%.
    local dst="$1" t0 t1 n list
    t0=$(date +%s.%N)
    list="$(mktemp)"
    find "$dst" -type f | sort > "$list"
    n=$(wc -l < "$list")
    awk 'NR % 10 == 0' "$list" |
        xargs -d '\n' -r -n 4096 -P "$RESET_JOBS" touch -d @1546300800 --
    rm -f "$list"
    t1=$(date +%s.%N)
    printf "  [setup] reset_mtime10 files=%s changed~%s jobs=%s wall=%.3fs\n" \
        "$n" "$((n/10))" "$RESET_JOBS" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')"
}

fresh_datadir() {  # echo a clean copy2 --data-dir
    local d="$WORK/copy2-data"
    if [[ -e "$d" ]]; then
        local trash="$d.__trash.$$.$RANDOM"
        mv -- "$d" "$trash"
        defer_misc_rm "$trash"
    fi
    mkdir -p "$d"; echo "$d"
}

trap 'cleanup_deferred_trash' EXIT

# Flush the page/dentry/inode caches so each run starts cold. Needs root, so
# this calls sudo. In the real target environment sudo is passwordless, so it
# just runs; on a box that requires a password, -A uses $SUDO_ASKPASS to prompt
# (a no-op when no password is needed). sudo caches the credential, so you are
# only prompted when its timestamp has expired.
drop_caches() {  # drop_caches PATH_ON_BENCHMARK_FILESYSTEM
    [[ -n "$DROP_CACHES" ]] || return 0
    local t0 t1
    # A bare `sync` flushes every mounted filesystem on the node and can spend
    # minutes waiting for unrelated cluster storage. syncfs(2), exposed by
    # `sync -f`, flushes only the filesystem used by this benchmark.
    t0=$(date +%s.%N)
    sync -f "$1"
    if [[ "$(id -u)" == 0 ]]; then
        echo "$DROP_CACHES" > /proc/sys/vm/drop_caches
    else
        sudo -A sh -c "echo $DROP_CACHES > /proc/sys/vm/drop_caches"
    fi
    t1=$(date +%s.%N)
    printf "  [setup] drop_caches=%s wall=%.3fs\n" "$DROP_CACHES" \
        "$(awk -v a="$t0" -v b="$t1" 'BEGIN{print b-a}')"
}

# ---- per-tool command builders --------------------------------------------
# Each echoes a command via a bash array through a nameref-free convention:
# we set CMD=(...) directly in run_one.
# RCLONE ONLY: pick rclone's --buffer-size (in MiB) for a given per-file size.
# rclone's buffer is a per-transfer read-ahead buffer that benefits from being
# large on big files, so we tier it. (copy2 does NOT use this -- it uses a small
# fixed --max-block-size; see build_cmd for why.) Tiers: <=1MiB -> BLOCK_MB
# (default 1); >1MiB & <10MiB -> 4; >=10MiB (incl. GB-file big_sweep) -> 128.
block_for_size() {  # block_for_size PER_FILE_BYTES -> echoes rclone buffer MiB
    local b="${1:-0}"
    if   [[ "$b" -ge 10485760 ]]; then echo 128
    elif [[ "$b" -gt 1048576  ]]; then echo 4
    else echo "$BLOCK_MB"; fi
}

build_cmd() {  # build_cmd TOOL SRC DST VARIANT [BLOCK_MiB]  -> sets CMD array
    local tool="$1" src="$2" dst="$3" variant="$4" blkmb="${5:-$BLOCK_MB}"
    case "$tool" in
        copy2)
            local dd; dd="$(fresh_datadir)"
            # copy2's --max-block-size is its UNIT OF PARALLEL WORK (files split
            # into blocks driven concurrently via O_DIRECT+libaio), NOT a buffer.
            # A block-size sweep on 64MB files (job 11538862) showed throughput
            # falls off monotonically as blocks/file drops: 1MB=8.35, 2MB=8.33,
            # 4MB=8.15, 8MB=7.90, 128MB~6.6 GiB/s. So copy2 always uses a small
            # fixed block (COPY2_BLOCK, default 1MB) -- deliberately decoupled from
            # rclone's --buffer-size tier below (blkmb), which is a read buffer that
            # *benefits* from being large. Unifying them starved copy2's parallelism.
            CMD=("$COPY2" --data-dir "$dd" --crawlers "$COPY2_CRAWLERS"
                 --file-copy-jobs=2048 --block-copy-jobs=256 --extra-stats
                 --transfers "$COPY2_TRANSFERS" --finish-processors "$COPY2_FINISHERS"
                 --max-block-size "$COPY2_BLOCK")
            [[ -n "$COPY2_BUFFER" ]] && CMD+=(--copy-buffer-size "$COPY2_BUFFER")
            [[ -n "$COPY2_OPTS" ]] && CMD+=($COPY2_OPTS)
            CMD+=("$src" "$dst")
            ;;
        rsync)
            local opts="$RSYNC_OPTS"
            [[ "$variant" == "hardlinks" ]] && opts="$opts -H"
            [[ -n "$RSYNC_BLOCK" ]] && opts="$opts --block-size $RSYNC_BLOCK"
            CMD=("$RSYNC" $opts "$src/" "$dst/")
            ;;
        rclone)
            local opts="$RCLONE_OPTS --transfers $RCLONE_TRANSFERS --checkers $RCLONE_CHECKERS --buffer-size ${blkmb}Mi"
            [[ "$variant" == "metadata" ]] && opts="$opts --metadata"
            CMD=("$RCLONE" sync $opts "$src" "$dst")
            ;;
        fpsync)
            local ro="$FPSYNC_RSYNC_OPTS"
            [[ "$variant" == "hardlinks" ]] && ro="$ro -H"
            local ft="$WORK/fpsync-tmp"; rm -rf "$ft"; mkdir -p "$ft"
            CMD=("$FPSYNC" -n "$FPSYNC_CONC" -t "$ft")
            [[ -n "$FPSYNC_CHUNK" ]] && CMD+=(-s "$FPSYNC_CHUNK")
            CMD+=(-o "$ro" "$(realpath "$src")/" "$(realpath "$dst")/")
            ;;
    esac
}

# Compact per-phase concurrency string for the CSV 'conc' column (no commas).
#   copy2 -> cr<crawlers>/tr<transfers>/fp<finishers>
#   rclone-> ck<checkers>/tr<transfers>
#   fpsync-> <workers>   rsync-> 1
tool_conc() {
    case "$1" in
        rsync)  echo 1 ;;
        copy2)  echo "cr${COPY2_CRAWLERS}/tr${COPY2_TRANSFERS}/fp${COPY2_FINISHERS}" ;;
        rclone) echo "ck${RCLONE_CHECKERS}/tr${RCLONE_TRANSFERS}" ;;
        fpsync) echo "$FPSYNC_CONC" ;;
    esac
}
tool_bin_ok() {
    case "$1" in
        copy2)  [[ -x "$COPY2" ]] ;;
        rsync)  [[ -n "$RSYNC" ]] ;;
        rclone) [[ -n "$RCLONE" ]] ;;
        fpsync) [[ -x "$FPSYNC" ]] ;;
    esac
}

run_one() {  # run_one BENCH VARIANT DATASET NFILES SIZE RESETKIND [SKIP_TOOLS]
    # SKIP_TOOLS: optional comma-separated tools to exclude for THIS bench only
    # (e.g. "rsync" for the 2TB-class sweeps where single-stream rsync would run
    # for hours). The tool still runs in every other bench unless removed from
    # --tools globally.
    local bench="$1" variant="$2" ds="$3" nfiles="$4" size="$5" reset="$6"
    local skip="${7:-}"
    local src dst="$ds/dst"
    src="$(src_dir_for "$ds")"
    [[ -d "$src" ]] || { echo "  (skip $bench: src $src missing; use --generate)"; return; }
    echo "[$bench] dataset=$ds nfiles=$nfiles"
    reap_stale_trash "$ds"
    for tool in copy2 rsync rclone fpsync; do
        have_tool "$tool" || continue
        [[ -n "$skip" && ",$skip," == *",$tool,"* ]] &&
            { echo "  (skip $tool: excluded from $bench)"; continue; }
        tool_bin_ok "$tool" || { echo "  (skip $tool: not installed)"; continue; }
        case "$reset" in
            empty)   reset_empty "$dst" ;;
            perms)   reset_perms "$dst" ;;
            mtime10) reset_mtime10 "$dst" ;;
            none)    : ;;
        esac
        build_cmd "$tool" "$src" "$dst" "$variant" "$(block_for_size "$size")"
        drop_caches "$dst"
        measure "$bench/$tool" -- "${CMD[@]}"
        record "$bench" "$variant" "$tool" "$nfiles" "$size" "$(tool_conc "$tool")"
    done
}

# ---- dataset generation ---------------------------------------------------
gen() {  # gen PROFILE ROOT [extra gen_dataset args...]
    local profile="$1" root="$2"; shift 2
    local extra=()
    # Cross-FS: place the src tree under SRC_ROOT; dst stays under $root.
    [[ "$SRC_ROOT" != "$DATA_ROOT" ]] && extra+=(--src-root "$(src_dir_for "$root")")
    python3 "$HERE/gen_dataset.py" --profile "$profile" --root "$root" \
        --fresh --jobs "$CONC" ${extra[@]+"${extra[@]}"} "$@"
}

# gen_maybe: honor --generate-missing. With --generate (force) it always
# (re)generates; with only --generate-missing it skips any dataset whose src/
# already exists (so an interrupted suite can be resumed without rebuilding the
# million-file trees). Same args as gen().
gen_maybe() {  # gen_maybe PROFILE ROOT [extra gen_dataset args...]
    local profile="$1" root="$2"
    local srcdir; srcdir="$(src_dir_for "$root")"
    if [[ "$GENERATE" != "1" && "$GENERATE_MISSING" == "1" && -d "$srcdir" ]]; then
        echo "  (skip gen $profile: $srcdir exists)"
        return 0
    fi
    gen "$@"
}

# Parallel-delete an entire sweep point (src + dst + any trash). Used by the
# big/incremental sweeps, which gen->run->wipe one point at a time to keep peak
# disk bounded to a single point rather than the sum of all points.
wipe_point() {  # wipe_point DATASET_ROOT
    local ds="$1" s; s="$(src_dir_for "$ds")"
    echo "  [sweep] wiping point $ds"
    rm_tree_parallel "$s"    # src (== $ds/src unless cross-FS)
    rm_tree_parallel "$ds"   # dst + trash (+ src when under $ds)
}

# big_sweep: {1,2,4,8} TB total across BIG_SWEEP_COUNT files, gen->run->wipe per
# point. Forces RESET_IMMEDIATE so dst-trash is reclaimed between tools.
run_big_sweep() {
    local i tag gb bytes ds saved_ri="$RESET_IMMEDIATE"
    RESET_IMMEDIATE=1   # reclaim dst-trash between tools (huge points)
    for i in "${!BIG_SWEEP_GB[@]}"; do
        gb="${BIG_SWEEP_GB[$i]}"; tag="${BIG_SWEEP_TAGS[$i]}"
        bytes=$(( gb * 1000000000 ))          # per-file bytes; *COUNT = total
        ds="$DATA_ROOT/big_$tag"
        local totgb=$(( gb * BIG_SWEEP_COUNT ))
        echo "== [big_sweep] $tag: $BIG_SWEEP_COUNT files x ${bytes}B (~${totgb}GB total) =="
        ( export BENCH_ALLOW_1TB=1
          gen transfer "$ds" --count "$BIG_SWEEP_COUNT" --size "$bytes" --content random )
        run_one big_sweep "$tag" "$ds" "$BIG_SWEEP_COUNT" "$bytes" empty "$BIG_SWEEP_SKIP"
        wipe_point "$ds"
    done
    RESET_IMMEDIATE="$saved_ri"
}

# incr_sweep: the size sweep as an incremental sync (identical dst mirror, ~10%
# mtime-shifted before each tool). gen->run->wipe per point.
run_incr_sweep() {
    local i tag bytes ds saved_opts="$COPY2_OPTS" saved_ri="$RESET_IMMEDIATE"
    # --sync mirrors the real incremental scenario (match rclone `sync`
    # semantics); with an identical dst there are no extras to delete.
    COPY2_OPTS="$saved_opts --sync"
    RESET_IMMEDIATE=0   # keep the mirror; reset_mtime10 only shifts, never deletes
    for i in "${!INCR_SWEEP_BYTES[@]}"; do
        tag="${INCR_SWEEP_TAGS[$i]}"; bytes="${INCR_SWEEP_BYTES[$i]}"
        ds="$DATA_ROOT/incr_$tag"
        echo "== [incr_sweep] $tag: $INCR_SWEEP_COUNT files x ${bytes}B, identical dst, 10% mtime-shifted =="
        gen transfer "$ds" --count "$INCR_SWEEP_COUNT" --size "$bytes" \
            --content random --dst identical
        run_one incr_sweep "$tag" "$ds" \
            "$INCR_SWEEP_COUNT" "$bytes" mtime10 "$INCR_SWEEP_SKIP"
        wipe_point "$ds"
    done
    COPY2_OPTS="$saved_opts"; RESET_IMMEDIATE="$saved_ri"
}

SM=$((SCALE/100)); [[ $SM -lt 1 ]] && SM=1
MD=$((SCALE/10));  [[ $MD -lt 1 ]] && MD=1

# 4c size_scaling: hold the file COUNT fixed (SIZE_SWEEP_COUNT, default 100k) and
# sweep the per-file SIZE from 2KB to 64MB in ~8x steps. This isolates how peak
# memory and throughput scale with file size (complementing mem_scaling, which
# sweeps file count at a fixed 2KB). Heavy: the 64MB step alone is 100k*64MiB =
# ~6.4TiB of source data, so it is opt-in (--enable-size-sweep) like transfer_1tb.
SIZE_SWEEP_COUNT="${SIZE_SWEEP_COUNT:-100000}"
SIZE_SWEEP_BYTES=(2048 16384 131072 1048576 8388608 67108864)
SIZE_SWEEP_TAGS=(2k 16k 128k 1m 8m 64m)
# Tools excluded from the size sweep (default: rsync, which is single-stream and
# slow on the large-size points). Set SIZE_SWEEP_SKIP="" to include everything.
SIZE_SWEEP_SKIP="${SIZE_SWEEP_SKIP-rsync}"

# 4d big_sweep (opt-in, --enable-big-sweep): few huge files -- the large-object
# throughput/power regime. BIG_SWEEP_COUNT files (default 100) at per-file sizes
# {1,2,4,8} GB, so the TOTAL per point is {0.1,0.2,0.4,0.8} TB (a tenth of the
# original 1000-file plan). Each point is gen->run->wiped in place so peak disk
# is bounded to a single point (src + dst). rsync excluded (single-stream).
# Uses incompressible data.
BIG_SWEEP_COUNT="${BIG_SWEEP_COUNT:-100}"
BIG_SWEEP_GB=(1 2 4 8)          # per-file size in GB
BIG_SWEEP_TAGS=(1g 2g 4g 8g)
BIG_SWEEP_SKIP="${BIG_SWEEP_SKIP-rsync}"

# 4e incr_sweep (opt-in, --enable-incr-sweep): size_scaling AS AN INCREMENTAL
# sync -- same shape (INCR_SWEEP_COUNT files, default = SIZE_SWEEP_COUNT = 100k,
# swept over the 6 size points 2KB->64MB), but dst is generated as a byte-
# identical mirror and before each tool ~10% of dst files have their mtime
# shifted (reset_mtime10) so only that 10% is re-transferred. Isolates crawl/
# compare + partial-transfer cost. gen->run->wiped per point (src + dst mirror is
# 2x the point size; the 64MB point is ~12.8TiB, still < free). rsync excluded.
INCR_SWEEP_COUNT="${INCR_SWEEP_COUNT:-$SIZE_SWEEP_COUNT}"
INCR_SWEEP_SKIP="${INCR_SWEEP_SKIP-rsync}"
# The incr sweep's size points default to the full size sweep, but can be
# restricted (e.g. to fit disk at high file counts) via space-separated env
# lists INCR_SWEEP_BYTES_LIST / INCR_SWEEP_TAGS_LIST (must be 1:1).
INCR_SWEEP_BYTES=(${INCR_SWEEP_BYTES_LIST:-${SIZE_SWEEP_BYTES[@]}})
INCR_SWEEP_TAGS=(${INCR_SWEEP_TAGS_LIST:-${SIZE_SWEEP_TAGS[@]}})

# 5d mem_deep: a per-path-storage sweep. File COUNT and per-file SIZE are held
# fixed; only the directory DEPTH (and thus path length) varies, so peak memory
# isolates how much each tool spends per path. copy2 shares path prefixes via
# its StringPart chain, so its per-path cost should grow sublinearly in depth.
MEM_DEEP_COUNT="${MEM_DEEP_COUNT:-200000}"   # files per depth point (fixed)
MEM_DEEP_COMP_LEN="${MEM_DEEP_COMP_LEN:-120}" # length of each path component
MEM_DEEP_DEPTHS=(2 4 8 16)                    # nesting depths (path grows ~depth*comp_len)

if [[ "$GENERATE" == "1" || "$GENERATE_MISSING" == "1" ]]; then
    if [[ "$GENERATE_MISSING" == "1" && "$GENERATE" != "1" ]]; then
        echo "== generating missing datasets (scale=$SCALE) =="
    else
        echo "== generating datasets (scale=$SCALE) =="
    fi
    # crawling uses a DEEP/irregular tree (deep nesting + long names) with a
    # byte-identical dst. A shallow uniform tree ties all tools; a deep tree is
    # where copy2's parallel traversal pulls ahead of rclone (as on real
    # home-dir trees). dst=identical -> compare-only, no data moved.
    # comp-len 24 keeps paths ~350 chars (deep nesting, not pathological 1.7KB
    # names) so generation finishes and the crawl measures traversal parallelism.
    # Tree shape is env-tunable: CRAWL_FPC (files per chain: smaller => many more
    # directories, which stresses parallel readdir), CRAWL_DEPTH, CRAWL_COMP_LEN.
    CRAWL_COMP_LEN="${CRAWL_COMP_LEN:-24}"
    _crawl_gen=(--count "$SCALE" --dst identical --comp-len "$CRAWL_COMP_LEN")
    [ -n "${CRAWL_FPC:-}" ]   && _crawl_gen+=(--files-per-chain "$CRAWL_FPC")
    [ -n "${CRAWL_DEPTH:-}" ] && _crawl_gen+=(--depth "$CRAWL_DEPTH")
    want_bench crawling  && gen_maybe deep      "$DATA_ROOT/crawl"     "${_crawl_gen[@]}"
    want_bench metadata  && gen_maybe metadata  "$DATA_ROOT/metadata"  --count "$SCALE"
    want_bench hardlinks && gen_maybe hardlinks "$DATA_ROOT/hardlinks" --count "$SCALE"
    # transfer is bandwidth-bound (2MB files); use incompressible per-file
    # content so a dedup/compressing backend can't inflate throughput.
    want_bench transfer  && gen_maybe transfer  "$DATA_ROOT/transfer"  --count "$SCALE" --content random
    if want_bench mem; then
        # mem_hlinks reuses the hardlinks dataset; ensure it exists even when the
        # standalone hardlinks bench isn't selected.
        [[ -d "$DATA_ROOT/hardlinks/src" ]] ||
            gen_maybe hardlinks "$DATA_ROOT/hardlinks" --count "$SCALE"
        # mem_scaling (opt-in): a true file-COUNT sweep at a FIXED 2KB size at
        # every point. The old code reused the 2MB `transfer` dataset for the top
        # point, which conflated a 1000x file-size jump with the count sweep.
        if [[ "$ENABLE_MEM_SCALING" == "1" ]]; then
            gen_maybe transfer "$DATA_ROOT/mem_sm" --count "$SM"    --size 2048
            gen_maybe transfer "$DATA_ROOT/mem_md" --count "$MD"    --size 2048
            gen_maybe transfer "$DATA_ROOT/mem_lg" --count "$SCALE" --size 2048
        fi
    fi
    if want_bench memstress; then
        # mem_deep depth sweep: one dataset per depth, fixed count + size.
        for d in "${MEM_DEEP_DEPTHS[@]}"; do
            gen_maybe deep "$DATA_ROOT/mem_deep_d$d" \
                --count "$MEM_DEEP_COUNT" --size 2048 \
                --depth "$d" --comp-len "$MEM_DEEP_COMP_LEN"
        done
        # mem_flat (opt-in): single-directory stress.
        [[ "$ENABLE_MEM_FLAT" == "1" ]] &&
            gen_maybe flat "$DATA_ROOT/mem_flat" --count "$SCALE"
    fi
    if [[ "$ENABLE_1TB" == "1" ]]; then
        echo "== generating 1TiB dataset (this is large) =="
        ( export BENCH_ALLOW_1TB=1; gen_maybe transfer_1tb "$DATA_ROOT/transfer_1tb" --content random )
    fi
    if want_bench size_scaling && [[ "$ENABLE_SIZE_SWEEP" == "1" ]]; then
        echo "== generating size-sweep datasets: $SIZE_SWEEP_COUNT files x {${SIZE_SWEEP_TAGS[*]}} (large) =="
        # Throughput benchmark: incompressible per-file content so dedup/
        # compression on the backend can't inflate the large-file steps.
        for i in "${!SIZE_SWEEP_BYTES[@]}"; do
            gen_maybe transfer "$DATA_ROOT/size_${SIZE_SWEEP_TAGS[$i]}" \
                --count "$SIZE_SWEEP_COUNT" --size "${SIZE_SWEEP_BYTES[$i]}" --content random
        done
    fi
fi

# ---- run ------------------------------------------------------------------
echo "== running (mem-method=$MEM_METHOD, conc=$CONC) -> $RESULTS =="

want_bench crawling  && run_one crawling  crawl     "$DATA_ROOT/crawl"     "$SCALE" 2048 none
want_bench metadata  && run_one metadata  metadata  "$DATA_ROOT/metadata"  "$SCALE" 2048 perms
want_bench hardlinks && run_one hardlinks hardlinks "$DATA_ROOT/hardlinks" "$SCALE" 2048 empty
want_bench transfer  && run_one transfer  transfer  "$DATA_ROOT/transfer"  "$SCALE" 2048 empty

if want_bench mem; then
    # mem_scaling (opt-in): file-count sweep at a fixed 2KB size. rsync is
    # excluded here -- it is single-stream and adds nothing to the memory story
    # while costing hours on the top point.
    if [[ "$ENABLE_MEM_SCALING" == "1" ]]; then
        run_one mem_scaling transfer "$DATA_ROOT/mem_sm" "$SM"    2048 empty rsync
        run_one mem_scaling transfer "$DATA_ROOT/mem_md" "$MD"    2048 empty rsync
        run_one mem_scaling transfer "$DATA_ROOT/mem_lg" "$SCALE" 2048 empty rsync
    else
        echo "[mem_scaling] defined but skipped (pass --enable-mem-scaling to run)"
    fi
    run_one mem_hlinks  hardlinks "$DATA_ROOT/hardlinks" "$SCALE" 2048 empty
fi

if want_bench memstress; then
    # mem_deep depth sweep: peak memory vs path depth (variant carries the depth).
    for d in "${MEM_DEEP_DEPTHS[@]}"; do
        run_one mem_deep "depth$d" "$DATA_ROOT/mem_deep_d$d" \
            "$MEM_DEEP_COUNT" 2048 empty
    done
    # mem_flat (opt-in): single-directory stress; wall time is FS-bound and it
    # yields no distinctive memory signal for copy2 (see README).
    if [[ "$ENABLE_MEM_FLAT" == "1" ]]; then
        run_one mem_flat transfer "$DATA_ROOT/mem_flat" "$SCALE" 2048 empty
    else
        echo "[mem_flat] defined but skipped (pass --enable-mem-flat to run)"
    fi
fi

# 4b: the 1 TiB transfer. Created (defined) but NOT run unless explicitly
# enabled -- generating/moving ~1 TiB is intentionally opt-in.
if want_bench transfer_1tb; then
    if [[ "$ENABLE_1TB" == "1" ]]; then
        run_one transfer_1tb transfer "$DATA_ROOT/transfer_1tb" 1000 1073741824 empty
    else
        echo "[transfer_1tb] defined but skipped (pass --enable-1tb to run)"
    fi
fi

# 4c: file-size sweep at fixed SIZE_SWEEP_COUNT files (default 100k), 2KB -> 64MB
# in ~8x steps. Defined but NOT run unless enabled -- the 64MB step alone moves
# ~6.4 TiB. rsync is excluded here for the same reason as mem_scaling
# (single-stream, hours on the large-size points).
if want_bench size_scaling; then
    if [[ "$ENABLE_SIZE_SWEEP" == "1" ]]; then
        for i in "${!SIZE_SWEEP_BYTES[@]}"; do
            run_one size_scaling transfer "$DATA_ROOT/size_${SIZE_SWEEP_TAGS[$i]}" \
                "$SIZE_SWEEP_COUNT" "${SIZE_SWEEP_BYTES[$i]}" empty "$SIZE_SWEEP_SKIP"
        done
    else
        echo "[size_scaling] defined but skipped (pass --enable-size-sweep to run)"
    fi
fi

# 4d: big-file sweep ({1,2,4,8} TB total across BIG_SWEEP_COUNT files). Opt-in;
# gen->run->wipe per point (datasets are NOT pre-generated in the gen block).
if want_bench big_sweep; then
    if [[ "$ENABLE_BIG_SWEEP" == "1" ]]; then
        run_big_sweep
    else
        echo "[big_sweep] defined but skipped (pass --enable-big-sweep to run)"
    fi
fi

# 4e: incremental sweep (size sweep with identical dst, 10% mtime-shifted).
# Opt-in; gen->run->wipe per point.
if want_bench incr_sweep; then
    if [[ "$ENABLE_INCR_SWEEP" == "1" ]]; then
        run_incr_sweep
    else
        echo "[incr_sweep] defined but skipped (pass --enable-incr-sweep to run)"
    fi
fi

# All measured runs are over: now do the deferred million-file deletes we have
# been queuing so they never contended with a timed tool.
echo "== cleaning up deferred deletions =="
cleanup_deferred_trash

echo "== done -> $RESULTS =="
