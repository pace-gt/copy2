#!/usr/bin/env bash
#
# copy2 benchmark harness: runs the benchmarks.txt scenarios against copy2 and
# the competitors (rsync, rclone, fpsync) and records wall time + peak memory
# to a CSV.
#
# Benchmarks (see ../benchmarks.txt):
#   1  crawling      identical trees, no data moved  -> crawl/compare cost
#   2  metadata      identical except mode           -> metadata-fix cost
#   3  hardlinks     many hardlinks, empty dst
#   4a transfer      1e6 x 2kb, empty dst
#   4b transfer_1tb  1000 x 1GiB, empty dst          -> GATED (opt-in only)
#   5a mem_scaling   transfer at scale/100, /10, /1  -> peak RSS vs file count
#   5b mem_hardlinks hardlink tree                   -> hlink-table memory
#   5c mem_flat      1e6 files in ONE directory      -> listing/readdir memory
#   5d mem_deep      deep tree + long path/file names-> per-path storage
#   (5c/5d are the "memstress" group)
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
#   run_benchmarks.sh --generate                 # build datasets, then run all
#   run_benchmarks.sh --only transfer,mem        # run a subset
#   run_benchmarks.sh --tools copy2,rsync        # subset of tools
#   run_benchmarks.sh --scale 500 --generate     # tiny smoke run
#   run_benchmarks.sh --conc 8 --rclone-conc 24  # per-tool concurrency override
#   run_benchmarks.sh --copy2-crawlers 16 --copy2-transfers 8 --copy2-finishers 4
#   run_benchmarks.sh --rclone-checkers 128 --rclone-transfers 32  # phase knobs
#   run_benchmarks.sh --copy2-bin /path/copy2 --copy2-opts "--readback"  # per-tool cmd
#   run_benchmarks.sh --rclone-opts "--stats 0 --checksum" --rsync-opts "-aH --inplace"
#   run_benchmarks.sh --enable-1tb --generate    # include the 1TiB case
#   run_benchmarks.sh --mem-method cgroup        # exact peaks (needs sudo here)
#   run_benchmarks.sh --drop-caches              # cold caches (sync+drop 2) per run
#   run_benchmarks.sh --drop-caches 3            # drop pagecache+dentries+inodes
#
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- defaults (override via flags or env) ---------------------------------
DATA_ROOT="${DATA_ROOT:-$HOME/copy2-bench-data}"
WORK="${WORK:-$HOME/copy2-bench-work}"
RESULTS="${RESULTS:-$HERE/results.csv}"
COPY2="${COPY2:-$(find "$HERE/../.." -name copy2 -type f 2>/dev/null | head -1)}"
CONC="${CONC:-8}"                 # global concurrency default (per-tool below)
BLOCK_MB="${BLOCK_MB:-1}"        # global transfer block/buffer size in MiB
SCALE="${SCALE:-1000000}"        # file count for the 1e6-class datasets
INTERVAL="${INTERVAL:-0.05}"     # memsample interval (s)
MEM_METHOD="${MEM_METHOD:-sampler}"   # sampler | cgroup
DROP_CACHES="${DROP_CACHES:-}"   # if set (e.g. 2) sync+drop caches before each run
GENERATE=0
ENABLE_1TB=0
ONLY=""
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
RCLONE_OPTS="${RCLONE_OPTS:---stats 0 --ignore-checksum}"
FPSYNC_RSYNC_OPTS="${FPSYNC_RSYNC_OPTS:--lptgoD}"

# ---- arg parsing ----------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --generate)     GENERATE=1 ;;
        --enable-1tb)   ENABLE_1TB=1 ;;
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
        --results)      RESULTS="$2"; shift ;;
        --mem-method)   MEM_METHOD="$2"; shift ;;
        --drop-caches)  DROP_CACHES=2
                        [[ "${2:-}" =~ ^[123]$ ]] && { DROP_CACHES="$2"; shift; } ;;
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

[[ -z "$COPY2" || ! -x "$COPY2" ]] && { echo "copy2 binary not found (set COPY2=)" >&2; exit 1; }
mkdir -p "$DATA_ROOT" "$WORK"

want_bench() { [[ -z "$ONLY" ]] || [[ ",$ONLY," == *",$1,"* ]]; }
have_tool()  { [[ ",$TOOLS," == *",$1,"* ]]; }

# ---- CSV ------------------------------------------------------------------
if [[ ! -f "$RESULTS" ]]; then
    echo "ts,bench,variant,tool,nfiles,filesize_bytes,conc,wall_s,peak_pss_mb,peak_rss_mb,rc,mem_method" > "$RESULTS"
fi

# Globals set by measure()
M_WALL="" M_PSS="" M_RSS="" M_RC=""

measure() {  # measure LABEL -- CMD...
    local label="$1"; shift
    [[ "$1" == "--" ]] && shift
    local js; js="$(mktemp)"
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
        python3 "$HERE/memsample.py" --interval "$INTERVAL" --json "$js" \
                --label "$label" -- "$@"
        M_RC=$(python3 -c "import json;d=json.load(open('$js'));print(d['rc'])")
        M_WALL=$(python3 -c "import json;print(json.load(open('$js'))['wall_s'])")
        M_PSS=$(python3 -c "import json;print(json.load(open('$js'))['peak_pss_mb'])")
        M_RSS=$(python3 -c "import json;print(json.load(open('$js'))['peak_rss_mb'])")
    fi
    rm -f "$js"
}

record() {  # record bench variant tool nfiles size conc
    echo "$(date -Is),$1,$2,$3,$4,$5,$6,$M_WALL,$M_PSS,$M_RSS,$M_RC,$MEM_METHOD" >> "$RESULTS"
    printf "  %-12s %-8s %-8s files=%-9s wall=%-8s pss=%-8s rc=%s\n" \
        "$1" "$2" "$3" "$4" "${M_WALL}s" "${M_PSS}MB" "$M_RC"
}

# ---- dst reset helpers ----------------------------------------------------
reset_empty() {  # reset_empty DSTDIR
    rm -rf "$1"; mkdir -p "$1"
}
reset_perms() {  # reset_perms DSTDIR  -> re-diverge modes so metadata fix reruns
    find "$1" -type f -exec chmod 600 {} + 2>/dev/null
}
fresh_datadir() {  # echo a clean copy2 --data-dir
    local d="$WORK/copy2-data"
    rm -rf "$d"; mkdir -p "$d"; echo "$d"
}

# Flush the page/dentry/inode caches so each run starts cold. Needs root, so
# this calls sudo. In the real target environment sudo is passwordless, so it
# just runs; on a box that requires a password, -A uses $SUDO_ASKPASS to prompt
# (a no-op when no password is needed). sudo caches the credential, so you are
# only prompted when its timestamp has expired.
drop_caches() {
    [[ -n "$DROP_CACHES" ]] || return 0
    sync
    if [[ "$(id -u)" == 0 ]]; then
        echo "$DROP_CACHES" > /proc/sys/vm/drop_caches
    else
        sudo -A sh -c "echo $DROP_CACHES > /proc/sys/vm/drop_caches"
    fi
}

# ---- per-tool command builders --------------------------------------------
# Each echoes a command via a bash array through a nameref-free convention:
# we set CMD=(...) directly in run_one.
build_cmd() {  # build_cmd TOOL SRC DST VARIANT  -> sets CMD array
    local tool="$1" src="$2" dst="$3" variant="$4"
    case "$tool" in
        copy2)
            local dd; dd="$(fresh_datadir)"
            CMD=("$COPY2" --data-dir "$dd" --crawlers "$COPY2_CRAWLERS"
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
            local opts="$RCLONE_OPTS --transfers $RCLONE_TRANSFERS --checkers $RCLONE_CHECKERS --buffer-size $RCLONE_BUFFER"
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

run_one() {  # run_one BENCH VARIANT DATASET NFILES SIZE RESETKIND
    local bench="$1" variant="$2" ds="$3" nfiles="$4" size="$5" reset="$6"
    local src="$ds/src" dst="$ds/dst"
    [[ -d "$src" ]] || { echo "  (skip $bench: dataset $ds missing; use --generate)"; return; }
    echo "[$bench] dataset=$ds nfiles=$nfiles"
    for tool in copy2 rsync rclone fpsync; do
        have_tool "$tool" || continue
        tool_bin_ok "$tool" || { echo "  (skip $tool: not installed)"; continue; }
        case "$reset" in
            empty) reset_empty "$dst" ;;
            perms) reset_perms "$dst" ;;
            none)  : ;;
        esac
        build_cmd "$tool" "$src" "$dst" "$variant"
        drop_caches
        measure "$bench/$tool" -- "${CMD[@]}"
        record "$bench" "$variant" "$tool" "$nfiles" "$size" "$(tool_conc "$tool")"
    done
}

# ---- dataset generation ---------------------------------------------------
gen() {  # gen PROFILE ROOT [extra gen_dataset args...]
    local profile="$1" root="$2"; shift 2
    python3 "$HERE/gen_dataset.py" --profile "$profile" --root "$root" \
        --fresh --jobs "$CONC" "$@"
}

SM=$((SCALE/100)); [[ $SM -lt 1 ]] && SM=1
MD=$((SCALE/10));  [[ $MD -lt 1 ]] && MD=1

if [[ "$GENERATE" == "1" ]]; then
    echo "== generating datasets (scale=$SCALE) =="
    want_bench crawling  && gen crawl     "$DATA_ROOT/crawl"     --count "$SCALE"
    want_bench metadata  && gen metadata  "$DATA_ROOT/metadata"  --count "$SCALE"
    want_bench hardlinks && gen hardlinks "$DATA_ROOT/hardlinks" --count "$SCALE"
    want_bench transfer  && gen transfer  "$DATA_ROOT/transfer"  --count "$SCALE"
    if want_bench mem; then
        gen transfer  "$DATA_ROOT/mem_sm" --count "$SM"
        gen transfer  "$DATA_ROOT/mem_md" --count "$MD"
        # top point (SCALE) reuses the transfer dataset
        [[ -d "$DATA_ROOT/transfer/src" ]] || gen transfer "$DATA_ROOT/transfer" --count "$SCALE"
    fi
    if want_bench memstress; then
        gen flat "$DATA_ROOT/mem_flat" --count "$SCALE"
        gen deep "$DATA_ROOT/mem_deep"   # deep uses its own (path-length) shape
    fi
    if [[ "$ENABLE_1TB" == "1" ]]; then
        echo "== generating 1TiB dataset (this is large) =="
        BENCH_ALLOW_1TB=1 gen transfer_1tb "$DATA_ROOT/transfer_1tb"
    fi
fi

# ---- run ------------------------------------------------------------------
echo "== running (mem-method=$MEM_METHOD, conc=$CONC) -> $RESULTS =="

want_bench crawling  && run_one crawling  crawl     "$DATA_ROOT/crawl"     "$SCALE" 2048 none
want_bench metadata  && run_one metadata  metadata  "$DATA_ROOT/metadata"  "$SCALE" 2048 perms
want_bench hardlinks && run_one hardlinks hardlinks "$DATA_ROOT/hardlinks" "$SCALE" 2048 empty
want_bench transfer  && run_one transfer  transfer  "$DATA_ROOT/transfer"  "$SCALE" 2048 empty

if want_bench mem; then
    run_one mem_scaling transfer  "$DATA_ROOT/mem_sm"    "$SM"    2048 empty
    run_one mem_scaling transfer  "$DATA_ROOT/mem_md"    "$MD"    2048 empty
    run_one mem_scaling transfer  "$DATA_ROOT/transfer"  "$SCALE" 2048 empty
    run_one mem_hlinks  hardlinks "$DATA_ROOT/hardlinks" "$SCALE" 2048 empty
fi

if want_bench memstress; then
    run_one mem_flat transfer "$DATA_ROOT/mem_flat" "$SCALE" 2048 empty
    run_one mem_deep transfer "$DATA_ROOT/mem_deep" 50000 2048 empty
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

echo "== done -> $RESULTS =="
