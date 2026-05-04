#!/usr/bin/env bash
# Same container, same kernel, same loopback, matched durability.
#
# Beyond uses RocksDB without per-write fsync (WAL on, sync=false).
# Redis is configured to match: AOF on, fsync=everysec, no RDB snapshots.
# Memory budgets are equal. The keyspace is sized to fit both with no eviction.

set -euo pipefail

DATA_BEYOND=/data/beyond
DATA_REDIS=/data/redis
LOG_DIR=/var/log/bench
mkdir -p "$DATA_BEYOND" "$DATA_REDIS" "$LOG_DIR"

# ── Matched configuration ──────────────────────────────────────────────────────
MEMORY_BYTES="${MEMORY_BYTES:-$((256 * 1024 * 1024))}"
BEYOND_PORT="${BEYOND_PORT:-6479}"
REDIS_PORT="${REDIS_PORT:-6480}"
# Number of Beyond worker threads. When > 1, each shard bench runs in parallel
# with a partitioned keyspace so each shard's L1 cache covers its 1/N slice.
BEYOND_SHARDS="${BEYOND_SHARDS:-1}"

# ── Cleanup ────────────────────────────────────────────────────────────────────
BEYOND_PID=
REDIS_PID=
cleanup() {
    local code=$?
    [[ -n "$BEYOND_PID" ]] && kill -TERM "$BEYOND_PID" 2>/dev/null || true
    [[ -n "$REDIS_PID"  ]] && kill -TERM "$REDIS_PID"  2>/dev/null || true
    wait 2>/dev/null || true
    exit "$code"
}
trap cleanup EXIT INT TERM

# ── Header (printed before the bench so output is self-documenting) ────────────
print_header() {
    echo "================================================================================"
    echo " Beyond KV vs Redis — same container, matched durability"
    echo "================================================================================"
    echo "kernel:        $(uname -srm)"
    echo "container cpu: $(nproc) logical cores"
    echo "memory budget: $MEMORY_BYTES bytes ($((MEMORY_BYTES / 1024 / 1024)) MiB)"
    echo "beyond shards: $BEYOND_SHARDS"
    echo
    echo "beyond-kv:     $(beyond-kv --version 2>/dev/null || echo 'unknown')"
    echo "redis-server:  $(redis-server --version)"
    echo
    echo "durability:    Beyond → RocksDB WAL, no per-write fsync (kernel-flushed)"
    echo "               Redis  → AOF on, appendfsync=everysec, RDB disabled"
    echo "               Both write WALs without per-op fsync. Apples to apples."
    echo "--------------------------------------------------------------------------------"
}

# ── Start Beyond ───────────────────────────────────────────────────────────────
start_beyond() {
    # Single thread so the full memory budget goes to one shard's L1 cache.
    # The bench sends random keys from the full keyspace to every connection —
    # it does not key-partition across shards. With N threads, each shard gets
    # memory_bytes/N cache for a working set that is still memory_bytes large,
    # giving ~1/N hit rate. Redis is single-threaded and uses all of maxmemory,
    # so single-thread Beyond is the apples-to-apples comparison.
    beyond-kv \
        --data-dir "$DATA_BEYOND" \
        --resp-port "$BEYOND_PORT" \
        --http-port 4870 \
        --memory-bytes "$MEMORY_BYTES" \
        --threads "$BEYOND_SHARDS" \
        > "$LOG_DIR/beyond.log" 2>&1 &
    BEYOND_PID=$!
}

# ── Start Redis with matched durability ────────────────────────────────────────
start_redis() {
    redis-server \
        --port "$REDIS_PORT" \
        --bind 127.0.0.1 \
        --dir "$DATA_REDIS" \
        --maxmemory "$MEMORY_BYTES" \
        --maxmemory-policy noeviction \
        --appendonly yes \
        --appendfsync everysec \
        --save "" \
        --daemonize no \
        --loglevel warning \
        > "$LOG_DIR/redis.log" 2>&1 &
    REDIS_PID=$!
}

# ── Wait until a TCP port answers, fail loudly if it doesn't ───────────────────
wait_for_port() {
    local port=$1 name=$2
    for _ in $(seq 1 100); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            exec 3<&- 3>&-
            return 0
        fi
        sleep 0.1
    done
    echo "ERROR: $name on port $port never came up" >&2
    echo "--- $name log ---" >&2
    tail -50 "$LOG_DIR/$(basename "$name").log" >&2 || true
    return 1
}

# ── Extract --out VALUE from an arg list, returning the rest ───────────────────
# Sets OUT_PATH and populates ARGS_WITHOUT_OUT.
extract_out() {
    OUT_PATH=""
    ARGS_WITHOUT_OUT=()
    local i=0
    local args=("$@")
    while [[ $i -lt ${#args[@]} ]]; do
        if [[ "${args[$i]}" == "--out" ]] && [[ $((i+1)) -lt ${#args[@]} ]]; then
            OUT_PATH="${args[$((i+1))]}"
            i=$((i+2))
        else
            ARGS_WITHOUT_OUT+=("${args[$i]}")
            i=$((i+1))
        fi
    done
}

# ── Run bench: single-shard or multi-shard ────────────────────────────────────
run_bench() {
    if [[ "$BEYOND_SHARDS" -le 1 ]]; then
        kv-bench \
            --target "beyond=redis://127.0.0.1:$BEYOND_PORT" \
            --target "redis=redis://127.0.0.1:$REDIS_PORT" \
            "$@"
        return
    fi

    # Multi-shard: run N parallel beyond bench processes (one per shard),
    # then one redis bench. Each beyond bench uses a partitioned keyspace so
    # each shard's L1 cache covers exactly its 1/N slice of the dataset.
    extract_out "$@"
    local base_args=("${ARGS_WITHOUT_OUT[@]}")

    echo "==> Running $BEYOND_SHARDS Beyond shards in parallel"
    local pids=()
    for ((s=0; s<BEYOND_SHARDS; s++)); do
        local shard_args=("${base_args[@]}" "--shards" "$BEYOND_SHARDS" "--shard-index" "$s")
        if [[ -n "$OUT_PATH" ]]; then
            shard_args+=("--out" "${OUT_PATH%.json}-beyond-shard${s}.json")
        fi
        kv-bench \
            --target "beyond-shard${s}=redis://127.0.0.1:$BEYOND_PORT" \
            "${shard_args[@]}" &
        pids+=($!)
    done

    local ok=0
    for pid in "${pids[@]}"; do
        wait "$pid" || ok=1
    done

    echo "==> Running Redis bench"
    local redis_args=("${base_args[@]}")
    if [[ -n "$OUT_PATH" ]]; then
        redis_args+=("--out" "${OUT_PATH%.json}-redis.json")
    fi
    kv-bench \
        --target "redis=redis://127.0.0.1:$REDIS_PORT" \
        "${redis_args[@]}"

    return $ok
}

main() {
    print_header
    start_beyond
    start_redis
    wait_for_port "$BEYOND_PORT" beyond
    wait_for_port "$REDIS_PORT"  redis

    # Forward run metadata so the bench can stamp it into saved JSON.
    export BENCH_KERNEL="$(uname -srm)"
    export BENCH_MEMORY_BYTES="$MEMORY_BYTES"
    export BENCH_REDIS_VERSION="$(redis-server --version)"
    # BENCH_GIT_SHA + BENCH_TIMESTAMP are passed in from run.sh on the host.

    run_bench "$@"
}

main "$@"
