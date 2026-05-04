#!/usr/bin/env bash
# Same container, same kernel, same loopback, matched durability.
#
# Beyond uses a log-structured engine without per-write fsync (WAL on, sync=false).
# Redis is configured to match: AOF on, fsync=everysec, no RDB snapshots.
# Memory budgets are equal. The keyspace is sized to fit both with no eviction.
#
# Multi-shard runs: set BEYOND_SHARDS > 1 to give Beyond N worker threads.
# MEMORY_BYTES should be large enough that each shard's 1/N slice of the dataset
# fits in its L1 cache (MEMORY_BYTES / N). Default 512 MiB comfortably covers
# 8 shards × 64 MiB against a ~16 MiB per-shard working set (500 k keys × 256 B / 8).

set -euo pipefail

DATA_BEYOND=/data/beyond
DATA_REDIS=/data/redis
LOG_DIR=/var/log/bench
mkdir -p "$DATA_BEYOND" "$DATA_REDIS" "$LOG_DIR"

# ── Matched configuration ──────────────────────────────────────────────────────
MEMORY_BYTES="${MEMORY_BYTES:-$((512 * 1024 * 1024))}"
BEYOND_PORT="${BEYOND_PORT:-6479}"
REDIS_PORT="${REDIS_PORT:-6480}"
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
    echo "durability:    Beyond → append-only log WAL, no per-write fsync (kernel-flushed)"
    echo "               Redis  → AOF on, appendfsync=everysec, RDB disabled"
    echo "               Both write WALs without per-op fsync. Apples to apples."
    echo "--------------------------------------------------------------------------------"
}

# ── Start Beyond ───────────────────────────────────────────────────────────────
start_beyond() {
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

# ── Run bench ─────────────────────────────────────────────────────────────────
run_bench() {
    kv-bench \
        --target "beyond=redis://127.0.0.1:$BEYOND_PORT" \
        --target "redis=redis://127.0.0.1:$REDIS_PORT" \
        "$@"
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
