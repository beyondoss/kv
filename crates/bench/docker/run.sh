#!/usr/bin/env bash
# Build the bench image, run a single ephemeral container, clean up no matter what.
#
# Usage:
#   ./run.sh                                   # default plan (see Dockerfile CMD)
#   ./run.sh --duration 60s --rate 100000      # override bench args
#   ./run.sh --rebuild                         # force a full image rebuild
#   ./run.sh --purge                           # remove the image after the run

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KV_ROOT="$(cd "$HERE/../../.." && pwd)"
BUILD_CTX="$(cd "$KV_ROOT/.." && pwd)"   # contains both `kv/` and `resp/`

IMAGE="beyond-kv-bench:latest"
CONTAINER="beyond-kv-bench-run"

REBUILD=0
PURGE=0
PASSTHROUGH=()
for arg in "$@"; do
    case "$arg" in
        --rebuild) REBUILD=1 ;;
        --purge)   PURGE=1 ;;
        *)         PASSTHROUGH+=("$arg") ;;
    esac
done

cleanup() {
    local code=$?
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [[ $PURGE -eq 1 ]]; then
        docker rmi "$IMAGE" >/dev/null 2>&1 || true
    fi
    exit "$code"
}
trap cleanup EXIT INT TERM

if [[ $REBUILD -eq 1 ]] || ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "==> Building $IMAGE (streaming kv + resp into build context)"
    # Tar+pipe instead of pointing docker at the parent dir directly: the
    # parent contains target/ trees and other crates that would balloon the
    # build context to multi-GB. We ship only what the Dockerfile COPYs.
    tar \
        -C "$BUILD_CTX" \
        --exclude='*/target' \
        --exclude='*/node_modules' \
        --exclude='*/.git' \
        --exclude='*/sdk' \
        -cf - kv resp \
    | docker build \
        --file "kv/crates/bench/docker/Dockerfile" \
        --tag  "$IMAGE" \
        -
fi

echo "==> Running bench"
# --network none keeps the container off the host's network — only loopback,
# which is all the bench and servers need. Removes one entire class of variance.
#
# Mount results/ so the bench can write archived JSON runs to a place that
# survives container teardown. Also pass git SHA + timestamp so saved files are
# attributable to a specific commit and a specific moment.
RESULTS_DIR="$KV_ROOT/crates/bench/results"
mkdir -p "$RESULTS_DIR"
GIT_SHA="$(git -C "$KV_ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

docker run \
    --rm \
    --name "$CONTAINER" \
    --network none \
    --init \
    --volume "$RESULTS_DIR:/results" \
    --env "BENCH_GIT_SHA=$GIT_SHA" \
    --env "BENCH_TIMESTAMP=$TIMESTAMP" \
    "$IMAGE" \
    "${PASSTHROUGH[@]}"
