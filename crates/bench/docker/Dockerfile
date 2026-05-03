# syntax=docker/dockerfile:1.7
#
# Honest KV bench: Beyond and Redis in the same Linux container, on loopback,
# with matched durability. Build context must be the `beyond/` parent directory
# so we can pull both `kv/` and the sibling `resp/` crate.

# ── Builder ─────────────────────────────────────────────────────────────────────
FROM rust:1.92-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libclang-dev \
        liburing-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# Sibling crates the workspace points at via `path = "../resp"`.
COPY resp /src/resp
COPY kv   /src/kv

WORKDIR /src/kv
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/kv/target \
    cargo build --release -p beyond-kv -p beyond-kv-bench && \
    install -Dm755 target/release/beyond-kv /out/beyond-kv && \
    install -Dm755 target/release/kv-bench  /out/kv-bench

# ── Runtime ─────────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        procps \
        redis-server \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/beyond-kv /usr/local/bin/beyond-kv
COPY --from=builder /out/kv-bench  /usr/local/bin/kv-bench
COPY kv/crates/bench/docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# `docker run --init` injects tini as PID 1 so signals reach our trap and zombie
# children (the killed servers) are reaped. We don't need our own tini layer.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default plan — overridable from `docker run … <args>`.
CMD ["--workload", "mixed:80", \
     "--keys", "100000", \
     "--value-size", "256", \
     "--concurrency", "64", \
     "--duration", "30s", \
     "--warmup", "5s", \
     "--rate", "50000", \
     "--populate"]
