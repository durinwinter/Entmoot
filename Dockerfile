# Static musl build -> distroless (PLAN.md Phase 2). Verified in-session:
# `cargo build -p entmoot-node --target x86_64-unknown-linux-musl --release`
# produces a genuinely static binary (`ldd` reports "statically linked");
# this Dockerfile wraps that same build. The image itself hasn't been built
# or run here — this sandbox has no working Docker daemon (see
# k8s/README.md) — build and test it on real Docker before trusting it in
# production.
#
#   docker build -t entmoot:latest .

FROM rust:1-bookworm AS builder
RUN rustup target add x86_64-unknown-linux-musl \
    && apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates crates
RUN cargo build --target x86_64-unknown-linux-musl --release -p entmoot-node \
    && ldd target/x86_64-unknown-linux-musl/release/entmoot 2>&1 | grep -q "not a dynamic executable\|statically linked"

# distroless/static has no libc, no shell, no package manager — matches the
# fully static musl binary above. :nonroot runs as uid/gid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/entmoot /usr/local/bin/entmoot
ENTRYPOINT ["/usr/local/bin/entmoot"]
