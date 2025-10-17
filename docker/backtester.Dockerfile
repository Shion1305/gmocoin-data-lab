FROM rust:1.74 as builder

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY proto ./proto
COPY crates ./crates

RUN cargo build --release --bin gmo-backtester-api

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/gmo-backtester-api /usr/local/bin/gmo-backtester-api

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/gmo-backtester-api"]
