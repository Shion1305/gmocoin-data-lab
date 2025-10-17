FROM rust:1.74 as builder

WORKDIR /build

COPY Cargo.toml Cargo.lock* ./
COPY proto ./proto
COPY crates ./crates

RUN cargo build --release --bin gmo-ingestor

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/gmo-ingestor /usr/local/bin/gmo-ingestor

ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/gmo-ingestor"]
