set shell := ["/bin/bash", "-c"]

default := "help"

alias b := build
alias t := test

help:
    @echo "Available recipes:"
    @just --list

bootstrap:
    @rustup component add rustfmt clippy
    @command -v protoc >/dev/null || (echo "Please install protoc" && exit 1)

proto:
    cargo build --package gmocoin-proto

build:
    cargo build --workspace --all-targets

test:
    cargo test --workspace --all-targets

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

up:
    docker compose up -d redpanda clickhouse prometheus grafana kafka-ui

down:
    docker compose down

ingest ENVFILE='.env.local':
    RUST_LOG=${RUST_LOG:-info} ENVFILE={{ENVFILE}} cargo run --bin gmo-ingestor

api ENVFILE='.env.local':
    RUST_LOG=${RUST_LOG:-info} ENVFILE={{ENVFILE}} cargo run --bin gmo-backtester-api

e2e:
    cargo test --workspace --all-targets -- --ignored e2e

ch-sql file='ops/clickhouse/ddl/init.sql':
    curl -sS --request POST --data-binary @{{file}} http://localhost:8123

grafana-url:
    @echo "Grafana → http://localhost:3000 (admin/admin)"

kafka-topics:
    docker exec -it redpanda rpk topic list --brokers redpanda:9092 || true
