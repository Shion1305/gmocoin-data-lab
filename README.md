# GMO Coin Market Data Lab

Self-hosted platform that ingests real-time market data from [GMO Coin's public WebSocket API](https://api.coin.z.com/docs/en/), publishes it onto Kafka, stores it in ClickHouse, and provides a gRPC backtesting API with integrated observability.

## Features

- Resilient WebSocket ingestor with auto-reconnect, ping/pong handling, and subscribe throttling (≤1 req/s per IP as per GMO Coin docs).
- Protobuf-based data contracts compiled with `prost`, shared across services.
- Durable Kafka topics (`acks=all`, idempotent producers) partitioned by symbol.
- ClickHouse historical storage with materialized views consuming Kafka engine topics.
- Tonic gRPC API for trades, bars, order book snapshots, and replay (optional bearer token auth).
- Prometheus metrics + curated Grafana dashboards for ingestor and API.
- Local developer stack via `docker-compose` and convenience `just` recipes.
- Kubernetes manifests (with HPA, PDB, probes) and ArgoCD Application examples.
- Comprehensive automated tests (unit, integration, end-to-end) using `testcontainers` utilities that provision isolated Kafka/ClickHouse resources.

## Quickstart

```bash
just bootstrap         # ensure toolchain components installed
cp .env.example .env.local
just up                # start Kafka, ClickHouse, Prometheus, Grafana
just proto             # compile protobuf stubs
just build             # build all crates
just test              # run unit + integration tests
just ingest            # run ingestor (uses .env.local)
just api               # run backtesting gRPC service
```

Grafana becomes available at `http://localhost:3000` (`admin/admin`). Prometheus listens on `http://localhost:9090`. Kafka UI (optional) is exposed on `http://localhost:8085`.

## Architecture

```
proto/market.proto        Data contracts (Trade/Ticker/OrderbookSnapshot/MarketMessage)
crates/common             Shared config, logging, metrics, health utilities
crates/ingestor           WebSocket → Kafka ingestion pipeline
crates/backtester_api     ClickHouse-backed gRPC service
crates/testutils          Test helpers (ephemeral Kafka topics, ClickHouse DBs, fake WS server)
ops/clickhouse/ddl        DDL for tables + Kafka engine materialised views
ops/prometheus            Scrape configs for local + k8s deployments
ops/grafana               Provisioned dashboards
deploy/k8s                Kubernetes manifests (Deployments, Services, HPA, PDB)
deploy/argocd             ArgoCD Application definitions
docker                    Service Dockerfiles; compose covers dependencies
```

### Data flow

1. `gmo-ingestor` connects to `wss://api.coin.z.com/ws/public/v1`, subscribes to `ticker`, `trades`, and `orderbooks` for BTC/JPY and ETH/JPY (per [GMO docs](https://api.coin.z.com/docs/en/)).
2. Messages are normalised into protobuf `MarketMessage` envelopes (retaining order book `timestamp`, added 2020-04-08 per API changelog) and produced to Kafka topics partitioned by `symbol`.
3. ClickHouse ingests via Kafka engine tables / materialised views into deduplicated history tables.
4. `gmo-backtester-api` exposes gRPC endpoints for trade queries, OHLCV bars, order book snapshots, and market replays (optionally pacing by replay speed).
5. Prometheus scrapes `/metrics`; Grafana dashboards visualise throughput, reconnects, query latency, etc.

## Testing

- Unit tests cover parsing, config, and helpers (`cargo test`).
- Integration tests spin up Kafka & ClickHouse via `testcontainers`, ensuring WS→Kafka→ClickHouse flow and gRPC queries function. Each test creates isolated DBs and topics using `crates/testutils`.
- End-to-end tests (`just e2e`) run against the compose stack, validating ClickHouse row counts and gRPC responses.

## Kubernetes Deployment

- Manifests under `deploy/k8s` target the `marketdata` namespace and include Deployments, Services, PodDisruptionBudgets, HPAs, and ConfigMaps/Secrets templates.
- Images are designed to be published to `ghcr.io/Shion1305/gmocoin-data-lab/*` (adjust via `values` or overlays).
- Annotated for Prometheus scraping (`/metrics`), with `/live` and `/ready` probes configured.
- Example ArgoCD Applications in `deploy/argocd` reference the repo path for GitOps rollout.

## Operations

- Apply ClickHouse schema: `just ch-sql file=ops/clickhouse/ddl/init.sql`.
- Kafka management: `just kafka-topics` (requires running Redpanda container).
- Grafana dashboards auto-provisioned; additional JSON can be dropped into `ops/grafana/dashboards`.
- Observability alerts can be layered via Prometheus rules (see `ops/prometheus`).

## Schema Evolution

`MarketMessage` never reuses protobuf field tags; new optional fields must consume new tags. Consumers should accept both new/old schemas, and ClickHouse views should be adapted to handle additions gracefully.

## References

- GMO Coin Crypto API (WebSocket endpoints, rate limits): <https://api.coin.z.com/docs/en/>
- Changelog noting `timestamp` in Order Books payloads: <https://api.coin.z.com/docs/en/>

## License

Licensed under Apache-2.0.
