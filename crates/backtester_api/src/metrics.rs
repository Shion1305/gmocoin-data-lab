//! Prometheus collectors for the backtesting API service.

use std::time::Duration;

use anyhow::Result;
use gmocoin_common::metrics::MetricsRegistry;
use prometheus::{exponential_buckets, HistogramOpts, HistogramVec, IntCounterVec, Opts};

/// Aggregated metrics for the gRPC API.
#[derive(Clone)]
pub struct BacktesterMetrics {
    _registry: MetricsRegistry,
    /// Total requests per RPC method.
    pub requests_total: IntCounterVec,
    request_duration_seconds: HistogramVec,
    clickhouse_latency_seconds: HistogramVec,
    rows_streamed_total: IntCounterVec,
}

impl BacktesterMetrics {
    /// Registers collectors with the global registry.
    pub fn register(registry: MetricsRegistry) -> Result<Self> {
        let requests_total = IntCounterVec::new(
            Opts::new("grpc_requests_total", "Total gRPC requests per RPC method."),
            &["method"],
        )?;

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "grpc_request_duration_seconds",
                "End-to-end latency per gRPC method.",
            )
            .buckets(exponential_buckets(0.001, 2.0, 15)?),
            &["method"],
        )?;

        let clickhouse_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "clickhouse_query_duration_seconds",
                "Latency of ClickHouse queries executed per method.",
            )
            .buckets(exponential_buckets(0.001, 2.0, 14)?),
            &["method"],
        )?;

        let rows_streamed_total = IntCounterVec::new(
            Opts::new(
                "grpc_rows_streamed_total",
                "Number of rows streamed to clients per method.",
            ),
            &["method"],
        )?;

        let reg = registry.registry();
        reg.register(Box::new(requests_total.clone()))?;
        reg.register(Box::new(request_duration_seconds.clone()))?;
        reg.register(Box::new(clickhouse_latency_seconds.clone()))?;
        reg.register(Box::new(rows_streamed_total.clone()))?;

        Ok(Self {
            _registry: registry,
            requests_total,
            request_duration_seconds,
            clickhouse_latency_seconds,
            rows_streamed_total,
        })
    }

    /// Increments the request counter for the given method.
    pub fn inc_request(&self, method: &str) {
        self.requests_total.with_label_values(&[method]).inc();
    }

    /// Records the full request duration.
    pub fn observe_request_duration(&self, method: &str, duration: Duration) {
        self.request_duration_seconds
            .with_label_values(&[method])
            .observe(duration.as_secs_f64());
    }

    /// Records ClickHouse query latency.
    pub fn observe_clickhouse_latency(&self, method: &str, duration: Duration) {
        self.clickhouse_latency_seconds
            .with_label_values(&[method])
            .observe(duration.as_secs_f64());
    }

    /// Increments the streamed rows counter.
    pub fn inc_rows(&self, method: &str, rows: u64) {
        self.rows_streamed_total
            .with_label_values(&[method])
            .inc_by(rows);
    }
}
