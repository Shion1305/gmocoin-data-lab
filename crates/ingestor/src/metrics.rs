//! Prometheus metric registration specific to the ingestor.

use anyhow::Result;
use gmocoin_common::metrics::MetricsRegistry;
use prometheus::{
    exponential_buckets, GaugeVec, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
};

/// Structured metric handles for the ingestor.
#[derive(Clone)]
pub struct IngestorMetrics {
    _registry: MetricsRegistry,
    /// WebSocket message counter labelled by channel and symbol.
    pub ws_messages_total: IntCounterVec,
    /// Successful reconnect counter.
    pub ws_reconnects_total: IntCounter,
    /// Gauge tracking last observed message age.
    pub last_message_age_seconds: GaugeVec,
    /// Kafka produce latency histogram in milliseconds.
    pub kafka_produce_latency_ms: HistogramVec,
    /// Counter for JSON parsing failures.
    pub parse_errors_total: IntCounter,
}

impl IngestorMetrics {
    /// Registers metric collectors with the shared registry.
    pub fn register(registry: MetricsRegistry) -> Result<Self> {
        let ws_messages_total = IntCounterVec::new(
            Opts::new(
                "ws_messages_total",
                "Count of WebSocket payloads handled per channel and symbol.",
            ),
            &["channel", "symbol"],
        )?;

        let ws_reconnects_total = IntCounter::with_opts(Opts::new(
            "ws_reconnects_total",
            "Total reconnect attempts.",
        ))?;

        let last_message_age_seconds = GaugeVec::new(
            Opts::new(
                "last_message_age_seconds",
                "Age of the newest message observed per channel/symbol.",
            ),
            &["channel", "symbol"],
        )?;

        let kafka_produce_latency_ms = HistogramVec::new(
            HistogramOpts::new(
                "kafka_produce_latency_ms",
                "Latency for Kafka produce acknowledgements in milliseconds.",
            )
            .buckets(exponential_buckets(1.0, 2.0, 12)?),
            &["topic"],
        )?;

        let parse_errors_total = IntCounter::with_opts(Opts::new(
            "parse_errors_total",
            "Failed JSON payload decodes.",
        ))?;

        let reg = registry.registry();
        reg.register(Box::new(ws_messages_total.clone()))?;
        reg.register(Box::new(ws_reconnects_total.clone()))?;
        reg.register(Box::new(last_message_age_seconds.clone()))?;
        reg.register(Box::new(kafka_produce_latency_ms.clone()))?;
        reg.register(Box::new(parse_errors_total.clone()))?;

        Ok(Self {
            _registry: registry,
            ws_messages_total,
            ws_reconnects_total,
            last_message_age_seconds,
            kafka_produce_latency_ms,
            parse_errors_total,
        })
    }

    /// Updates derived metrics after processing a market message.
    pub fn observe_ws_message(
        &self,
        channel: &str,
        symbol: &str,
        exchange_ts_ns: i64,
        received_at_ns: i64,
    ) {
        self.ws_messages_total
            .with_label_values(&[channel, symbol])
            .inc();

        if exchange_ts_ns > 0 && received_at_ns >= exchange_ts_ns {
            let age = (received_at_ns - exchange_ts_ns) as f64 / 1e9;
            self.last_message_age_seconds
                .with_label_values(&[channel, symbol])
                .set(age);
        }
    }

    /// Records Kafka produce latency in milliseconds.
    pub fn observe_kafka_latency_ms(&self, topic: &str, latency_ms: f64) {
        self.kafka_produce_latency_ms
            .with_label_values(&[topic])
            .observe(latency_ms);
    }

    /// Increments reconnect counter.
    pub fn inc_reconnects(&self) {
        self.ws_reconnects_total.inc();
    }

    /// Marks a parse error occurrence.
    pub fn inc_parse_error(&self) {
        self.parse_errors_total.inc();
    }
}
