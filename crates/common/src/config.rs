//! Strongly-typed configuration structures loaded from environment variables.

use std::time::Duration;

use anyhow::Result;
use config::{Config, ConfigError};
use serde::Deserialize;

const DEFAULT_SYMBOLS: &[&str] = &["BTC_JPY", "ETH_JPY"];
const DEFAULT_CHANNELS: &[&str] = &["ticker", "trades", "orderbooks"];

/// Configuration for the auxiliary HTTP server exposing health and metrics.
#[derive(Debug, Clone, Deserialize)]
pub struct AdminConfig {
    /// Socket address to bind, e.g. `0.0.0.0:9090`.
    #[serde(default = "defaults::admin_bind_addr")]
    pub bind_addr: String,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bind_addr: defaults::admin_bind_addr(),
        }
    }
}

/// Kafka producer configuration shared across services that publish messages.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    /// Comma-separated broker list, e.g. `localhost:9092`.
    #[serde(default = "defaults::kafka_brokers")]
    pub brokers: String,
    /// Client identifier for observability.
    #[serde(default = "defaults::kafka_client_id")]
    pub client_id: String,
    /// Kafka linger.ms setting in milliseconds.
    #[serde(default = "defaults::kafka_linger_ms")]
    pub linger_ms: u64,
    /// Kafka batch.size setting in bytes.
    #[serde(default = "defaults::kafka_batch_bytes")]
    pub batch_bytes: usize,
    /// Number of retries on temporary failures.
    #[serde(default = "defaults::kafka_retries")]
    pub retries: usize,
    /// Optional SASL mechanism (e.g. `PLAIN`); empty to disable.
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    /// Optional SASL username.
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// Optional SASL password / token.
    #[serde(default)]
    pub sasl_password: Option<String>,
    /// Optional security protocol override (e.g. `SASL_SSL`).
    #[serde(default)]
    pub security_protocol: Option<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            brokers: defaults::kafka_brokers(),
            client_id: defaults::kafka_client_id(),
            linger_ms: defaults::kafka_linger_ms(),
            batch_bytes: defaults::kafka_batch_bytes(),
            retries: defaults::kafka_retries(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            security_protocol: None,
        }
    }
}

/// TOPIC names used by the ingestor publisher.
#[derive(Debug, Clone, Deserialize)]
pub struct KafkaTopics {
    /// Trades topic (protobuf payloads).
    #[serde(default = "defaults::topic_trades")]
    pub trades: String,
    /// Ticker topic (protobuf payloads).
    #[serde(default = "defaults::topic_ticker")]
    pub ticker: String,
    /// Order book snapshot topic (protobuf payloads).
    #[serde(default = "defaults::topic_orderbook")]
    pub orderbook: String,
}

impl Default for KafkaTopics {
    fn default() -> Self {
        Self {
            trades: defaults::topic_trades(),
            ticker: defaults::topic_ticker(),
            orderbook: defaults::topic_orderbook(),
        }
    }
}

/// ClickHouse connection configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseConfig {
    /// ClickHouse HTTP endpoint, e.g. `http://localhost:8123`.
    #[serde(default = "defaults::clickhouse_url")]
    pub url: String,
    /// Database to query against.
    #[serde(default = "defaults::clickhouse_database")]
    pub database: String,
    /// Optional username for authentication.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password/token for authentication.
    #[serde(default)]
    pub password: Option<String>,
    /// Query timeout in seconds.
    #[serde(default = "defaults::clickhouse_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: defaults::clickhouse_url(),
            database: defaults::clickhouse_database(),
            username: None,
            password: None,
            timeout_secs: defaults::clickhouse_timeout_secs(),
        }
    }
}

impl ClickHouseConfig {
    /// Returns the configured timeout as a [`Duration`].
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_secs)
    }
}

/// Configuration for the WebSocket ingestor service.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestorConfig {
    /// Public GMO Coin WebSocket endpoint. Defaults to v1.
    #[serde(default = "defaults::ws_endpoint")]
    pub ws_endpoint: String,
    /// Symbols to subscribe to (e.g. `BTC_JPY`).
    #[serde(default = "defaults::symbols")]
    pub symbols: Vec<String>,
    /// Channels to subscribe to (ticker/trades/orderbooks).
    #[serde(default = "defaults::channels")]
    pub channels: Vec<String>,
    /// Minimum delay between subscription requests in milliseconds.
    #[serde(default = "defaults::subscribe_min_interval_ms")]
    pub subscribe_min_interval_ms: u64,
    /// Interval for sending ping frames to the server in seconds.
    #[serde(default = "defaults::ping_interval_secs")]
    pub ping_interval_secs: u64,
    /// Timeout waiting for a pong before reconnecting in seconds.
    #[serde(default = "defaults::pong_timeout_secs")]
    pub pong_timeout_secs: u64,
    /// Base backoff duration in milliseconds for reconnect attempts.
    #[serde(default = "defaults::reconnect_base_ms")]
    pub reconnect_base_delay_ms: u64,
    /// Maximum backoff duration in milliseconds.
    #[serde(default = "defaults::reconnect_max_ms")]
    pub reconnect_max_delay_ms: u64,
    /// Jitter upper bound in milliseconds.
    #[serde(default = "defaults::reconnect_jitter_ms")]
    pub reconnect_jitter_ms: u64,
    /// Kafka publication settings.
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// Logical topic names for each channel.
    #[serde(default)]
    pub topics: KafkaTopics,
    /// HTTP admin endpoints (liveness/readiness/metrics).
    #[serde(default)]
    pub admin: AdminConfig,
}

impl Default for IngestorConfig {
    fn default() -> Self {
        Self {
            ws_endpoint: defaults::ws_endpoint(),
            symbols: defaults::symbols(),
            channels: defaults::channels(),
            subscribe_min_interval_ms: defaults::subscribe_min_interval_ms(),
            ping_interval_secs: defaults::ping_interval_secs(),
            pong_timeout_secs: defaults::pong_timeout_secs(),
            reconnect_base_delay_ms: defaults::reconnect_base_ms(),
            reconnect_max_delay_ms: defaults::reconnect_max_ms(),
            reconnect_jitter_ms: defaults::reconnect_jitter_ms(),
            kafka: KafkaConfig::default(),
            topics: KafkaTopics::default(),
            admin: AdminConfig::default(),
        }
    }
}

impl IngestorConfig {
    /// Minimum spacing between subscription calls.
    pub fn subscribe_min_interval(&self) -> Duration {
        Duration::from_millis(self.subscribe_min_interval_ms.max(1))
    }

    /// Base reconnect delay.
    pub fn reconnect_base_delay(&self) -> Duration {
        Duration::from_millis(self.reconnect_base_delay_ms.max(1))
    }

    /// Maximum reconnect delay.
    pub fn reconnect_max_delay(&self) -> Duration {
        Duration::from_millis(self.reconnect_max_delay_ms.max(1))
    }

    /// Ping interval as [`Duration`].
    pub fn ping_interval(&self) -> Duration {
        Duration::from_secs(self.ping_interval_secs.max(1))
    }

    /// Pong timeout as [`Duration`].
    pub fn pong_timeout(&self) -> Duration {
        Duration::from_secs(self.pong_timeout_secs.max(1))
    }
}

/// Optional bearer token configuration for the gRPC API.
#[derive(Debug, Clone, Deserialize)]
pub struct BearerAuthConfig {
    /// Static bearer token value; if omitted, auth is disabled.
    pub token: String,
}

/// Runtime query limits for the backtesting API.
#[derive(Debug, Clone, Deserialize)]
pub struct QueryLimits {
    /// Default page size for ClickHouse streaming queries.
    #[serde(default = "defaults::query_default_page_size")]
    pub default_page_size: usize,
    /// Maximum page size clients may request.
    #[serde(default = "defaults::query_max_page_size")]
    pub max_page_size: usize,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            default_page_size: defaults::query_default_page_size(),
            max_page_size: defaults::query_max_page_size(),
        }
    }
}

/// Backtesting API configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct BacktesterConfig {
    /// gRPC bind address.
    #[serde(default = "defaults::grpc_bind_addr")]
    pub grpc_bind_addr: String,
    /// Admin HTTP endpoints (health/metrics).
    #[serde(default)]
    pub admin: AdminConfig,
    /// ClickHouse access configuration.
    #[serde(default)]
    pub clickhouse: ClickHouseConfig,
    /// Optional bearer authentication.
    #[serde(default)]
    pub bearer_auth: Option<BearerAuthConfig>,
    /// Query constraint defaults.
    #[serde(default)]
    pub limits: QueryLimits,
}

impl Default for BacktesterConfig {
    fn default() -> Self {
        Self {
            grpc_bind_addr: defaults::grpc_bind_addr(),
            admin: AdminConfig::default(),
            clickhouse: ClickHouseConfig::default(),
            bearer_auth: None,
            limits: QueryLimits::default(),
        }
    }
}

impl BacktesterConfig {
    /// Determines if bearer authentication is enabled.
    pub fn bearer_enabled(&self) -> bool {
        self.bearer_auth.is_some()
    }
}

/// Loads ingestor configuration using the `INGESTOR__` environment prefix.
pub fn load_ingestor_config() -> Result<IngestorConfig> {
    dotenvy::dotenv().ok();
    build_config_loader("INGESTOR")?
        .try_deserialize()
        .map_err(Into::into)
}

/// Loads backtester configuration using the `BACKTEST__` environment prefix.
pub fn load_backtester_config() -> Result<BacktesterConfig> {
    dotenvy::dotenv().ok();
    build_config_loader("BACKTEST")?
        .try_deserialize()
        .map_err(Into::into)
}

fn build_config_loader(prefix: &str) -> Result<Config, ConfigError> {
    let builder = Config::builder().add_source(
        config::Environment::with_prefix(prefix)
            .separator("__")
            .list_separator(","),
    );
    builder.build()
}

mod defaults {
    pub(super) fn admin_bind_addr() -> String {
        "0.0.0.0:9090".to_string()
    }

    pub(super) fn kafka_brokers() -> String {
        "localhost:9092".to_string()
    }

    pub(super) fn kafka_client_id() -> String {
        "gmocoin-service".to_string()
    }

    pub(super) fn kafka_linger_ms() -> u64 {
        5
    }

    pub(super) fn kafka_batch_bytes() -> usize {
        512 * 1024
    }

    pub(super) fn kafka_retries() -> usize {
        5
    }

    pub(super) fn topic_trades() -> String {
        "gmo.market.trades".to_string()
    }

    pub(super) fn topic_ticker() -> String {
        "gmo.market.ticker".to_string()
    }

    pub(super) fn topic_orderbook() -> String {
        "gmo.market.orderbook.snapshot".to_string()
    }

    pub(super) fn clickhouse_url() -> String {
        "http://localhost:8123".to_string()
    }

    pub(super) fn clickhouse_database() -> String {
        "gmo".to_string()
    }

    pub(super) fn clickhouse_timeout_secs() -> u64 {
        30
    }

    pub(super) fn ws_endpoint() -> String {
        // GMO Coin public WebSocket endpoint (v1). See https://api.coin.z.com/docs/en/
        "wss://api.coin.z.com/ws/public/v1".to_string()
    }

    pub(super) fn symbols() -> Vec<String> {
        super::DEFAULT_SYMBOLS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub(super) fn channels() -> Vec<String> {
        super::DEFAULT_CHANNELS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    pub(super) fn subscribe_min_interval_ms() -> u64 {
        1_000
    }

    pub(super) fn ping_interval_secs() -> u64 {
        30
    }

    pub(super) fn pong_timeout_secs() -> u64 {
        10
    }

    pub(super) fn reconnect_base_ms() -> u64 {
        1_000
    }

    pub(super) fn reconnect_max_ms() -> u64 {
        30_000
    }

    pub(super) fn reconnect_jitter_ms() -> u64 {
        500
    }

    pub(super) fn grpc_bind_addr() -> String {
        "0.0.0.0:8080".to_string()
    }

    pub(super) fn query_default_page_size() -> usize {
        5_000
    }

    pub(super) fn query_max_page_size() -> usize {
        50_000
    }
}
