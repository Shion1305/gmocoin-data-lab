//! Minimal ClickHouse HTTP client for streaming JSON rows.

use std::{
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use futures::{Stream, StreamExt};
use gmocoin_common::config::ClickHouseConfig;
use reqwest::Url;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_stream::wrappers::LinesStream;
use tokio_util::io::StreamReader;

type JsonStream = Pin<Box<dyn Stream<Item = Result<Value>> + Send>>;

/// A streaming JSON query handle including query execution latency.
pub struct QueryStream {
    /// Stream of JSON objects (one per ClickHouse row).
    pub rows: JsonStream,
    /// Wall-clock duration spent waiting for ClickHouse before the first byte.
    pub elapsed: Duration,
}

/// Materialised query result storing all rows in memory.
pub struct CollectedQuery {
    /// Result rows as JSON objects.
    pub rows: Vec<Value>,
    /// ClickHouse latency prior to streaming.
    pub elapsed: Duration,
}

#[derive(Clone)]
pub struct ClickHouseClient {
    inner: Arc<Inner>,
}

struct Inner {
    http: reqwest::Client,
    endpoint: Url,
    database: String,
    username: Option<String>,
    password: Option<String>,
    timeout: Duration,
}

impl ClickHouseClient {
    /// Creates a new client from shared configuration.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        let endpoint = Url::parse(&config.url).context("invalid ClickHouse URL")?;
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(4)
            .build()?;

        Ok(Self {
            inner: Arc::new(Inner {
                http,
                endpoint,
                database: config.database.clone(),
                username: config.username.clone(),
                password: config.password.clone(),
                timeout: config.timeout(),
            }),
        })
    }

    /// Executes a query returning a JSONEachRow stream.
    pub async fn query_stream(&self, sql: String) -> Result<QueryStream> {
        let inner = &self.inner;
        let mut request = inner
            .http
            .post(inner.endpoint.clone())
            .query(&[("database", inner.database.clone())])
            .timeout(inner.timeout)
            .body(sql);

        if let Some(username) = &inner.username {
            request = request.basic_auth(username, inner.password.as_ref());
        }

        let started = Instant::now();
        let response = request.send().await.context("clickhouse request failed")?;
        let elapsed = started.elapsed();
        let status = response.status();

        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<unavailable>".to_string());
            return Err(anyhow!(
                "clickhouse error: status={} body={}",
                status,
                body
            ));
        }

        let byte_stream = response
            .bytes_stream()
            .map(|chunk| chunk.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)));
        let reader = StreamReader::new(byte_stream);
        let lines = BufReader::new(reader).lines();
        let stream = LinesStream::new(lines).filter_map(|result| async {
            match result {
                Ok(line) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        match serde_json::from_str::<Value>(trimmed) {
                            Ok(value) => Some(Ok(value)),
                            Err(err) => Some(Err(anyhow!("invalid JSON row: {err}"))),
                        }
                    }
                }
                Err(err) => Some(Err(anyhow!("failed to read ClickHouse row: {err}"))),
            }
        });

        Ok(QueryStream {
            rows: Box::pin(stream),
            elapsed,
        })
    }

    /// Runs a query and collects all rows into memory.
    pub async fn query_collect(&self, sql: String) -> Result<CollectedQuery> {
        let stream = self.query_stream(sql).await?;
        let elapsed = stream.elapsed;
        let rows = stream
            .rows
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(CollectedQuery { rows, elapsed })
    }

    /// Executes a scalar query returning the first row if available.
    pub async fn query_one(&self, sql: String) -> Result<(Option<Value>, Duration)> {
        let stream = self.query_stream(sql).await?;
        let elapsed = stream.elapsed;
        let mut rows = stream.rows;
        let value = rows.next().await.transpose()?;
        Ok((value, elapsed))
    }
}
