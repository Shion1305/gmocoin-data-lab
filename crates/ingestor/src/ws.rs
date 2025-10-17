//! WebSocket client with reconnect, rate limiting, and message forwarding.

use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
use futures::{SinkExt, StreamExt};
use gmocoin_common::{config::IngestorConfig, http::HealthState};
use prost::Message;
use serde_json::json;
use tokio::time::MissedTickBehavior;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message as WsMessage,
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    kafka::KafkaPublisher,
    metrics::IngestorMetrics,
    model::{now_ns, parse_ws_payload, ChannelKind, ParsedMessage},
};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Drives the WebSocket polling loop and forwards protobuf payloads to Kafka.
pub struct IngestorRunner {
    config: IngestorConfig,
    kafka: KafkaPublisher,
    telemetry: IngestorMetrics,
    health: HealthState,
}

impl IngestorRunner {
    /// Creates a new runner instance.
    pub fn new(
        config: IngestorConfig,
        kafka: KafkaPublisher,
        telemetry: IngestorMetrics,
        health: HealthState,
    ) -> Self {
        Self {
            config,
            kafka,
            telemetry,
            health,
        }
    }

    /// Runs until shutdown or unrecoverable error.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        let randomization = (self.config.reconnect_jitter_ms as f64
            / self.config.reconnect_base_delay_ms.max(1) as f64)
            .clamp(0.0, 1.0);
        let mut backoff = ExponentialBackoffBuilder::default()
            .with_initial_interval(self.config.reconnect_base_delay())
            .with_max_interval(self.config.reconnect_max_delay())
            .with_randomization_factor(randomization)
            .with_multiplier(2.0)
            .build();

        loop {
            if shutdown.is_cancelled() {
                tracing::info!("runner shutdown before connection established");
                return Ok(());
            }

            match self.loop_once(&shutdown).await {
                Ok(()) => {
                    self.health.mark_ready();
                    backoff.reset();
                }
                Err(err) => {
                    self.health.mark_not_ready();
                    self.telemetry.inc_reconnects();
                    tracing::warn!(error = ?err, "websocket loop exited; scheduling reconnect");

                    let sleep = backoff
                        .next_backoff()
                        .unwrap_or(self.config.reconnect_max_delay());
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = tokio::time::sleep(sleep) => continue,
                    }
                }
            }
        }

        Ok(())
    }

    async fn loop_once(&self, shutdown: &CancellationToken) -> Result<()> {
        let url = Url::parse(&self.config.ws_endpoint)?;
        tracing::info!(endpoint = %url, "connecting to GMO Coin WebSocket");
        let (stream, _response) = connect_async(&url)
            .await
            .context("websocket handshake failed")?;
        self.health.mark_ready();
        self.handle_stream(stream, shutdown).await
    }

    async fn handle_stream(&self, stream: WsStream, shutdown: &CancellationToken) -> Result<()> {
        let (mut writer, mut reader) = stream.split();
        self.send_subscriptions(&mut writer, shutdown).await?;
        tracing::info!("subscriptions completed");

        let mut ping_interval = tokio::time::interval(self.config.ping_interval());
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_pong = Instant::now();

        loop {
            if shutdown.is_cancelled() {
                tracing::info!("shutdown requested; closing websocket");
                writer.send(WsMessage::Close(None)).await.ok();
                return Ok(());
            }

            if last_pong.elapsed() > self.config.pong_timeout() {
                return Err(anyhow!("pong timeout exceeded"));
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("shutdown requested while polling");
                    writer.send(WsMessage::Close(None)).await.ok();
                    return Ok(());
                }
                _ = ping_interval.tick() => {
                    writer.send(WsMessage::Ping(Vec::new())).await?;
                }
                message = reader.next() => {
                    match message {
                        Some(Ok(WsMessage::Text(text))) => {
                            self.on_text(&text).await?;
                        }
                        Some(Ok(WsMessage::Binary(bin))) => {
                            if let Ok(text) = String::from_utf8(bin) {
                                self.on_text(&text).await?;
                            } else {
                                tracing::warn!("binary payload ignored");
                            }
                        }
                        Some(Ok(WsMessage::Ping(payload))) => {
                            writer.send(WsMessage::Pong(payload)).await?;
                        }
                        Some(Ok(WsMessage::Pong(_))) => {
                            last_pong = Instant::now();
                        }
                        Some(Ok(WsMessage::Close(frame))) => {
                            return Err(anyhow!("server closed connection: {:?}", frame));
                        }
                        Some(Ok(WsMessage::Frame(_))) => {}
                        Some(Err(err)) => return Err(err.into()),
                        None => return Err(anyhow!("websocket stream ended")),
                    }
                }
            }
        }
    }

    async fn send_subscriptions(
        &self,
        writer: &mut futures::stream::SplitSink<WsStream, WsMessage>,
        shutdown: &CancellationToken,
    ) -> Result<()> {
        for channel in &self.config.channels {
            for symbol in &self.config.symbols {
                // GMO Coin public WebSocket rate limit: ≤1 subscribe/unsubscribe per second per IP.
                // Reference: https://api.coin.z.com/docs/en/
                let payload = json!({
                    "command": "subscribe",
                    "channel": channel,
                    "symbol": symbol,
                });
                writer
                    .send(WsMessage::Text(payload.to_string()))
                    .await
                    .with_context(|| format!("failed to send subscribe for {channel}:{symbol}"))?;

                tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(self.config.subscribe_min_interval()) => {}
                }
            }
        }
        Ok(())
    }

    async fn on_text(&self, payload: &str) -> Result<()> {
        let received_at_ns = now_ns();
        match parse_ws_payload(payload, received_at_ns) {
            Ok(messages) => {
                for parsed in messages {
                    self.publish(parsed).await?;
                }
            }
            Err(err) => {
                self.telemetry.inc_parse_error();
                tracing::warn!(error = ?err, payload, "failed to parse websocket message");
            }
        }
        Ok(())
    }

    async fn publish(&self, parsed: ParsedMessage) -> Result<()> {
        let topics = self.kafka.topics();
        let topic = match parsed.channel {
            ChannelKind::Trades => &topics.trades,
            ChannelKind::Ticker => &topics.ticker,
            ChannelKind::Orderbooks => &topics.orderbook,
        };

        let exchange_ts_ns = match &parsed.message.msg {
            Some(gmocoin_proto::market::market_message::Msg::Trade(trade)) => trade.exchange_ts_ns,
            Some(gmocoin_proto::market::market_message::Msg::Ticker(ticker)) => {
                ticker.exchange_ts_ns
            }
            Some(gmocoin_proto::market::market_message::Msg::ObSnapshot(snapshot)) => {
                snapshot.exchange_ts_ns
            }
            None => 0,
        };
        self.telemetry.observe_ws_message(
            parsed.channel.as_str(),
            &parsed.symbol,
            exchange_ts_ns,
            parsed.message.received_at_ns,
        );

        let mut buf = Vec::with_capacity(parsed.message.encoded_len());
        parsed.message.encode(&mut buf)?;
        self.kafka
            .publish(topic, &parsed.symbol, buf)
            .await
            .with_context(|| format!("publish failure for topic {topic}"))?;

        Ok(())
    }
}
