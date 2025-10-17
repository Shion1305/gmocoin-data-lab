//! Kafka publisher utilities.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use gmocoin_common::config::IngestorConfig;
use rdkafka::{
    producer::{FutureProducer, FutureRecord, Producer},
    ClientConfig,
};

use crate::metrics::IngestorMetrics;

/// Wrapper around a rdkafka producer with observability integration.
#[derive(Clone)]
pub struct KafkaPublisher {
    producer: FutureProducer,
    telemetry: IngestorMetrics,
    topics: crate::model::KafkaTopics,
}

impl KafkaPublisher {
    /// Builds a new Kafka publisher using the provided configuration.
    pub fn new(config: &IngestorConfig, telemetry: IngestorMetrics) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.kafka.brokers)
            .set("client.id", &config.kafka.client_id)
            .set("message.timeout.ms", "60000")
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("compression.type", "lz4")
            .set("linger.ms", config.kafka.linger_ms.to_string())
            .set("batch.size", config.kafka.batch_bytes.to_string())
            .set("retries", config.kafka.retries.to_string())
            .set("statistics.interval.ms", "60000")
            .set("socket.keepalive.enable", "true");

        if let Some(protocol) = &config.kafka.security_protocol {
            client_config.set("security.protocol", protocol);
        }
        if let (Some(username), Some(password)) =
            (&config.kafka.sasl_username, &config.kafka.sasl_password)
        {
            client_config
                .set("sasl.username", username)
                .set("sasl.password", password);
        }
        if let Some(mechanism) = &config.kafka.sasl_mechanism {
            client_config.set("sasl.mechanism", mechanism);
        }

        let producer = client_config
            .create()
            .context("failed to create Kafka producer")?;

        Ok(Self {
            producer,
            telemetry,
            topics: crate::model::KafkaTopics {
                trades: config.topics.trades.clone(),
                ticker: config.topics.ticker.clone(),
                orderbook: config.topics.orderbook.clone(),
            },
        })
    }

    /// Returns configured topics.
    pub fn topics(&self) -> &crate::model::KafkaTopics {
        &self.topics
    }

    /// Publishes an encoded payload to Kafka with the symbol as key.
    pub async fn publish(&self, topic: &str, key: &str, payload: Vec<u8>) -> Result<()> {
        let record = FutureRecord::to(topic).key(key);
        let payload_bytes = payload;

        let start = Instant::now();
        let delivery = self
            .producer
            .send(record.payload(&payload_bytes), rdkafka::util::Timeout::Never)
            .await;
        let latency = start.elapsed().as_secs_f64() * 1_000.0;
        self.telemetry.observe_kafka_latency_ms(topic, latency);

        match delivery {
            Ok((_partition, _offset)) => Ok(()),
            Err((err, _owned)) => Err(anyhow::anyhow!("kafka publish failed: {err}")),
        }
    }

    /// Flushes outstanding messages before shutdown.
    pub async fn flush(&self) -> Result<()> {
        let producer = self.producer.clone();
        tokio::task::spawn_blocking(move || {
            let _ = producer.flush(Duration::from_secs(10));
        })
        .await
        .context("join blocking flush task")?;
        Ok(())
    }
}
