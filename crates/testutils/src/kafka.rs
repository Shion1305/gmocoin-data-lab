//! Kafka topic helpers for isolated integration tests.

use std::time::Duration;

use anyhow::{anyhow, Result};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
    ClientConfig,
};
use tokio::runtime::Handle;
use uuid::Uuid;

/// Wrapper around a temporary Kafka topic that deletes itself on drop.
pub struct TestKafkaTopic {
    name: String,
    brokers: String,
    cleanup: bool,
}

impl TestKafkaTopic {
    /// Creates a new topic with the given prefix and partition count.
    pub async fn create(brokers: &str, prefix: &str, partitions: i32) -> Result<Self> {
        let name = format!("{}_{}", prefix, Uuid::new_v4().simple());
        let admin = create_admin(brokers)?;

        let new_topic = NewTopic::new(&name, partitions, TopicReplication::Fixed(1))
            .set("cleanup.policy", "delete")
            .set("retention.ms", "1209600000"); // 14 days

        let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
        let results = admin.create_topics(&[new_topic], &opts).await?;
        for result in results {
            if let Err((topic, code)) = result {
                return Err(anyhow!("create topic {topic} failed: {code:?}"));
            }
        }

        Ok(Self {
            name,
            brokers: brokers.to_string(),
            cleanup: true,
        })
    }

    /// Returns the full topic name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Deletes the topic immediately.
    pub async fn delete(mut self) -> Result<()> {
        self.cleanup = false;
        let admin = create_admin(&self.brokers)?;
        delete_topic(&admin, &self.name).await
    }
}

impl Drop for TestKafkaTopic {
    fn drop(&mut self) {
        if !self.cleanup {
            return;
        }
        let brokers = self.brokers.clone();
        let topic = self.name.clone();
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                if let Ok(admin) = create_admin(&brokers) {
                    let _ = delete_topic(&admin, &topic).await;
                }
            });
        }
    }
}

fn create_admin(brokers: &str) -> Result<AdminClient<DefaultClientContext>> {
    let client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("socket.timeout.ms", "5000")
        .set("message.timeout.ms", "5000")
        .create()?;
    Ok(client)
}

async fn delete_topic(admin: &AdminClient<DefaultClientContext>, topic: &str) -> Result<()> {
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin.delete_topics(&[topic], &opts).await?;
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => return Ok(()),
            Err((topic, code)) => return Err(anyhow!("delete topic {topic} failed: {code:?}")),
        }
    }
    Ok(())
}
