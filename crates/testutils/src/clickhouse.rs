//! ClickHouse integration helpers for tests.

use anyhow::Result;
use clickhouse::Client;
use tokio::runtime::Handle;
use uuid::Uuid;

/// Handle to an isolated ClickHouse database created for a single test.
pub struct TestClickHouse {
    client: Client,
    admin: Client,
    database: String,
    cleanup_on_drop: bool,
}

impl TestClickHouse {
    /// Creates a new database using the provided ClickHouse endpoint URL.
    pub async fn new(base_url: &str) -> Result<Self> {
        let database = format!("test_{}", Uuid::new_v4().simple());
        let admin = Client::default().with_url(base_url);
        admin
            .query(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .execute()
            .await?;

        let client = Client::default()
            .with_url(base_url)
            .with_database(&database);

        Ok(Self {
            client,
            admin,
            database,
            cleanup_on_drop: true,
        })
    }

    /// Reuses an existing database handle without creating/dropping.
    pub fn from_existing(base_url: &str, database: &str) -> Self {
        let admin = Client::default().with_url(base_url);
        let client = Client::default().with_url(base_url).with_database(database);
        Self {
            client,
            admin,
            database: database.to_string(),
            cleanup_on_drop: false,
        }
    }

    /// Returns the database name.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns a `clickhouse::Client` scoped to the test database.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Applies a raw SQL statement against the test database.
    pub async fn apply_sql(&self, sql: &str) -> Result<()> {
        self.client.query(sql).execute().await?;
        Ok(())
    }

    /// Drops the test database eagerly.
    pub async fn drop(self) -> Result<()> {
        if self.cleanup_on_drop {
            self.admin
                .query(&format!("DROP DATABASE IF EXISTS {}", self.database))
                .execute()
                .await?;
        }
        Ok(())
    }
}

impl Drop for TestClickHouse {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            return;
        }
        let database = self.database.clone();
        let admin = self.admin.clone();
        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                let _ = admin
                    .query(&format!("DROP DATABASE IF EXISTS {database}"))
                    .execute()
                    .await;
            });
        }
    }
}
