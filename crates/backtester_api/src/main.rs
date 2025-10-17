#![forbid(unsafe_code)]

mod app;
mod auth;
mod clickhouse;
mod metrics;
mod service;

use anyhow::Result;
use gmocoin_common::logging;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() -> Result<()> {
    logging::init_tracing("gmocoin-backtester-api")?;
    if let Err(err) = app::run().await {
        tracing::error!(error = ?err, "backtester API exited with error");
        return Err(err);
    }
    Ok(())
}
