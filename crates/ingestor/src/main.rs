#![forbid(unsafe_code)]

mod app;
mod kafka;
mod metrics;
mod model;
mod ws;

use anyhow::Result;
use gmocoin_common::logging;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    logging::init_tracing("gmocoin-ingestor")?;
    if let Err(err) = app::run().await {
        tracing::error!(error = ?err, "ingestor exited with error");
        return Err(err);
    }
    Ok(())
}
