//! Application bootstrap for the WebSocket → Kafka ingestor.

use anyhow::{Context, Result};
use axum::Router;
use gmocoin_common::{
    config::{load_ingestor_config, IngestorConfig},
    http::{admin_router, HealthState},
    metrics::MetricsRegistry,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::{kafka::KafkaPublisher, metrics::IngestorMetrics, ws::IngestorRunner};

/// Entry point invoked by `main`.
pub async fn run() -> Result<()> {
    let config = load_ingestor_config().context("failed to load ingestor config")?;
    tracing::info!(
        ws_endpoint = %config.ws_endpoint,
        ?config.symbols,
        ?config.channels,
        "starting GMO Coin ingestor"
    );

    let metrics_registry = MetricsRegistry::new("gmocoin_ingestor")?;
    let health = HealthState::new();
    let telemetry = IngestorMetrics::register(metrics_registry.clone())?;

    let admin = start_admin(&config, metrics_registry.clone(), health.clone()).await?;

    let kafka = KafkaPublisher::new(&config, telemetry.clone())?;
    let runner = IngestorRunner::new(
        config.clone(),
        kafka.clone(),
        telemetry.clone(),
        health.clone(),
    );

    let shutdown = CancellationToken::new();
    let ws_shutdown = shutdown.clone();
    let mut ws_task = tokio::spawn(async move { runner.run(ws_shutdown).await });

    let signal = shutdown_signal();
    tokio::pin!(signal);
    let result = tokio::select! {
        _ = &mut signal => {
            tracing::info!("shutdown signal received");
            shutdown.cancel();
            ws_task.await
        }
        res = &mut ws_task => res,
    };

    shutdown.cancel();

    match result {
        Ok(Ok(())) => tracing::info!("websocket runner exited cleanly"),
        Ok(Err(err)) => return Err(err),
        Err(err) => return Err(anyhow::anyhow!("websocket task join error: {err}")),
    }

    kafka.flush().await?;
    tracing::info!("kafka buffers flushed");

    admin
        .shutdown()
        .await
        .context("failed to drain admin server")?;

    tracing::info!("ingestor shutdown complete");
    Ok(())
}

async fn start_admin(
    config: &IngestorConfig,
    metrics_registry: MetricsRegistry,
    health: HealthState,
) -> Result<AdminServer> {
    let listener = TcpListener::bind(&config.admin.bind_addr)
        .await
        .with_context(|| format!("failed to bind admin listener {}", config.admin.bind_addr))?;
    let addr = listener.local_addr()?;
    tracing::info!(%addr, "admin server listening");
    let router: Router = admin_router(metrics_registry, health);
    let std_listener = listener.into_std().expect("convert listener to std");

    let shutdown = CancellationToken::new();
    let handle = tokio::spawn({
        let shutdown = shutdown.clone();
        let router = router.into_make_service();
        async move {
            let server = axum::Server::from_tcp(std_listener)
                .expect("valid listener")
                .serve(router);
            if let Err(err) = server
                .with_graceful_shutdown(async move {
                    shutdown.cancelled().await;
                })
                .await
            {
                tracing::error!(error = ?err, "admin server failed");
            }
        }
    });

    Ok(AdminServer { shutdown, handle })
}

struct AdminServer {
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl AdminServer {
    async fn shutdown(self) -> Result<()> {
        self.shutdown.cancel();
        self.handle.await.context("admin server join")?;
        Ok(())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term_signal =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term_signal.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
