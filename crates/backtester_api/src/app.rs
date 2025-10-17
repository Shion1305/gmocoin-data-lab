//! Application bootstrap for the ClickHouse-backed backtesting gRPC API.

use std::net::SocketAddr;

use anyhow::{Context, Error, Result};
use axum::Router;
use gmocoin_common::{
    config::{load_backtester_config, BacktesterConfig},
    http::{admin_router, HealthState},
    metrics::MetricsRegistry,
};
use gmocoin_proto::backtest::backtest_service_server::BacktestServiceServer;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::{
    auth::BearerInterceptor, clickhouse::ClickHouseClient, metrics::BacktesterMetrics,
    service::BacktestServiceImpl,
};

/// Launches the API server and supporting HTTP endpoints.
pub async fn run() -> Result<()> {
    let config = load_backtester_config().context("failed to load backtester config")?;
    tracing::info!(grpc = %config.grpc_bind_addr, "starting backtester API");

    let metrics_registry = MetricsRegistry::new("gmocoin_backtester")?;
    let metrics = BacktesterMetrics::register(metrics_registry.clone())?;
    let health = HealthState::new();

    let admin = start_admin(&config, metrics_registry.clone(), health.clone()).await?;

    let client = ClickHouseClient::new(&config.clickhouse)?;
    // Run a lightweight readiness probe.
    let (probe, latency) = client
        .query_one("SELECT 1 FORMAT JSONEachRow".to_string())
        .await
        .context("failed to reach ClickHouse during startup")?;
    if probe.is_none() {
        tracing::warn!("startup ClickHouse probe returned no rows");
    }
    metrics.observe_clickhouse_latency("startup_probe", latency);
    health.mark_ready();

    let service_impl =
        BacktestServiceImpl::new(client.clone(), metrics.clone(), config.limits.clone());
    let auth = BearerInterceptor::new(config.bearer_auth.map(|b| b.token));

    let backtest_service = BacktestServiceServer::with_interceptor(service_impl, move |req| {
        auth.clone().intercept(req)
    });

    let addr: SocketAddr = config
        .grpc_bind_addr
        .parse()
        .context("invalid gRPC bind address")?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<BacktestServiceServer<BacktestServiceImpl>>()
        .await;

    let shutdown = CancellationToken::new();
    let grpc_shutdown = shutdown.clone();

    let grpc_task = tokio::spawn(async move {
        Server::builder()
            .add_service(backtest_service)
            .add_service(health_service)
            .serve_with_shutdown(addr, async move {
                grpc_shutdown.cancelled().await;
            })
            .await
            .context("gRPC server failed")
    });

    let signal = shutdown_signal();
    tokio::pin!(signal);
    let mut grpc_task = grpc_task;
    let result = tokio::select! {
        _ = &mut signal => {
            tracing::info!("shutdown signal received");
            shutdown.cancel();
            grpc_task.await
        }
        res = &mut grpc_task => res,
    };

    shutdown.cancel();

    match result {
        Ok(Ok(_)) => tracing::info!("gRPC server exited cleanly"),
        Ok(Err(err)) => return Err(err),
        Err(err) => return Err(Error::new(err).context("gRPC join error")),
    }

    admin
        .shutdown()
        .await
        .context("failed to stop admin server")?;
    tracing::info!("backtester API shutdown complete");
    Ok(())
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

async fn start_admin(
    config: &BacktesterConfig,
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
                tracing::error!(error = ?err, "admin server error");
            }
        }
    });

    Ok(AdminServer { shutdown, handle })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
