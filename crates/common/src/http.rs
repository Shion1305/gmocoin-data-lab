//! Shared HTTP utilities for health checks and metrics serving.

use std::sync::Arc;

use axum::{
    http::{header, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use parking_lot::Mutex;
use serde::Serialize;

use crate::metrics::{metrics_content_type, metrics_handler, MetricsRegistry};

/// In-memory readiness state toggled by services when dependencies are available.
#[derive(Clone)]
pub struct HealthState {
    inner: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    live: bool,
    ready: bool,
}

impl HealthState {
    /// Creates a new health state with liveness enabled by default.
    pub fn new() -> Self {
        let mut state = State::default();
        state.live = true;
        Self {
            inner: Arc::new(Mutex::new(state)),
        }
    }

    /// Marks the service as ready.
    pub fn mark_ready(&self) {
        self.inner.lock().ready = true;
    }

    /// Marks the service as not ready.
    pub fn mark_not_ready(&self) {
        self.inner.lock().ready = false;
    }

    /// Marks the service as live.
    pub fn mark_live(&self) {
        self.inner.lock().live = true;
    }

    /// Marks the service as not live (terminal failure).
    pub fn mark_not_live(&self) {
        self.inner.lock().live = false;
    }

    fn snapshot(&self) -> State {
        self.inner.lock().clone()
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            live: self.live,
            ready: self.ready,
        }
    }
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Builds an admin router exposing `/live`, `/ready`, and `/metrics`.
pub fn admin_router(metrics: MetricsRegistry, health: HealthState) -> Router {
    Router::new()
        .route("/metrics", get(move || metrics_endpoint(metrics.clone())))
        .route("/live", get({
            let health = health.clone();
            move || health_endpoint(health.clone(), Probe::Liveness)
        }))
        .route("/ready", get(move || health_endpoint(health.clone(), Probe::Readiness)))
}

#[derive(Clone, Copy)]
enum Probe {
    Liveness,
    Readiness,
}

async fn metrics_endpoint(metrics: MetricsRegistry) -> impl IntoResponse {
    match metrics_handler(metrics).await {
        Ok((status, body)) => {
            let mut response = (status, body).into_response();
            let content_type = metrics_content_type();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(content_type.as_ref()).expect("valid metrics content type"),
            );
            response
        }
        Err(status) => status.into_response(),
    }
}

async fn health_endpoint(health: HealthState, probe: Probe) -> impl IntoResponse {
    let state = health.snapshot();
    let (ok, status) = match probe {
        Probe::Liveness => (state.live, StatusCode::OK),
        Probe::Readiness => (state.ready, StatusCode::OK),
    };

    if ok {
        (status, axum::Json(HealthResponse { status: "ok" })).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(HealthResponse {
                status: "unavailable",
            }),
        )
            .into_response()
    }
}
