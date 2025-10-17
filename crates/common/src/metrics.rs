//! Prometheus metrics helpers shared by services.

use anyhow::Result;
use once_cell::sync::Lazy;
use prometheus::{Encoder, Registry, TextEncoder};
use std::{borrow::Cow, sync::Arc};

static ENCODER: Lazy<TextEncoder> = Lazy::new(TextEncoder::new);

/// Lightweight wrapper around a Prometheus [`Registry`] to simplify sharing.
#[derive(Clone)]
pub struct MetricsRegistry {
    registry: Arc<Registry>,
}

impl MetricsRegistry {
    /// Creates a new registry using the provided service name as namespace.
    pub fn new<S: Into<String>>(service_name: S) -> Result<Self> {
        let name = service_name.into();
        let registry = Registry::new_custom(Some(name), None)?;
        Ok(Self {
            registry: Arc::new(registry),
        })
    }

    /// Returns the underlying registry for metric registration.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Clones the registry handle.
    pub fn clone_registry(&self) -> Registry {
        (*self.registry).clone()
    }

    /// Gathers metrics and returns the encoded text format.
    pub fn gather(&self) -> Result<String> {
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        ENCODER.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

/// Axum handler that renders Prometheus metrics using the provided registry.
pub async fn metrics_handler(
    registry: MetricsRegistry,
) -> Result<(http::StatusCode, String), http::StatusCode> {
    registry
        .gather()
        .map(|body| (http::StatusCode::OK, body))
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)
}

/// Helper to produce the correct `Content-Type` header for metrics responses.
pub fn metrics_content_type() -> Cow<'static, str> {
    Cow::Borrowed(ENCODER.format_type())
}
