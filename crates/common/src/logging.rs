//! Tracing / logging bootstrap utilities.

use anyhow::Result;
use tracing_subscriber::{
    fmt::{self, time::UtcTime},
    layer::{Layer as _, SubscriberExt},
    util::SubscriberInitExt,
    EnvFilter,
};

/// Supported log output formats.
#[derive(Clone, Copy, Debug)]
pub enum LogFormat {
    /// Newline-delimited JSON.
    Json,
    /// Human-readable text.
    Text,
}

impl LogFormat {
    fn from_env() -> Self {
        match std::env::var("LOG_FORMAT")
            .unwrap_or_else(|_| "json".to_string())
            .to_lowercase()
            .as_str()
        {
            "text" | "pretty" => Self::Text,
            _ => Self::Json,
        }
    }
}

/// Initialises global tracing with service defaults.
pub fn init_tracing(service_name: &str) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,general=info"));
    let timer = UtcTime::rfc_3339();
    let format = LogFormat::from_env();

    let fmt_layer = match format {
        LogFormat::Json => fmt::layer()
            .event_format(
                fmt::format()
                    .json()
                    .with_current_span(false)
                    .with_timer(timer),
            )
            .with_target(true)
            .with_level(true)
            .boxed(),
        LogFormat::Text => fmt::layer()
            .event_format(fmt::format().compact().with_timer(timer))
            .with_target(true)
            .with_level(true)
            .boxed(),
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();

    tracing::info!(%service_name, "tracing initialised");
    Ok(())
}
