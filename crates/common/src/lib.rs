#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! Common utilities shared across gmocoin data platform crates.

pub mod config;
pub mod http;
pub mod logging;
pub mod metrics;

pub use gmocoin_proto as proto;
