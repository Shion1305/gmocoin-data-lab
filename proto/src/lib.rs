#![forbid(unsafe_code)]
#![allow(missing_docs)]

//! Generated protobuf/gRPC types for the GMO Coin market data platform.

/// Market data protobuf messages.
pub mod market {
    include!(concat!(env!("OUT_DIR"), "/gmocoin.market.rs"));
}

/// Backtesting gRPC service definitions.
pub mod backtest {
    include!(concat!(env!("OUT_DIR"), "/gmocoin.backtest.rs"));
}
