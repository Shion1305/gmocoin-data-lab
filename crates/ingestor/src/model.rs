//! WebSocket payload parsing and protobuf mapping.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use gmocoin_proto::market::{
    market_message::Msg, MarketMessage, OrderbookLevel, OrderbookSnapshot, Ticker, Trade,
};
use serde_json::Value;

/// Kafka topic names.
#[derive(Clone)]
pub struct KafkaTopics {
    /// Trades topic.
    pub trades: String,
    /// Ticker topic.
    pub ticker: String,
    /// Orderbook snapshots topic.
    pub orderbook: String,
}

/// Subscription channel kinds offered by the GMO Coin API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    /// Trade executions.
    Trades,
    /// Top-of-book ticker updates.
    Ticker,
    /// Order book depth snapshots.
    Orderbooks,
}

impl ChannelKind {
    /// Returns the canonical string name for this channel.
    pub fn as_str(self) -> &'static str {
        match self {
            ChannelKind::Trades => "trades",
            ChannelKind::Ticker => "ticker",
            ChannelKind::Orderbooks => "orderbooks",
        }
    }
}

/// Parsed market message enriched with channel metadata.
pub struct ParsedMessage {
    /// Channel identifier.
    pub channel: ChannelKind,
    /// Trading symbol (e.g. BTC_JPY).
    pub symbol: String,
    /// Encoded protobuf message ready for publication.
    pub message: MarketMessage,
}

/// Parses a raw JSON WebSocket payload into protobuf messages.
pub fn parse_ws_payload(raw: &str, received_at_ns: i64) -> Result<Vec<ParsedMessage>> {
    let value: Value = serde_json::from_str(raw).context("invalid JSON payload")?;
    let channel = value
        .get("channel")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing channel in websocket payload"))?;

    let channel_kind = match channel {
        "trades" => ChannelKind::Trades,
        "ticker" => ChannelKind::Ticker,
        "orderbooks" => ChannelKind::Orderbooks,
        other => {
            tracing::debug!(channel = other, "ignoring unsupported channel");
            return Ok(vec![]);
        }
    };

    match channel_kind {
        ChannelKind::Trades => parse_trades(&value, received_at_ns),
        ChannelKind::Ticker => parse_ticker(&value, received_at_ns),
        ChannelKind::Orderbooks => parse_orderbook(&value, received_at_ns),
    }
}

fn parse_ticker(value: &Value, received_at_ns: i64) -> Result<Vec<ParsedMessage>> {
    let obj = if let Some(data) = value.get("data").and_then(Value::as_object) {
        data
    } else {
        value
            .as_object()
            .ok_or_else(|| anyhow!("ticker payload missing object body"))?
    };
    let symbol = obj
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("ticker missing symbol"))?;

    let best_bid = parse_f64(obj.get("bid").or_else(|| obj.get("bestBid")))?;
    let best_ask = parse_f64(obj.get("ask").or_else(|| obj.get("bestAsk")))?;
    let last = parse_f64(obj.get("last").or_else(|| obj.get("lastTradedPrice")))?;
    let volume = parse_f64(obj.get("volume").or_else(|| obj.get("volume24h")))?;
    let exchange_ts_ns = parse_timestamp(obj.get("timestamp"))?;

    let ticker = Ticker {
        symbol: symbol.to_string(),
        exchange_ts_ns,
        best_bid,
        best_ask,
        last,
        volume_24h: volume,
    };

    let message = MarketMessage {
        msg: Some(Msg::Ticker(ticker)),
        received_at_ns,
    };

    Ok(vec![ParsedMessage {
        channel: ChannelKind::Ticker,
        symbol: symbol.to_string(),
        message,
    }])
}

fn parse_trades(value: &Value, received_at_ns: i64) -> Result<Vec<ParsedMessage>> {
    let data = value
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("trade payload missing data array"))?;
    let symbol = value
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| {
            data.first()
                .and_then(|entry| entry.get("symbol"))
                .and_then(Value::as_str)
        })
        .ok_or_else(|| anyhow!("trade payload missing symbol"))?;

    let mut out = Vec::with_capacity(data.len());
    for entry in data {
        let price = parse_f64(entry.get("price"))?;
        let amount = parse_f64(entry.get("size").or_else(|| entry.get("amount")))?;
        let side = entry
            .get("side")
            .and_then(Value::as_str)
            .unwrap_or("BUY")
            .to_string();
        let exchange_ts_ns = parse_timestamp(
            entry
                .get("timestamp")
                .or_else(|| entry.get("timestampms"))
                .or_else(|| value.get("timestamp")),
        )?;
        let event_id = entry
            .get("id")
            .or_else(|| entry.get("tradeId"))
            .or_else(|| entry.get("transaction_id"))
            .map(value_to_string)
            .unwrap_or_else(|| {
                format!(
                    "{}-{}-{}",
                    symbol,
                    exchange_ts_ns,
                    price.to_bits() ^ amount.to_bits()
                )
            });

        let trade = Trade {
            event_id,
            symbol: symbol.to_string(),
            exchange_ts_ns,
            price,
            amount,
            side,
        };

        out.push(ParsedMessage {
            channel: ChannelKind::Trades,
            symbol: symbol.to_string(),
            message: MarketMessage {
                msg: Some(Msg::Trade(trade)),
                received_at_ns,
            },
        });
    }
    Ok(out)
}

fn parse_orderbook(value: &Value, received_at_ns: i64) -> Result<Vec<ParsedMessage>> {
    let payload = value
        .get("data")
        .unwrap_or(value)
        .as_object()
        .ok_or_else(|| anyhow!("orderbook payload missing data object"))?;
    let symbol = payload
        .get("symbol")
        .and_then(Value::as_str)
        .or_else(|| value.get("symbol").and_then(Value::as_str))
        .ok_or_else(|| anyhow!("orderbook payload missing symbol"))?;

    let bids = parse_levels(payload.get("bids"))?;
    let asks = parse_levels(payload.get("asks"))?;
    let exchange_ts_ns = parse_timestamp(
        payload
            .get("timestamp")
            .or_else(|| value.get("timestamp"))
            .or_else(|| payload.get("timestampms")),
    )?;

    let snapshot = OrderbookSnapshot {
        symbol: symbol.to_string(),
        exchange_ts_ns,
        bids,
        asks,
    };
    Ok(vec![ParsedMessage {
        channel: ChannelKind::Orderbooks,
        symbol: symbol.to_string(),
        message: MarketMessage {
            msg: Some(Msg::ObSnapshot(snapshot)),
            received_at_ns,
        },
    }])
}

fn parse_timestamp(value: Option<&Value>) -> Result<i64> {
    let value = value.ok_or_else(|| anyhow!("missing timestamp"))?;
    if let Some(num) = value.as_i64() {
        // GMO docs report milliseconds since epoch.
        return Ok(num * 1_000_000);
    }
    if let Some(num) = value.as_f64() {
        return Ok((num * 1_000_000.0) as i64);
    }
    if let Some(str_val) = value.as_str() {
        if let Ok(num) = str_val.parse::<i64>() {
            return Ok(num * 1_000_000);
        }
        let dt: DateTime<Utc> = str_val.parse()?;
        let duration = dt
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("invalid timestamp"))?;
        return Ok(duration);
    }
    Err(anyhow!("unsupported timestamp type"))
}

fn parse_f64(value: Option<&Value>) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow!("missing numeric field"))?;
    match value {
        Value::String(s) => Ok(s.parse()?),
        Value::Number(n) => n.as_f64().ok_or_else(|| anyhow!("invalid number")),
        other => Err(anyhow!("unexpected numeric type: {other:?}")),
    }
}

fn parse_levels(value: Option<&Value>) -> Result<Vec<OrderbookLevel>> {
    let array = value
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("orderbook levels missing"))?;
    let mut out = Vec::with_capacity(array.len());
    for entry in array {
        match entry {
            Value::Array(values) if values.len() >= 2 => {
                let price = parse_f64(values.get(0))?;
                let size = parse_f64(values.get(1))?;
                out.push(OrderbookLevel { price, size });
            }
            Value::Object(obj) => {
                let price = parse_f64(obj.get("price"))?;
                let size = parse_f64(obj.get("size"))?;
                out.push(OrderbookLevel { price, size });
            }
            _ => return Err(anyhow!("unrecognised order book level format: {entry:?}")),
        }
    }
    Ok(out)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

/// Generates a monotonic nanosecond timestamp representing "now".
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ticker_payload() {
        let json = r#"{
            "channel": "ticker",
            "data": {
                "symbol": "BTC_JPY",
                "bid": "5000000",
                "ask": "5001000",
                "last": "5000500",
                "volume": "12.5",
                "timestamp": 1700000000000
            }
        }"#;

        let messages = parse_ws_payload(json, 1700000000000000).expect("parse ticker");
        assert_eq!(messages.len(), 1);
        match &messages[0].message.msg {
            Some(Msg::Ticker(ticker)) => {
                assert_eq!(ticker.symbol, "BTC_JPY");
                assert_eq!(ticker.exchange_ts_ns, 1700000000000 * 1_000_000);
                assert!((ticker.best_bid - 5_000_000.0).abs() < f64::EPSILON);
            }
            other => panic!("unexpected message variant: {other:?}"),
        }
    }

    #[test]
    fn parses_trade_payload_array() {
        let json = r#"{
            "channel": "trades",
            "symbol": "ETH_JPY",
            "data": [
                {
                    "price": "300000",
                    "size": "0.1",
                    "side": "SELL",
                    "timestamp": 1700000001000,
                    "id": 123
                },
                {
                    "price": "300500",
                    "size": "0.2",
                    "side": "BUY",
                    "timestamp": "1700000002000",
                    "tradeId": "tx-456"
                }
            ]
        }"#;

        let messages = parse_ws_payload(json, 1700000003000000).expect("parse trades");
        assert_eq!(messages.len(), 2);
        for parsed in messages {
            match parsed.message.msg.expect("trade") {
                Msg::Trade(trade) => {
                    assert_eq!(trade.symbol, "ETH_JPY");
                    assert!(trade.price > 0.0);
                }
                other => panic!("unexpected variant: {other:?}"),
            }
        }
    }

    #[test]
    fn parses_orderbook_snapshot() {
        let json = r#"{
            "channel": "orderbooks",
            "symbol": "BTC_JPY",
            "data": {
                "symbol": "BTC_JPY",
                "timestamp": "2024-01-01T00:00:00Z",
                "bids": [["5000000", "0.3"]],
                "asks": [{"price": "5001000", "size": "0.4"}]
            }
        }"#;

        let messages = parse_ws_payload(json, 1704067200000000000).expect("parse orderbook");
        assert_eq!(messages.len(), 1);
        match messages[0].message.msg.as_ref().expect("snapshot") {
            Msg::ObSnapshot(snapshot) => {
                assert_eq!(snapshot.bids.len(), 1);
                assert_eq!(snapshot.asks.len(), 1);
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }
}
