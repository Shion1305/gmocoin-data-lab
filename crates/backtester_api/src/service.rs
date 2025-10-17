//! gRPC service implementation for historical queries and replay.

use std::{
    pin::Pin,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use gmocoin_common::config::QueryLimits;
use gmocoin_proto::backtest::backtest_service_server::BacktestService;
use gmocoin_proto::backtest::{
    Bar, BarAgg, BarInterval, GetBarsRequest, GetOrderbookSnapshotRequest, GetTradesRequest,
    ReplayRequest,
};
use gmocoin_proto::market::{
    market_message::Msg, MarketMessage, OrderbookLevel, OrderbookSnapshot, Ticker, Trade,
};
use serde_json::Value;
use tokio::time;
use tonic::{Request, Response, Status};

use crate::{clickhouse::ClickHouseClient, metrics::BacktesterMetrics};

const METHOD_GET_TRADES: &str = "GetTrades";
const METHOD_GET_BARS: &str = "GetBars";
const METHOD_GET_ORDERBOOK_SNAPSHOT: &str = "GetOrderbookSnapshot";
const METHOD_REPLAY: &str = "Replay";

type TradeStream = Pin<Box<dyn Stream<Item = Result<Trade, Status>> + Send>>;
type BarStream = Pin<Box<dyn Stream<Item = Result<Bar, Status>> + Send>>;
type MarketStream = Pin<Box<dyn Stream<Item = Result<MarketMessage, Status>> + Send>>;

/// Concrete tonic service.
#[derive(Clone)]
pub struct BacktestServiceImpl {
    client: ClickHouseClient,
    metrics: BacktesterMetrics,
    _limits: QueryLimits,
}

impl BacktestServiceImpl {
    /// Creates a new service implementation.
    pub fn new(client: ClickHouseClient, metrics: BacktesterMetrics, limits: QueryLimits) -> Self {
        Self {
            client,
            metrics,
            _limits: limits,
        }
    }

    fn validate_symbol<'a>(symbol: &'a str) -> Result<&'a str, Status> {
        if symbol.is_empty() {
            return Err(Status::invalid_argument("symbol must not be empty"));
        }
        if !symbol
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(Status::invalid_argument("symbol must match [A-Z0-9_]+"));
        }
        Ok(symbol)
    }

    fn validate_range(start_ns: i64, end_ns: i64) -> Result<(i64, i64), Status> {
        if start_ns <= 0 || end_ns <= 0 {
            return Err(Status::invalid_argument("timestamps must be positive"));
        }
        if end_ns < start_ns {
            return Err(Status::invalid_argument("end_ns must be >= start_ns"));
        }
        Ok((start_ns, end_ns))
    }

    fn interval_seconds(request: &GetBarsRequest) -> Result<u64, Status> {
        let seconds = match request.interval() {
            BarInterval::BarInterval1s => 1,
            BarInterval::BarInterval5s => 5,
            BarInterval::BarInterval1m => 60,
            BarInterval::Custom => {
                if request.custom_interval_ns == 0 {
                    return Err(Status::invalid_argument("custom interval must be > 0"));
                }
                (request.custom_interval_ns / 1_000_000_000).max(1)
            }
            BarInterval::Unspecified => {
                return Err(Status::invalid_argument("interval must be specified"));
            }
        };
        Ok(seconds)
    }

    fn parse_trade(row: &Value) -> Result<(Trade, i64)> {
        let trade = Trade {
            event_id: row_value(row, "event_id")?,
            symbol: row_value(row, "symbol")?,
            exchange_ts_ns: row_i64(row, "exchange_ts_ns")?,
            price: row_f64(row, "price")?,
            amount: row_f64(row, "amount")?,
            side: row_value(row, "side")?,
        };
        let received_at_ns = row_i64(row, "received_at_ns")?;
        Ok((trade, received_at_ns))
    }

    fn parse_ticker(row: &Value) -> Result<(Ticker, i64)> {
        let ticker = Ticker {
            symbol: row_value(row, "symbol")?,
            exchange_ts_ns: row_i64(row, "exchange_ts_ns")?,
            best_bid: row_f64(row, "best_bid")?,
            best_ask: row_f64(row, "best_ask")?,
            last: row_f64(row, "last")?,
            volume_24h: row_f64(row, "volume_24h")?,
        };
        let received_at_ns = row_i64(row, "received_at_ns")?;
        Ok((ticker, received_at_ns))
    }

    fn parse_orderbook(row: &Value) -> Result<(OrderbookSnapshot, i64)> {
        let bids = parse_levels(row, "bids")?;
        let asks = parse_levels(row, "asks")?;
        let snapshot = OrderbookSnapshot {
            symbol: row_value(row, "symbol")?,
            exchange_ts_ns: row_i64(row, "exchange_ts_ns")?,
            bids,
            asks,
        };
        let received_at_ns = row_i64(row, "received_at_ns")?;
        Ok((snapshot, received_at_ns))
    }
}

fn row_value(row: &Value, key: &str) -> Result<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("missing field '{key}'"))
}

fn row_i64(row: &Value, key: &str) -> Result<i64> {
    if let Some(num) = row.get(key).and_then(Value::as_i64) {
        Ok(num)
    } else if let Some(str_val) = row.get(key).and_then(Value::as_str) {
        Ok(str_val.parse()?)
    } else {
        Err(anyhow!("missing integer field '{key}'"))
    }
}

fn row_f64(row: &Value, key: &str) -> Result<f64> {
    if let Some(num) = row.get(key).and_then(Value::as_f64) {
        Ok(num)
    } else if let Some(str_val) = row.get(key).and_then(Value::as_str) {
        Ok(str_val.parse()?)
    } else {
        Err(anyhow!("missing float field '{key}'"))
    }
}

fn parse_levels(row: &Value, key: &str) -> Result<Vec<OrderbookLevel>> {
    let entries = row
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing array '{key}'"))?;
    let mut levels = Vec::with_capacity(entries.len());
    for entry in entries {
        match entry {
            Value::Array(values) if values.len() >= 2 => {
                let price = values
                    .get(0)
                    .and_then(Value::as_f64)
                    .ok_or_else(|| anyhow!("invalid price array"))?;
                let size = values
                    .get(1)
                    .and_then(Value::as_f64)
                    .ok_or_else(|| anyhow!("invalid size array"))?;
                levels.push(OrderbookLevel { price, size });
            }
            Value::Object(obj) => {
                let price = obj
                    .get("price")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| anyhow!("invalid price object"))?;
                let size = obj
                    .get("size")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| anyhow!("invalid size object"))?;
                levels.push(OrderbookLevel { price, size });
            }
            other => return Err(anyhow!("unexpected orderbook level: {:?}", other)),
        }
    }
    Ok(levels)
}

fn internal_error(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

#[tonic::async_trait]
impl BacktestService for BacktestServiceImpl {
    type GetTradesStream = TradeStream;
    type GetBarsStream = BarStream;
    type ReplayStream = MarketStream;

    async fn get_trades(
        &self,
        request: Request<GetTradesRequest>,
    ) -> Result<Response<Self::GetTradesStream>, Status> {
        self.metrics.inc_request(METHOD_GET_TRADES);
        let payload = request.into_inner();
        let symbol = Self::validate_symbol(&payload.symbol)?;
        let (start_ns, end_ns) = Self::validate_range(payload.start_ns, payload.end_ns)?;

        let sql = format!(
            "SELECT symbol, event_id, toUnixTimestamp64Nano(exchange_ts) AS exchange_ts_ns, \
             toUnixTimestamp64Nano(received_at) AS received_at_ns, price, amount, side \
             FROM gmo.trades \
             WHERE symbol = '{symbol}' \
               AND exchange_ts >= fromUnixTimestamp64Nano({start_ns}) \
               AND exchange_ts <= fromUnixTimestamp64Nano({end_ns}) \
             ORDER BY exchange_ts \
             FORMAT JSONEachRow"
        );

        let query = self
            .client
            .query_stream(sql)
            .await
            .map_err(internal_error)?;
        self.metrics
            .observe_clickhouse_latency(METHOD_GET_TRADES, query.elapsed);

        let metrics = self.metrics.clone();
        let stream = try_stream! {
            let mut rows = query.rows;
            let started = Instant::now();
            while let Some(row) = rows.next().await {
                let (trade, _received_at_ns) =
                    Self::parse_trade(&row.map_err(internal_error)?)
                        .map_err(internal_error)?;
                metrics.inc_rows(METHOD_GET_TRADES, 1);
                yield trade;
            }
            metrics.observe_request_duration(METHOD_GET_TRADES, started.elapsed());
        };

        Ok(Response::new(Box::pin(stream) as TradeStream))
    }

    async fn get_bars(
        &self,
        request: Request<GetBarsRequest>,
    ) -> Result<Response<Self::GetBarsStream>, Status> {
        self.metrics.inc_request(METHOD_GET_BARS);
        let payload = request.into_inner();
        let symbol = Self::validate_symbol(&payload.symbol)?;
        let (start_ns, end_ns) = Self::validate_range(payload.start_ns, payload.end_ns)?;
        if payload.aggregation() != BarAgg::Ohlcv {
            return Err(Status::invalid_argument("unsupported aggregation"));
        }
        let interval = Self::interval_seconds(&payload)?;

        let sql = format!(
            "SELECT symbol, bucket, \
                toUnixTimestamp64Nano(bucket) AS start_ns, \
                toUnixTimestamp64Nano(addSeconds(bucket, {interval})) - 1 AS end_ns, \
                argMin(price, exchange_ts) AS open, \
                max(price) AS high, \
                min(price) AS low, \
                argMax(price, exchange_ts) AS close, \
                sum(amount) AS volume \
            FROM ( \
                SELECT symbol, price, amount, exchange_ts, \
                       toStartOfInterval(exchange_ts, INTERVAL {interval} SECOND) AS bucket \
                FROM gmo.trades \
                WHERE symbol = '{symbol}' \
                  AND exchange_ts >= fromUnixTimestamp64Nano({start_ns}) \
                  AND exchange_ts <= fromUnixTimestamp64Nano({end_ns}) \
            ) \
            GROUP BY symbol, bucket \
            ORDER BY bucket \
            FORMAT JSONEachRow"
        );

        let query = self
            .client
            .query_stream(sql)
            .await
            .map_err(internal_error)?;
        self.metrics
            .observe_clickhouse_latency(METHOD_GET_BARS, query.elapsed);

        let metrics = self.metrics.clone();
        let stream = try_stream! {
            let mut rows = query.rows;
            let started = Instant::now();
            while let Some(row) = rows.next().await {
                let row = row.map_err(internal_error)?;
                let bar = Bar {
                    symbol: row_value(&row, "symbol").map_err(internal_error)?,
                    start_ts_ns: row_i64(&row, "start_ns").map_err(internal_error)?,
                    end_ts_ns: row_i64(&row, "end_ns").map_err(internal_error)?,
                    open: row_f64(&row, "open").map_err(internal_error)?,
                    high: row_f64(&row, "high").map_err(internal_error)?,
                    low: row_f64(&row, "low").map_err(internal_error)?,
                    close: row_f64(&row, "close").map_err(internal_error)?,
                    volume: row_f64(&row, "volume").map_err(internal_error)?,
                };
                metrics.inc_rows(METHOD_GET_BARS, 1);
                yield bar;
            }
            metrics.observe_request_duration(METHOD_GET_BARS, started.elapsed());
        };

        Ok(Response::new(Box::pin(stream) as BarStream))
    }

    async fn get_orderbook_snapshot(
        &self,
        request: Request<GetOrderbookSnapshotRequest>,
    ) -> Result<Response<OrderbookSnapshot>, Status> {
        self.metrics.inc_request(METHOD_GET_ORDERBOOK_SNAPSHOT);
        let payload = request.into_inner();
        let symbol = Self::validate_symbol(&payload.symbol)?;
        let (start_ns, _) = Self::validate_range(payload.at_ns, payload.at_ns)?;

        let started = Instant::now();
        let find_sql = format!(
            "SELECT toUnixTimestamp64Nano(max(exchange_ts)) AS ts \
             FROM gmo.orderbooks \
             WHERE symbol = '{symbol}' \
               AND exchange_ts <= fromUnixTimestamp64Nano({start_ns}) \
             FORMAT JSONEachRow"
        );

        let (row_opt, elapsed) = self
            .client
            .query_one(find_sql)
            .await
            .map_err(internal_error)?;
        self.metrics
            .observe_clickhouse_latency(METHOD_GET_ORDERBOOK_SNAPSHOT, elapsed);

        let Some(row) = row_opt else {
            return Err(Status::not_found(
                "no snapshot available before requested time",
            ));
        };
        let ts = row_i64(&row, "ts").map_err(internal_error)?;
        if ts == 0 {
            return Err(Status::not_found(
                "no snapshot available before requested time",
            ));
        }

        let snapshot_sql = format!(
            "SELECT symbol, side, price, size, \
                    toUnixTimestamp64Nano(exchange_ts) AS exchange_ts_ns, \
                    toUnixTimestamp64Nano(received_at) AS received_at_ns \
             FROM gmo.orderbooks \
             WHERE symbol = '{symbol}' \
               AND exchange_ts = fromUnixTimestamp64Nano({ts}) \
             FORMAT JSONEachRow"
        );

        let query = self
            .client
            .query_collect(snapshot_sql)
            .await
            .map_err(internal_error)?;
        self.metrics
            .observe_clickhouse_latency(METHOD_GET_ORDERBOOK_SNAPSHOT, query.elapsed);

        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut exchange_ts_ns = ts;
        for row in query.rows {
            exchange_ts_ns = row_i64(&row, "exchange_ts_ns").map_err(internal_error)?;
            let price = row_f64(&row, "price").map_err(internal_error)?;
            let size = row_f64(&row, "size").map_err(internal_error)?;
            match row_value(&row, "side").map_err(internal_error)?.as_str() {
                "BID" => bids.push(OrderbookLevel { price, size }),
                "ASK" => asks.push(OrderbookLevel { price, size }),
                other => return Err(Status::internal(format!("unexpected side {other}"))),
            }
        }

        if bids.is_empty() && asks.is_empty() {
            return Err(Status::not_found("orderbook snapshot empty"));
        }

        let snapshot = OrderbookSnapshot {
            symbol: symbol.to_string(),
            exchange_ts_ns,
            bids,
            asks,
        };
        self.metrics
            .observe_request_duration(METHOD_GET_ORDERBOOK_SNAPSHOT, started.elapsed());
        Ok(Response::new(snapshot))
    }

    async fn replay(
        &self,
        request: Request<ReplayRequest>,
    ) -> Result<Response<Self::ReplayStream>, Status> {
        self.metrics.inc_request(METHOD_REPLAY);
        let payload = request.into_inner();
        if payload.symbols.is_empty() {
            return Err(Status::invalid_argument(
                "at least one symbol must be provided",
            ));
        }
        let (start_ns, end_ns) = Self::validate_range(payload.start_ns, payload.end_ns)?;
        let speed = if payload.speed <= 0.0 {
            1.0
        } else {
            payload.speed
        };

        let mut events = Vec::new();
        for symbol in payload.symbols.iter() {
            let symbol = Self::validate_symbol(symbol)?;

            let trades_sql = format!(
                "SELECT symbol, event_id, toUnixTimestamp64Nano(exchange_ts) AS exchange_ts_ns, \
                        toUnixTimestamp64Nano(received_at) AS received_at_ns, price, amount, side \
                 FROM gmo.trades \
                 WHERE symbol = '{symbol}' \
                   AND exchange_ts >= fromUnixTimestamp64Nano({start_ns}) \
                   AND exchange_ts <= fromUnixTimestamp64Nano({end_ns}) \
                 ORDER BY exchange_ts \
                 FORMAT JSONEachRow"
            );
            let trades = self
                .client
                .query_collect(trades_sql)
                .await
                .map_err(internal_error)?;
            self.metrics
                .observe_clickhouse_latency(METHOD_REPLAY, trades.elapsed);
            for row in trades.rows {
                let (trade, received) = Self::parse_trade(&row).map_err(internal_error)?;
                events.push((
                    trade.exchange_ts_ns,
                    MarketMessage {
                        msg: Some(Msg::Trade(trade)),
                        received_at_ns: received,
                    },
                ));
            }

            let tickers_sql = format!(
                "SELECT symbol, toUnixTimestamp64Nano(exchange_ts) AS exchange_ts_ns, \
                        toUnixTimestamp64Nano(received_at) AS received_at_ns, best_bid, best_ask, last, volume_24h \
                 FROM gmo.tickers \
                 WHERE symbol = '{symbol}' \
                   AND exchange_ts >= fromUnixTimestamp64Nano({start_ns}) \
                   AND exchange_ts <= fromUnixTimestamp64Nano({end_ns}) \
                 ORDER BY exchange_ts \
                 FORMAT JSONEachRow"
            );
            let tickers = self
                .client
                .query_collect(tickers_sql)
                .await
                .map_err(internal_error)?;
            self.metrics
                .observe_clickhouse_latency(METHOD_REPLAY, tickers.elapsed);
            for row in tickers.rows {
                let (ticker, received) = Self::parse_ticker(&row).map_err(internal_error)?;
                events.push((
                    ticker.exchange_ts_ns,
                    MarketMessage {
                        msg: Some(Msg::Ticker(ticker)),
                        received_at_ns: received,
                    },
                ));
            }

            let orderbook_sql = format!(
                "SELECT symbol, \
                        toUnixTimestamp64Nano(exchange_ts) AS exchange_ts_ns, \
                        toUnixTimestamp64Nano(received_at) AS received_at_ns, \
                        groupArrayIf([price, size], side = 'BID') AS bids, \
                        groupArrayIf([price, size], side = 'ASK') AS asks \
                 FROM gmo.orderbooks \
                 WHERE symbol = '{symbol}' \
                   AND exchange_ts >= fromUnixTimestamp64Nano({start_ns}) \
                   AND exchange_ts <= fromUnixTimestamp64Nano({end_ns}) \
                 GROUP BY symbol, exchange_ts, received_at \
                 ORDER BY exchange_ts \
                 FORMAT JSONEachRow"
            );
            let orderbooks = self
                .client
                .query_collect(orderbook_sql)
                .await
                .map_err(internal_error)?;
            self.metrics
                .observe_clickhouse_latency(METHOD_REPLAY, orderbooks.elapsed);
            for row in orderbooks.rows {
                let (snapshot, received) = Self::parse_orderbook(&row).map_err(internal_error)?;
                events.push((
                    snapshot.exchange_ts_ns,
                    MarketMessage {
                        msg: Some(Msg::ObSnapshot(snapshot)),
                        received_at_ns: received,
                    },
                ));
            }
        }

        events.sort_by_key(|(ts, _)| *ts);

        let metrics = self.metrics.clone();
        let stream = try_stream! {
            let mut previous_ts: Option<i64> = None;
            let started = Instant::now();
            for (ts, message) in events.into_iter() {
                if let Some(prev) = previous_ts {
                    let delta = ts.saturating_sub(prev);
                    if delta > 0 {
                        let scaled = (delta as f64 / speed).max(0.0);
                        if scaled > 0.0 {
                            let sleep_ns = scaled.min(5.0 * 1e9).round() as u64; // cap gaps at ~5s
                            if sleep_ns > 0 {
                                time::sleep(Duration::from_nanos(sleep_ns)).await;
                            }
                        }
                    }
                }
                metrics.inc_rows(METHOD_REPLAY, 1);
                yield message;
                previous_ts = Some(ts);
            }
            metrics.observe_request_duration(METHOD_REPLAY, started.elapsed());
        };

        Ok(Response::new(Box::pin(stream) as MarketStream))
    }
}

/// Helper to build server.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_validation_rejects_lowercase() {
        assert!(BacktestServiceImpl::validate_symbol("BTC_JPY").is_ok());
        assert!(BacktestServiceImpl::validate_symbol("btc").is_err());
    }

    #[test]
    fn interval_seconds_resolves_builtin() {
        let req = GetBarsRequest {
            symbol: "BTC_JPY".into(),
            interval: BarInterval::BarInterval5s.into(),
            aggregation: BarAgg::Ohlcv.into(),
            start_ns: 1,
            end_ns: 2,
            custom_interval_ns: 0,
        };
        assert_eq!(BacktestServiceImpl::interval_seconds(&req).unwrap(), 5);
    }
}
