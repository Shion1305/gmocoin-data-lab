CREATE DATABASE IF NOT EXISTS gmo;

-- Trades storage table. Deduplicated by event_id.
CREATE TABLE IF NOT EXISTS gmo.trades (
    symbol String,
    event_id String,
    exchange_ts DateTime64(9),
    received_at DateTime64(9),
    price Float64,
    amount Float64,
    side Enum8('BUY' = 1, 'SELL' = 2)
) ENGINE = ReplacingMergeTree(exchange_ts)
ORDER BY (symbol, exchange_ts);

CREATE TABLE IF NOT EXISTS gmo.tickers (
    symbol String,
    exchange_ts DateTime64(9),
    received_at DateTime64(9),
    best_bid Float64,
    best_ask Float64,
    last Float64,
    volume_24h Float64
) ENGINE = MergeTree
ORDER BY (symbol, exchange_ts);

CREATE TABLE IF NOT EXISTS gmo.orderbooks (
    symbol String,
    exchange_ts DateTime64(9),
    received_at DateTime64(9),
    side Enum8('BID' = 1, 'ASK' = 2),
    price Float64,
    size Float64
) ENGINE = MergeTree
ORDER BY (symbol, exchange_ts, price, side);
