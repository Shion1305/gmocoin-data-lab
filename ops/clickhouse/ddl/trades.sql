-- Trades storage table. Deduplicated by event_id to avoid replays.
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
