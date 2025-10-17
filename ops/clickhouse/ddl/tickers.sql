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

