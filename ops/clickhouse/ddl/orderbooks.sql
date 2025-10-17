CREATE TABLE IF NOT EXISTS gmo.orderbooks (
    symbol String,
    exchange_ts DateTime64(9),
    received_at DateTime64(9),
    side Enum8('BID' = 1, 'ASK' = 2),
    price Float64,
    size Float64
) ENGINE = MergeTree
ORDER BY (symbol, exchange_ts, price, side);

