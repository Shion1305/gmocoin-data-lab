-- Kafka engine tables that ingest protobuf payloads emitted by the ingestor.
-- Requires mounting proto/market.proto into ClickHouse's format schema path.

) ENGINE = Kafka
SETTINGS kafka_broker_list = '{KAFKA_BROKERS}',
        kafka_topic_list = 'gmo.market.trades',
        kafka_group_name = 'clickhouse-trades-consumer',
        kafka_format = 'Protobuf',
        format_schema = 'market.proto:gmocoin.market.MarketMessage',
        kafka_num_consumers = 1,
        kafka_max_block_size = 1048576;

CREATE TABLE IF NOT EXISTS gmo.trades_kafka (
    received_at_ns Int64,
    trade Nested (
        symbol String,
        event_id String,
        exchange_ts_ns Int64,
        price Float64,
        amount Float64,
        side String
    )
) ENGINE = Kafka
SETTINGS kafka_broker_list = '{KAFKA_BROKERS}',
        kafka_topic_list = 'gmo.market.trades',
        kafka_group_name = 'clickhouse-trades-consumer',
        kafka_format = 'Protobuf',
        format_schema = 'market.proto:gmocoin.market.MarketMessage',
        kafka_num_consumers = 1,
        kafka_max_block_size = 1048576;

CREATE TABLE IF NOT EXISTS gmo.tickers_kafka (
    received_at_ns Int64,
    ticker Nested (
        symbol String,
        exchange_ts_ns Int64,
        best_bid Float64,
        best_ask Float64,
        last Float64,
        volume_24h Float64
    )
) ENGINE = Kafka
SETTINGS kafka_broker_list = '{KAFKA_BROKERS}',
        kafka_topic_list = 'gmo.market.ticker',
        kafka_group_name = 'clickhouse-ticker-consumer',
        kafka_format = 'Protobuf',
        format_schema = 'market.proto:gmocoin.market.MarketMessage',
        kafka_num_consumers = 1,
        kafka_max_block_size = 1048576;

CREATE TABLE IF NOT EXISTS gmo.orderbooks_kafka (
    received_at_ns Int64,
    ob_snapshot Nested (
        symbol String,
        exchange_ts_ns Int64,
        bids Nested (
            price Float64,
            size Float64
        ),
        asks Nested (
            price Float64,
            size Float64
        )
    )
) ENGINE = Kafka
SETTINGS kafka_broker_list = '{KAFKA_BROKERS}',
        kafka_topic_list = 'gmo.market.orderbook.snapshot',
        kafka_group_name = 'clickhouse-orderbook-consumer',
        kafka_format = 'Protobuf',
        format_schema = 'market.proto:gmocoin.market.MarketMessage',
        kafka_num_consumers = 1,
        kafka_max_block_size = 1048576;

-- Materialized views streaming data into the historical tables.
CREATE MATERIALIZED VIEW IF NOT EXISTS gmo.trades_mv TO gmo.trades AS
SELECT
    trade.symbol[1] AS symbol,
    trade.event_id[1] AS event_id,
    toDateTime64(trade.exchange_ts_ns[1] / 1e9, 9) AS exchange_ts,
    toDateTime64(received_at_ns / 1e9, 9) AS received_at,
    trade.price[1] AS price,
    trade.amount[1] AS amount,
    multiIf(lowerUTF8(trade.side[1]) = 'buy', 'BUY', lowerUTF8(trade.side[1]) = 'sell', 'SELL', 'BUY') AS side
FROM gmo.trades_kafka
WHERE length(trade.symbol) = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS gmo.tickers_mv TO gmo.tickers AS
SELECT
    ticker.symbol[1] AS symbol,
    toDateTime64(ticker.exchange_ts_ns[1] / 1e9, 9) AS exchange_ts,
    toDateTime64(received_at_ns / 1e9, 9) AS received_at,
    ticker.best_bid[1] AS best_bid,
    ticker.best_ask[1] AS best_ask,
    ticker.last[1] AS last,
    ticker.volume_24h[1] AS volume_24h
FROM gmo.tickers_kafka
WHERE length(ticker.symbol) = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS gmo.orderbooks_mv TO gmo.orderbooks AS
SELECT
    ob_snapshot.symbol[1] AS symbol,
    toDateTime64(ob_snapshot.exchange_ts_ns[1] / 1e9, 9) AS exchange_ts,
    toDateTime64(received_at_ns / 1e9, 9) AS received_at,
    'BID' AS side,
    bid_price AS price,
    bid_size AS size
FROM gmo.orderbooks_kafka
ARRAY JOIN ob_snapshot.bids.price AS bid_price, ob_snapshot.bids.size AS bid_size
UNION ALL
SELECT
    ob_snapshot.symbol[1] AS symbol,
    toDateTime64(ob_snapshot.exchange_ts_ns[1] / 1e9, 9) AS exchange_ts,
    toDateTime64(received_at_ns / 1e9, 9) AS received_at,
    'ASK' AS side,
    ask_price AS price,
    ask_size AS size
FROM gmo.orderbooks_kafka
ARRAY JOIN ob_snapshot.asks.price AS ask_price, ob_snapshot.asks.size AS ask_size;
