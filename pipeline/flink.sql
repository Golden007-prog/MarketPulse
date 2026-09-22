-- MarketPulse core, revision 2. DRAFT: NOT EXECUTED / VERIFIED IN CLOUD.
-- Run EACH numbered statement separately, in order; select the Kafka cluster's
-- database and its environment catalog in the workspace first. Do not paste
-- the entire file into one cell. See execution-plan.md and validation.sql.
-- Three continuous jobs; parsing is a view, not an extra running job/data topic.
-- Confluent reserves the view name with a metadata-only topic.
-- Expected raw value: ONE Coinbase trade object per Kafka record, JSON_SR.
-- DO NOT run if DESCRIBE shows an array/wrapper instead of the five fields.

-- 1. Shared parsing view. Raw strings preserve rejected values for inspection.
CREATE VIEW marketpulse_parsed AS
SELECT symbol, raw_trade_id, raw_price, raw_size, raw_side, raw_time,
  trade_id, price, size, raw_side AS maker_side, event_time,
  CASE
    WHEN trade_id IS NULL OR trade_id <= 0 THEN 'INVALID_TRADE_ID'
    WHEN price IS NULL OR price <= 0 THEN 'INVALID_PRICE'
    WHEN size IS NULL OR size <= 0 THEN 'INVALID_SIZE'
    WHEN raw_side IS NULL OR raw_side NOT IN ('buy', 'sell') THEN 'INVALID_MAKER_SIDE'
    WHEN raw_time IS NULL OR raw_time NOT LIKE '%Z' OR event_time IS NULL THEN 'INVALID_UTC_TIME'
    ELSE CAST(NULL AS STRING)
  END AS invalid_reason
FROM (
  SELECT symbol, raw_trade_id, raw_price, raw_size, raw_side, raw_time,
    TRY_CAST(raw_trade_id AS BIGINT) AS trade_id,
    TRY_CAST(raw_price AS DECIMAL(24, 8)) AS price,
    TRY_CAST(raw_size AS DECIMAL(24, 8)) AS size,
    TRY_CAST(REPLACE(REPLACE(raw_time, 'T', ' '), 'Z', '') AS TIMESTAMP(3)) AS event_time
  FROM (
    SELECT 'BTC-USD' AS symbol, CAST(trade_id AS STRING) AS raw_trade_id,
      CAST(price AS STRING) AS raw_price, CAST(size AS STRING) AS raw_size,
      CAST(side AS STRING) AS raw_side, CAST(`time` AS STRING) AS raw_time
    FROM `marketpulse_trades_BTC-USD`
    UNION ALL
    SELECT 'ETH-USD', CAST(trade_id AS STRING), CAST(price AS STRING),
      CAST(size AS STRING), CAST(side AS STRING), CAST(`time` AS STRING)
    FROM `marketpulse_trades_ETH-USD`
    UNION ALL
    SELECT 'SOL-USD', CAST(trade_id AS STRING), CAST(price AS STRING),
      CAST(size AS STRING), CAST(side AS STRING), CAST(`time` AS STRING)
    FROM `marketpulse_trades_SOL-USD`
  ) AS raw_rows
) AS parsed_rows;

-- 2. Valid event topic: duplicate polls still exist here intentionally.
CREATE MATERIALIZED TABLE marketpulse_events (
  symbol STRING,
  trade_id BIGINT,
  price DECIMAL(24, 8),
  size DECIMAL(24, 8),
  maker_side STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
)
WITH ('value.format' = 'json-registry', 'kafka.retention.time' = '86400000 ms')
AS SELECT symbol, trade_id, price, size, maker_side, event_time
FROM marketpulse_parsed
WHERE invalid_reason IS NULL;

-- 3. Bounded event-time deduplication and aggregation in ONE continuous job.
-- Keeping window_start/window_end in both partition and group keys is required.
-- Replay with the same product/trade ID AND original event timestamp maps to
-- the same window. Events behind the watermark may be discarded. Changed trade
-- timestamps or conflicting payloads are not resolved by this demo policy.
CREATE MATERIALIZED TABLE marketpulse_windows (
  symbol STRING,
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  observed_trades BIGINT,
  observed_notional DOUBLE,
  observed_quantity DOUBLE,
  sample_vwap DOUBLE,
  price_range_pct DOUBLE,
  taker_buy_pct DOUBLE,
  sample_span_seconds BIGINT
)
WITH ('value.format' = 'json-registry', 'kafka.retention.time' = '86400000 ms')
AS
SELECT symbol, window_start, window_end, COUNT(*) AS observed_trades,
  SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) AS observed_notional,
  SUM(CAST(size AS DOUBLE)) AS observed_quantity,
  SUM(CAST(price AS DOUBLE) * CAST(size AS DOUBLE)) / SUM(CAST(size AS DOUBLE)) AS sample_vwap,
  (MAX(CAST(price AS DOUBLE)) / MIN(CAST(price AS DOUBLE)) - 1.0) * 100.0 AS price_range_pct,
  SUM(CASE WHEN maker_side = 'sell' THEN CAST(size AS DOUBLE) ELSE CAST(0 AS DOUBLE) END)
    / SUM(CAST(size AS DOUBLE)) * 100.0 AS taker_buy_pct,
  CAST(TIMESTAMPDIFF(SECOND, MIN(event_time), MAX(event_time)) AS BIGINT) AS sample_span_seconds
FROM (
  SELECT symbol, trade_id, price, size, maker_side, event_time,
    window_start, window_end, window_time
  FROM (
    SELECT symbol, trade_id, price, size, maker_side, event_time,
      window_start, window_end, window_time,
      ROW_NUMBER() OVER (
        PARTITION BY window_start, window_end, symbol, trade_id
        ORDER BY event_time ASC
      ) AS rownum
    FROM TABLE(TUMBLE(TABLE marketpulse_events, DESCRIPTOR(event_time), INTERVAL '30' SECOND))
  ) AS ranked_trades
  WHERE rownum = 1
) AS unique_window_trades
GROUP BY symbol, window_start, window_end;

-- 4. Transparent operational rules. No ML model is configured or implied.
-- Legacy forecast fields remain explicit nulls for the existing consumer.
-- The UI must prefer model_status + operational_signal over legacy signal.
CREATE MATERIALIZED TABLE marketpulse_signals (
  symbol STRING,
  window_end TIMESTAMP(3),
  observed_trades BIGINT,
  observed_notional DOUBLE,
  sample_vwap DOUBLE,
  forecast_trades DOUBLE,
  lower_bound DOUBLE,
  upper_bound DOUBLE,
  signal STRING,
  model_status STRING,
  operational_signal STRING,
  price_range_pct DOUBLE,
  taker_buy_pct DOUBLE,
  observed_quantity DOUBLE,
  sample_span_seconds BIGINT
)
WITH ('value.format' = 'json-registry', 'kafka.retention.time' = '86400000 ms')
AS
SELECT symbol, window_end, observed_trades, observed_notional, sample_vwap,
  CAST(NULL AS DOUBLE) AS forecast_trades,
  CAST(NULL AS DOUBLE) AS lower_bound,
  CAST(NULL AS DOUBLE) AS upper_bound,
  CAST('WARMING_UP' AS STRING) AS signal,
  CAST('NOT_CONFIGURED' AS STRING) AS model_status,
  CASE
    WHEN observed_trades < 10 OR sample_span_seconds < 10 THEN 'LOW_SAMPLE'
    WHEN price_range_pct >= 0.5 THEN 'WIDE_PRICE_RANGE'
    WHEN taker_buy_pct >= 80.0 THEN 'BUY_PRESSURE'
    WHEN taker_buy_pct <= 20.0 THEN 'SELL_PRESSURE'
    ELSE 'OBSERVING'
  END AS operational_signal,
  price_range_pct, taker_buy_pct, observed_quantity, sample_span_seconds
FROM marketpulse_windows;
