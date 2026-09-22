-- OPTIONAL additional continuous job/topic. DRAFT: NOT CLOUD VERIFIED.
-- Run after core statement 1 creates marketpulse_parsed, only with pool capacity.
-- This catches typed business-field validation failures, not connector failures
-- or unparseable Kafka bytes. It is NOT a deserialization dead-letter queue.
-- Valid source data can leave this topic empty; do not inject fake live trades.
CREATE MATERIALIZED TABLE marketpulse_quarantine (
  symbol STRING,
  raw_trade_id STRING,
  raw_price STRING,
  raw_size STRING,
  raw_side STRING,
  raw_time STRING,
  invalid_reason STRING
)
WITH ('value.format' = 'json-registry', 'kafka.retention.time' = '86400000 ms')
AS SELECT symbol, raw_trade_id, raw_price, raw_size, raw_side, raw_time, invalid_reason
FROM marketpulse_parsed
WHERE invalid_reason IS NOT NULL;
