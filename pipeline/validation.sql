-- READ-ONLY validation. Run one statement at a time; cancel inspections when done.
-- DRAFT: the operator must record actual cloud results separately.

-- A. Required source schemas: each record must expose these five value fields.
DESCRIBE `marketpulse_trades_BTC-USD`;
DESCRIBE `marketpulse_trades_ETH-USD`;
DESCRIBE `marketpulse_trades_SOL-USD`;
SELECT trade_id, price, size, side, `time` FROM `marketpulse_trades_BTC-USD` LIMIT 3;
SELECT trade_id, price, size, side, `time` FROM `marketpulse_trades_ETH-USD` LIMIT 3;
SELECT trade_id, price, size, side, `time` FROM `marketpulse_trades_SOL-USD` LIMIT 3;

-- B. After core statement 1: conversion and side semantics before writes.
SELECT symbol, raw_time, event_time, trade_id, price, size, maker_side, invalid_reason
FROM marketpulse_parsed LIMIT 12;

-- C. After core statement 2: inspect the declared event-time watermark.
SHOW CREATE MATERIALIZED TABLE marketpulse_events;
SELECT symbol, trade_id, price, size, maker_side, event_time FROM marketpulse_events LIMIT 12;

-- D. Explain before creating core statement 3. Expect bounded window dedup and
-- aggregation, append-only output; not global trade-ID state that grows forever.
EXPLAIN
SELECT symbol, window_start, window_end, COUNT(*) AS observed_trades
FROM (
  SELECT symbol, trade_id, window_start, window_end, window_time,
    ROW_NUMBER() OVER (
      PARTITION BY window_start, window_end, symbol, trade_id
      ORDER BY event_time ASC
    ) AS rownum
  FROM TABLE(TUMBLE(TABLE marketpulse_events, DESCRIPTOR(event_time), INTERVAL '30' SECOND))
) AS ranked_trades
WHERE rownum = 1
GROUP BY symbol, window_start, window_end;

-- E. After core statements 3/4: nonempty closed windows while feed advances.
SELECT * FROM marketpulse_windows LIMIT 9;
SELECT * FROM marketpulse_signals LIMIT 9;

-- F. Each rule_match and honest_forecast_contract should be true.
SELECT symbol, window_end, operational_signal,
  operational_signal = CASE
    WHEN observed_trades < 10 OR sample_span_seconds < 10 THEN 'LOW_SAMPLE'
    WHEN price_range_pct >= 0.5 THEN 'WIDE_PRICE_RANGE'
    WHEN taker_buy_pct >= 80.0 THEN 'BUY_PRESSURE'
    WHEN taker_buy_pct <= 20.0 THEN 'SELL_PRESSURE'
    ELSE 'OBSERVING'
  END AS rule_match,
  forecast_trades IS NULL AND lower_bound IS NULL AND upper_bound IS NULL
    AND model_status = 'NOT_CONFIGURED' AS honest_forecast_contract
FROM marketpulse_signals LIMIT 9;

-- G. Optional quarantine can remain empty indefinitely on valid input.
-- SELECT * FROM marketpulse_quarantine LIMIT 3;
-- Cancel if empty; absence of output is not proof of zero errors.
