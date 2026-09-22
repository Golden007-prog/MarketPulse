# Execute one statement at a time

Status: prepared from official Confluent documentation; **not cloud validated**. Record actual statement IDs, results and screenshots when executed. Files on disk do not establish a running pipeline or Stream Lineage.

## Preflight

1. Select the environment catalog and `marketpulse` Kafka cluster database in the Flink workspace. Confirm compatible compute-pool region and capacity in the console.
2. Confirm the HTTP Source connector is running and all three `marketpulse_trades_*` topics receive real Coinbase records. Each record must be one object with `trade_id`, `price`, `size`, `side`, `time`. A top-level array/envelope requires connector extraction changes or explicit unnesting before this SQL.
3. Run validation.sql section A individually. Inspect existing registered schemas rather than registering a guessed schema over a live topic. Keep JSON_SR output; the app decoder cannot read Avro.
4. Inspect any existing target definitions. Do not drop tables to force reruns. Core creates three persistent jobs, three topics and schemas. Optional quarantine adds a fourth job. Streaming inspection SELECTs also consume resources until cancelled.

## Core: four statements, three running jobs

| Order | Statement in flink.sql | Verify before proceeding |
| --- | --- | --- |
| 1 | CREATE VIEW marketpulse_parsed | Validation B: positive typed values, UTC event time, null invalid_reason on valid rows. The view reserves a metadata-only topic name; it stores no data and is not a running job. |
| 2 | CREATE MATERIALIZED TABLE marketpulse_events | RUNNING state, topic receives records, SHOW definition contains event_time watermark. |
| 3 | CREATE MATERIALIZED TABLE marketpulse_windows | First run validation D EXPLAIN; expect bounded window dedup plus aggregation. Then verify output for all three symbols. |
| 4 | CREATE MATERIALIZED TABLE marketpulse_signals | Observe new rows; validation F rule_match and honest_forecast_contract are true. |

Copy one complete semicolon-terminated statement per workspace cell. Wait for RUNNING state before advancing. Submission success is not execution success: inspect failures and topic output. If materialized-table syntax is unavailable in the account/runtime, use documented separate CREATE TABLE and INSERT INTO statements with the same schema/SELECT, and retain INSERT job IDs. Never silently rewrite existing schemas.

Windows wait for event-time progress through a 30-second window and a 30-second watermark allowance. Idle partitions add delay. Polling newest-first means out-of-order arrival; this tolerance does not guarantee coverage. Events behind the watermark can be discarded. Startup history may emit old windows first: inspect timestamps. A stopped feed generates no zero-volume windows and can stop watermark progress. Use the app's independent freshness monitor.

## Optional quarantine and lineage evidence

After core succeeds and pool capacity permits, run quarantine-optional.sql. It preserves typed values that fail field checks, not errors occurring before SQL deserialization. Empty quarantine is plausible; do not fabricate live records for a screenshot.

Open Stream Lineage around marketpulse_events or marketpulse_signals and expand upstream/downstream. Expect three raw topics, events, windows and signals, plus quarantine if actually deployed. The parsed view may expand into its sources instead of a topic node. Connector/query edges depend on the observed graph. Save a real screenshot with capture time; the repository diagram is only intended design.

## Handoff checks

- Require recent rows for each symbol, not merely a RUNNING connector. Inspect task errors, failed/deserialization metrics and app rejected-message counters.
- Coinbase side is maker side. Taker BUY_PRESSURE uses maker-side sell quantity.
- Notional/VWAP are approximate floating-point aggregates over sampled unique trades, not full venue volume or accounting values.
- price_range_pct measures maximum/minimum range, not first-to-last return. sample_span_seconds is integer event-time span, not measured coverage.
- Connect the server consumer to raw topics and marketpulse_signals. GitHub Pages/browser code must use the backend and never receive Kafka credentials.
- Cancel temporary inspections. Keep required jobs running for the demo; stop/pause resources via console when finished. Do not delete topics as a stopping shortcut.

## Forecast follow-up, outside this deployment

The prior draft compared a current observation with an unaligned future forecast. This revision removes that comparison. Before adding a model, persist prediction issue time and returned target timestamp, match the actual for that target only after it closes, bound join state, define missing-window handling and backtest against a simple baseline. A non-null prediction is not proof of correctness. No forecast accuracy or financial benefit is claimed.
