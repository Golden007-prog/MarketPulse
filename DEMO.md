# MarketPulse: Signal Wind Tunnel

**Pitch:** Before trusting a market alert, prove how it behaves when the data goes wrong.

Market operations teams need to distinguish genuine movement from duplicated, missing or late records. Signal Wind Tunnel freezes a real public trade sample, introduces a controlled fault, and compares an unguarded alert with a decision that checks data quality. Every result includes the records and rules needed to reproduce it offline.

## A 90-second demonstration

1. Open http://127.0.0.1:4317. Point out the actual source status and sampled-data label.
2. Click **Freeze live window**. Show its source, capture timestamp, trade count and time span.
3. Choose **Duplicate burst**, moderate severity, and **Run stress test**. Compare inflated activity with the deduplicated decision. A calm baseline should produce ALERT versus CLEAR; existing real movement may correctly keep both at ALERT.
4. Choose **Missing events** and run again. The guarded decision becomes HOLD because known records from this frozen baseline are absent.
5. Choose **Price shock**. A synthetic 3% shift in the final third tests whether a meaningful movement survives the quality guard. Explain that the injected movement is simulated.
6. Download the evidence. Run `node verify-evidence.mjs evidence/duplicate.json` to show that the saved example reproduces its result without a network connection.

## Recorded experiment

Captured **22 September 2026, 10:52:19 UTC**, from Coinbase public REST. Each experiment used the same 500 observed BTC-USD trades, moderate severity and a 0.5% price threshold.

| Controlled scenario | Records evaluated before deduplication | Accepted records | Unguarded decision | Guarded decision |
| --- | ---: | ---: | --- | --- |
| Add two duplicate copies per trade | 1,500 | 500 | ALERT | CLEAR |
| Remove half the baseline records | 250 | 250 | CLEAR | HOLD |
| Delay half beyond their replay deadlines | 250 | 250 | CLEAR | HOLD |
| Inject a 3% price shift into the final third | 500 | 500 | ALERT | ALERT |

All four exported artifacts passed SHA-256 integrity checking and deterministic replay. Files are in `evidence/`; `results.json` summarizes the run. These are controlled demonstration results, not production accuracy or savings measurements. New live samples can produce different decisions.

## What makes the demonstration distinctive

The combination is an interactive decision experiment: a frozen baseline, adjustable fault severity, side-by-side decisions, an event-time timeline, explicit checks and a portable replay artifact. It makes the consequences of streaming quality visible to a judge in seconds.

Do not claim that replay or feed-quality monitoring has never existed. [Corvil market-data analytics](https://www.pico.net/products/corvil-analytics/market-data-analytics/) already addresses feed-quality problems, and [Nasdaq surveillance](https://www.nasdaq.com/products/fintech/surveillance) includes investigation and replay capabilities. This project demonstrates its particular combination and transparent evidence rather than claiming an unverified invention.

## Business hypothesis to validate

Fewer duplicate-driven escalations, less analyst time investigating incomplete data, and faster reproduction of incidents. A pilot should measure unnecessary escalations per day, investigation minutes per incident, and time to reproduce a disputed decision. No financial savings or accuracy percentage has been measured here.

## Confluent connection and current boundary

The intended ingestion path is HTTP Source connector -> Kafka -> Flink -> dashboard. The app includes an authenticated Kafka consumer and an optional consumer for actual Flink results. The wind-tunnel experiment currently runs in the local deterministic JavaScript engine against the app's observed feed. It can freeze Kafka-sourced samples once that mode is configured; it does not currently execute fault experiments in Flink.

The replacement Basic cluster exists, but the paid HTTP Source connector remains at review pending a running-budget choice. `pipeline/flink.sql` is a draft and has not been executed. Present the current demo as a working public-data application with prepared Confluent integration, not an already verified end-to-end cloud pipeline. End-to-end ingestion and Flink execution remain the most important steps before a Confluent challenge submission.

## Interpretation of the evidence

- Coverage means records received from this known frozen sample. REST polling cannot establish completeness of the whole exchange feed.
- Late-event experiments use each event's timestamp plus a 20-second replay deadline. This is a controlled model, not a measured transport latency.
- SHA-256 detects changes relative to the stored digest. It is not a digital signature or independent proof of source authenticity.
- Snapshots and downloadable server artifacts expire after 30 minutes and reset when the server restarts. Save an export for later replay.
- Publicly accessible data is not automatically openly licensed. Source semantics are documented in the [Coinbase trade API](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades).
