# MarketPulse

[GitHub Pages app](https://golden007-prog.github.io/MarketPulse/) · [90-second demo](DEMO.md) · [Cloud pipeline](pipeline/README.md)

## GitHub Pages and Confluent

The Pages build runs live public-data collection and Signal Wind Tunnel in the browser, using the same analytics and replay engine as the Node service. All assets work under `/MarketPulse/`. It does not need or expose credentials. The pipeline explorer is explicitly a blueprint until cloud execution is verified.

`npm run build:pages` produces an allowlisted `dist/`. The repository workflow runs tests, builds and deploys that folder to GitHub Pages. An optional `MARKETPULSE_API_BASE=https://your-backend.example` at build time selects an HTTPS API bridge instead of browser polling. That bridge must serve the Node API contract and allow the Pages origin through CORS. There is no silent public-data fallback in bridge mode. Kafka credentials belong only on the backend.

GitHub Pages cannot host the persistent Kafka consumer. The local Node service supports Confluent, while the published default remains clearly labeled public-data mode until a hosted consumer bridge is configured. See [DEPLOYMENT.md](DEPLOYMENT.md).

A market operations dashboard built around public Coinbase Exchange data, with an optional Confluent Cloud HTTP Source → Kafka → dashboard path. It helps an operations team spot stale feeds and unusually sharp observed price movement. It does not place trades, recommend investments, or invent forecast results.

## Run locally

The main demonstration is now **Signal Wind Tunnel**: freeze a real observed sample, inject duplicates, missing events, late arrivals or a synthetic price shock, and compare unguarded and quality-aware decisions. The original live market observatory remains below the experiment. See [DEMO.md](DEMO.md) for a 90-second walkthrough and recorded results.

Requires Node.js 22 or newer. From this folder:

```powershell
npm install
npm test
npm start
```

Open http://127.0.0.1:4317. The default public API preview needs no account credentials. On Windows where Node is not on PATH, use `C:/Program Files/nodejs/node.exe server.mjs`.

The server fetches actual public trades and ticker data for BTC-USD, ETH-USD and SOL-USD every 10 seconds and one-minute candle history every minute. Requests have an eight-second timeout and bounded retry for transport failures, 429 and 5xx responses. Last successful data remains visible with age and error status. No fabricated fallback data is provided.

## Confluent mode

Install the dependency, copy `.env.example` to `.env`, then set:

```text
CONFLUENT_BOOTSTRAP_SERVER=your-cluster.region.provider.confluent.cloud:9092
CONFLUENT_API_KEY=your-key
CONFLUENT_API_SECRET=your-secret
CONFLUENT_TOPIC_PREFIX=marketpulse_trades_
CONFLUENT_GROUP_ID=marketpulse-dashboard-v1
```

`npm start` loads `.env` on the server. Never place credentials in frontend files, commit `.env`, or publish it with a submission.

The expected source topics are `marketpulse_trades_BTC-USD`, `marketpulse_trades_ETH-USD`, and `marketpulse_trades_SOL-USD`. Each value is a Coinbase trade object with `time`, `trade_id`, `price`, `size`, and `side`, encoded as JSON_SR (Confluent magic byte + schema ID + JSON) or plain JSON. Arrays of trade objects are also accepted. This consumer does not decode Avro.

With all three credentials configured, trades are consumed exclusively from Kafka over TLS with SASL/PLAIN. There is no hidden fallback to the direct public trade endpoint. The UI reports “Confluent live” only after fresh Kafka consumption. Charts continue to use explicitly labeled public Coinbase candle history. Kafka records are deduplicated for analytics by product and trade ID. Consumer records from a previous session may replay and are filtered by event-time retention.

Cloud connector and Flink setup belong in the `pipeline/` folder. Local movement detection is labeled “Local analytics.”

### Optional Flink output

After creating the output topic, set `CONFLUENT_SIGNALS_TOPIC=marketpulse_signals` and restart. A separate Kafka consumer group (`<group ID>-signals`) reads the output; a missing or broken output topic does not stop raw trade consumption. Revision 2 publishes transparent operational signals and sets `model_status=NOT_CONFIGURED`, with all forecast fields null. The card can also display a future timestamp-aligned forecast when one is actually supplied; the current SQL does not train a model or claim forecasting accuracy.

Expected JSON/JSON_SR fields: `symbol` (one of the three pairs), `window_end` (UTC timestamp), `observed_trades` (nonnegative safe integer), `observed_notional`, `sample_vwap`, `forecast_trades`, `lower_bound`, `upper_bound`, and `signal` (`WARMING_UP`, `ACTIVITY_SPIKE`, or `NORMAL`). All numeric fields must be finite and nonnegative; the three forecast fields may be null. Clamp count forecasts and bounds at zero in SQL. Timezone-free Flink TIMESTAMP(3) strings are interpreted as UTC.

The latest completed window is retained per symbol; an older replay cannot replace a newer window. “Flink output live” requires a connected signal consumer, no active signal error, and both window-end age and receipt age no more than 180 seconds. Stale results remain visible as historical. A null forecast stays blank; no prediction or spike is fabricated while the pipeline warms up.

## Controls and calculations

### Signal Wind Tunnel

Choose an experiment market and click **Freeze live window**. A fresh source update and at least three distinct trades spanning 30 seconds are required. The frozen copy includes up to 1,000 trades from the last two minutes. Choose a scenario, severity and price-move threshold, then run the stress test. Changing controls marks the previous result as outdated until rerun; the experiment never modifies the live feed.

The comparison shows baseline, unguarded and guarded decisions, known sample coverage, a timeline and explicit checks. Missing known baseline IDs cause HOLD; duplicates are removed by product and trade ID. A valid price-move alert survives the guard. These rules are deterministic local processing, not unverified Flink output.

Use **Download evidence** to preserve the baseline, transformed events, inputs, rules, report and SHA-256 digest. Reproduce a saved artifact offline:

```powershell
node verify-evidence.mjs evidence/duplicate.json
```

The verifier checks both the digest and an exact recomputation. A digest is not a signed attestation of origin. Server snapshots and artifacts expire after 30 minutes; at most 12 of each are retained in memory. The included `evidence/` examples preserve a completed real-data experiment after restart.

### Market observatory

- Select a market from the watchlist; choose 15-minute, one-hour or four-hour candle history.
- Change the 60-second movement threshold between 0.2%, 0.5% and 1.0%.
- Pause/resume the visible view; collection continues on the server. Refresh view immediately retrieves the latest cached server state.
- Sample VWAP = sum(price × size) / sum(size), using scaled BigInt arithmetic with eight decimal places before display conversion. Prices and quantities are preserved as original strings on records.
- The movement rule requires at least 30 seconds between first and last observed trades in the last 60 seconds; otherwise it reports that the window is still building.
- Coinbase reports the **maker** order side. The table intentionally shows the opposite **taker** side.
- The server retains at most 10,000 sampled trades per product, and at most 15 minutes of event history. This in-memory demo resets on restart.

## Honest coverage and impact

REST polling of recent trades can miss activity between requests. Counts, volume, notional, movement and VWAP describe observed samples, never full exchange activity. Candle history is a separate aggregated source; a partially formed current minute may change. There is no “money saved” or forecasting accuracy claim. Potential business value is earlier operations triage and reduced manual feed checking; measure those outcomes in a real pilot.

The source is publicly accessible market data, not an assertion that the data has an open-source license. Review Coinbase terms before redistribution or commercial deployment. The app runs locally and is not deployed by these instructions.

## Sources

- [Coinbase trade endpoint and maker-side semantics](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-trades)
- [Coinbase candle field order and granularity](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)
- [Coinbase ticker endpoint](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-ticker)
- [KafkaJS consumer configuration](https://kafka.js.org/docs/consuming)

The server binds to localhost, keeps credentials server-side, serves only an explicit public-file allowlist and uses a restrictive content security policy. It is a local demonstration, without authentication or persistent storage; do not expose it directly as a production service.
