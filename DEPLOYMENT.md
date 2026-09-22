# Deployment and verified boundaries

## Published application

Target: https://golden007-prog.github.io/MarketPulse/

The `pages.yml` workflow installs pinned dependencies, runs tests, builds the allowlisted `dist/` folder and deploys it with GitHub Pages. The public build uses Coinbase browser requests and the shared deterministic engine. A browser session needs at least 30 seconds of observed history before it can freeze an experiment. Background-tab throttling, provider restrictions or rate limits can interrupt collection; the app shows the error and retains the last observations.

Source files, schemas, tests and example replay evidence are in this repository. `.env`, `node_modules`, `.git` and server code never enter the Pages artifact. Evidence describes public market samples, not account or trading data.

## Confluent path

The replacement Basic cluster is created, and HTTP Source is configured at final review. Launching requires the requested running-budget choice because connector, Kafka and Flink usage are billed. Run the inspected statement sequence in [pipeline/execution-plan.md](pipeline/execution-plan.md) only after source schemas and actual records are confirmed.

The three continuous jobs produce `marketpulse_events`, `marketpulse_windows` and `marketpulse_signals`. The optional quarantine branch explains field-validation failures. Capture actual Confluent Stream Lineage once these are running, alongside source and sink record checks. A rendered blueprint in the app is not evidence of executed cloud jobs.

For a public dashboard that consumes Confluent rather than the public API directly, deploy the Node consumer to an always-on backend, configure its server-side Kafka credentials and allowed Pages origin, and build with `MARKETPULSE_API_BASE` pointing to that HTTPS origin. Use only public market records in this unauthenticated demo API. Do not expose private topics or administrative endpoints. This repository does not provision a backend host or silently enable paid services.

## Stop paid processing after a demonstration

Pause the HTTP connector and suspend each materialized-table refresh according to the account UI/current documented syntax. Verify every statement is stopped and the connector is paused. Pausing ingestion or compute does not itself remove retained Kafka storage or all possible cloud charges. Do not delete resources unless separately requested.

## Final verification checklist

1. GitHub deployment succeeds and the HTTPS URL opens.
2. The public page reports recent actual trade observations for all three markets.
3. Freezing, scenario changes, evidence download, watchlist, chart ranges and pause/resume work.
4. Source topics contain individual JSON_SR trade records before executing Flink SQL.
5. Each cloud output topic receives fresh, schema-valid rows; deduplicated windows and operational signals match their documented rules.
6. The hosted bridge, when added, shows fresh Kafka and signal consumption. No claim of cloud connectivity is based solely on configuration.

[GitHub Pages custom workflows](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages) describes the deployment mechanism. Cloud syntax references are linked in the pipeline directory.
