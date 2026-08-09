# BigQuery Emulator Console

The console is the shared frontend for the Go emulator and the compatibility lab. It uses the public BigQuery REST surface for datasets, tables, queries, previews, and jobs. Emulator-only project lifecycle calls are isolated in the HTTP adapter under `/emulator/v1`.

## Run

```bash
npm ci
VITE_USE_MOCK=true npm run dev
```

Open `http://localhost:4173/console/`. To use a running emulator instead of the mock adapter:

```bash
VITE_API_TARGET=http://localhost:9050 npm run dev
```

## Build and test

```bash
npm run typecheck
npm test
npm run build
npm run test:e2e
```

The container serves the same static bundle and forwards `/bigquery` and `/emulator` to `BQEMU_API_UPSTREAM`.

## Runtime toggle

The Go emulator owns `--ui-enabled` and `BQEMU_UI_ENABLED`. When disabled, it does not mount the console routes. The compatibility lab starts the console image only through its `ui` Compose profile. REST, gRPC, Spark, and `bq --api` remain independent of the UI.
