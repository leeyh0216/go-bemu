# Client Compose Labs

Each directory is a Compose override for the repository root service. From a
lab directory run:

```bash
docker compose -f ../../../compose.yaml -f compose.yaml up --build --wait
```

The client container reaches REST at `http://bqemu:9050` and Storage gRPC at
`bqemu:9060`; the host reaches `http://localhost:9050` and `localhost:9060`.
The labs intentionally contain client configuration only: BQEMU has no
client-specific runtime branch.

- `python/`: official REST client environment.
- `spark/`: Spark connector endpoint variables.
- `trino/`: a network-ready Trino shell environment; install/configure a
  BigQuery-capable connector separately because BQEMU does not ship one.
- `aws/`: AWS SDK shell environment for S3-compatible indirect-load setup;
  AWS APIs themselves are not emulated.
