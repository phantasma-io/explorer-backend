# Phantasma Explorer Backend

A Rust workspace that indexes the Phantasma blockchain into Postgres and serves the
Explorer HTTP API. A worker syncs blocks/transactions/events (plus derived data such
as balances, staking/Soul-Masters series, token supplies, NFT/series metadata, and
token prices) from Phantasma JSON-RPC nodes; an Axum API serves that data to the
Explorer frontend.

It runs against the Explorer Postgres schema (snake_case).

## Workspace Shape

- `crates/domain` – shared domain primitives and constants.
- `crates/config` – environment- and TOML-driven configuration.
- `crates/db` – `sqlx` Postgres access: connection, health checks, migrations, and
  schema writes. The crate root holds `DbError`, the model structs (`BlockRecord`,
  `TransactionRecord`, `EventUpsert`, …), and core block/transaction/address/balance
  CRUD; subsystems live in submodules: `db::staking` (current-stake upsert plus the
  forward Soul-Masters daily/monthly projector), `db::rpc_metadata`
  (contract/NFT/series RPC metadata), and `db::events` (event projection and its
  side effects). `db::reads` is the API read-model layer: one submodule per resource
  owns that resource's read SQL, so the HTTP crate embeds no SQL.
- `crates/rpc` – Phantasma SDK-backed JSON-RPC client with round-robin + failover
  across the configured endpoints.
- `crates/ingestion` – the `BlockIngestionDriver` sync/maintenance orchestrator.
- `crates/http-api` – the API router, handlers, and DTOs over the schema.
- `crates/runtime` – process lifecycle (tracing/logging setup, shutdown signals).
- `bins/explorer-api` – HTTP API service.
- `bins/explorer-worker` – ingestion/maintenance worker.
- `bins/explorer-migrate` – migration runner.
- `bins/explorer-parity` – database parity tooling (compares two Explorer DBs).

## Local Checks

```bash
just f       # cargo fmt
just check   # cargo check
just test    # cargo test
just lint    # cargo clippy -D warnings
just q       # fmt + check + test in one step
```

## Continuous Integration

`.github/workflows/ci.yml` runs the quality gate on every push/PR: `cargo fmt --all
--check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test
--workspace`. The workspace builds fully offline (only runtime `sqlx::query(...)`, no
compile-time `query!` macros), so no database is needed to compile or to run the
non-DB tests. The `crates/db` integration tests self-skip unless
`EXPLORER_TEST_DATABASE_URL` is set; CI runs them against a `postgres:17` service
seeded from `ci/test-schema.sql` (a `pg_dump --schema-only` of the schema) plus
`ci/test-seed.sql` (reference rows), and sets `EXPLORER_REQUIRE_DB_TESTS=1` so a
guard test fails if that URL is ever missing — the DB tests cannot silently skip in
CI. One test that needs a large data fixture is gated behind
`EXPLORER_TEST_FULL_BASELINE` and runs locally/nightly, not in CI.

## Configuration

Configuration is read from environment variables (prefixed `EXPLORER_`) or a TOML
file, with environment taking precedence. The repo ships annotated templates in
`config/`; copy them to the gitignored `config/local-*.toml` and set your database
URL and RPC endpoint:

```bash
cp config/example-sync.toml config/local-sync.toml
cp config/example-api.toml  config/local-api.toml
```

Key settings: `EXPLORER_DATABASE_URL`, `EXPLORER_RPC_ENDPOINTS` (one or more node
JSON-RPC URLs, tried round-robin with failover), `EXPLORER_BIND_ADDR`,
`EXPLORER_CHAIN` (`main`), and `EXPLORER_NEXUS` (an RPC/worker label, not a database
namespace). The Explorer database contains only the legacy chains `main` and
`main-generation-1`.

### Rate limiting

The worker can present an API key to a rate-limited node via `EXPLORER_RPC_API_KEY`
(sent as the `X-Api-Key` header, mapping the worker to its key tier). The API
enforces its own inbound rate limiting, modeled on the node's: per-key tiers, an
optional keys-only mode (`require_api_key`), a per-IP fallback (only behind trusted
proxies), and an always-on global in-flight cap — over a limit it returns HTTP 429
with `Retry-After`. Configure it in the `[rate_limiting]` section of
`config/example-api.toml`; clients send their key in the `X-Api-Key` header.

## Running

Apply migrations to the target database:

```bash
export EXPLORER_RS_DATABASE_URL='postgres://.../explorer'
just rs-migrate
```

Serve the API and run the worker:

```bash
just api-local           # API over config/local-api.toml
just worker-local-sync   # worker over config/local-sync.toml
```

Both write compact logs to the console and append them to a file under `logs/`
(`[logging].console = true` and `[logging].file` are both set in the templates).

### Worker sync modes

The worker defaults to `EXPLORER_WORKER_SYNC_MODE=normal`, which fetches blocks
concurrently and projects them for higher throughput while still advancing the cursor
strictly in height order (so crash recovery and reader-visible cursor semantics stay
deterministic). `sequential` projects one block at a time in deterministic insert
order, for parity/debug runs. `relief` forces one-block fetch/project windows for
difficult ranges. `EXPLORER_WORKER_INTER_BLOCK_DELAY_MS` and
`EXPLORER_WORKER_BATCH_DELAY_MS` add explicit throttling for heavy chain sections.

### Near-tip maintenance

Once caught up to the tip, the worker also runs best-effort maintenance from the same
config: token-supply refresh, dirty address-balance refresh, current stake snapshots,
failed-transaction `debug_comment` recovery, CoinGecko token prices, and off-chain
TTRS NFT metadata. Each can be run once for inspection with the matching
`just worker-local-*` recipe or `--*-sync-once` worker flag (`explorer-worker --help`
lists them all).

## Parity Tooling

`explorer-parity` compares two Explorer databases by semantic digest, ignoring
insertion-order surrogate IDs, so a freshly synced database can be checked against a
reference:

```bash
just parity-block-range "$REFERENCE_DATABASE_URL" "$CANDIDATE_DATABASE_URL" main 1000000 1000100
```

It digests `blocks`, `transactions`, `events`, and `address_transactions` for the
height range. Use `just parity-block-range-strict-ids ...` to additionally verify
insertion-order ID parity.

## Deployment (Docker)

`docker/compose/docker-compose.yml` builds and runs the `api` and `worker` services
against an existing Postgres on the external Docker network `postgresql-network`. The
backend connects to an existing database and indexes forward from the state already in
it; it does not bootstrap a database or reconstruct history from genesis.

1. Ensure Postgres is running and reachable on the `postgresql-network` Docker network
   (create it if needed: `docker network create postgresql-network`).
2. Create the target database and restore the prepared gen2 zero-state dump into it
   (the same dump for every network — use a separate DB per network, see "Networks"):

   ```bash
   createdb explorer_<network>
   pg_restore -d explorer_<network> -j 4 <prepared-dump>.bak
   ```

   The worker indexes forward from that baseline; the first migration requires this
   restored schema and fails on an empty database.
3. Configure the stack: `cp .env.example .env`, then set the database URL, RPC
   endpoint(s), and published ports. The API must bind `0.0.0.0` inside the container
   for the published port to be reachable (`.env.example` sets
   `EXPLORER_BIND_ADDR=0.0.0.0:9000`; the code default `localhost` is container-local).
4. Apply database migrations (idempotent):

   ```bash
   docker build -f docker/migrate/Dockerfile -t explorer-backend-rust-migrate .
   docker run --rm --network postgresql-network --env-file .env explorer-backend-rust-migrate
   ```

5. Start the API and worker:

   ```bash
   docker compose -f docker/compose/docker-compose.yml up -d --build
   ```

6. Verify: the API answers (`curl http://<host>:<API_PORT>/api/v1/chains`) and the
   worker log reports `caught up to tip <height>` once it has indexed forward to the
   chain tip.

### Networks (mainnet / devnet / testnet)

All Phantasma networks share the same gen2 zero-state base (`main` block 6,422,526):
they restore the **same** prepared dump and diverge only above the boundary. Deploy a
non-mainnet instance into its **own separate database** (never the mainnet one) and set
the network in `.env`:

| Network | `EXPLORER_NEXUS` | `EXPLORER_RPC_ENDPOINTS` |
| --- | --- | --- |
| mainnet | `mainnet` | `https://pharpc1.phantasma.info/rpc` |
| devnet  | `testnet` | `https://devnet.phantasma.info/rpc` |
| testnet | `testnet` | `https://testnet.phantasma.info/rpc` |

`EXPLORER_CHAIN=main` on every network. Notes:
- The worker refuses to start if the configured RPC's block at the DB cursor does not
  match the stored block — a guard against pairing a devnet DB with a mainnet RPC (or
  vice-versa). Always pair the right DB with the right RPC.
- RPC endpoint URLs must include the `/rpc` path.
- Do not run the worker's `--stake-boundary-slice-capture-once` on devnet/testnet (it
  is mainnet-specific; the Soul-Masters curve simply stays empty otherwise).
- Rate limiting is on by default; per-IP limits apply only when `trusted_proxies` is
  set (behind a reverse proxy). See `[rate_limiting]` in `config/example-api.toml`.

## SDK Dependency

The workspace uses published `phantasma-sdk = "1.2.0"`.

## License

MIT — see [LICENSE](LICENSE).
