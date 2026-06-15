[private]
default:
    just --list

set dotenv-load

[group('quality')]
format:
    cargo fmt --all

alias f := format

[group('quality')]
check:
    cargo check --workspace --all-targets

[group('quality')]
test:
    cargo test --workspace

[group('quality')]
lint:
    cargo clippy --workspace --all-targets -- -D warnings

[group('quality')]
q:
    just format
    just check
    just test

[group('build')]
b:
    cargo build --workspace

[group('build')]
br:
    cargo build --workspace --release

[group('run')]
api:
    cargo run --bin explorer-api

[group('run')]
api-local:
    ./target/release/explorer-api --config config/local-api.toml

[group('run')]
worker:
    cargo run --bin explorer-worker -- --sync-once

[group('run')]
worker-local-sync-build:
    cargo build --release -p explorer-worker
    just worker-local-sync

[group('run')]
worker-local-sync:
    ./target/release/explorer-worker --config config/local-sync.toml

[group('run')]
worker-local-balance-sync:
    ./target/release/explorer-worker --config config/local-sync.toml --balance-sync-once

[group('run')]
worker-local-token-supply-sync:
    ./target/release/explorer-worker --config config/local-sync.toml --token-supply-sync-once

[group('run')]
worker-local-failed-tx-debug-sync:
    ./target/release/explorer-worker --config config/local-sync.toml --failed-tx-debug-sync-once

[group('run')]
migrate:
    cargo run --bin explorer-migrate

[group('run')]
parity-count REFERENCE_DATABASE_URL CANDIDATE_DATABASE_URL TABLE:
    cargo run --bin explorer-parity -- --reference-database-url {{REFERENCE_DATABASE_URL}} --candidate-database-url {{CANDIDATE_DATABASE_URL}} count --table {{TABLE}}

[group('run')]
parity-block-range REFERENCE_DATABASE_URL CANDIDATE_DATABASE_URL CHAIN FROM TO:
    cargo run --bin explorer-parity -- --reference-database-url {{REFERENCE_DATABASE_URL}} --candidate-database-url {{CANDIDATE_DATABASE_URL}} block-range --chain {{CHAIN}} --from {{FROM}} --to {{TO}}

[group('run')]
parity-block-range-strict-ids REFERENCE_DATABASE_URL CANDIDATE_DATABASE_URL CHAIN FROM TO:
    cargo run --bin explorer-parity -- --reference-database-url {{REFERENCE_DATABASE_URL}} --candidate-database-url {{CANDIDATE_DATABASE_URL}} block-range --mode strict-ids --chain {{CHAIN}} --from {{FROM}} --to {{TO}}

[group('smoke')]
rs-migrate:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; refusing to guess a database."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" cargo run --bin explorer-migrate'

[group('smoke')]
rs-fetch-project HEIGHT:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; worker writes must target an intentional DB."; exit 2; }; test -n "${EXPLORER_RPC_ENDPOINTS:-}" || { echo "Set EXPLORER_RPC_ENDPOINTS explicitly; refusing to fall back to a public RPC."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" EXPLORER_NEXUS="${EXPLORER_NEXUS:-mainnet}" EXPLORER_CHAIN="${EXPLORER_CHAIN:-main}" cargo run --bin explorer-worker -- --fetch-project-block {{HEIGHT}}'

[group('smoke')]
rs-sync-once:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; worker writes must target an intentional DB."; exit 2; }; test -n "${EXPLORER_RPC_ENDPOINTS:-}" || { echo "Set EXPLORER_RPC_ENDPOINTS explicitly; refusing to fall back to a public RPC."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" EXPLORER_NEXUS="${EXPLORER_NEXUS:-mainnet}" EXPLORER_CHAIN="${EXPLORER_CHAIN:-main}" cargo run --bin explorer-worker -- --sync-once'

[group('smoke')]
rs-shared-main-probe:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; probe must target an intentional DB."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" EXPLORER_RPC_ENDPOINTS="${EXPLORER_RPC_ENDPOINTS:-http://localhost:5172/rpc}" EXPLORER_NEXUS="${EXPLORER_NEXUS:-mainnet}" EXPLORER_CHAIN="${EXPLORER_CHAIN:-main}" cargo run --bin explorer-worker -- --once'

[group('smoke')]
rs-shared-main-sync-once:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; shared-main sync must target an intentional DB."; exit 2; }; test -n "${EXPLORER_WORKER_HEIGHT_LIMIT:-}" || { echo "Set EXPLORER_WORKER_HEIGHT_LIMIT to the hash-proven shared-main boundary for this run."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" EXPLORER_RPC_ENDPOINTS="${EXPLORER_RPC_ENDPOINTS:-http://localhost:5172/rpc}" EXPLORER_NEXUS="${EXPLORER_NEXUS:-mainnet}" EXPLORER_CHAIN="${EXPLORER_CHAIN:-main}" cargo run --bin explorer-worker -- --sync-once'

[group('smoke')]
rs-smoke HEIGHT:
    just rs-migrate
    just rs-fetch-project {{HEIGHT}}

[group('smoke')]
rs-api:
    sh -eu -c 'test -n "${EXPLORER_RS_DATABASE_URL:-}" || { echo "Set EXPLORER_RS_DATABASE_URL explicitly; refusing to guess a database."; exit 2; }; EXPLORER_DATABASE_URL="${EXPLORER_RS_DATABASE_URL}" EXPLORER_NEXUS="${EXPLORER_NEXUS:-mainnet}" EXPLORER_CHAIN="${EXPLORER_CHAIN:-main}" cargo run --bin explorer-api'
