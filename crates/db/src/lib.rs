use chrono::{DateTime, Utc};
use explorer_config::DatabaseConfig;
use explorer_domain::{BlockHeight, ChainName};
use num_bigint::BigInt;
use num_traits::Zero;
use serde::Serialize;
use serde_json::Value;
use sqlx::postgres::{PgConnection, PgPoolOptions};
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use thiserror::Error;
use uuid::Uuid;

// Stake-snapshot subsystem (current-stake upsert + forward Soul-Masters
// projector). Public items are re-exported so the crate API
// (`explorer_db::project_stake_snapshots_forward`, etc.) stays flat.
mod staking;
pub use staking::*;

// RPC-driven contract/NFT/series metadata hydration. Public items re-exported
// to keep the `explorer_db::*` API unchanged.
mod rpc_metadata;
pub use rpc_metadata::*;

// Event projection + C#-parity side effects (token/NFT/series/infusion/burn).
// Public items re-exported to keep the `explorer_db::*` API unchanged.
mod events;
pub use events::*;

// Read-model queries for the HTTP API (typed read-records; the API maps them to
// wire DTOs). Keeps SQL in the db crate and makes read paths testable.
mod reads;
pub use reads::*;

const LEGACY_TOKEN_BURN_EVENT_KIND: &str = "TokenBurn";

fn is_nft_side_effect_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        "TokenMint"
            | "TokenClaim"
            | "TokenBurn"
            | "TokenSend"
            | "TokenReceive"
            | "TokenStake"
            | "CrownRewards"
            | "Inflation"
            | "Infusion"
            | "OrderCancelled"
            | "OrderClosed"
            | "OrderCreated"
            | "OrderFilled"
            | "OrderBid"
    )
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("database operation failed")]
    Sqlx(#[from] sqlx::Error),
    #[error("database JSON payload serialization failed")]
    Json(#[from] serde_json::Error),
    #[error("migration operation failed")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("block height {height} exceeds PostgreSQL bigint range")]
    BlockHeightOutOfRange { height: u64 },
    #[error("stored block height {height} cannot be represented as unsigned block height")]
    StoredBlockHeightOutOfRange { height: i64 },
    #[error("chain {chain:?} was not found in the database")]
    ChainMissing { chain: String },
    #[error("chain {chain:?} is ambiguous: found {matches} matching rows")]
    ChainAmbiguous { chain: String, matches: usize },
    #[error("token {symbol:?} for chain id {chain_id} was not found in the database")]
    TokenMissing { chain_id: i32, symbol: String },
    #[error("token {symbol:?} for chain id {chain_id} is ambiguous: found {matches} matching rows")]
    TokenAmbiguous {
        chain_id: i32,
        symbol: String,
        matches: usize,
    },
    #[error("staking snapshot projector cannot parse {field} raw integer value {value:?}")]
    StakeSnapshotInvalidRaw { field: &'static str, value: String },
    #[error("staking snapshot projector replay failed: {reason}")]
    StakeSnapshotReplay { reason: String },
    #[error("unknown event payload format {format:?}")]
    UnknownPayloadFormat { format: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct DatabaseHealth {
    pub ok: bool,
    pub checked_at: DateTime<Utc>,
    pub server_version: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MigrationReport {
    pub migrations_dir: PathBuf,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawBlockRecord {
    pub id: Uuid,
    pub nexus: String,
    pub chain: String,
    pub height: i64,
    pub hash: Option<String>,
    pub rpc_node: String,
    pub payload_json: Value,
    pub payload_bytes: i32,
    pub fetched_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventSource {
    Legacy,
    Extended,
    Synthetic,
}

impl EventSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Extended => "extended",
            Self::Synthetic => "synthetic",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlockUpsert {
    pub chain: ChainName,
    pub height: BlockHeight,
    pub hash: String,
    pub protocol: Option<i32>,
    pub chain_address: Option<String>,
    pub validator_address: Option<String>,
    /// Gas-model-v2 consensus-covered fee-payout identity; None for pre-v2
    /// blocks and the flip block itself. Not defaulted to the validator: the
    /// two coincide today but are distinct on the wire by design.
    pub producer_address: Option<String>,
    pub timestamp_unix_seconds: i64,
    pub reward: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockRecord {
    pub id: i32,
    pub chain_id: i32,
    pub chain: String,
    pub height: i64,
    pub hash: String,
    pub protocol: Option<i32>,
    pub chain_address_id: i32,
    pub chain_address: Option<String>,
    pub validator_address_id: i32,
    pub validator_address: Option<String>,
    pub producer_address_id: Option<i32>,
    pub producer_address: Option<String>,
    pub timestamp_unix_seconds: i64,
    pub reward: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TransactionSignatureUpsert {
    pub signature_index: i32,
    pub kind: String,
    pub data: String,
}

#[derive(Debug, Clone)]
pub struct TransactionUpsert {
    pub block_id: i32,
    pub chain_id: i32,
    pub tx_index: i32,
    pub hash: String,
    pub timestamp_unix_seconds: i64,
    pub state: String,
    pub result: Option<String>,
    pub debug_comment: Option<String>,
    pub payload: Option<String>,
    pub script_raw: Option<String>,
    pub fee_raw: Option<String>,
    pub gas_price_raw: Option<String>,
    pub gas_limit_raw: Option<String>,
    pub sender: Option<String>,
    pub gas_payer: Option<String>,
    pub gas_target: Option<String>,
    pub carbon_tx_type: Option<i32>,
    pub carbon_tx_data: Option<String>,
    pub expiration_unix_seconds: i64,
    pub signatures: Vec<TransactionSignatureUpsert>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransactionRecord {
    pub id: i32,
    pub block_id: i32,
    pub chain_id: i32,
    pub tx_index: i32,
    pub hash: String,
    pub timestamp_unix_seconds: i64,
    pub state: String,
    pub result: Option<String>,
    pub debug_comment: Option<String>,
    pub payload: Option<String>,
    pub script_raw: Option<String>,
    pub fee_raw: Option<String>,
    pub gas_price_raw: Option<String>,
    pub gas_limit_raw: Option<String>,
    pub sender_id: i32,
    pub gas_payer_id: i32,
    pub gas_target_id: i32,
    pub carbon_tx_type: Option<i32>,
    pub carbon_tx_data: Option<String>,
    pub expiration_unix_seconds: i64,
}

#[derive(Debug, Clone)]
pub struct EventUpsert {
    pub transaction_id: i32,
    pub chain_id: i32,
    pub event_index: i32,
    pub event_kind: String,
    /// ABI-declared event name for self-describing contract events (kind
    /// "Custom_V2"); None for native kinds, where the kind itself is the name.
    pub event_name: Option<String>,
    pub address: Option<String>,
    pub target_address: Option<String>,
    pub contract: Option<String>,
    pub token_id: Option<String>,
    pub raw_data: Option<String>,
    pub payload_format: Option<String>,
    pub payload_json: Option<Value>,
    pub timestamp_unix_seconds: i64,
    pub burned: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DirtyAddress {
    pub id: i32,
    pub address: String,
    pub balance_dirty_block: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ContractStringEventSideEffectReport {
    pub upserted_contracts: u64,
    pub linked_contract_creates: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractRpcMetadataCandidate {
    pub id: i32,
    pub name: String,
    pub insert_current_method: bool,
}

#[derive(Debug, Clone)]
pub struct ContractRpcMetadataUpsert {
    pub contract_id: i32,
    pub address: Option<String>,
    pub script_raw: Option<String>,
    pub methods: Option<Value>,
    pub insert_current_method: bool,
    pub last_updated_unix_seconds: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ContractRpcMetadataUpsertResult {
    pub updated_contract: bool,
    pub inserted_method: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractUpgradeMethodCandidate {
    pub contract_id: i32,
    pub name: String,
    pub timestamp_unix_seconds: i64,
}

#[derive(Debug, Clone)]
pub struct ContractUpgradeMethodUpsert {
    pub contract_id: i32,
    pub methods: Value,
    pub timestamp_unix_seconds: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ContractUpgradeMethodUpsertResult {
    pub inserted_method: bool,
    pub linked_contract: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct NftRpcMetadataCandidate {
    pub symbol: String,
    pub token_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct NftRpcMetadataUpsert {
    pub symbol: String,
    pub token_id: String,
    pub series_id: Option<String>,
    pub creator_address: Option<String>,
    pub mint_number: Option<i32>,
    pub mint_date_unix_seconds: Option<i64>,
    pub rom: Option<String>,
    pub ram: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub info_url: Option<String>,
    pub metadata: Value,
    pub chain_api_response: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesRpcMetadataCandidate {
    pub symbol: String,
    pub series_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesRpcMetadataUpsert {
    pub symbol: String,
    pub series_id: String,
    pub current_supply: Option<i32>,
    pub max_supply: Option<i32>,
    pub mode: Option<String>,
    pub creator_address: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub royalties: Option<i32>,
    pub series_type: Option<i32>,
    pub has_locked: Option<bool>,
    pub metadata: Value,
    pub chain_api_response: Value,
}

#[derive(Debug, Clone)]
pub struct AddressBalanceUpsert {
    pub symbol: String,
    pub amount_raw: String,
}

/// The balance-sync projection of one account. Carries only what the new
/// lightweight account endpoints provide: name, stake and balance rows.
///
/// The legacy `validator`/`storage`/`avatar` fields are no longer written — the
/// gen3 RPC does not supply them and hardcodes them on the endpoints that still
/// echo them. Their COLUMNS stay: in the zero state they hold gen1/gen2 account
/// state that no live source can reproduce (3,459 distinct `storage_available`
/// values, 32 distinct `storage_used`, 3 distinct avatars, and the 4 addresses
/// marked `Primary` validators). They are simply frozen at those values now.
#[derive(Debug, Clone)]
pub struct AddressAccountUpsert {
    pub address_id: i32,
    pub address_name: Option<String>,
    pub name_last_updated_unix_seconds: i64,
    pub stake_timestamp: i64,
    pub staked_amount_raw: String,
    pub unclaimed_amount_raw: String,
    pub soul_balance_raw: String,
    pub balances: Vec<AddressBalanceUpsert>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddressAccountUpsertResult {
    pub missing_balance_symbols: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TokenSupplyUpsert {
    pub symbol: String,
    pub carbon_id: Option<i64>,
    pub current_supply_raw: String,
    pub max_supply_raw: String,
    pub burned_supply_raw: String,
}

/// A token's on-chain metadata as the node answers it: one JSON object whose values
/// keep their VM shape (a scalar is a string, an array stays an array, a struct stays
/// an object).
#[derive(Debug, Clone)]
pub struct TokenMetadataUpsert {
    pub symbol: String,
    pub metadata: Value,
}

/// Live token USD price. `None` is left untouched on update so an unavailable
/// pairing never clobbers a value that is already there. USD is the only currency
/// the system prices (202608030004 dropped the per-currency columns).
#[derive(Debug, Clone)]
pub struct TokenPriceUpsert {
    pub symbol: String,
    pub price_usd: Option<f64>,
}

/// One historical daily USD close for a token, feeding `token_daily_prices`
/// (the `/historyPrices` chart series).
#[derive(Debug, Clone)]
pub struct TokenDailyPriceUpsert {
    pub symbol: String,
    pub date_unix_seconds: i64,
    pub price_usd: f64,
}

/// Off-chain NFT metadata fetched from an external store (TTRS / 22series), keyed by
/// the NFT's on-chain `token_id`. `offchain_api_response` is the raw JSON text written
/// to the `nfts.offchain_api_response` jsonb column; the rest patch the materialized
/// display fields. `None` fields are left untouched.
#[derive(Debug, Clone)]
pub struct NftOffchainUpsert {
    pub token_id: String,
    pub offchain_api_response: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub mint_number: Option<i32>,
    pub mint_date_unix_seconds: Option<i64>,
}

pub async fn connect(config: &DatabaseConfig) -> Result<PgPool, DbError> {
    let mut options = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.acquire_timeout);
    if let Some(statement_timeout) = config.statement_timeout {
        // Apply the per-connection statement_timeout so an abandoned slow request
        // cannot keep burning a pooled connection after the client has given up.
        let millis = u64::try_from(statement_timeout.as_millis()).unwrap_or(u64::MAX);
        options = options.after_connect(move |conn, _meta| {
            Box::pin(async move {
                sqlx::query(&format!("SET statement_timeout = {millis}"))
                    .execute(conn)
                    .await
                    .map(|_| ())
            })
        });
    }
    options.connect(&config.url).await.map_err(DbError::Sqlx)
}

pub async fn check_health(pool: &PgPool) -> Result<DatabaseHealth, DbError> {
    // `version()` gives a cheap connectivity check and useful runtime metadata
    // without depending on application tables that may not exist before migrate.
    let row = sqlx::query("SELECT version() AS server_version")
        .fetch_one(pool)
        .await?;
    let server_version = row.try_get::<String, _>("server_version").ok();

    Ok(DatabaseHealth {
        ok: true,
        checked_at: Utc::now(),
        server_version,
    })
}

pub async fn run_migrations(
    pool: &PgPool,
    migrations_dir: &Path,
) -> Result<MigrationReport, DbError> {
    let migrator = sqlx::migrate::Migrator::new(migrations_dir).await?;
    migrator.run(pool).await?;
    Ok(MigrationReport {
        migrations_dir: migrations_dir.to_owned(),
        completed_at: Utc::now(),
    })
}

pub fn default_migrations_dir() -> PathBuf {
    std::env::var_os("EXPLORER_MIGRATIONS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("migrations"))
}

/// Refresh planner statistics for the whole database. `pg_restore` does not carry
/// planner stats, so a freshly restored deploy plans the ingestion writes with
/// default estimates until autovacuum catches up — which makes the first
/// catch-up sync crawl. Running `ANALYZE` right after restore/migrate closes that
/// window.
pub async fn analyze_database(pool: &PgPool) -> Result<(), DbError> {
    sqlx::query("ANALYZE").execute(pool).await?;
    Ok(())
}

pub async fn resolve_chain_id(conn: &mut PgConnection, chain: &ChainName) -> Result<i32, DbError> {
    let rows = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM chains
        WHERE name = $1
        ORDER BY id
        LIMIT 2
        "#,
    )
    .bind(chain.as_str())
    .fetch_all(&mut *conn)
    .await?;

    match rows.as_slice() {
        [id] => Ok(*id),
        [] => Err(DbError::ChainMissing {
            chain: chain.to_string(),
        }),
        _ => Err(DbError::ChainAmbiguous {
            chain: chain.to_string(),
            matches: rows.len(),
        }),
    }
}

pub async fn get_cursor_height(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<BlockHeight>, DbError> {
    let height = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT current_height
        FROM chains
        WHERE id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(&mut *conn)
    .await?;

    height
        .map(|value| {
            u64::try_from(value)
                .map(BlockHeight::new)
                .map_err(|_| DbError::StoredBlockHeightOutOfRange { height: value })
        })
        .transpose()
}

/// Hash of the stored block at `height` on the chain, if present. Used by the
/// worker's startup guard to detect a wrong-network RPC (the node's block at our
/// cursor height must match the block we already stored there).
pub async fn block_hash_at_height(
    conn: &mut PgConnection,
    chain_id: i32,
    height: BlockHeight,
) -> Result<Option<String>, DbError> {
    let stored_height = i64::try_from(height.value())
        .map_err(|_| DbError::StoredBlockHeightOutOfRange { height: i64::MAX })?;
    let hash = sqlx::query_scalar::<_, String>(
        r#"
        SELECT hash
        FROM blocks
        WHERE chain_id = $1 AND height = $2
        "#,
    )
    .bind(chain_id)
    .bind(stored_height)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(hash)
}

pub async fn get_token_decimals(
    conn: &mut PgConnection,
    chain_id: i32,
    symbol: &str,
) -> Result<i32, DbError> {
    let rows = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT decimals
        FROM tokens
        WHERE chain_id = $1 AND symbol = $2
        ORDER BY id
        LIMIT 2
        "#,
    )
    .bind(chain_id)
    .bind(symbol)
    .fetch_all(&mut *conn)
    .await?;

    match rows.as_slice() {
        [decimals] => Ok(*decimals),
        [] => Err(DbError::TokenMissing {
            chain_id,
            symbol: symbol.to_owned(),
        }),
        _ => Err(DbError::TokenAmbiguous {
            chain_id,
            symbol: symbol.to_owned(),
            matches: rows.len(),
        }),
    }
}

pub async fn advance_cursor(
    conn: &mut PgConnection,
    chain_id: i32,
    height: BlockHeight,
) -> Result<BlockHeight, DbError> {
    let height = block_height_to_i64(height)?;
    let stored = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE chains
        SET current_height = greatest(current_height, $2)
        WHERE id = $1
        RETURNING current_height
        "#,
    )
    .bind(chain_id)
    .bind(height)
    .fetch_one(&mut *conn)
    .await?;

    u64::try_from(stored)
        .map(BlockHeight::new)
        .map_err(|_| DbError::StoredBlockHeightOutOfRange { height: stored })
}

/// Resolves an address to its id, creating the row on first sight.
///
/// SELECT-first, like `upsert_transaction_state_id`'s idiom: the overwhelmingly
/// common case is an address that already has a row, and the previous
/// `ON CONFLICT … DO UPDATE SET address = addresses.address` idiom turned every
/// such hit into a real no-op UPDATE — a dead tuple plus WAL per resolve
/// (measured 2026-08-05 on the local full resync: 17.4M updates against a 40k-row
/// table). The insert keeps `DO NOTHING` + re-select for the concurrent-insert
/// race (block projection vs the balance/metadata passes on hot addresses):
/// whichever writer loses the insert still finds the winner's row.
pub async fn upsert_address_id(
    conn: &mut PgConnection,
    chain_id: i32,
    address: &str,
) -> Result<i32, DbError> {
    let select = r#"
        SELECT id
        FROM addresses
        WHERE chain_id = $1 AND address = $2
        "#;
    if let Some(id) = sqlx::query_scalar::<_, i32>(select)
        .bind(chain_id)
        .bind(address)
        .fetch_optional(&mut *conn)
        .await?
    {
        return Ok(id);
    }

    if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO addresses (
            address,
            chain_id,
            name_last_updated_unix_seconds,
            stake_timestamp,
            total_soul_amount,
            balance_dirty_block
        )
        VALUES ($1, $2, 0, 0, 0, 0)
        ON CONFLICT (chain_id, address) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(address)
    .bind(chain_id)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(id);
    }

    // The insert conflicted: a concurrent writer created the row between the
    // select and the insert, so this select must find it.
    let id = sqlx::query_scalar::<_, i32>(select)
        .bind(chain_id)
        .bind(address)
        .fetch_one(&mut *conn)
        .await?;

    Ok(id)
}

pub async fn mark_block_addresses_dirty(
    conn: &mut PgConnection,
    block_id: i32,
    block_height: BlockHeight,
) -> Result<u64, DbError> {
    let block_height = block_height_to_i64(block_height)?;
    let result = sqlx::query(
        r#"
        WITH touched_addresses AS (
            SELECT chain_address_id AS address_id
            FROM blocks
            WHERE id = $1
            UNION
            SELECT validator_address_id AS address_id
            FROM blocks
            WHERE id = $1
            UNION
            -- Under gas model v2 the producer is credited a share of every gas
            -- bill with no event, so without this its balance never refreshes
            -- once the payout address stops coinciding with the validator.
            SELECT producer_address_id AS address_id
            FROM blocks
            WHERE id = $1
              AND producer_address_id IS NOT NULL
            UNION
            SELECT sender_id AS address_id
            FROM transactions
            WHERE block_id = $1
            UNION
            SELECT gas_payer_id AS address_id
            FROM transactions
            WHERE block_id = $1
            UNION
            SELECT gas_target_id AS address_id
            FROM transactions
            WHERE block_id = $1
            UNION
            SELECT event.address_id
            FROM events event
            JOIN transactions tx ON tx.id = event.transaction_id
            WHERE tx.block_id = $1
            UNION
            SELECT event.target_address_id AS address_id
            FROM events event
            JOIN transactions tx ON tx.id = event.transaction_id
            WHERE tx.block_id = $1
        )
        UPDATE addresses address
        SET balance_dirty_block = $2
        FROM touched_addresses touched
        WHERE address.id = touched.address_id
          AND address.address <> 'NULL'
          AND address.balance_dirty_block < $2
        "#,
    )
    .bind(block_id)
    .bind(block_height)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub async fn mark_all_chain_addresses_dirty(
    conn: &mut PgConnection,
    chain_id: i32,
    block_height: BlockHeight,
) -> Result<u64, DbError> {
    let block_height = block_height_to_i64(block_height)?;
    let result = sqlx::query(
        r#"
        UPDATE addresses
        SET balance_dirty_block = $2
        WHERE chain_id = $1
          AND address <> 'NULL'
          AND balance_dirty_block < $2
        "#,
    )
    .bind(chain_id)
    .bind(block_height)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub async fn count_dirty_addresses(conn: &mut PgConnection, chain_id: i32) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM addresses
        WHERE chain_id = $1
          AND balance_dirty_block > 0
          AND address <> 'NULL'
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(count)
}

pub async fn fetch_dirty_address_batch(
    conn: &mut PgConnection,
    chain_id: i32,
    batch_size: i64,
) -> Result<Vec<DirtyAddress>, DbError> {
    let rows = sqlx::query_as::<_, (i32, String, i64)>(
        r#"
        SELECT id, address, balance_dirty_block
        FROM addresses
        WHERE chain_id = $1
          AND balance_dirty_block > 0
          AND address <> 'NULL'
        ORDER BY balance_dirty_block ASC, id ASC
        LIMIT $2
        "#,
    )
    .bind(chain_id)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, address, balance_dirty_block)| DirtyAddress {
            id,
            address,
            balance_dirty_block,
        })
        .collect())
}

pub async fn reset_dirty_balance_flags(
    conn: &mut PgConnection,
    dirty_addresses: &[DirtyAddress],
) -> Result<u64, DbError> {
    if dirty_addresses.is_empty() {
        return Ok(0);
    }

    let address_ids = dirty_addresses
        .iter()
        .map(|address| address.id)
        .collect::<Vec<_>>();
    let dirty_blocks = dirty_addresses
        .iter()
        .map(|address| address.balance_dirty_block)
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE addresses address
        SET balance_dirty_block = 0
        FROM UNNEST($1::integer[], $2::bigint[]) AS dirty(address_id, dirty_block)
        WHERE address.id = dirty.address_id
          AND address.balance_dirty_block = dirty.dirty_block
        "#,
    )
    .bind(&address_ids)
    .bind(&dirty_blocks)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub async fn upsert_address_account(
    conn: &mut PgConnection,
    chain_id: i32,
    account: &AddressAccountUpsert,
) -> Result<AddressAccountUpsertResult, DbError> {
    sqlx::query(
        r#"
        UPDATE addresses
        SET
            address_name = $2,
            name_last_updated_unix_seconds = $3,
            stake_timestamp = $4,
            staked_amount_raw = NULLIF($5, '')::numeric,
            unclaimed_amount_raw = NULLIF($6, '')::numeric,
            total_soul_amount =
                COALESCE(NULLIF($7, '')::numeric, 0)
                + COALESCE(NULLIF($5, '')::numeric, 0)
        WHERE id = $1
          AND chain_id = $8
        "#,
    )
    .bind(account.address_id)
    .bind(&account.address_name)
    .bind(account.name_last_updated_unix_seconds)
    .bind(account.stake_timestamp)
    .bind(&account.staked_amount_raw)
    .bind(&account.unclaimed_amount_raw)
    .bind(&account.soul_balance_raw)
    .bind(chain_id)
    .execute(&mut *conn)
    .await?;

    let missing_balance_symbols =
        replace_address_balances(conn, chain_id, account.address_id, &account.balances).await?;
    Ok(AddressAccountUpsertResult {
        missing_balance_symbols,
    })
}

pub async fn reconcile_stake_memberships(
    conn: &mut PgConnection,
    address_ids: &[i32],
) -> Result<(), DbError> {
    if address_ids.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        WITH scoped AS (
            SELECT
                id AS address_id,
                COALESCE(staked_amount_raw, 0) AS staked_raw
            FROM addresses
            WHERE id = ANY($1)
        ),
        scoped_orgs AS (
            SELECT id, name
            FROM organizations
            WHERE name IN ('stakers', 'masters')
        )
        DELETE FROM organization_addresses membership
        USING scoped, scoped_orgs org
        WHERE membership.address_id = scoped.address_id
          AND membership.organization_id = org.id
          AND (
              (org.name = 'stakers' AND scoped.staked_raw <= 0)
              OR (org.name = 'masters' AND scoped.staked_raw < 5000000000000)
          )
        "#,
    )
    .bind(address_ids)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        WITH scoped AS (
            SELECT
                id AS address_id,
                COALESCE(staked_amount_raw, 0) AS staked_raw
            FROM addresses
            WHERE id = ANY($1)
        ),
        desired AS (
            SELECT org.id AS organization_id, scoped.address_id
            FROM scoped
            JOIN organizations org ON org.name = 'stakers'
            WHERE scoped.staked_raw > 0
            UNION ALL
            SELECT org.id AS organization_id, scoped.address_id
            FROM scoped
            JOIN organizations org ON org.name = 'masters'
            WHERE scoped.staked_raw >= 5000000000000
        )
        INSERT INTO organization_addresses (organization_id, address_id)
        SELECT organization_id, address_id
        FROM desired
        ON CONFLICT (organization_id, address_id) DO NOTHING
        "#,
    )
    .bind(address_ids)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Refreshes the supply columns of the tokens the node answered for.
///
/// Only rows whose values actually differ are written. Supplies barely move, so without
/// that guard the minute-cadence sync rewrote every token row on every pass — pure write
/// amplification, and it also made the caller's "log only when something changed" check
/// fire every single minute, because `rows_affected` counted rewrites, not changes.
pub async fn update_token_supplies(
    conn: &mut PgConnection,
    chain_id: i32,
    supplies: &[TokenSupplyUpsert],
) -> Result<u64, DbError> {
    if supplies.is_empty() {
        return Ok(0);
    }

    let symbols = supplies
        .iter()
        .map(|supply| supply.symbol.clone())
        .collect::<Vec<_>>();
    let carbon_ids = supplies
        .iter()
        .map(|supply| supply.carbon_id)
        .collect::<Vec<_>>();
    let current_supply_raws = supplies
        .iter()
        .map(|supply| supply.current_supply_raw.clone())
        .collect::<Vec<_>>();
    let max_supply_raws = supplies
        .iter()
        .map(|supply| supply.max_supply_raw.clone())
        .collect::<Vec<_>>();
    let burned_supply_raws = supplies
        .iter()
        .map(|supply| supply.burned_supply_raw.clone())
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE tokens token
        SET
            carbon_id = COALESCE(desired.carbon_id, token.carbon_id),
            current_supply_raw = NULLIF(desired.current_supply_raw, '')::numeric,
            max_supply_raw = NULLIF(desired.max_supply_raw, '')::numeric,
            burned_supply_raw = NULLIF(desired.burned_supply_raw, '')::numeric
        FROM UNNEST(
            $2::text[],
            $3::bigint[],
            $4::text[],
            $5::text[],
            $6::text[]
        ) AS desired(
            symbol,
            carbon_id,
            current_supply_raw,
            max_supply_raw,
            burned_supply_raw
        )
        WHERE token.chain_id = $1
          AND token.symbol = desired.symbol
          AND (
                token.carbon_id,
                token.current_supply_raw,
                token.max_supply_raw,
                token.burned_supply_raw
              ) IS DISTINCT FROM (
                COALESCE(desired.carbon_id, token.carbon_id),
                NULLIF(desired.current_supply_raw, '')::numeric,
                NULLIF(desired.max_supply_raw, '')::numeric,
                NULLIF(desired.burned_supply_raw, '')::numeric
              )
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&carbon_ids)
    .bind(&current_supply_raws)
    .bind(&max_supply_raws)
    .bind(&burned_supply_raws)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

/// Refreshes `tokens.metadata` from the node's current answer.
///
/// The node reports metadata as live state, not history: a key removed on chain is
/// simply absent from the next answer, so the column is replaced rather than merged —
/// merging would keep resurrecting keys the chain no longer has. Tokens the node does
/// not answer for are left untouched, so a token that only exists in the historical
/// zero state keeps its NULL instead of being blanked.
pub async fn update_token_metadata(
    conn: &mut PgConnection,
    chain_id: i32,
    metadata: &[TokenMetadataUpsert],
) -> Result<u64, DbError> {
    if metadata.is_empty() {
        return Ok(0);
    }

    let symbols = metadata
        .iter()
        .map(|token| token.symbol.clone())
        .collect::<Vec<_>>();
    let values = metadata
        .iter()
        .map(|token| token.metadata.to_string())
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE tokens token
        SET metadata = desired.metadata
        FROM UNNEST($2::text[], $3::jsonb[]) AS desired(symbol, metadata)
        WHERE token.chain_id = $1
          AND token.symbol = desired.symbol
          AND token.metadata IS DISTINCT FROM desired.metadata
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&values)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

/// Refreshes the live `tokens.price_usd` column from an external price feed,
/// `COALESCE`-guarded so a missing pairing never clobbers a value that is already
/// there. Returns the number of token rows touched.
pub async fn update_token_prices(
    conn: &mut PgConnection,
    chain_id: i32,
    prices: &[TokenPriceUpsert],
) -> Result<u64, DbError> {
    if prices.is_empty() {
        return Ok(0);
    }

    let symbols = prices.iter().map(|p| p.symbol.clone()).collect::<Vec<_>>();
    let usd = prices.iter().map(|p| p.price_usd).collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE tokens token
        SET price_usd = COALESCE(desired.price_usd, token.price_usd)
        FROM UNNEST($2::text[], $3::double precision[]) AS desired(symbol, price_usd)
        WHERE token.chain_id = $1
          AND token.symbol = desired.symbol
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&usd)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

/// Appends/refreshes daily USD closes in `token_daily_prices`. That table has no
/// unique constraint on (token_id, date_unix_seconds), so instead of `ON CONFLICT`
/// this does an explicit UPDATE-existing-then-INSERT-missing in one statement — no
/// schema change is required. Returns rows inserted.
pub async fn upsert_token_daily_prices(
    conn: &mut PgConnection,
    chain_id: i32,
    prices: &[TokenDailyPriceUpsert],
) -> Result<u64, DbError> {
    if prices.is_empty() {
        return Ok(0);
    }

    let symbols = prices.iter().map(|p| p.symbol.clone()).collect::<Vec<_>>();
    let dates = prices
        .iter()
        .map(|p| p.date_unix_seconds)
        .collect::<Vec<_>>();
    let usd = prices.iter().map(|p| p.price_usd).collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        WITH input AS (
            SELECT desired.symbol, desired.date_unix_seconds, desired.price_usd
            FROM UNNEST($2::text[], $3::bigint[], $4::double precision[])
                AS desired(symbol, date_unix_seconds, price_usd)
        ),
        resolved AS (
            SELECT token.id AS token_id,
                   input.date_unix_seconds,
                   input.price_usd::numeric AS price_usd
            FROM input
            JOIN tokens token
              ON token.chain_id = $1
             AND token.symbol = input.symbol
        ),
        updated AS (
            UPDATE token_daily_prices price
            SET price_usd = resolved.price_usd
            FROM resolved
            WHERE price.token_id = resolved.token_id
              AND price.date_unix_seconds = resolved.date_unix_seconds
            RETURNING price.token_id, price.date_unix_seconds
        )
        INSERT INTO token_daily_prices (token_id, date_unix_seconds, price_usd)
        SELECT resolved.token_id, resolved.date_unix_seconds, resolved.price_usd
        FROM resolved
        WHERE NOT EXISTS (
            SELECT 1 FROM updated
            WHERE updated.token_id = resolved.token_id
              AND updated.date_unix_seconds = resolved.date_unix_seconds
        )
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&dates)
    .bind(&usd)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

/// Most recent day already stored in `token_daily_prices` for the chain, so the
/// price job knows where to resume the daily-history backfill (mirrors the C#
/// plugin reading the latest `DATE_UNIX_SECONDS`). `None` when the table is empty.
pub async fn latest_token_daily_price_date(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<i64>, DbError> {
    let latest: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT MAX(price.date_unix_seconds)
        FROM token_daily_prices price
        JOIN tokens token ON token.id = price.token_id
        WHERE token.chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(latest)
}

/// Token ids of NFTs under the named contract that still lack off-chain metadata
/// (not burned, never fetched). Mirrors the C# `Nft.TTRS` selection. Bounded by
/// `limit` so the worker drains the backlog in batches.
pub async fn list_contract_nfts_missing_offchain(
    conn: &mut PgConnection,
    chain_id: i32,
    contract_name: &str,
    limit: i64,
) -> Result<Vec<String>, DbError> {
    let token_ids: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT nft.token_id
        FROM nfts nft
        JOIN contracts contract ON contract.id = nft.contract_id
        WHERE nft.chain_id = $1
          AND contract.name = $2
          AND nft.offchain_api_response IS NULL
          AND COALESCE(nft.burned, false) = false
          AND nft.token_id IS NOT NULL
        ORDER BY nft.id
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(contract_name)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;

    Ok(token_ids)
}

/// Writes off-chain NFT metadata for the named contract, matched by `token_id`. The
/// raw JSON goes to `offchain_api_response`; display fields are `COALESCE`-patched so
/// a missing field never clobbers an existing value. Returns rows updated.
pub async fn update_nft_offchain_metadata(
    conn: &mut PgConnection,
    chain_id: i32,
    contract_name: &str,
    records: &[NftOffchainUpsert],
) -> Result<u64, DbError> {
    if records.is_empty() {
        return Ok(0);
    }

    let token_ids = records
        .iter()
        .map(|record| record.token_id.clone())
        .collect::<Vec<_>>();
    let offchain = records
        .iter()
        .map(|record| record.offchain_api_response.clone())
        .collect::<Vec<_>>();
    let names = records
        .iter()
        .map(|record| record.name.clone())
        .collect::<Vec<_>>();
    let descriptions = records
        .iter()
        .map(|record| record.description.clone())
        .collect::<Vec<_>>();
    let images = records
        .iter()
        .map(|record| record.image.clone())
        .collect::<Vec<_>>();
    let mint_numbers = records
        .iter()
        .map(|record| record.mint_number)
        .collect::<Vec<_>>();
    let mint_dates = records
        .iter()
        .map(|record| record.mint_date_unix_seconds)
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE nfts nft
        SET
            offchain_api_response = desired.offchain::jsonb,
            name = COALESCE(desired.name, nft.name),
            description = COALESCE(desired.description, nft.description),
            image = COALESCE(desired.image, nft.image),
            mint_number = COALESCE(desired.mint_number, nft.mint_number),
            mint_date_unix_seconds = COALESCE(desired.mint_date, nft.mint_date_unix_seconds)
        FROM UNNEST(
            $3::text[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::int[],
            $9::bigint[]
        ) AS desired(token_id, offchain, name, description, image, mint_number, mint_date)
        WHERE nft.chain_id = $1
          AND nft.contract_id = (SELECT id FROM contracts WHERE name = $2 LIMIT 1)
          AND nft.token_id = desired.token_id
        "#,
    )
    .bind(chain_id)
    .bind(contract_name)
    .bind(&token_ids)
    .bind(&offchain)
    .bind(&names)
    .bind(&descriptions)
    .bind(&images)
    .bind(&mint_numbers)
    .bind(&mint_dates)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub async fn fetch_failed_transactions_missing_debug_comment(
    conn: &mut PgConnection,
    chain_id: i32,
    cutoff_unix_seconds: i64,
    limit: i64,
) -> Result<Vec<String>, DbError> {
    let hashes = sqlx::query_scalar::<_, String>(
        r#"
        SELECT tx.hash
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN transaction_states state ON state.id = tx.state_id
        WHERE block.chain_id = $1
          AND state.name IN ('Break', 'Fault')
          AND tx.timestamp_unix_seconds >= $2
          AND NULLIF(BTRIM(COALESCE(tx.debug_comment, '')), '') IS NULL
          AND NULLIF(BTRIM(COALESCE(tx.hash, '')), '') IS NOT NULL
        ORDER BY tx.timestamp_unix_seconds DESC, tx.id DESC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(cutoff_unix_seconds)
    .bind(limit)
    .fetch_all(conn)
    .await?;

    Ok(hashes)
}

pub async fn update_failed_transaction_debug_comment(
    conn: &mut PgConnection,
    hash: &str,
    result: Option<&str>,
    debug_comment: &str,
) -> Result<bool, DbError> {
    let rows_affected = sqlx::query(
        r#"
        UPDATE transactions tx
        SET debug_comment = CASE
                WHEN NULLIF(BTRIM($2), '') IS NOT NULL
                     AND NULLIF(BTRIM(COALESCE(tx.debug_comment, '')), '') IS NULL
                    THEN $2
                ELSE tx.debug_comment
            END,
            result = CASE
                WHEN NULLIF(BTRIM(COALESCE($3, '')), '') IS NOT NULL
                     AND NULLIF(BTRIM(COALESCE(tx.result, '')), '') IS NULL
                    THEN $3
                ELSE tx.result
            END
        FROM transaction_states state
        WHERE tx.hash = $1
          AND tx.state_id = state.id
          AND state.name IN ('Break', 'Fault')
          AND (
              (
                  NULLIF(BTRIM($2), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(tx.debug_comment, '')), '') IS NULL
              )
              OR (
                  NULLIF(BTRIM(COALESCE($3, '')), '') IS NOT NULL
                  AND NULLIF(BTRIM(COALESCE(tx.result, '')), '') IS NULL
              )
          )
        "#,
    )
    .bind(hash)
    .bind(debug_comment)
    .bind(result)
    .execute(conn)
    .await?
    .rows_affected();

    Ok(rows_affected > 0)
}

async fn replace_address_balances(
    conn: &mut PgConnection,
    chain_id: i32,
    address_id: i32,
    balances: &[AddressBalanceUpsert],
) -> Result<Vec<String>, DbError> {
    let symbols = balances
        .iter()
        .map(|balance| balance.symbol.clone())
        .collect::<Vec<_>>();
    let amount_raws = balances
        .iter()
        .map(|balance| balance.amount_raw.clone())
        .collect::<Vec<_>>();

    let missing_symbols = sqlx::query_scalar::<_, String>(
        r#"
        SELECT desired.symbol
        FROM (
            SELECT DISTINCT symbol
            FROM UNNEST($1::text[]) AS desired(symbol)
        ) desired
        LEFT JOIN tokens token
          ON token.chain_id = $2
         AND token.symbol = desired.symbol
        WHERE token.id IS NULL
        ORDER BY desired.symbol
        "#,
    )
    .bind(&symbols)
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        INSERT INTO address_balances (address_id, token_id, amount_raw)
        SELECT
            $1,
            token.id,
            COALESCE(NULLIF(desired.amount_raw, '')::numeric, 0)
        FROM UNNEST($2::text[], $3::text[]) AS desired(symbol, amount_raw)
        JOIN tokens token
          ON token.chain_id = $4
         AND token.symbol = desired.symbol
        ON CONFLICT (address_id, token_id) DO UPDATE SET
            amount_raw = EXCLUDED.amount_raw
        "#,
    )
    .bind(address_id)
    .bind(&symbols)
    .bind(&amount_raws)
    .bind(chain_id)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        DELETE FROM address_balances balance
        WHERE balance.address_id = $1
          AND NOT EXISTS (
              SELECT 1
              FROM UNNEST($2::text[]) AS desired(symbol)
              JOIN tokens token
                ON token.chain_id = $3
               AND token.symbol = desired.symbol
              WHERE token.id = balance.token_id
          )
        "#,
    )
    .bind(address_id)
    .bind(&symbols)
    .bind(chain_id)
    .execute(&mut *conn)
    .await?;

    Ok(missing_symbols)
}

/// Resolves a contract (by its event-path name-as-hash) to its id, creating the
/// row on first sight.
///
/// Unlike the address/event-kind upserts this one is not a pure self-assign: on
/// conflict it also backfills an empty `name`/`symbol` from the resolved value.
/// The SELECT therefore checks whether the stored row still needs that backfill
/// and only falls through to the write when it does (or when the row is
/// missing). The write statement is the previous upsert unchanged, so the
/// backfill-and-race semantics stay exactly as before; the common
/// already-filled case stops producing a no-op UPDATE per resolve (4.8M updates
/// against this 179-row table over one full resync).
pub async fn upsert_contract_id(
    conn: &mut PgConnection,
    chain_id: i32,
    contract: &str,
) -> Result<i32, DbError> {
    if let Some((id, name, symbol)) = sqlx::query_as::<_, (i32, Option<String>, Option<String>)>(
        r#"
        SELECT id, name, symbol
        FROM contracts
        WHERE chain_id = $1 AND hash = $2
        "#,
    )
    .bind(chain_id)
    .bind(contract)
    .fetch_optional(&mut *conn)
    .await?
    {
        let filled = |value: Option<String>| value.is_some_and(|value| !value.is_empty());
        if filled(name) && filled(symbol) {
            return Ok(id);
        }
    }

    let id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO contracts (name, hash, symbol, chain_id, last_updated_unix_seconds)
        VALUES ($1, $1, $1, $2, 0)
        ON CONFLICT (chain_id, hash) DO UPDATE SET
            name = CASE
                WHEN contracts.name IS NULL OR contracts.name = '' THEN EXCLUDED.name
                ELSE contracts.name
            END,
            symbol = CASE
                WHEN contracts.symbol IS NULL OR contracts.symbol = '' THEN EXCLUDED.symbol
                ELSE contracts.symbol
            END
        RETURNING id
        "#,
    )
    .bind(contract)
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(id)
}

/// Resolves an event-kind name to its id, creating the row on first sight.
///
/// The dimension is global: an event kind is a protocol concept, so the same name is the
/// same row on every chain.
///
/// SELECT-first for the same reason as `upsert_address_id`: the old self-assign
/// `DO UPDATE` produced a real no-op UPDATE per resolve — 11.9M updates against
/// this 70-row table over one full resync.
pub async fn upsert_event_kind_id(conn: &mut PgConnection, name: &str) -> Result<i32, DbError> {
    let select = r#"
        SELECT id
        FROM event_kinds
        WHERE name = $1
        "#;
    if let Some(id) = sqlx::query_scalar::<_, i32>(select)
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?
    {
        return Ok(id);
    }

    if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO event_kinds (name)
        VALUES ($1)
        ON CONFLICT (name) DO NOTHING
        RETURNING id
        "#,
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(id);
    }

    // Lost a concurrent-insert race; the winner's committed row must be there.
    let id = sqlx::query_scalar::<_, i32>(select)
        .bind(name)
        .fetch_one(&mut *conn)
        .await?;

    Ok(id)
}

pub async fn upsert_transaction_state_id(
    conn: &mut PgConnection,
    name: &str,
) -> Result<i32, DbError> {
    if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM transaction_states
        WHERE name = $1
        "#,
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(id);
    }

    let id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO transaction_states (name)
        VALUES ($1)
        RETURNING id
        "#,
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await?;

    Ok(id)
}

pub async fn upsert_block(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    block: BlockUpsert,
) -> Result<BlockRecord, DbError> {
    let chain_id = resolve_chain_id(conn, &block.chain).await?;
    let chain_address = block.chain_address.as_deref().unwrap_or("NULL");
    let validator_address = block.validator_address.as_deref().unwrap_or("NULL");
    let chain_address_id = cache
        .header_address_id(conn, chain_id, chain_address)
        .await?;
    let validator_address_id = cache
        .header_address_id(conn, chain_id, validator_address)
        .await?;
    // Resolved only when present — never through the "NULL" sentinel used for
    // chain/validator, or every pre-v2 block would gain a bogus address link.
    let producer_address_id = match block.producer_address.as_deref() {
        Some(producer_address) => Some(
            cache
                .header_address_id(conn, chain_id, producer_address)
                .await?,
        ),
        None => None,
    };
    let height = block_height_to_i64(block.height)?;

    let id = if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM blocks
        WHERE chain_id = $1 AND height = $2
        "#,
    )
    .bind(chain_id)
    .bind(height)
    .fetch_optional(&mut *conn)
    .await?
    {
        sqlx::query(
            r#"
            UPDATE blocks
            SET hash = $2,
                protocol = $3,
                chain_address_id = $4,
                validator_address_id = $5,
                producer_address_id = $6,
                timestamp_unix_seconds = $7,
                reward = NULLIF($8, '')::numeric
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(&block.hash)
        .bind(block.protocol.unwrap_or_default())
        .bind(chain_address_id)
        .bind(validator_address_id)
        .bind(producer_address_id)
        .bind(block.timestamp_unix_seconds)
        .bind(&block.reward)
        .execute(&mut *conn)
        .await?;
        id
    } else {
        sqlx::query_scalar::<_, i32>(
            r#"
            INSERT INTO blocks (
                height,
                timestamp_unix_seconds,
                chain_id,
                hash,
                protocol,
                chain_address_id,
                validator_address_id,
                producer_address_id,
                reward
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, '')::numeric)
            RETURNING id
            "#,
        )
        .bind(height)
        .bind(block.timestamp_unix_seconds)
        .bind(chain_id)
        .bind(&block.hash)
        .bind(block.protocol.unwrap_or_default())
        .bind(chain_address_id)
        .bind(validator_address_id)
        .bind(producer_address_id)
        .bind(&block.reward)
        .fetch_one(&mut *conn)
        .await?
    };

    Ok(BlockRecord {
        id,
        chain_id,
        chain: block.chain.to_string(),
        height,
        hash: block.hash,
        protocol: block.protocol,
        chain_address_id,
        chain_address: Some(chain_address.to_owned()),
        validator_address_id,
        validator_address: Some(validator_address.to_owned()),
        producer_address_id,
        producer_address: block.producer_address,
        timestamp_unix_seconds: block.timestamp_unix_seconds,
        reward: block.reward,
    })
}

/// Memoization of dimension lookups (addresses, transaction states, event
/// kinds, contracts) for the ingestion projection. Each distinct value is
/// resolved once via the underlying upsert and then reused, removing the
/// redundant per-transaction/per-event resolve round-trips (and their no-op WAL
/// writes). Resolution order is unchanged — values are still resolved on first
/// encounter in transaction/event order — so any newly inserted dimension rows
/// receive identical surrogate ids.
///
/// Lifetimes differ per family. `addresses` and `transaction_states` are
/// per-block scopes. `event_kinds`, `contracts` and `header_addresses` may
/// survive across blocks via [`Self::take_for_block`] /
/// [`Self::restore_after_commit`]: those dimensions are insert-only (rows are
/// never deleted and ids never change — dimension deletes are FK-forbidden), so
/// an entry proven committed can never go stale, and the three block-header
/// addresses repeat block after block — re-resolving them per block was one
/// no-op round-trip per address per block.
///
/// The take/restore pair exists because a cross-block cache must not outlive a
/// rollback: an id minted inside a block transaction that later fails would
/// poison every later block (FK breaks, or worse a re-minted id pointing at a
/// different row). The holder therefore hands the long-lived maps TO the block
/// write and takes them back only after its COMMIT returns; on any error or
/// cancellation the maps die with the block cache and the next block re-reads
/// ids from the database, which only ever costs a handful of SELECTs.
#[derive(Default)]
pub struct ProjectionDimensionCache {
    addresses: HashMap<(i32, String), i32>,
    header_addresses: HashMap<(i32, String), i32>,
    transaction_states: HashMap<String, i32>,
    event_kinds: HashMap<String, i32>,
    contracts: HashMap<(i32, String), i32>,
}

impl ProjectionDimensionCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves the cross-block families into a fresh per-block cache (leaving
    /// `self` — the process-lifetime holder — empty of them until
    /// [`Self::restore_after_commit`] brings them back enriched).
    pub fn take_for_block(&mut self) -> ProjectionDimensionCache {
        ProjectionDimensionCache {
            addresses: HashMap::new(),
            transaction_states: HashMap::new(),
            header_addresses: std::mem::take(&mut self.header_addresses),
            event_kinds: std::mem::take(&mut self.event_kinds),
            contracts: std::mem::take(&mut self.contracts),
        }
    }

    /// Returns the cross-block families after the block's transaction has
    /// COMMITTED — the only point where their new entries are proven durable.
    /// Never call this on a failed block: dropping the block cache instead is
    /// what keeps rolled-back ids out of the process-lifetime maps.
    pub fn restore_after_commit(&mut self, block_cache: ProjectionDimensionCache) {
        self.header_addresses = block_cache.header_addresses;
        self.event_kinds = block_cache.event_kinds;
        self.contracts = block_cache.contracts;
    }

    /// Pre-resolve a batch of (distinct) addresses in one round-trip instead of a
    /// serial `INSERT … ON CONFLICT … RETURNING` per first encounter: new addresses
    /// are inserted set-based, then every id is loaded into the cache. Surrogate
    /// address ids are not API-observable (everything keys by `address_id`; the API
    /// renders the address string), so the batch insert order does not change any
    /// observable data — it only removes per-address round-trips from block writes.
    pub async fn prefetch_addresses(
        &mut self,
        conn: &mut PgConnection,
        chain_id: i32,
        addresses: &[String],
    ) -> Result<(), DbError> {
        let missing: Vec<String> = addresses
            .iter()
            .filter(|address| !self.addresses.contains_key(&(chain_id, (*address).clone())))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        sqlx::query(
            r#"
            INSERT INTO addresses (
                address, chain_id, name_last_updated_unix_seconds, stake_timestamp,
                total_soul_amount, balance_dirty_block
            )
            SELECT address, $2, 0, 0, 0, 0
            FROM unnest($1::text[]) AS address
            ON CONFLICT (chain_id, address) DO NOTHING
            "#,
        )
        .bind(&missing)
        .bind(chain_id)
        .execute(&mut *conn)
        .await?;
        let rows = sqlx::query_as::<_, (String, i32)>(
            "SELECT address, id FROM addresses WHERE chain_id = $1 AND address = ANY($2)",
        )
        .bind(chain_id)
        .bind(&missing)
        .fetch_all(&mut *conn)
        .await?;
        for (address, id) in rows {
            self.addresses.insert((chain_id, address), id);
        }
        Ok(())
    }

    /// Resolves a block-header address (chain/validator/producer) through the
    /// process-lifetime header map. Kept separate from the per-block `addresses`
    /// map so `begin_block` cannot evict it: the same handful of header
    /// addresses recurs on every block.
    async fn header_address_id(
        &mut self,
        conn: &mut PgConnection,
        chain_id: i32,
        address: &str,
    ) -> Result<i32, DbError> {
        if let Some(&id) = self.header_addresses.get(&(chain_id, address.to_owned())) {
            return Ok(id);
        }
        let id = upsert_address_id(conn, chain_id, address).await?;
        self.header_addresses
            .insert((chain_id, address.to_owned()), id);
        Ok(id)
    }

    // Public: the legacy-raw decode tool resolves its contract/target-address
    // enrichments through the same cached upserts the block projection uses,
    // so both paths mint identical dimension ids.
    pub async fn address_id(
        &mut self,
        conn: &mut PgConnection,
        chain_id: i32,
        address: &str,
    ) -> Result<i32, DbError> {
        if let Some(&id) = self.addresses.get(&(chain_id, address.to_owned())) {
            return Ok(id);
        }
        let id = upsert_address_id(conn, chain_id, address).await?;
        self.addresses.insert((chain_id, address.to_owned()), id);
        Ok(id)
    }

    async fn transaction_state_id(
        &mut self,
        conn: &mut PgConnection,
        name: &str,
    ) -> Result<i32, DbError> {
        if let Some(&id) = self.transaction_states.get(name) {
            return Ok(id);
        }
        let id = upsert_transaction_state_id(conn, name).await?;
        self.transaction_states.insert(name.to_owned(), id);
        Ok(id)
    }

    async fn event_kind_id(&mut self, conn: &mut PgConnection, name: &str) -> Result<i32, DbError> {
        if let Some(&id) = self.event_kinds.get(name) {
            return Ok(id);
        }
        let id = upsert_event_kind_id(conn, name).await?;
        self.event_kinds.insert(name.to_owned(), id);
        Ok(id)
    }

    pub async fn contract_id(
        &mut self,
        conn: &mut PgConnection,
        chain_id: i32,
        contract: &str,
    ) -> Result<i32, DbError> {
        if let Some(&id) = self.contracts.get(&(chain_id, contract.to_owned())) {
            return Ok(id);
        }
        let id = upsert_contract_id(conn, chain_id, contract).await?;
        self.contracts.insert((chain_id, contract.to_owned()), id);
        Ok(id)
    }
}

/// Upsert a transaction, resolving its addresses/state through a fresh dimension
/// cache. Inside a block projection prefer [`upsert_transaction_cached`] so the
/// cache is shared across the block's transactions and events.
pub async fn upsert_transaction(
    conn: &mut PgConnection,
    transaction: TransactionUpsert,
) -> Result<TransactionRecord, DbError> {
    let mut cache = ProjectionDimensionCache::new();
    upsert_transaction_cached(conn, &mut cache, transaction).await
}

/// Upsert a transaction, resolving its addresses/state through the supplied
/// per-block dimension cache.
pub async fn upsert_transaction_cached(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    transaction: TransactionUpsert,
) -> Result<TransactionRecord, DbError> {
    let state_id = cache.transaction_state_id(conn, &transaction.state).await?;
    let sender = transaction.sender.as_deref().unwrap_or("NULL");
    let gas_payer = transaction.gas_payer.as_deref().unwrap_or("NULL");
    let gas_target = transaction.gas_target.as_deref().unwrap_or("NULL");
    let sender_id = cache.address_id(conn, transaction.chain_id, sender).await?;
    let gas_payer_id = cache
        .address_id(conn, transaction.chain_id, gas_payer)
        .await?;
    let gas_target_id = cache
        .address_id(conn, transaction.chain_id, gas_target)
        .await?;

    let id = if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM transactions
        WHERE block_id = $1 AND tx_index = $2
        "#,
    )
    .bind(transaction.block_id)
    .bind(transaction.tx_index)
    .fetch_optional(&mut *conn)
    .await?
    {
        sqlx::query(
            r#"
            UPDATE transactions
            SET hash = $2,
                timestamp_unix_seconds = $3,
                payload = $4,
                script_raw = decode($5, 'hex'),
                result = $6,
                expiration = $7,
                state_id = $8,
                sender_id = $9,
                gas_payer_id = $10,
                gas_target_id = $11,
                fee_raw = NULLIF($12, '')::numeric,
                gas_limit_raw = NULLIF($13, '')::numeric,
                gas_price_raw = NULLIF($14, '')::numeric,
                carbon_tx_data = decode($15, 'hex'),
                carbon_tx_type = $16,
                debug_comment = $17
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(&transaction.hash)
        .bind(transaction.timestamp_unix_seconds)
        .bind(&transaction.payload)
        .bind(&transaction.script_raw)
        .bind(&transaction.result)
        .bind(transaction.expiration_unix_seconds)
        .bind(state_id)
        .bind(sender_id)
        .bind(gas_payer_id)
        .bind(gas_target_id)
        .bind(&transaction.fee_raw)
        .bind(&transaction.gas_limit_raw)
        .bind(&transaction.gas_price_raw)
        .bind(&transaction.carbon_tx_data)
        .bind(
            transaction
                .carbon_tx_type
                .and_then(|value| i16::try_from(value).ok()),
        )
        .bind(&transaction.debug_comment)
        .execute(&mut *conn)
        .await?;
        id
    } else {
        sqlx::query_scalar::<_, i32>(
            r#"
            INSERT INTO transactions (
                hash,
                tx_index,
                block_id,
                timestamp_unix_seconds,
                payload,
                script_raw,
                result,
                expiration,
                state_id,
                sender_id,
                gas_payer_id,
                gas_target_id,
                fee_raw,
                gas_limit_raw,
                gas_price_raw,
                carbon_tx_data,
                carbon_tx_type,
                debug_comment
            )
            VALUES (
                $1, $2, $3, $4, $5, decode($6, 'hex'), $7, $8, $9, $10,
                $11, $12, NULLIF($13, '')::numeric, NULLIF($14, '')::numeric,
                NULLIF($15, '')::numeric, decode($16, 'hex'), $17, $18
            )
            RETURNING id
            "#,
        )
        .bind(&transaction.hash)
        .bind(transaction.tx_index)
        .bind(transaction.block_id)
        .bind(transaction.timestamp_unix_seconds)
        .bind(&transaction.payload)
        .bind(&transaction.script_raw)
        .bind(&transaction.result)
        .bind(transaction.expiration_unix_seconds)
        .bind(state_id)
        .bind(sender_id)
        .bind(gas_payer_id)
        .bind(gas_target_id)
        .bind(&transaction.fee_raw)
        .bind(&transaction.gas_limit_raw)
        .bind(&transaction.gas_price_raw)
        .bind(&transaction.carbon_tx_data)
        .bind(
            transaction
                .carbon_tx_type
                .and_then(|value| i16::try_from(value).ok()),
        )
        .bind(&transaction.debug_comment)
        .fetch_one(&mut *conn)
        .await?
    };

    replace_transaction_signatures(conn, id, &transaction.signatures).await?;

    Ok(transaction_record_from_upsert(
        transaction,
        id,
        sender_id,
        gas_payer_id,
        gas_target_id,
    ))
}

fn transaction_record_from_upsert(
    transaction: TransactionUpsert,
    id: i32,
    sender_id: i32,
    gas_payer_id: i32,
    gas_target_id: i32,
) -> TransactionRecord {
    TransactionRecord {
        id,
        block_id: transaction.block_id,
        chain_id: transaction.chain_id,
        tx_index: transaction.tx_index,
        hash: transaction.hash,
        timestamp_unix_seconds: transaction.timestamp_unix_seconds,
        state: transaction.state,
        result: transaction.result,
        debug_comment: transaction.debug_comment,
        payload: transaction.payload,
        script_raw: transaction.script_raw,
        fee_raw: transaction.fee_raw,
        gas_price_raw: transaction.gas_price_raw,
        gas_limit_raw: transaction.gas_limit_raw,
        sender_id,
        gas_payer_id,
        gas_target_id,
        carbon_tx_type: transaction.carbon_tx_type,
        carbon_tx_data: transaction.carbon_tx_data,
        expiration_unix_seconds: transaction.expiration_unix_seconds,
    }
}

/// Upsert all of a block's transactions, returning their records in input order.
/// On a fresh projection (no rows yet for this block) the rows are inserted in
/// one set-based `unnest` pass ordered by `tx_index` (so the serial ids match the
/// per-transaction order, mirroring the C# reserve-ids + batch insert), then each
/// transaction's signatures are written. A re-projection (some rows already
/// exist) falls back to the row-by-row upsert.
pub async fn batch_upsert_transactions(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    transactions: Vec<TransactionUpsert>,
) -> Result<Vec<TransactionRecord>, DbError> {
    if transactions.is_empty() {
        return Ok(Vec::new());
    }
    let block_id = transactions[0].block_id;

    // Resolve dimensions for every transaction once, in order.
    let mut resolved = Vec::with_capacity(transactions.len());
    for transaction in &transactions {
        let state_id = cache.transaction_state_id(conn, &transaction.state).await?;
        let sender_id = cache
            .address_id(
                conn,
                transaction.chain_id,
                transaction.sender.as_deref().unwrap_or("NULL"),
            )
            .await?;
        let gas_payer_id = cache
            .address_id(
                conn,
                transaction.chain_id,
                transaction.gas_payer.as_deref().unwrap_or("NULL"),
            )
            .await?;
        let gas_target_id = cache
            .address_id(
                conn,
                transaction.chain_id,
                transaction.gas_target.as_deref().unwrap_or("NULL"),
            )
            .await?;
        resolved.push((state_id, sender_id, gas_payer_id, gas_target_id));
    }

    let existing = sqlx::query_as::<_, (i32, i32)>(
        "SELECT tx_index, id FROM transactions WHERE block_id = $1",
    )
    .bind(block_id)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect::<HashMap<i32, i32>>();

    if !existing.is_empty() {
        // Re-projection: row-by-row upsert (handles insert-or-update + signatures).
        let mut records = Vec::with_capacity(transactions.len());
        for transaction in transactions {
            records.push(upsert_transaction_cached(conn, cache, transaction).await?);
        }
        return Ok(records);
    }

    // Fresh projection: one set-based insert preserving tx_index order.
    let mut hash = Vec::with_capacity(transactions.len());
    let mut tx_index = Vec::with_capacity(transactions.len());
    let mut block = Vec::with_capacity(transactions.len());
    let mut timestamp = Vec::with_capacity(transactions.len());
    let mut payload = Vec::with_capacity(transactions.len());
    let mut script_raw = Vec::with_capacity(transactions.len());
    let mut result = Vec::with_capacity(transactions.len());
    let mut expiration = Vec::with_capacity(transactions.len());
    let mut state_id = Vec::with_capacity(transactions.len());
    let mut sender_id = Vec::with_capacity(transactions.len());
    let mut gas_payer_id = Vec::with_capacity(transactions.len());
    let mut gas_target_id = Vec::with_capacity(transactions.len());
    let mut fee_raw = Vec::with_capacity(transactions.len());
    let mut gas_limit_raw = Vec::with_capacity(transactions.len());
    let mut gas_price_raw = Vec::with_capacity(transactions.len());
    let mut carbon_tx_data = Vec::with_capacity(transactions.len());
    let mut carbon_tx_type = Vec::with_capacity(transactions.len());
    let mut debug_comment = Vec::with_capacity(transactions.len());
    for (transaction, (resolved_state, resolved_sender, resolved_gas_payer, resolved_gas_target)) in
        transactions.iter().zip(resolved.iter())
    {
        hash.push(transaction.hash.clone());
        tx_index.push(transaction.tx_index);
        block.push(transaction.block_id);
        timestamp.push(transaction.timestamp_unix_seconds);
        payload.push(transaction.payload.clone());
        script_raw.push(transaction.script_raw.clone());
        result.push(transaction.result.clone());
        expiration.push(transaction.expiration_unix_seconds);
        state_id.push(*resolved_state);
        sender_id.push(*resolved_sender);
        gas_payer_id.push(*resolved_gas_payer);
        gas_target_id.push(*resolved_gas_target);
        fee_raw.push(transaction.fee_raw.clone());
        gas_limit_raw.push(transaction.gas_limit_raw.clone());
        gas_price_raw.push(transaction.gas_price_raw.clone());
        carbon_tx_data.push(transaction.carbon_tx_data.clone());
        carbon_tx_type.push(
            transaction
                .carbon_tx_type
                .and_then(|value| i16::try_from(value).ok()),
        );
        debug_comment.push(transaction.debug_comment.clone());
    }

    let inserted = sqlx::query_as::<_, (i32, i32)>(
        r#"
        INSERT INTO transactions (
            hash, tx_index, block_id, timestamp_unix_seconds, payload, script_raw, result,
            expiration, state_id, sender_id, gas_payer_id, gas_target_id,
            fee_raw, gas_limit_raw, gas_price_raw, carbon_tx_data, carbon_tx_type, debug_comment
        )
        SELECT
            t.hash, t.tx_index, t.block_id, t.timestamp, t.payload, decode(t.script_raw, 'hex'),
            t.result, t.expiration, t.state_id, t.sender_id, t.gas_payer_id, t.gas_target_id,
            NULLIF(t.fee_raw, '')::numeric, NULLIF(t.gas_limit_raw, '')::numeric,
            NULLIF(t.gas_price_raw, '')::numeric, decode(t.carbon_tx_data, 'hex'),
            t.carbon_tx_type, t.debug_comment
        FROM unnest(
            $1::text[], $2::int[], $3::int[], $4::bigint[], $5::text[], $6::text[], $7::text[],
            $8::bigint[], $9::int[], $10::int[], $11::int[],
            $12::int[], $13::text[], $14::text[], $15::text[], $16::text[], $17::smallint[],
            $18::text[]
        ) AS t(
            hash, tx_index, block_id, timestamp, payload, script_raw, result, expiration,
            state_id, sender_id, gas_payer_id, gas_target_id, fee_raw,
            gas_limit_raw, gas_price_raw, carbon_tx_data, carbon_tx_type, debug_comment
        )
        ORDER BY t.tx_index
        RETURNING id, tx_index
        "#,
    )
    .bind(&hash)
    .bind(&tx_index)
    .bind(&block)
    .bind(&timestamp)
    .bind(&payload)
    .bind(&script_raw)
    .bind(&result)
    .bind(&expiration)
    .bind(&state_id)
    .bind(&sender_id)
    .bind(&gas_payer_id)
    .bind(&gas_target_id)
    .bind(&fee_raw)
    .bind(&gas_limit_raw)
    .bind(&gas_price_raw)
    .bind(&carbon_tx_data)
    .bind(&carbon_tx_type)
    .bind(&debug_comment)
    .fetch_all(&mut *conn)
    .await?;
    let id_by_tx_index = inserted
        .into_iter()
        .map(|(id, tx_index)| (tx_index, id))
        .collect::<HashMap<i32, i32>>();

    let mut records = Vec::with_capacity(transactions.len());
    for (transaction, (_, resolved_sender, resolved_gas_payer, resolved_gas_target)) in
        transactions.into_iter().zip(resolved.into_iter())
    {
        let id = id_by_tx_index
            .get(&transaction.tx_index)
            .copied()
            .ok_or(DbError::Sqlx(sqlx::Error::RowNotFound))?;
        replace_transaction_signatures(conn, id, &transaction.signatures).await?;
        records.push(transaction_record_from_upsert(
            transaction,
            id,
            resolved_sender,
            resolved_gas_payer,
            resolved_gas_target,
        ));
    }

    Ok(records)
}

async fn replace_transaction_signatures(
    conn: &mut PgConnection,
    transaction_id: i32,
    signatures: &[TransactionSignatureUpsert],
) -> Result<(), DbError> {
    sqlx::query("DELETE FROM signatures WHERE transaction_id = $1")
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;

    for signature in signatures {
        let kind_id = upsert_signature_kind_id(conn, &signature.kind).await?;
        sqlx::query(
            r#"
            INSERT INTO signatures (signature_kind_id, data, transaction_id)
            VALUES ($1, decode($2, 'hex'), $3)
            "#,
        )
        .bind(kind_id)
        .bind(&signature.data)
        .bind(transaction_id)
        .execute(&mut *conn)
        .await?;
    }

    Ok(())
}

async fn upsert_signature_kind_id(conn: &mut PgConnection, name: &str) -> Result<i32, DbError> {
    if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM signature_kinds
        WHERE name = $1
        "#,
    )
    .bind(name)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(id);
    }

    let id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO signature_kinds (name)
        VALUES ($1)
        RETURNING id
        "#,
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await?;

    Ok(id)
}

pub async fn replace_address_transactions_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<u64, DbError> {
    replace_address_transactions_for_block(conn, &[transaction_id]).await
}

/// Replace the AddressTransaction links for every transaction in a block in one
/// set-based pass (mirrors the C# block-level `InsertBatchAsync`). The per-block
/// batch is equivalent to running [`replace_address_transactions_for_transaction`]
/// for each id in order: links are deduped per transaction before the insert.
pub async fn replace_address_transactions_for_block(
    conn: &mut PgConnection,
    transaction_ids: &[i32],
) -> Result<u64, DbError> {
    if transaction_ids.is_empty() {
        return Ok(0);
    }

    sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT id AS transaction_id, sender_id AS address_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT id, gas_payer_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT id, gas_target_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT transaction_id, address_id FROM events WHERE transaction_id = ANY($1)
        ),
        desired_links AS (
            SELECT DISTINCT transaction_id, address_id
            FROM candidate_address_links
            WHERE address_id IS NOT NULL
        )
        DELETE FROM address_transactions address_tx
        WHERE address_tx.transaction_id = ANY($1)
          AND NOT EXISTS (
              SELECT 1
              FROM desired_links desired
              WHERE desired.transaction_id = address_tx.transaction_id
                AND desired.address_id = address_tx.address_id
          )
        "#,
    )
    .bind(transaction_ids)
    .execute(&mut *conn)
    .await?;

    // C# queues AddressTransaction rows in a HashSet, so duplicates are removed
    // before the batch insert; MIN(ord) keeps the first-seen link (sender, gas
    // payer, gas target, then event addresses). The table keys on
    // (address_id, transaction_id) with no surrogate id, so insert order is not
    // observable; the ORDER BY stays only to keep the write deterministic.
    let result = sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT id AS transaction_id, 1 AS ord, sender_id AS address_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT id, 2, gas_payer_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT id, 3, gas_target_id FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT transaction_id,
                   1000 + row_number() OVER (PARTITION BY transaction_id ORDER BY event_index, id),
                   address_id
            FROM events
            WHERE transaction_id = ANY($1)
        ),
        first_address_links AS (
            SELECT transaction_id, address_id, MIN(ord) AS ord
            FROM candidate_address_links
            WHERE address_id IS NOT NULL
            GROUP BY transaction_id, address_id
        )
        INSERT INTO address_transactions (address_id, transaction_id, timestamp_unix_seconds)
        SELECT first_address_links.address_id, first_address_links.transaction_id, tx.timestamp_unix_seconds
        FROM first_address_links
        JOIN transactions tx ON tx.id = first_address_links.transaction_id
        WHERE NOT EXISTS (
            SELECT 1
            FROM address_transactions existing
            WHERE existing.address_id = first_address_links.address_id
              AND existing.transaction_id = first_address_links.transaction_id
        )
        ORDER BY first_address_links.transaction_id, first_address_links.ord
        ON CONFLICT (address_id, transaction_id) DO NOTHING
        "#,
    )
    .bind(transaction_ids)
    .execute(&mut *conn)
    .await?;

    // Keep the denormalized first transaction timestamp in sync with
    // the AddressTransaction links. C# updates this before duplicate checks,
    // so Rust must also run it even when no new link row is inserted. The
    // per-address minimum over the block equals the sequential per-transaction
    // minimums.
    sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT sender_id AS address_id, timestamp_unix_seconds FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT gas_payer_id, timestamp_unix_seconds FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT gas_target_id, timestamp_unix_seconds FROM transactions WHERE id = ANY($1)
            UNION ALL
            SELECT event.address_id, tx.timestamp_unix_seconds
            FROM events event
            JOIN transactions tx ON tx.id = event.transaction_id
            WHERE event.transaction_id = ANY($1)
        ),
        first_address_links AS (
            SELECT address_id, MIN(timestamp_unix_seconds) AS first_tx_unix_seconds
            FROM candidate_address_links
            WHERE address_id IS NOT NULL
            GROUP BY address_id
        )
        UPDATE addresses address
        SET first_tx_unix_seconds = first_address_links.first_tx_unix_seconds
        FROM first_address_links
        WHERE address.id = first_address_links.address_id
          AND (
              address.first_tx_unix_seconds IS NULL
              OR address.first_tx_unix_seconds > first_address_links.first_tx_unix_seconds
          )
        "#,
    )
    .bind(transaction_ids)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub(crate) fn block_height_to_i64(height: BlockHeight) -> Result<i64, DbError> {
    i64::try_from(height.value()).map_err(|_| DbError::BlockHeightOutOfRange {
        height: height.value(),
    })
}

fn usable_address(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_migrations_path_is_repo_relative() {
        // The migration runner is normally launched from the workspace root in
        // local/dev containers, so the default path should stay simple.
        assert_eq!(default_migrations_dir(), PathBuf::from("migrations"));
    }

    #[tokio::test]
    async fn token_supply_sync_writes_only_what_changed() -> Result<(), Box<dyn std::error::Error>>
    {
        // The supply sync runs every minute against a token set that barely moves. It
        // must touch a row only when a value really differs: otherwise every pass
        // rewrites the whole token table, and `rows_affected` stops meaning "changed",
        // which is exactly what the caller logs on.
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let symbol = format!("RSTSUP{}", &suffix[..8]);
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &symbol).await?;
        let owner_id =
            upsert_address_id(&mut transaction, chain_id, &format!("PTESTOWNER{suffix}")).await?;

        sqlx::query(
            r#"
            INSERT INTO tokens (
                symbol, fungible, transferable, finite, divisible, fuel, stakable, fiat,
                swappable, burnable, decimals,
                address_id, owner_id, price_usd, chain_id, contract_id,
                burned_supply_raw, current_supply_raw, max_supply_raw, mintable, name
            )
            VALUES (
                $1, TRUE, TRUE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, TRUE, 8,
                $2, $2, 0, $3, $4,
                0, 0, 0, TRUE, $1
            )
            "#,
        )
        .bind(&symbol)
        .bind(owner_id)
        .bind(chain_id)
        .bind(contract_id)
        .execute(&mut *transaction)
        .await?;

        let supply = TokenSupplyUpsert {
            symbol: symbol.clone(),
            carbon_id: Some(4242),
            current_supply_raw: "1000000000".to_owned(),
            max_supply_raw: "2000000000".to_owned(),
            burned_supply_raw: "100000000".to_owned(),
        };

        let first =
            update_token_supplies(&mut transaction, chain_id, std::slice::from_ref(&supply))
                .await?;
        assert_eq!(first, 1, "the first sync must write the new supply");

        let second =
            update_token_supplies(&mut transaction, chain_id, std::slice::from_ref(&supply))
                .await?;
        assert_eq!(
            second, 0,
            "an unchanged answer must not rewrite the row a second time"
        );

        let moved = TokenSupplyUpsert {
            current_supply_raw: "1100000000".to_owned(),
            ..supply
        };
        let third = update_token_supplies(&mut transaction, chain_id, &[moved]).await?;
        assert_eq!(third, 1, "a real supply change must still be written");

        let stored = sqlx::query_scalar::<_, String>(
            "SELECT current_supply_raw::text FROM tokens WHERE chain_id = $1 AND symbol = $2",
        )
        .bind(chain_id)
        .bind(&symbol)
        .fetch_one(&mut *transaction)
        .await?;
        assert_eq!(stored, "1100000000");

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn dimension_upserts_do_not_update_existing_rows()
    -> Result<(), Box<dyn std::error::Error>> {
        // The block projection resolves the same addresses/kinds/contracts millions
        // of times per resync. The old `ON CONFLICT … DO UPDATE SET col = col` idiom
        // made every such resolve a real no-op UPDATE — dead tuple + WAL (measured on
        // a full local resync: 17.4M updates on the 40k-row addresses table, 11.9M on
        // 70 event_kinds, 4.8M on 179 contracts). The resolvers are SELECT-first now,
        // and this test pins that: re-resolving an existing row must perform ZERO
        // tuple updates. `pg_stat_xact_user_tables` counts only the current
        // transaction's actions, so the assertions are synchronous and exact.
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let address = format!("PTESTIDIOM{suffix}");
        let kind = format!("RstIdiomKind{}", &suffix[..8]);
        let contract = format!("rstidiom{}", &suffix[..8]);

        let address_id = upsert_address_id(&mut transaction, chain_id, &address).await?;
        let kind_id = upsert_event_kind_id(&mut transaction, &kind).await?;
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &contract).await?;

        // Re-resolving must come back from the plain SELECT with the same id.
        assert_eq!(
            upsert_address_id(&mut transaction, chain_id, &address).await?,
            address_id
        );
        assert_eq!(
            upsert_event_kind_id(&mut transaction, &kind).await?,
            kind_id
        );
        assert_eq!(
            upsert_contract_id(&mut transaction, chain_id, &contract).await?,
            contract_id
        );

        async fn xact_tuple_counts(
            conn: &mut PgConnection,
            table: &str,
        ) -> Result<(i64, i64), DbError> {
            let counts = sqlx::query_as::<_, (Option<i64>, Option<i64>)>(
                "SELECT n_tup_ins, n_tup_upd FROM pg_stat_xact_user_tables WHERE relname = $1",
            )
            .bind(table)
            .fetch_one(&mut *conn)
            .await?;
            Ok((counts.0.unwrap_or(0), counts.1.unwrap_or(0)))
        }

        // Exactly one insert per dimension (the mint), zero updates anywhere.
        assert_eq!(
            xact_tuple_counts(&mut transaction, "addresses").await?,
            (1, 0),
            "address re-resolve must not touch the stored row"
        );
        assert_eq!(
            xact_tuple_counts(&mut transaction, "event_kinds").await?,
            (1, 0),
            "event-kind re-resolve must not touch the stored row"
        );
        assert_eq!(
            xact_tuple_counts(&mut transaction, "contracts").await?,
            (1, 0),
            "contract re-resolve must not touch the stored row"
        );

        // The one case where the contract resolver must still write: an existing row
        // with an empty name/symbol gets them backfilled from the resolved value.
        let hollow = format!("rsthollow{}", &suffix[..8]);
        sqlx::query(
            r#"
            INSERT INTO contracts (name, hash, symbol, chain_id, last_updated_unix_seconds)
            VALUES ('', $1, '', $2, 0)
            "#,
        )
        .bind(&hollow)
        .bind(chain_id)
        .execute(&mut *transaction)
        .await?;
        let hollow_id = upsert_contract_id(&mut transaction, chain_id, &hollow).await?;
        let (name, symbol) = sqlx::query_as::<_, (String, String)>(
            "SELECT name, symbol FROM contracts WHERE id = $1",
        )
        .bind(hollow_id)
        .fetch_one(&mut *transaction)
        .await?;
        assert_eq!(name, hollow, "empty name must be backfilled");
        assert_eq!(symbol, hollow, "empty symbol must be backfilled");
        assert_eq!(
            xact_tuple_counts(&mut transaction, "contracts").await?,
            (2, 1),
            "the backfill is the only contract update"
        );

        // Once filled, the row drops back onto the read-only path.
        assert_eq!(
            upsert_contract_id(&mut transaction, chain_id, &hollow).await?,
            hollow_id
        );
        assert_eq!(
            xact_tuple_counts(&mut transaction, "contracts").await?,
            (2, 1),
            "a filled contract must not be rewritten again"
        );

        transaction.rollback().await?;
        Ok(())
    }

    // CI guard: the db-integration tests self-skip when EXPLORER_TEST_DATABASE_URL
    // is unset, which is convenient locally but dangerous in CI (they would pass
    // without ever exercising the database). CI sets EXPLORER_REQUIRE_DB_TESTS=1,
    // and this test then fails unless a test database URL is configured, so the
    // suite cannot silently "pass on skip".
    #[test]
    fn db_integration_tests_must_run_when_required() {
        if std::env::var("EXPLORER_REQUIRE_DB_TESTS").is_ok() {
            // `assert!` (not `.expect()`) because the workspace denies expect_used.
            assert!(
                std::env::var("EXPLORER_TEST_DATABASE_URL").is_ok(),
                "EXPLORER_TEST_DATABASE_URL must be set when EXPLORER_REQUIRE_DB_TESTS=1 (CI)"
            );
        }
    }
}
