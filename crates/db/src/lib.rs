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
    pub previous_hash: Option<String>,
    pub protocol: Option<i32>,
    pub chain_address: Option<String>,
    pub validator_address: Option<String>,
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
    pub previous_hash: Option<String>,
    pub protocol: Option<i32>,
    pub chain_address_id: i32,
    pub chain_address: Option<String>,
    pub validator_address_id: i32,
    pub validator_address: Option<String>,
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
    pub fee: Option<String>,
    pub fee_raw: Option<String>,
    pub gas_price: Option<String>,
    pub gas_price_raw: Option<String>,
    pub gas_limit: Option<String>,
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
    pub fee: Option<String>,
    pub fee_raw: Option<String>,
    pub gas_price: Option<String>,
    pub gas_price_raw: Option<String>,
    pub gas_limit: Option<String>,
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
    pub address: Option<String>,
    pub target_address: Option<String>,
    pub contract: Option<String>,
    pub token_id: Option<String>,
    pub raw_data: Option<String>,
    pub payload_format: Option<String>,
    pub payload_json: Option<Value>,
    pub timestamp_unix_seconds: i64,
    pub date_unix_seconds: i64,
    pub burned: Option<bool>,
    pub nsfw: bool,
    pub blacklisted: bool,
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
    pub amount: String,
    pub amount_raw: String,
}

#[derive(Debug, Clone)]
pub struct AddressAccountUpsert {
    pub address_id: i32,
    pub address_name: Option<String>,
    pub name_last_updated_unix_seconds: i64,
    pub stake_timestamp: i64,
    pub staked_amount: String,
    pub staked_amount_raw: String,
    pub unclaimed_amount: String,
    pub unclaimed_amount_raw: String,
    pub soul_balance_raw: String,
    pub storage_available: i64,
    pub storage_used: i64,
    pub avatar: Option<String>,
    pub validator_kind: String,
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
    pub current_supply: String,
    pub current_supply_raw: String,
    pub max_supply: String,
    pub max_supply_raw: String,
    pub burned_supply: String,
    pub burned_supply_raw: String,
}

/// Live token price in every fiat currency the explorer serves. `None` fields are
/// left untouched on update so an unavailable fiat pairing never clobbers a good one.
#[derive(Debug, Clone)]
pub struct TokenPriceUpsert {
    pub symbol: String,
    pub price_usd: Option<f64>,
    pub price_eur: Option<f64>,
    pub price_gbp: Option<f64>,
    pub price_jpy: Option<f64>,
    pub price_cad: Option<f64>,
    pub price_aud: Option<f64>,
    pub price_cny: Option<f64>,
    pub price_rub: Option<f64>,
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
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .acquire_timeout(config.acquire_timeout)
        .connect(&config.url)
        .await
        .map_err(DbError::Sqlx)
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

pub async fn upsert_address_id(
    conn: &mut PgConnection,
    chain_id: i32,
    address: &str,
) -> Result<i32, DbError> {
    let id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO addresses (
            address,
            chain_id,
            name_last_updated_unix_seconds,
            stake_timestamp,
            storage_available,
            storage_used,
            total_soul_amount,
            balance_dirty_block
        )
        VALUES ($1, $2, 0, 0, 0, 0, 0, 0)
        ON CONFLICT (chain_id, address) DO UPDATE SET
            address = addresses.address
        RETURNING id
        "#,
    )
    .bind(address)
    .bind(chain_id)
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
    let validator_kind_id = upsert_address_validator_kind_id(conn, &account.validator_kind).await?;

    sqlx::query(
        r#"
        UPDATE addresses
        SET
            address_name = $2,
            name_last_updated_unix_seconds = $3,
            stake_timestamp = $4,
            staked_amount = $5,
            staked_amount_raw = $6,
            unclaimed_amount = $7,
            unclaimed_amount_raw = $8,
            total_soul_amount =
                COALESCE(NULLIF($9, '')::numeric, 0)
                + COALESCE(NULLIF($6, '')::numeric, 0),
            storage_available = $10,
            storage_used = $11,
            avatar = $12,
            address_validator_kind_id = $13
        WHERE id = $1
          AND chain_id = $14
        "#,
    )
    .bind(account.address_id)
    .bind(&account.address_name)
    .bind(account.name_last_updated_unix_seconds)
    .bind(account.stake_timestamp)
    .bind(&account.staked_amount)
    .bind(&account.staked_amount_raw)
    .bind(&account.unclaimed_amount)
    .bind(&account.unclaimed_amount_raw)
    .bind(&account.soul_balance_raw)
    .bind(account.storage_available)
    .bind(account.storage_used)
    .bind(&account.avatar)
    .bind(validator_kind_id)
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
                COALESCE(NULLIF(staked_amount_raw, '')::numeric, 0) AS staked_raw
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
                COALESCE(NULLIF(staked_amount_raw, '')::numeric, 0) AS staked_raw
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
    let current_supplies = supplies
        .iter()
        .map(|supply| supply.current_supply.clone())
        .collect::<Vec<_>>();
    let carbon_ids = supplies
        .iter()
        .map(|supply| supply.carbon_id)
        .collect::<Vec<_>>();
    let current_supply_raws = supplies
        .iter()
        .map(|supply| supply.current_supply_raw.clone())
        .collect::<Vec<_>>();
    let max_supplies = supplies
        .iter()
        .map(|supply| supply.max_supply.clone())
        .collect::<Vec<_>>();
    let max_supply_raws = supplies
        .iter()
        .map(|supply| supply.max_supply_raw.clone())
        .collect::<Vec<_>>();
    let burned_supplies = supplies
        .iter()
        .map(|supply| supply.burned_supply.clone())
        .collect::<Vec<_>>();
    let burned_supply_raws = supplies
        .iter()
        .map(|supply| supply.burned_supply_raw.clone())
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE tokens token
        SET
            current_supply = desired.current_supply,
            carbon_id = COALESCE(desired.carbon_id, token.carbon_id),
            current_supply_raw = desired.current_supply_raw,
            max_supply = desired.max_supply,
            max_supply_raw = desired.max_supply_raw,
            burned_supply = desired.burned_supply,
            burned_supply_raw = desired.burned_supply_raw
        FROM UNNEST(
            $2::text[],
            $3::bigint[],
            $4::text[],
            $5::text[],
            $6::text[],
            $7::text[],
            $8::text[],
            $9::text[]
        ) AS desired(
            symbol,
            carbon_id,
            current_supply,
            current_supply_raw,
            max_supply,
            max_supply_raw,
            burned_supply,
            burned_supply_raw
        )
        WHERE token.chain_id = $1
          AND token.symbol = desired.symbol
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&carbon_ids)
    .bind(&current_supplies)
    .bind(&current_supply_raws)
    .bind(&max_supplies)
    .bind(&max_supply_raws)
    .bind(&burned_supplies)
    .bind(&burned_supply_raws)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

/// Refreshes the live `tokens.price_*` columns from an external price feed.
/// Mirrors the C# CoinGecko plugin's `TokenMethods.SetPrice`: one UPDATE across all
/// fiat columns, `COALESCE`-guarded so a missing fiat pairing never clobbers a value
/// that is already there. Returns the number of token rows touched.
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
    let eur = prices.iter().map(|p| p.price_eur).collect::<Vec<_>>();
    let gbp = prices.iter().map(|p| p.price_gbp).collect::<Vec<_>>();
    let jpy = prices.iter().map(|p| p.price_jpy).collect::<Vec<_>>();
    let cad = prices.iter().map(|p| p.price_cad).collect::<Vec<_>>();
    let aud = prices.iter().map(|p| p.price_aud).collect::<Vec<_>>();
    let cny = prices.iter().map(|p| p.price_cny).collect::<Vec<_>>();
    let rub = prices.iter().map(|p| p.price_rub).collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        UPDATE tokens token
        SET
            price_usd = COALESCE(desired.price_usd, token.price_usd),
            price_eur = COALESCE(desired.price_eur, token.price_eur),
            price_gbp = COALESCE(desired.price_gbp, token.price_gbp),
            price_jpy = COALESCE(desired.price_jpy, token.price_jpy),
            price_cad = COALESCE(desired.price_cad, token.price_cad),
            price_aud = COALESCE(desired.price_aud, token.price_aud),
            price_cny = COALESCE(desired.price_cny, token.price_cny),
            price_rub = COALESCE(desired.price_rub, token.price_rub)
        FROM UNNEST(
            $2::text[],
            $3::double precision[],
            $4::double precision[],
            $5::double precision[],
            $6::double precision[],
            $7::double precision[],
            $8::double precision[],
            $9::double precision[],
            $10::double precision[]
        ) AS desired(
            symbol,
            price_usd,
            price_eur,
            price_gbp,
            price_jpy,
            price_cad,
            price_aud,
            price_cny,
            price_rub
        )
        WHERE token.chain_id = $1
          AND token.symbol = desired.symbol
        "#,
    )
    .bind(chain_id)
    .bind(&symbols)
    .bind(&usd)
    .bind(&eur)
    .bind(&gbp)
    .bind(&jpy)
    .bind(&cad)
    .bind(&aud)
    .bind(&cny)
    .bind(&rub)
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
    let amounts = balances
        .iter()
        .map(|balance| balance.amount.clone())
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
        INSERT INTO address_balances (address_id, token_id, amount, amount_raw)
        SELECT
            $1,
            token.id,
            desired.amount,
            COALESCE(NULLIF(desired.amount_raw, '')::numeric, 0)
        FROM UNNEST($2::text[], $3::text[], $4::text[]) AS desired(symbol, amount, amount_raw)
        JOIN tokens token
          ON token.chain_id = $5
         AND token.symbol = desired.symbol
        ON CONFLICT (address_id, token_id) DO UPDATE SET
            amount = EXCLUDED.amount,
            amount_raw = EXCLUDED.amount_raw
        "#,
    )
    .bind(address_id)
    .bind(&symbols)
    .bind(&amounts)
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

async fn upsert_address_validator_kind_id(
    conn: &mut PgConnection,
    name: &str,
) -> Result<i32, DbError> {
    if let Some(id) = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM address_validator_kinds
        WHERE name = $1
        ORDER BY id
        LIMIT 1
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
        INSERT INTO address_validator_kinds (name)
        VALUES ($1)
        RETURNING id
        "#,
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await?;

    Ok(id)
}

pub async fn upsert_contract_id(
    conn: &mut PgConnection,
    chain_id: i32,
    contract: &str,
) -> Result<i32, DbError> {
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

pub async fn upsert_event_kind_id(
    conn: &mut PgConnection,
    chain_id: i32,
    name: &str,
) -> Result<i32, DbError> {
    let id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO event_kinds (name, chain_id)
        VALUES ($1, $2)
        ON CONFLICT (chain_id, name) DO UPDATE SET
            name = event_kinds.name
        RETURNING id
        "#,
    )
    .bind(name)
    .bind(chain_id)
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
    block: BlockUpsert,
) -> Result<BlockRecord, DbError> {
    let chain_id = resolve_chain_id(conn, &block.chain).await?;
    let chain_address = block.chain_address.as_deref().unwrap_or("NULL");
    let validator_address = block.validator_address.as_deref().unwrap_or("NULL");
    let chain_address_id = upsert_address_id(conn, chain_id, chain_address).await?;
    let validator_address_id = upsert_address_id(conn, chain_id, validator_address).await?;
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
                previous_hash = $3,
                protocol = $4,
                chain_address_id = $5,
                validator_address_id = $6,
                timestamp_unix_seconds = $7,
                reward = $8
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(&block.hash)
        .bind(&block.previous_hash)
        .bind(block.protocol.unwrap_or_default())
        .bind(chain_address_id)
        .bind(validator_address_id)
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
                previous_hash,
                protocol,
                chain_address_id,
                validator_address_id,
                reward
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
        )
        .bind(height)
        .bind(block.timestamp_unix_seconds)
        .bind(chain_id)
        .bind(&block.hash)
        .bind(&block.previous_hash)
        .bind(block.protocol.unwrap_or_default())
        .bind(chain_address_id)
        .bind(validator_address_id)
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
        previous_hash: block.previous_hash,
        protocol: block.protocol,
        chain_address_id,
        chain_address: Some(chain_address.to_owned()),
        validator_address_id,
        validator_address: Some(validator_address.to_owned()),
        timestamp_unix_seconds: block.timestamp_unix_seconds,
        reward: block.reward,
    })
}

pub async fn upsert_transaction(
    conn: &mut PgConnection,
    transaction: TransactionUpsert,
) -> Result<TransactionRecord, DbError> {
    let state_id = upsert_transaction_state_id(conn, &transaction.state).await?;
    let sender = transaction.sender.as_deref().unwrap_or("NULL");
    let gas_payer = transaction.gas_payer.as_deref().unwrap_or("NULL");
    let gas_target = transaction.gas_target.as_deref().unwrap_or("NULL");
    let sender_id = upsert_address_id(conn, transaction.chain_id, sender).await?;
    let gas_payer_id = upsert_address_id(conn, transaction.chain_id, gas_payer).await?;
    let gas_target_id = upsert_address_id(conn, transaction.chain_id, gas_target).await?;

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
                script_raw = $5,
                result = $6,
                fee = $7,
                expiration = $8,
                state_id = $9,
                gas_price = $10,
                gas_limit = $11,
                sender_id = $12,
                gas_payer_id = $13,
                gas_target_id = $14,
                fee_raw = $15,
                gas_limit_raw = $16,
                gas_price_raw = $17,
                carbon_tx_data = $18,
                carbon_tx_type = $19,
                debug_comment = $20
            WHERE id = $1
            "#,
        )
        .bind(id)
        .bind(&transaction.hash)
        .bind(transaction.timestamp_unix_seconds)
        .bind(&transaction.payload)
        .bind(&transaction.script_raw)
        .bind(&transaction.result)
        .bind(&transaction.fee)
        .bind(transaction.expiration_unix_seconds)
        .bind(state_id)
        .bind(&transaction.gas_price)
        .bind(&transaction.gas_limit)
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
                fee,
                expiration,
                state_id,
                gas_price,
                gas_limit,
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
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
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
        .bind(&transaction.fee)
        .bind(transaction.expiration_unix_seconds)
        .bind(state_id)
        .bind(&transaction.gas_price)
        .bind(&transaction.gas_limit)
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

    Ok(TransactionRecord {
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
        fee: transaction.fee,
        fee_raw: transaction.fee_raw,
        gas_price: transaction.gas_price,
        gas_price_raw: transaction.gas_price_raw,
        gas_limit: transaction.gas_limit,
        gas_limit_raw: transaction.gas_limit_raw,
        sender_id,
        gas_payer_id,
        gas_target_id,
        carbon_tx_type: transaction.carbon_tx_type,
        carbon_tx_data: transaction.carbon_tx_data,
        expiration_unix_seconds: transaction.expiration_unix_seconds,
    })
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
            VALUES ($1, $2, $3)
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
    sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT 1 AS ord, sender_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 2 AS ord, gas_payer_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 3 AS ord, gas_target_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 1000 + row_number() OVER (ORDER BY event_index, id) AS ord, address_id
            FROM events
            WHERE transaction_id = $1
        ),
        first_address_links AS (
            SELECT address_id, MIN(ord) AS ord
            FROM candidate_address_links
            WHERE address_id IS NOT NULL
            GROUP BY address_id
        )
        DELETE FROM address_transactions address_tx
        WHERE address_tx.transaction_id = $1
          AND NOT EXISTS (
              SELECT 1
              FROM first_address_links desired
              WHERE desired.address_id = address_tx.address_id
          )
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    // C# queues AddressTransaction rows in a HashSet: first-seen order is
    // sender, gas payer, gas target, then event addresses, while duplicates are
    // removed before the batch insert. The surrogate IDs are observable in the
    // legacy DB, so Rust must preserve both the order and the pre-insert dedupe.
    let result = sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT 1 AS ord, sender_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 2 AS ord, gas_payer_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 3 AS ord, gas_target_id AS address_id FROM transactions WHERE id = $1
            UNION ALL
            SELECT 1000 + row_number() OVER (ORDER BY event_index, id) AS ord, address_id
            FROM events
            WHERE transaction_id = $1
        ),
        first_address_links AS (
            SELECT address_id, MIN(ord) AS ord
            FROM candidate_address_links
            WHERE address_id IS NOT NULL
            GROUP BY address_id
        )
        INSERT INTO address_transactions (address_id, transaction_id, timestamp_unix_seconds)
        SELECT address_id, $1, (SELECT timestamp_unix_seconds FROM transactions WHERE id = $1)
        FROM first_address_links
        WHERE NOT EXISTS (
            SELECT 1
            FROM address_transactions existing
            WHERE existing.address_id = first_address_links.address_id
              AND existing.transaction_id = $1
        )
        ORDER BY ord
        ON CONFLICT (address_id, transaction_id) DO NOTHING
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    // Keep the denormalized first transaction timestamp in sync with
    // the AddressTransaction links. C# updates this before duplicate checks,
    // so Rust must also run it even when no new link row is inserted.
    sqlx::query(
        r#"
        WITH candidate_address_links AS (
            SELECT sender_id AS address_id, timestamp_unix_seconds
            FROM transactions
            WHERE id = $1
            UNION ALL
            SELECT gas_payer_id AS address_id, timestamp_unix_seconds
            FROM transactions
            WHERE id = $1
            UNION ALL
            SELECT gas_target_id AS address_id, timestamp_unix_seconds
            FROM transactions
            WHERE id = $1
            UNION ALL
            SELECT event.address_id, tx.timestamp_unix_seconds
            FROM events event
            JOIN transactions tx ON tx.id = event.transaction_id
            WHERE event.transaction_id = $1
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
    .bind(transaction_id)
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
