//! Blocks list read model (the `/blocks` endpoint).
//!
//! Returns the block-list rows; the API maps them with `block_from_row` and,
//! when `with_transactions` is requested, hydrates each block's transactions
//! through the transaction read helpers. See the design note in
//! [`super::tokens`] for why the wide list reads return rows.
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the blocks list.
#[derive(Debug, Clone, Copy)]
pub enum BlockOrderBy {
    Id,
    Height,
    Hash,
    Date,
}

impl BlockOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "height" => Some(Self::Height),
            "hash" => Some(Self::Hash),
            "date" => Some(Self::Date),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "block.id",
            Self::Height => "block.height",
            Self::Hash => "block.hash",
            Self::Date => "block.timestamp_unix_seconds",
        }
    }
}

/// Filters for the blocks list. `id`/`id_height` carry the combined id lookup
/// (a value that may be a height or a hash); `q_height`/`q_hash` carry the free
/// `q` lookup. All of these are parsed and shaped by the API layer.
#[derive(Debug, Default, Clone, Copy)]
pub struct BlockFilter<'a> {
    pub chain_id: i32,
    pub id: Option<&'a str>,
    pub id_height: Option<i64>,
    pub hash: Option<&'a str>,
    pub hash_partial: Option<&'a str>,
    pub height: Option<i64>,
    pub q_height: Option<i64>,
    pub q_hash: Option<&'a str>,
    pub date_less: Option<i64>,
    pub date_greater: Option<i64>,
}

/// List blocks for a chain matching the filter, ordered by the chosen column
/// then `block.id`. The caller passes `limit + 1` to detect a following page.
pub async fn list_blocks(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &BlockFilter<'_>,
    order_by: BlockOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            block.id,
            block.height,
            block.hash,
            block.previous_hash,
            block.protocol,
            chain_address.address AS chain_address,
            validator_address.address AS validator_address,
            block.timestamp_unix_seconds,
            block.reward,
            (
                SELECT COUNT(*)::integer
                FROM transactions tx
                WHERE tx.block_id = block.id
            ) AS transaction_count
        FROM blocks block
        LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
        LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
        WHERE block.chain_id = $1
          AND ($2::text IS NULL OR block.hash = $2 OR block.height = $3)
          AND ($4::text IS NULL OR block.hash = $4)
          AND ($5::text IS NULL OR block.hash ILIKE $5)
          AND ($6::bigint IS NULL OR block.height = $6)
          AND ($7::bigint IS NULL OR block.height = $7 OR block.hash = $8)
          AND ($9::bigint IS NULL OR block.timestamp_unix_seconds <= $9)
          AND ($10::bigint IS NULL OR block.timestamp_unix_seconds >= $10)
        ORDER BY {column} {dir}, block.id {dir}
        LIMIT $11 OFFSET $12
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.id)
        .bind(filter.id_height)
        .bind(filter.hash)
        .bind(filter.hash_partial)
        .bind(filter.height)
        .bind(filter.q_height)
        .bind(filter.q_hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Fetch a single block's detail by height or hash within a chain. Returns
/// `None` when no block matches; the API maps that to a 404.
pub async fn block_detail(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: i32,
    height: Option<i64>,
    hash: Option<&str>,
) -> Result<Option<PgRow>, DbError> {
    let row = sqlx::query(
        r#"
        SELECT
            block.height,
            block.hash,
            block.previous_hash,
            block.protocol,
            chain_address.address AS chain_address,
            validator_address.address AS validator_address,
            block.timestamp_unix_seconds,
            block.reward,
            (
                SELECT COUNT(*)::integer
                FROM transactions tx
                WHERE tx.block_id = block.id
            ) AS transaction_count
        FROM blocks block
        LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
        LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
        WHERE block.chain_id = $1
          AND (
              ($2::bigint IS NOT NULL AND block.height = $2)
              OR ($3::text IS NOT NULL AND block.hash = $3)
          )
        "#,
    )
    .bind(chain_id)
    .bind(height)
    .bind(hash)
    .fetch_optional(executor)
    .await?;

    Ok(row)
}
