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
            COALESCE(previous_block.hash, repeat('0', 64)) AS previous_hash,
            block.protocol,
            chain_address.address AS chain_address,
            validator_address.address AS validator_address,
            producer_address.address AS producer_address,
            block.timestamp_unix_seconds,
            block.reward::text AS reward,
            (
                SELECT COUNT(*)::integer
                FROM transactions tx
                WHERE tx.block_id = block.id
            ) AS transaction_count
        FROM blocks block
        LEFT JOIN blocks previous_block
            ON previous_block.chain_id = block.chain_id
            AND previous_block.height = block.height - 1
        LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
        LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
        LEFT JOIN addresses producer_address ON producer_address.id = block.producer_address_id
        WHERE block.chain_id = $1
          AND ($2::text IS NULL OR block.hash = $2 OR block.height = $3)
          AND ($4::text IS NULL OR block.hash = $4)
          AND ($5::bigint IS NULL OR block.height = $5)
          AND ($6::bigint IS NULL OR block.height = $6 OR block.hash = $7)
          AND ($8::bigint IS NULL OR block.timestamp_unix_seconds <= $8)
          AND ($9::bigint IS NULL OR block.timestamp_unix_seconds >= $9)
        ORDER BY {column} {dir}, block.id {dir}
        LIMIT $10 OFFSET $11
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.id)
        .bind(filter.id_height)
        .bind(filter.hash)
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
            COALESCE(previous_block.hash, repeat('0', 64)) AS previous_hash,
            block.protocol,
            chain_address.address AS chain_address,
            validator_address.address AS validator_address,
            producer_address.address AS producer_address,
            block.timestamp_unix_seconds,
            block.reward::text AS reward,
            (
                SELECT COUNT(*)::integer
                FROM transactions tx
                WHERE tx.block_id = block.id
            ) AS transaction_count
        FROM blocks block
        LEFT JOIN blocks previous_block
            ON previous_block.chain_id = block.chain_id
            AND previous_block.height = block.height - 1
        LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
        LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
        LEFT JOIN addresses producer_address ON producer_address.id = block.producer_address_id
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

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Round-trip for the gas-model-v2 producer address: a block upserted with a
    // producer must expose it through the detail read (joined to the addresses
    // dimension), a block without one must stay NULL — never the "NULL"
    // sentinel row — and the producer must be balance-dirtied even though its
    // fee credit emits no event. Runs inside a rolled-back transaction.
    #[tokio::test]
    async fn block_detail_exposes_and_dirties_the_producer_address()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut tx = pool.begin().await?;

        let chain = ChainName::new("main")?;
        let chain_id = resolve_chain_id(&mut tx, &chain).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let producer = format!("PTESTPRODUCER{suffix}");

        let with_producer = upsert_block(
            &mut tx,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_200_000),
                hash: format!("TESTPRODBLOCKA{suffix}"),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: Some(producer.clone()),
                timestamp_unix_seconds: 1_800_200_000,
                reward: None,
            },
        )
        .await?;
        let without_producer = upsert_block(
            &mut tx,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_200_001),
                hash: format!("TESTPRODBLOCKB{suffix}"),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: None,
                timestamp_unix_seconds: 1_800_200_001,
                reward: None,
            },
        )
        .await?;

        let row = block_detail(&mut *tx, chain_id, Some(with_producer.height), None)
            .await?
            .ok_or("block with producer not found")?;
        assert_eq!(
            row.get::<Option<String>, _>("producer_address").as_deref(),
            Some(producer.as_str())
        );

        let row = block_detail(&mut *tx, chain_id, Some(without_producer.height), None)
            .await?
            .ok_or("block without producer not found")?;
        assert_eq!(row.get::<Option<String>, _>("producer_address"), None);

        mark_block_addresses_dirty(&mut tx, with_producer.id, BlockHeight::new(9_900_200_000))
            .await?;
        let dirty_block = sqlx::query_scalar::<_, i64>(
            "SELECT balance_dirty_block FROM addresses WHERE chain_id = $1 AND address = $2",
        )
        .bind(chain_id)
        .bind(&producer)
        .fetch_one(&mut *tx)
        .await?;
        assert_eq!(dirty_block, 9_900_200_000);

        Ok(())
    }
}
