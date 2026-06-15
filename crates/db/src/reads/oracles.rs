//! Oracles list read model (the `/oracles` endpoint).
//!
//! Oracles are listed per block: the caller selects a block (by hash or height)
//! within a chain, and the read returns that block's oracle URLs/contents. The
//! sortable columns live here as a closed enum so the column name is never
//! caller-controlled.
use crate::*;

/// One row of the oracles list read model. Both columns are nullable in schema.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OracleRow {
    pub url: Option<String>,
    pub content: Option<String>,
}

/// Sortable columns for the oracles list.
#[derive(Debug, Clone, Copy)]
pub enum OracleOrderBy {
    Id,
    Url,
    Content,
}

impl OracleOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "url" => Some(Self::Url),
            "content" => Some(Self::Content),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "oracle.id",
            Self::Url => "oracle.url",
            Self::Content => "oracle.content",
        }
    }
}

/// Block selector + chain scope for an oracles query.
#[derive(Debug, Clone, Copy)]
pub struct OracleFilter<'a> {
    pub chain_id: i32,
    pub block_hash: Option<&'a str>,
    pub block_height: Option<i64>,
}

/// List a block's oracles (selected by `filter`), ordered by the chosen column
/// then `oracle.id`, paged by `limit`/`offset`.
pub async fn list_oracles(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &OracleFilter<'_>,
    order_by: OracleOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<OracleRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT oracle.url, oracle.content
        FROM block_oracles block_oracle
        JOIN oracles oracle ON oracle.id = block_oracle.oracle_id
        JOIN blocks block ON block.id = block_oracle.block_id
        WHERE block.chain_id = $1
          AND ($2::text IS NULL OR block.hash = $2)
          AND ($3::bigint IS NULL OR block.height = $3)
        ORDER BY {column} {dir}, oracle.id {dir}
        LIMIT $4 OFFSET $5
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query_as::<_, OracleRow>(&sql)
        .bind(filter.chain_id)
        .bind(filter.block_hash)
        .bind(filter.block_height)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Count a block's oracles matching the same block filter, for `with_total`.
pub async fn count_oracles(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &OracleFilter<'_>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM block_oracles block_oracle
        JOIN blocks block ON block.id = block_oracle.block_id
        WHERE block.chain_id = $1
          AND ($2::text IS NULL OR block.hash = $2)
          AND ($3::bigint IS NULL OR block.height = $3)
        "#,
    )
    .bind(filter.chain_id)
    .bind(filter.block_hash)
    .bind(filter.block_height)
    .fetch_one(executor)
    .await?;

    Ok(count)
}
