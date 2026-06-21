//! Series list read model (the `/series` endpoint).
//!
//! A materialized `page` CTE selects/pages the visible (non-blacklisted) series
//! ids matching the filter, then the outer SELECT projects each series with its
//! contract/chain/creator/mode. The API maps the rows with `series_from_row`.
//! See the design note in [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the series list.
#[derive(Debug, Clone, Copy)]
pub enum SeriesOrderBy {
    Id,
    Created,
    SeriesId,
    Name,
}

impl SeriesOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "created" => Some(Self::Created),
            "series_id" => Some(Self::SeriesId),
            "name" => Some(Self::Name),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "series.id",
            Self::Created => "series.series_created_unix_seconds",
            Self::SeriesId => "series.series_id",
            Self::Name => "series.name",
        }
    }
}

/// Filters for the series list. `name`/`q` are already in `%...%` form; `id` is
/// the numeric surrogate id.
#[derive(Debug, Clone, Copy)]
pub struct SeriesFilter<'a> {
    pub chain_id: i32,
    pub id: Option<i32>,
    pub series_id: Option<&'a str>,
    pub creator: Option<&'a str>,
    pub name: Option<&'a str>,
    pub q: Option<&'a str>,
    pub contract: Option<&'a str>,
    pub symbol: Option<&'a str>,
    pub token_id: Option<&'a str>,
    /// Numeric `q` also matches the surrogate `series.id` (C# parity).
    pub q_id: Option<i64>,
}

/// List visible series for a chain matching the filter, ordered by the chosen
/// column then `series.id`. The caller passes `limit + 1` to detect a next page.
pub async fn list_series(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &SeriesFilter<'_>,
    order_by: SeriesOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let column = order_by.column();
    let sql = format!(
        r#"
        WITH page AS MATERIALIZED (
            SELECT series.id
            FROM series series
            JOIN contracts contract ON contract.id = series.contract_id
            LEFT JOIN addresses creator ON creator.id = series.creator_address_id
            WHERE contract.chain_id = $1
              AND (series.blacklisted IS NULL OR series.blacklisted = false)
              AND ($2::integer IS NULL OR series.id = $2)
              AND ($3::text IS NULL OR series.series_id = $3)
              AND ($4::text IS NULL OR creator.address = $4)
              AND ($5::text IS NULL OR series.name ILIKE $5 OR series.description ILIKE $5)
              AND ($6::text IS NULL OR series.name ILIKE $6 OR series.description ILIKE $6 OR series.series_id ILIKE $6 OR contract.symbol ILIKE $6 OR contract.hash ILIKE $6 OR ($12::bigint IS NOT NULL AND series.id = $12))
              AND ($7::text IS NULL OR contract.hash = $7)
              AND ($8::text IS NULL OR contract.symbol = $8)
              AND (
                  $9::text IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM nfts nft
                      WHERE nft.series_id = series.id
                        AND nft.token_id = $9
                  )
              )
            ORDER BY {column} {dir}, series.id {dir}
            LIMIT $10 OFFSET $11
        )
        SELECT
            series.id,
            series.series_id,
            creator.address AS creator,
            chain.name AS chain_name,
            contract.hash AS contract_hash,
            contract.symbol,
            series.series_created_unix_seconds,
            series.current_supply,
            series.max_supply,
            series_mode.mode_name,
            series.name,
            series.description,
            series.image,
            series.royalties::text AS royalties,
            series.type,
            series.attr_type_1,
            series.attr_value_1,
            series.attr_type_2,
            series.attr_value_2,
            series.attr_type_3,
            series.attr_value_3,
            series.metadata
        FROM page
        JOIN series series ON series.id = page.id
        JOIN contracts contract ON contract.id = series.contract_id
        JOIN chains chain ON chain.id = contract.chain_id
        LEFT JOIN addresses creator ON creator.id = series.creator_address_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        ORDER BY {column} {dir}, series.id {dir}
        "#,
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.id)
        .bind(filter.series_id)
        .bind(filter.creator)
        .bind(filter.name)
        .bind(filter.q)
        .bind(filter.contract)
        .bind(filter.symbol)
        .bind(filter.token_id)
        .bind(limit)
        .bind(offset)
        .bind(filter.q_id)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}
