//! Contract-method-histories read model (the `/contractMethodHistories` endpoint).
//!
//! Lists historical contract-method snapshots (one per `contract_methods` row)
//! joined to their contract. The API maps the rows with `contract_from_row` plus
//! the snapshot date. See the design note in [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the contract-method-histories list.
#[derive(Debug, Clone, Copy)]
pub enum ContractMethodHistoryOrderBy {
    Id,
    Symbol,
    Name,
    Date,
}

impl ContractMethodHistoryOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "symbol" => Some(Self::Symbol),
            "name" => Some(Self::Name),
            "date" => Some(Self::Date),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "method.id",
            Self::Symbol => "contract.symbol",
            Self::Name => "contract.name",
            Self::Date => "method.timestamp_unix_seconds",
        }
    }
}

/// Filter (contract symbol/hash + date window) for a method-histories query.
#[derive(Debug, Clone, Copy)]
pub struct ContractMethodHistoryFilter<'a> {
    pub chain_id: i32,
    pub symbol: Option<&'a str>,
    pub hash: Option<&'a str>,
    pub date_less: Option<i64>,
    pub date_greater: Option<i64>,
}

/// List contract-method history snapshots matching the filter, ordered by the
/// chosen column then `method.id`, paged by `limit`/`offset`.
pub async fn list_contract_method_histories(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &ContractMethodHistoryFilter<'_>,
    order_by: ContractMethodHistoryOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            method.timestamp_unix_seconds,
            contract.name,
            contract.hash,
            contract.symbol,
            NULL::text AS script_raw,
            address.address,
            address.address_name,
            method.methods AS methods_json,
            NULL::jsonb AS token_json
        FROM contract_methods method
        JOIN contracts contract ON contract.contract_method_id = method.id
        LEFT JOIN addresses address ON address.id = contract.address_id
        WHERE contract.chain_id = $1
          AND ($2::text IS NULL OR contract.symbol = $2)
          AND ($3::text IS NULL OR contract.hash = $3)
          AND ($4::bigint IS NULL OR method.timestamp_unix_seconds <= $4)
          AND ($5::bigint IS NULL OR method.timestamp_unix_seconds >= $5)
        ORDER BY {column} {dir}, method.id {dir}
        LIMIT $6 OFFSET $7
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.symbol)
        .bind(filter.hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Count contract-method history snapshots matching the filter, for
/// `with_total` responses.
pub async fn count_contract_method_histories(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &ContractMethodHistoryFilter<'_>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM contract_methods method
        JOIN contracts contract ON contract.contract_method_id = method.id
        WHERE contract.chain_id = $1
          AND ($2::text IS NULL OR contract.symbol = $2)
          AND ($3::text IS NULL OR contract.hash = $3)
          AND ($4::bigint IS NULL OR method.timestamp_unix_seconds <= $4)
          AND ($5::bigint IS NULL OR method.timestamp_unix_seconds >= $5)
        "#,
    )
    .bind(filter.chain_id)
    .bind(filter.symbol)
    .bind(filter.hash)
    .bind(filter.date_less)
    .bind(filter.date_greater)
    .fetch_one(executor)
    .await?;

    Ok(count)
}
