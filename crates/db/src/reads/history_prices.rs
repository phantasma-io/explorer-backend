//! History-prices list read model (the `/historyPrices` endpoint).
//!
//! Daily USD price points for a token symbol (defaulting to SOUL in the API
//! layer), optionally bounded by a date window and optionally embedding the
//! token record as a JSON blob. `limit = None` means "all matching points".
use crate::*;

/// One row of the history-prices list read model. `token_json` is the optional
/// embedded token object (built in SQL when `with_token` is requested).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct HistoryPriceRow {
    pub symbol: Option<String>,
    pub date_unix_seconds: i64,
    pub price_usd: f64,
    pub token_json: Option<Value>,
}

/// Sortable columns for the history-prices list.
#[derive(Debug, Clone, Copy)]
pub enum HistoryPriceOrderBy {
    Id,
    Symbol,
    Date,
}

impl HistoryPriceOrderBy {
    /// Parse the public `order_by` query param, defaulting to `date`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("date") {
            "id" => Some(Self::Id),
            "symbol" => Some(Self::Symbol),
            "date" => Some(Self::Date),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "price.id",
            Self::Symbol => "token.symbol",
            Self::Date => "price.date_unix_seconds",
        }
    }
}

/// Filter + scope for a history-prices query. The symbol, date bounds, and the
/// `with_token` embed flag travel together so the read fn stays under the
/// argument-count lint.
#[derive(Debug, Clone, Copy)]
pub struct HistoryPriceFilter<'a> {
    pub symbol: &'a str,
    pub date_less: Option<i64>,
    pub date_greater: Option<i64>,
    pub with_token: bool,
}

/// List daily price points for the filtered symbol, bounded by the optional
/// date window, ordered by the chosen column then `price.id`. `limit = None`
/// returns all matching points; `with_token` embeds the token JSON object.
pub async fn list_history_prices(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &HistoryPriceFilter<'_>,
    order_by: HistoryPriceOrderBy,
    direction: SortDirection,
    limit: Option<i64>,
    offset: i64,
) -> Result<Vec<HistoryPriceRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            token.symbol,
            price.date_unix_seconds,
            price.price_usd::double precision AS price_usd,
            CASE WHEN $4::boolean THEN jsonb_build_object(
                'name', token.name,
                'symbol', token.symbol,
                'fungible', token.fungible,
                'transferable', token.transferable,
                'finite', token.finite,
                'divisible', token.divisible,
                'fuel', token.fuel,
                'stakable', token.stakable,
                'fiat', token.fiat,
                'swappable', token.swappable,
                'burnable', token.burnable,
                'mintable', token.mintable,
                'decimals', token.decimals,
                'current_supply', token.current_supply,
                'current_supply_raw', token.current_supply_raw,
                'max_supply', token.max_supply,
                'max_supply_raw', token.max_supply_raw,
                'burned_supply', token.burned_supply,
                'burned_supply_raw', token.burned_supply_raw,
                'script_raw', token.script_raw,
                'price', NULL,
                'token_logos', NULL
            ) ELSE NULL END AS token_json
        FROM token_daily_prices price
        JOIN tokens token ON token.id = price.token_id
        WHERE token.symbol = $1
          AND ($2::bigint IS NULL OR price.date_unix_seconds <= $2)
          AND ($3::bigint IS NULL OR price.date_unix_seconds >= $3)
        ORDER BY {column} {dir}, price.id {dir}
        LIMIT $5::bigint OFFSET $6::bigint
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query_as::<_, HistoryPriceRow>(&sql)
        .bind(filter.symbol)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.with_token)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Count price points for `symbol` within the optional date window, for
/// `with_total` responses.
pub async fn count_history_prices(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &HistoryPriceFilter<'_>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM token_daily_prices price
        JOIN tokens token ON token.id = price.token_id
        WHERE token.symbol = $1
          AND ($2::bigint IS NULL OR price.date_unix_seconds <= $2)
          AND ($3::bigint IS NULL OR price.date_unix_seconds >= $3)
        "#,
    )
    .bind(filter.symbol)
    .bind(filter.date_less)
    .bind(filter.date_greater)
    .fetch_one(executor)
    .await?;

    Ok(count)
}
