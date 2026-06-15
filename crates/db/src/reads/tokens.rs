//! Tokens list read model (the `/tokens` endpoint).
//!
//! A wide projection that feeds the rich `TokenResponse` tree (per-currency
//! prices and the logos JSON). The db layer owns the SQL and returns the raw
//! rows; the API maps them with `token_from_row`.
//!
//! Design note (applies to the wide read models): these endpoints select 20-30
//! columns straight into a nested DTO, several of them read only when a `with_*`
//! flag is set. Returning the rows here — rather than a typed record that would
//! merely restate the DTO field-for-field and force eager decoding of every
//! optional column — keeps the read path simple and behaviour identical to the
//! previous in-handler query. Narrow, stable lists (chains, oracles, ...) use
//! typed `*Row` records instead, where the record is small and worth having.
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the tokens list.
#[derive(Debug, Clone, Copy)]
pub enum TokenOrderBy {
    Id,
    Symbol,
}

impl TokenOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "symbol" => Some(Self::Symbol),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "token.id",
            Self::Symbol => "token.symbol",
        }
    }
}

/// Filter + scope for a tokens query. `q` must already be in the caller's
/// `%...%` form; `with_logo` toggles the embedded logos JSON in the projection.
#[derive(Debug, Clone, Copy)]
pub struct TokenFilter<'a> {
    pub chain_id: i32,
    pub symbol: Option<&'a str>,
    pub q: Option<&'a str>,
    pub with_logo: bool,
}

/// List tokens for a chain, filtered by exact symbol and/or a free `q` substring
/// over symbol/name, ordered by the chosen column then `token.id`. The caller
/// passes `limit + 1` to detect a following page.
pub async fn list_tokens(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &TokenFilter<'_>,
    order_by: TokenOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            token.id,
            token.symbol,
            token.name,
            token.fungible,
            token.transferable,
            token.finite,
            token.divisible,
            token.fuel,
            token.stakable,
            token.fiat,
            token.swappable,
            token.burnable,
            token.mintable,
            token.decimals,
            token.current_supply,
            token.current_supply_raw,
            token.max_supply,
            token.max_supply_raw,
            token.burned_supply,
            token.burned_supply_raw,
            token.script_raw,
            token.price_usd::double precision AS price_usd,
            token.price_eur::double precision AS price_eur,
            token.price_gbp::double precision AS price_gbp,
            token.price_jpy::double precision AS price_jpy,
            token.price_cad::double precision AS price_cad,
            token.price_aud::double precision AS price_aud,
            token.price_cny::double precision AS price_cny,
            token.price_rub::double precision AS price_rub,
            CASE WHEN $4::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'type', logo_type.name,
                    'url', logo.url
                ) ORDER BY logo.id), '[]'::jsonb)
                FROM token_logos logo
                JOIN token_logo_types logo_type ON logo_type.id = logo.token_logo_type_id
                WHERE logo.token_id = token.id
            ) ELSE NULL END AS token_logos_json
        FROM tokens token
        WHERE token.chain_id = $1
          AND ($2::text IS NULL OR token.symbol = $2)
          AND ($3::text IS NULL OR token.symbol ILIKE $3 OR token.name ILIKE $3)
        ORDER BY {column} {dir}, token.id {dir}
        LIMIT $5 OFFSET $6
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.symbol)
        .bind(filter.q)
        .bind(filter.with_logo)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}
