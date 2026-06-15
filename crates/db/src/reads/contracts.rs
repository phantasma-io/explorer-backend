//! Contracts list read model (the `/contracts` endpoint).
//!
//! Wide projection feeding `ContractResponse` (optional script, methods JSON,
//! and an embedded token object). The API maps the rows with `contract_from_row`.
//! See the design note in [`super::tokens`] for why these reads return rows.
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the contracts list.
#[derive(Debug, Clone, Copy)]
pub enum ContractOrderBy {
    Id,
    Symbol,
    Name,
}

impl ContractOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "symbol" => Some(Self::Symbol),
            "name" => Some(Self::Name),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "contract.id",
            Self::Symbol => "contract.symbol",
            Self::Name => "contract.name",
        }
    }
}

/// Filter + embed flags for a contracts query. `q` is already in `%...%` form.
#[derive(Debug, Clone, Copy)]
pub struct ContractFilter<'a> {
    pub chain_id: i32,
    pub symbol: Option<&'a str>,
    pub hash: Option<&'a str>,
    pub q: Option<&'a str>,
    pub with_script: bool,
    pub with_methods: bool,
    pub with_token: bool,
}

/// List contracts for a chain matching the filter, ordered by the chosen column
/// then `contract.id`. The caller passes `limit + 1` to detect a following page.
pub async fn list_contracts(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &ContractFilter<'_>,
    order_by: ContractOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            contract.id,
            contract.name,
            contract.hash,
            contract.symbol,
            CASE WHEN $5::boolean THEN contract.script_raw ELSE NULL END AS script_raw,
            address.address,
            address.address_name,
            CASE WHEN $6::boolean THEN method.methods ELSE NULL END AS methods_json,
            CASE WHEN $7::boolean AND token.id IS NOT NULL THEN jsonb_build_object(
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
                'script_raw', NULL,
                'price', NULL,
                'token_logos', NULL
            ) ELSE NULL END AS token_json
        FROM contracts contract
        LEFT JOIN addresses address ON address.id = contract.address_id
        LEFT JOIN contract_methods method ON method.id = contract.contract_method_id
        LEFT JOIN tokens token ON token.id = contract.token_id
        WHERE contract.chain_id = $1
          AND ($2::text IS NULL OR contract.symbol = $2)
          AND ($3::text IS NULL OR lower(contract.hash) = lower($3))
          AND ($4::text IS NULL OR contract.symbol ILIKE $4 OR contract.name ILIKE $4 OR contract.hash ILIKE $4)
        ORDER BY {column} {dir}, contract.id {dir}
        LIMIT $8 OFFSET $9
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.symbol)
        .bind(filter.hash)
        .bind(filter.q)
        .bind(filter.with_script)
        .bind(filter.with_methods)
        .bind(filter.with_token)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}
