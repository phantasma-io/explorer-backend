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
                -- Same shape the /tokens projection serves: the formatted columns are gone
                -- (202608040001), so the decimal string is derived, and the raw numerics are
                -- cast back to text — jsonb_build_object would otherwise emit them as JSON
                -- numbers where this embed has always carried strings.
                'current_supply', trim_scale(token.current_supply_raw * power(10::numeric, -token.decimals))::text,
                'current_supply_raw', token.current_supply_raw::text,
                'max_supply', trim_scale(token.max_supply_raw * power(10::numeric, -token.decimals))::text,
                'max_supply_raw', token.max_supply_raw::text,
                'burned_supply', trim_scale(token.burned_supply_raw * power(10::numeric, -token.decimals))::text,
                'burned_supply_raw', token.burned_supply_raw::text,
                'script_raw', NULL,
                'price', NULL,
                'token_logos', NULL
            ) ELSE NULL END AS token_json
        FROM contracts contract
        LEFT JOIN addresses address ON address.id = contract.address_id
        LEFT JOIN contract_methods method ON method.id = contract.contract_method_id
        -- `contracts.token_id` was the unmaintained half of a circular 1:1 and is gone
        -- (202608030004); `tokens.contract_id` is the side the write path maintains.
        LEFT JOIN tokens token ON token.contract_id = contract.id
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

#[cfg(test)]
mod tests {
    use super::*;

    // The contracts list must PLAN against the current schema, with every embed on.
    //
    // This projection is the one place that still read `contracts.token_id` and the
    // formatted `tokens.current_supply/max_supply/burned_supply` after the 2026-08 batch
    // dropped all four, which made every `/contracts` request a 500 — a whole endpoint
    // dead with nothing failing in CI. PostgreSQL rejects an unknown column while parsing,
    // so the query does not need matching rows to catch it: executing it at all is the
    // assertion. Rows are read too, so a projection that parses but cannot decode also
    // fails here. Runs inside a rolled-back transaction.
    #[tokio::test]
    async fn contracts_list_projection_matches_the_schema() -> Result<(), Box<dyn std::error::Error>>
    {
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
        let filter = ContractFilter {
            chain_id,
            symbol: None,
            hash: None,
            q: None,
            with_script: true,
            with_methods: true,
            with_token: true,
        };

        for order_by in [
            ContractOrderBy::Id,
            ContractOrderBy::Name,
            ContractOrderBy::Symbol,
        ] {
            let rows =
                list_contracts(&mut *tx, &filter, order_by, SortDirection::Desc, 5, 0).await?;
            for row in &rows {
                // Touch the embed so a column that parses but no longer decodes fails here.
                let _: Option<serde_json::Value> = row.get("token_json");
            }
        }

        Ok(())
    }
}
