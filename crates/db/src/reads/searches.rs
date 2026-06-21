//! Search read model (the `/searches` endpoint).
//!
//! Checks whether a search value exists as each kind of entity. The API layer
//! validates/normalizes the value and turns these booleans into the endpoint's
//! per-target result list.
use crate::*;

/// Whether the search value matches an entity of each kind. `contracts`/`tokens`
/// match case-insensitively (against the lowercased value).
#[derive(Debug, Clone, Copy)]
pub struct SearchExistence {
    pub addresses: bool,
    pub blocks: bool,
    pub chains: bool,
    pub contracts: bool,
    pub organizations: bool,
    pub platforms: bool,
    pub tokens: bool,
    pub transactions: bool,
}

/// Probe each entity kind for the search value. `value` is the trimmed input;
/// `value_lower` is its lowercased form (used for the case-insensitive targets).
pub async fn search_existence(
    pool: &PgPool,
    value: &str,
    value_lower: &str,
) -> Result<SearchExistence, DbError> {
    let addresses = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM addresses WHERE address = $1 OR user_name = $1 OR address_name = $1)",
    )
    .bind(value)
    .fetch_one(pool)
    .await?;
    // A block search value is either a hash or a height; they never overlap in
    // shape (a height parses as a non-negative integer, a hash does not). Probe
    // the matching column directly so each branch stays sargable: `hash = $1`
    // uses the unique hash index and `height = $1` the height index. The former
    // `hash = $1 OR height::text = $1` formulation forced a full table scan.
    let blocks = match value.parse::<i64>() {
        Ok(height) if height >= 0 => {
            sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM blocks WHERE height = $1)")
                .bind(height)
                .fetch_one(pool)
                .await?
        }
        _ => {
            sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM blocks WHERE hash = $1)")
                .bind(value)
                .fetch_one(pool)
                .await?
        }
    };
    let chains =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM chains WHERE name = $1)")
            .bind(value)
            .fetch_one(pool)
            .await?;
    let contracts = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM contracts WHERE lower(hash) = $1)",
    )
    .bind(value_lower)
    .fetch_one(pool)
    .await?;
    let organizations = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM organizations WHERE name = $1 OR organization_id = $1)",
    )
    .bind(value)
    .fetch_one(pool)
    .await?;
    let platforms =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM platforms WHERE name = $1)")
            .bind(value)
            .fetch_one(pool)
            .await?;
    let tokens = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM tokens WHERE lower(symbol) = $1)",
    )
    .bind(value_lower)
    .fetch_one(pool)
    .await?;
    let transactions =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM transactions WHERE hash = $1)")
            .bind(value)
            .fetch_one(pool)
            .await?;

    Ok(SearchExistence {
        addresses,
        blocks,
        chains,
        contracts,
        organizations,
        platforms,
        tokens,
        transactions,
    })
}
