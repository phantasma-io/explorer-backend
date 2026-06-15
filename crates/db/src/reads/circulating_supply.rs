//! Circulating-supply read model (the `/circulatingSupply` endpoint).
//!
//! Returns the raw `current_supply` string of the SOUL token; the API layer
//! parses it to a number and maps the absent-token case to 404. A missing SOUL
//! row surfaces as a DB error (preserving the endpoint's existing behaviour),
//! while a present row with a NULL supply returns `None`.
use crate::*;

/// Fetch the SOUL token's `current_supply` (nullable). Errors if no SOUL row
/// exists; returns `Ok(None)` if the row exists but the supply is NULL.
pub async fn circulating_soul_supply(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<Option<String>, DbError> {
    let supply = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT current_supply
        FROM tokens
        WHERE symbol = 'SOUL'
        ORDER BY id
        LIMIT 1
        "#,
    )
    .fetch_one(executor)
    .await?;

    Ok(supply)
}
