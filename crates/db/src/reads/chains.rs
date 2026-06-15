//! Chains list read model (the `/chains` endpoint).
//!
//! `use crate::*` pulls the crate-root data-access primitives (`PgPool`,
//! `DbError`, the sqlx imports) into this submodule, the same way the flat
//! crate-root modules rely on `use super::*`.
use crate::*;

/// One row of the chains list read model.
#[derive(Debug, Clone)]
pub struct ChainRow {
    pub name: String,
    pub current_height: i64,
}

/// List chains (optionally filtered by exact name), ordered by id, paged by
/// `limit`/`offset`. A `None` filter returns every chain.
pub async fn list_chains(
    executor: impl sqlx::PgExecutor<'_>,
    name_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<ChainRow>, DbError> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        r#"
        SELECT name, current_height
        FROM chains
        WHERE ($1::text IS NULL OR name = $1)
        ORDER BY id ASC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(name_filter)
    .bind(limit)
    .bind(offset)
    .fetch_all(executor)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(name, current_height)| ChainRow {
            name,
            current_height,
        })
        .collect())
}

/// Count chains matching the optional name filter, for `with_total` responses.
pub async fn count_chains(
    executor: impl sqlx::PgExecutor<'_>,
    name_filter: Option<&str>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM chains
        WHERE ($1::text IS NULL OR name = $1)
        "#,
    )
    .bind(name_filter)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

/// Resolve a chain name to its id(s), capped at 2 so the API can detect an
/// ambiguous match. Returns an empty vec when the chain is unknown; the API
/// maps 0 -> 404, exactly 1 -> ok, more than 1 -> 500.
pub async fn chain_ids_by_name(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
) -> Result<Vec<i32>, DbError> {
    let ids = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM chains
        WHERE name = $1
        ORDER BY id
        LIMIT 2
        "#,
    )
    .bind(chain)
    .fetch_all(executor)
    .await?;

    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Read-only test against the seeded chains (main, main-generation-1): the
    // list, the name filter, and the count must agree. No mutation, so no
    // rollback is needed; self-skips when no test database is configured.
    #[tokio::test]
    async fn chains_read_model_lists_filters_and_counts() -> Result<(), Box<dyn std::error::Error>>
    {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;

        let all = list_chains(&pool, None, 100, 0).await?;
        assert!(
            all.iter().any(|chain| chain.name == "main"),
            "seeded chains must include main"
        );
        assert_eq!(
            count_chains(&pool, None).await? as usize,
            all.len(),
            "count must match the unfiltered list length"
        );

        let filtered = list_chains(&pool, Some("main"), 100, 0).await?;
        assert_eq!(filtered.len(), 1, "exact name filter returns one chain");
        assert_eq!(filtered[0].name, "main");
        assert_eq!(count_chains(&pool, Some("main")).await?, 1);

        Ok(())
    }
}
