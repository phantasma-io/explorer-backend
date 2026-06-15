//! Validator-kinds list read model (the `/validatorKinds` endpoint).
//!
//! `address_validator_kinds` is a small lookup table. The list is filtered by an
//! optional exact name and ordered by a caller-chosen column. The set of
//! sortable columns lives here, next to the SQL (not in the API layer), and is
//! expressed as a closed enum so the column name can never be caller-controlled.
use crate::*;

/// One row of the validator-kinds list read model. `name` is nullable in the
/// schema, so it is surfaced as `Option`.
#[derive(Debug, Clone)]
pub struct ValidatorKindRow {
    pub name: Option<String>,
}

/// Sortable columns for the validator-kinds list.
#[derive(Debug, Clone, Copy)]
pub enum ValidatorKindOrderBy {
    Id,
    Name,
}

impl ValidatorKindOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`. Returns
    /// `None` for unrecognised values so the API layer can answer 400.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "name" => Some(Self::Name),
            _ => None,
        }
    }

    /// The SQL column for this key. Always a fixed literal, never user input.
    fn column(self) -> &'static str {
        match self {
            Self::Id => "id",
            Self::Name => "name",
        }
    }
}

/// List validator kinds (optionally filtered by exact name), ordered by the
/// chosen column then `id` as a stable tiebreak, paged by `limit`/`offset`.
pub async fn list_validator_kinds(
    executor: impl sqlx::PgExecutor<'_>,
    name_filter: Option<&str>,
    order_by: ValidatorKindOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<ValidatorKindRow>, DbError> {
    // The order column and direction come from closed enums, so interpolating
    // them is injection-safe; the user-controlled name/limit/offset stay bound.
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT name
        FROM address_validator_kinds
        WHERE ($1::text IS NULL OR name = $1)
        ORDER BY {column} {dir}, id {dir}
        LIMIT $2 OFFSET $3
        "#,
        column = order_by.column(),
    );
    let names = sqlx::query_scalar::<_, Option<String>>(&sql)
        .bind(name_filter)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(names
        .into_iter()
        .map(|name| ValidatorKindRow { name })
        .collect())
}

/// Count validator kinds matching the optional name filter, for `with_total`.
pub async fn count_validator_kinds(
    executor: impl sqlx::PgExecutor<'_>,
    name_filter: Option<&str>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM address_validator_kinds
        WHERE ($1::text IS NULL OR name = $1)
        "#,
    )
    .bind(name_filter)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    // The seed has no validator kinds, so the test inserts its own fixtures
    // inside a transaction and rolls back (drop without commit). This isolates
    // it from any other test sharing the table and leaves the harness clean.
    // It verifies ordering by name (ASC/DESC), the exact-name filter, and the
    // count for both the unfiltered and filtered cases.
    #[tokio::test]
    async fn validator_kinds_read_model_orders_filters_and_counts()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut tx = pool.begin().await?;

        // High ids cannot collide with real rows; the rollback removes them.
        sqlx::query(
            "INSERT INTO address_validator_kinds (id, name) \
             VALUES (900001, 'Primary'), (900002, 'Secondary')",
        )
        .execute(&mut *tx)
        .await?;

        let asc = list_validator_kinds(
            &mut *tx,
            None,
            ValidatorKindOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(
            asc.iter()
                .filter_map(|r| r.name.as_deref())
                .collect::<Vec<_>>(),
            vec!["Primary", "Secondary"],
            "name ASC orders Primary before Secondary"
        );

        let desc = list_validator_kinds(
            &mut *tx,
            None,
            ValidatorKindOrderBy::Name,
            SortDirection::Desc,
            100,
            0,
        )
        .await?;
        assert_eq!(
            desc.first().and_then(|r| r.name.as_deref()),
            Some("Secondary"),
            "name DESC puts Secondary first"
        );

        let filtered = list_validator_kinds(
            &mut *tx,
            Some("Primary"),
            ValidatorKindOrderBy::Id,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(filtered.len(), 1, "exact name filter returns one row");
        assert_eq!(filtered[0].name.as_deref(), Some("Primary"));

        assert_eq!(
            count_validator_kinds(&mut *tx, None).await?,
            2,
            "count of the two inserted rows"
        );
        assert_eq!(
            count_validator_kinds(&mut *tx, Some("Primary")).await?,
            1,
            "filtered count matches the single named row"
        );

        // `tx` is dropped here without commit -> rollback, table left untouched.
        Ok(())
    }
}
