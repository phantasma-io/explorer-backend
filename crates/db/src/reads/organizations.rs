//! Organizations list read model (the `/organizations` endpoint).
//!
//! Lists organizations with their member count (`size`, a correlated count over
//! `organization_addresses`), supporting exact and partial filters plus a free
//! `q` substring across id/name/address. Cursor pagination (limit+1) is handled
//! by the API layer; this read just runs the bounded query.
use crate::*;

/// One row of the organizations list read model. `id` here is the textual
/// `organization_id`; the surrogate `org.id` is only used for ordering.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrganizationRow {
    pub organization_id: Option<String>,
    pub name: Option<String>,
    pub address: Option<String>,
    pub address_name: Option<String>,
    pub size: i64,
}

/// Sortable columns for the organizations list.
#[derive(Debug, Clone, Copy)]
pub enum OrganizationOrderBy {
    Id,
    Name,
    OrganizationId,
}

impl OrganizationOrderBy {
    /// Parse the public `order_by` query param, defaulting to `name`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("name") {
            "id" => Some(Self::Id),
            "name" => Some(Self::Name),
            "organization_id" => Some(Self::OrganizationId),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "org.id",
            Self::Name => "org.name",
            Self::OrganizationId => "org.organization_id",
        }
    }
}

/// Filters for the organizations list. Partial/`q` values must already be
/// wrapped in the caller's `%...%` form (kept in the API layer, which owns the
/// public query semantics).
#[derive(Debug, Default)]
pub struct OrganizationFilter<'a> {
    pub organization_id: Option<&'a str>,
    pub organization_id_partial: Option<&'a str>,
    pub organization_name: Option<&'a str>,
    pub organization_name_partial: Option<&'a str>,
    pub q: Option<&'a str>,
}

/// List organizations matching the filter, ordered by the chosen column then
/// `org.id`, bounded by `limit`/`offset`. The caller passes `limit + 1` to
/// detect a following page.
pub async fn list_organizations(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &OrganizationFilter<'_>,
    order_by: OrganizationOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<OrganizationRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            org.id,
            org.organization_id,
            org.name,
            org.address,
            org.address_name,
            (
                SELECT COUNT(*)::bigint
                FROM organization_addresses org_address
                WHERE org_address.organization_id = org.id
            ) AS size
        FROM organizations org
        WHERE ($1::text IS NULL OR org.organization_id = $1)
          AND ($2::text IS NULL OR org.organization_id ILIKE $2)
          AND ($3::text IS NULL OR org.name = $3)
          AND ($4::text IS NULL OR org.name ILIKE $4)
          AND ($5::text IS NULL OR org.organization_id ILIKE $5 OR org.name ILIKE $5 OR org.address ILIKE $5 OR org.address_name ILIKE $5)
        ORDER BY {column} {dir}, org.id {dir}
        LIMIT $6 OFFSET $7
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query_as::<_, OrganizationRow>(&sql)
        .bind(filter.organization_id)
        .bind(filter.organization_id_partial)
        .bind(filter.organization_name)
        .bind(filter.organization_name_partial)
        .bind(filter.q)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Representative cursor-list test for the simple-resource cluster: insert
    // two organizations (one with a member address, one without) inside a
    // transaction and roll back. Verifies ordering by name, the exact-id filter,
    // the free `q` substring, and the correlated `size` count.
    #[tokio::test]
    async fn organizations_read_model_orders_filters_and_counts_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut tx = pool.begin().await?;

        // High ids avoid colliding with real rows; the rollback removes them.
        sqlx::query(
            "INSERT INTO organizations (id, organization_id, name, address, address_name) VALUES \
             (900001, 'org.alpha', 'Alpha', 'P2Kalpha', 'alpha.addr'), \
             (900002, 'org.beta', 'Beta', 'P2Kbeta', 'beta.addr')",
        )
        .execute(&mut *tx)
        .await?;
        // Two member addresses for Alpha only, so size differs between the rows.
        // organization_addresses.address_id FKs to addresses, so the address
        // rows must exist first (chain 1 is seeded; the rest use column defaults).
        sqlx::query(
            "INSERT INTO addresses (id, chain_id, name_last_updated_unix_seconds) \
             VALUES (900001, 1, 0), (900002, 1, 0)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO organization_addresses (id, organization_id, address_id) VALUES \
             (900001, 900001, 900001), (900002, 900001, 900002)",
        )
        .execute(&mut *tx)
        .await?;

        let all = list_organizations(
            &mut *tx,
            &OrganizationFilter::default(),
            OrganizationOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        let names: Vec<_> = all.iter().filter_map(|r| r.name.as_deref()).collect();
        assert_eq!(names, vec!["Alpha", "Beta"], "name ASC orders Alpha first");
        let alpha_size = all
            .iter()
            .find(|r| r.organization_id.as_deref() == Some("org.alpha"))
            .map(|r| r.size);
        assert_eq!(alpha_size, Some(2), "Alpha has two member addresses");
        let beta_size = all
            .iter()
            .find(|r| r.organization_id.as_deref() == Some("org.beta"))
            .map(|r| r.size);
        assert_eq!(beta_size, Some(0), "Beta has no member addresses");

        let by_id = list_organizations(
            &mut *tx,
            &OrganizationFilter {
                organization_id: Some("org.beta"),
                ..OrganizationFilter::default()
            },
            OrganizationOrderBy::Id,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(by_id.len(), 1, "exact organization_id filter returns one");
        assert_eq!(by_id[0].name.as_deref(), Some("Beta"));

        let by_q = list_organizations(
            &mut *tx,
            &OrganizationFilter {
                q: Some("%alpha%"),
                ..OrganizationFilter::default()
            },
            OrganizationOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(
            by_q.len(),
            1,
            "free q matches Alpha by id/address substring"
        );
        assert_eq!(by_q[0].organization_id.as_deref(), Some("org.alpha"));

        // `tx` dropped without commit -> rollback, leaving the tables untouched.
        Ok(())
    }
}
