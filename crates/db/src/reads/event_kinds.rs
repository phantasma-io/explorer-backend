//! Event-kinds list read models (`/eventKinds` and `/eventKindsWithEvents`).
//!
//! `/eventKinds` lists distinct event-kind names (grouped across chains) with
//! cursor pagination; `/eventKindsWithEvents` lists only those kinds that have
//! at least one projected event. Both optionally scope to a chain.
use crate::*;

/// One row of an event-kinds list: just the kind name.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EventKindNameRow {
    pub name: Option<String>,
}

/// Sortable keys for the `/eventKinds` list.
#[derive(Debug, Clone, Copy)]
pub enum EventKindOrderBy {
    Id,
    Name,
}

impl EventKindOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "name" => Some(Self::Name),
            _ => None,
        }
    }

    /// The SQL ordering expression. `sort_id` is the per-name `MIN(id)` alias
    /// from the grouped query; both options are fixed literals, never user input.
    fn column(self) -> &'static str {
        match self {
            Self::Id => "sort_id",
            Self::Name => "event_kind.name",
        }
    }
}

/// List distinct event-kind names (grouped), optionally scoped to a chain and
/// filtered by exact name, ordered by the chosen key then name. The caller
/// passes `limit + 1` to detect a following page.
pub async fn list_event_kinds(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: Option<i32>,
    event_kind: Option<&str>,
    order_by: EventKindOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<EventKindNameRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT event_kind.name, MIN(event_kind.id) AS sort_id
        FROM event_kinds event_kind
        WHERE ($1::integer IS NULL OR event_kind.chain_id = $1)
          AND ($2::text IS NULL OR event_kind.name = $2)
        GROUP BY event_kind.name
        ORDER BY {column} {dir}, event_kind.name {dir}
        LIMIT $3 OFFSET $4
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query_as::<_, EventKindNameRow>(&sql)
        .bind(chain_id)
        .bind(event_kind)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Count event-kind rows matching the chain/name filter, for `with_total`.
///
/// This counts raw `event_kinds` rows (not the grouped distinct names), matching
/// the existing endpoint behaviour.
pub async fn count_event_kinds(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: Option<i32>,
    event_kind: Option<&str>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM event_kinds event_kind
        WHERE ($1::integer IS NULL OR event_kind.chain_id = $1)
          AND ($2::text IS NULL OR event_kind.name = $2)
        "#,
    )
    .bind(chain_id)
    .bind(event_kind)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

/// List distinct event-kind names that have at least one projected event,
/// optionally scoped to a chain, ordered by name ascending.
pub async fn list_event_kinds_with_events(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: Option<i32>,
) -> Result<Vec<EventKindNameRow>, DbError> {
    let rows = sqlx::query_as::<_, EventKindNameRow>(
        r#"
        SELECT DISTINCT event_kind.name
        FROM event_kinds event_kind
        WHERE ($1::integer IS NULL OR event_kind.chain_id = $1)
          AND EXISTS (
              SELECT 1
              FROM events event
              WHERE event.event_kind_id = event_kind.id
          )
        ORDER BY event_kind.name ASC
        "#,
    )
    .bind(chain_id)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}
