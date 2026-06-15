//! Platforms list read model (the `/platforms` endpoint).
//!
//! Lists non-hidden platforms with optional embedded externals/interops/tokens
//! JSON and an optional creation-event object. The API maps the rows with
//! `platform_from_row`. See the design note in [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable columns for the platforms list.
#[derive(Debug, Clone, Copy)]
pub enum PlatformOrderBy {
    Id,
    Name,
}

impl PlatformOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "name" => Some(Self::Name),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "platform.id",
            Self::Name => "platform.name",
        }
    }
}

/// Filter + embed flags for a platforms query.
#[derive(Debug, Clone, Copy)]
pub struct PlatformFilter<'a> {
    pub name: Option<&'a str>,
    pub with_external: bool,
    pub with_interops: bool,
    pub with_token: bool,
    pub with_creation_event: bool,
}

/// List non-hidden platforms matching the optional name filter, ordered by the
/// chosen column then `platform.id`, paged by `limit`/`offset`.
pub async fn list_platforms(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &PlatformFilter<'_>,
    order_by: PlatformOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    let sql = format!(
        r#"
        SELECT
            platform.id,
            platform.name,
            platform.chain,
            platform.fuel,
            CASE WHEN $2::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'hash', external.hash,
                    'token', jsonb_build_object('symbol', token.symbol)
                ) ORDER BY external.id), '[]'::jsonb)
                FROM externals external
                LEFT JOIN tokens token ON token.id = external.token_id
                WHERE external.platform_id = platform.id
            ) ELSE NULL END AS externals_json,
            CASE WHEN $3::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'external_address', interop.external,
                    'local_address', jsonb_build_object(
                        'address', address.address,
                        'address_name', address.address_name
                    )
                ) ORDER BY interop.id), '[]'::jsonb)
                FROM platform_interops interop
                LEFT JOIN addresses address ON address.id = interop.local_address_id
                WHERE interop.platform_id = platform.id
            ) ELSE NULL END AS platform_interops_json,
            CASE WHEN $4::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'name', platform_token.name
                ) ORDER BY platform_token.id), '[]'::jsonb)
                FROM platform_tokens platform_token
                WHERE platform_token.platform_id = platform.id
            ) ELSE NULL END AS platform_tokens_json,
            create_event.id AS create_event_id,
            create_event.event_index AS create_event_index,
            create_chain.name AS create_chain,
            create_event.timestamp_unix_seconds AS create_timestamp_unix_seconds,
            create_block.hash AS create_block_hash,
            create_tx.hash AS create_transaction_hash,
            create_kind.name AS create_event_kind,
            create_address.address AS create_address,
            create_address.address_name AS create_address_name,
            create_contract.name AS create_contract_name,
            create_contract.hash AS create_contract_hash,
            create_contract.symbol AS create_contract_symbol,
            create_event.token_id AS create_token_id,
            create_event.payload_json AS create_payload_json,
            create_event.raw_data AS create_raw_data
        FROM platforms platform
        LEFT JOIN events create_event ON $5::boolean AND create_event.id = platform.create_event_id
        LEFT JOIN chains create_chain ON create_chain.id = create_event.chain_id
        LEFT JOIN transactions create_tx ON create_tx.id = create_event.transaction_id
        LEFT JOIN blocks create_block ON create_block.id = create_tx.block_id
        LEFT JOIN event_kinds create_kind ON create_kind.id = create_event.event_kind_id
        LEFT JOIN addresses create_address ON create_address.id = create_event.address_id
        LEFT JOIN contracts create_contract ON create_contract.id = create_event.contract_id
        WHERE platform.hidden = false
          AND ($1::text IS NULL OR platform.name = $1)
        ORDER BY {column} {dir}, platform.id {dir}
        LIMIT $6 OFFSET $7
        "#,
        column = order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.name)
        .bind(filter.with_external)
        .bind(filter.with_interops)
        .bind(filter.with_token)
        .bind(filter.with_creation_event)
        .bind(limit)
        .bind(offset)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Count non-hidden platforms matching the optional name filter, for
/// `with_total` responses.
pub async fn count_platforms(
    executor: impl sqlx::PgExecutor<'_>,
    name: Option<&str>,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM platforms
        WHERE hidden = false
          AND ($1::text IS NULL OR name = $1)
        "#,
    )
    .bind(name)
    .fetch_one(executor)
    .await?;

    Ok(count)
}
