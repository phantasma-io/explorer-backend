//! Organizations list read model (the `/organizations` endpoint).
//!
//! Lists organizations with their member count (`size`, a correlated count over
//! `organization_addresses`), supporting exact and partial filters plus a free
//! `q` substring across id/name/address. Cursor pagination (limit+1) is handled
//! by the API layer; this read just runs the bounded query.
use crate::*;

/// One row of the organizations list read model. `id` here is the textual
/// `organization_id`; the surrogate `org.id` is only used for ordering.
///
/// The `create_*` columns carry the organization's creation event and are NULL
/// unless the query ran with `with_creation_event` (or the organization predates
/// event coverage, like the seeded legacy DAOs).
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct OrganizationRow {
    pub organization_id: Option<String>,
    pub name: Option<String>,
    pub address: Option<String>,
    pub address_name: Option<String>,
    pub size: i64,
    pub create_event_id: Option<i32>,
    pub create_event_index: Option<i32>,
    pub create_chain: Option<String>,
    pub create_timestamp_unix_seconds: Option<i64>,
    pub create_block_hash: Option<String>,
    pub create_transaction_hash: Option<String>,
    pub create_event_kind: Option<String>,
    pub create_address: Option<String>,
    pub create_address_name: Option<String>,
    pub create_contract_name: Option<String>,
    pub create_contract_hash: Option<String>,
    pub create_contract_symbol: Option<String>,
    pub create_token_id: Option<String>,
    pub create_payload_json: Option<Value>,
    pub create_raw_data: Option<String>,
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
    pub with_creation_event: bool,
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
    // The worker never populates organizations.create_event_id, so the creation
    // event is resolved through the OrganizationCreate event itself: its
    // string_event payload carries the organization name, and names are unique
    // chain-wide (IX_Organizations_NAME). Organizations without such an event
    // (the restored legacy DAOs) simply get NULLs, matching the C# behavior of
    // omitting create_event when the link is absent.
    //
    // The kind id is resolved in an InitPlan and events are filtered by
    // event_kind_id, NOT by a name join: with the join the planner satisfied
    // `ORDER BY id LIMIT 1` by walking PK_Events across all 76M rows (30 s+),
    // while the resolved id drives IX_Events_EventKindId_ID (<1 ms) — the same
    // trap the contract-upgrade lookup hit (U8f).
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
            ) AS size,
            ce.create_event_id,
            ce.create_event_index,
            ce.create_chain,
            ce.create_timestamp_unix_seconds,
            ce.create_block_hash,
            ce.create_transaction_hash,
            ce.create_event_kind,
            ce.create_address,
            ce.create_address_name,
            ce.create_contract_name,
            ce.create_contract_hash,
            ce.create_contract_symbol,
            ce.create_token_id,
            ce.create_payload_json,
            ce.create_raw_data
        FROM organizations org
        LEFT JOIN LATERAL (
            SELECT
                create_event.id AS create_event_id,
                create_event.event_index AS create_event_index,
                create_chain.name AS create_chain,
                create_event.timestamp_unix_seconds AS create_timestamp_unix_seconds,
                create_block.hash AS create_block_hash,
                create_tx.hash AS create_transaction_hash,
                'OrganizationCreate'::text AS create_event_kind,
                create_address.address AS create_address,
                create_address.address_name AS create_address_name,
                create_contract.name AS create_contract_name,
                create_contract.hash AS create_contract_hash,
                create_contract.symbol AS create_contract_symbol,
                create_event.token_id AS create_token_id,
                create_event.payload_json AS create_payload_json,
                create_event.raw_data AS create_raw_data
            FROM events create_event
            JOIN chains create_chain ON create_chain.id = create_event.chain_id
            JOIN transactions create_tx ON create_tx.id = create_event.transaction_id
            JOIN blocks create_block ON create_block.id = create_tx.block_id
            LEFT JOIN addresses create_address ON create_address.id = create_event.address_id
            LEFT JOIN contracts create_contract ON create_contract.id = create_event.contract_id
            WHERE $8::boolean
              AND create_event.event_kind_id =
                  (SELECT create_kind.id FROM event_kinds create_kind WHERE create_kind.name = 'OrganizationCreate')
              AND create_event.payload_json #>> '{{string_event,string_value}}' = org.name
            ORDER BY create_event.id
            LIMIT 1
        ) ce ON TRUE
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
        .bind(filter.with_creation_event)
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
        // The base seed carries real organizations (stakers/masters), so assert on
        // the relative order of the two test rows rather than the whole listing.
        let names: Vec<_> = all
            .iter()
            .filter_map(|r| r.name.as_deref())
            .filter(|name| *name == "Alpha" || *name == "Beta")
            .collect();
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

        // Creation-event embed: seed the OrganizationCreate event for Alpha with its
        // full FK chain (block -> transaction -> event). The event carries the
        // organization name in its string_event payload — that is the only link the
        // worker writes, organizations.create_event_id stays NULL.
        sqlx::query("INSERT INTO transaction_states (id, name) VALUES (900001, 'TestHalt')")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO blocks (id, height, timestamp_unix_seconds, chain_id, hash, protocol, chain_address_id, validator_address_id) \
             VALUES (900001, 900001, 1700000100, 1, 'ORGTESTBLOCK', 1, 900001, 900001)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO transactions (id, hash, tx_index, block_id, timestamp_unix_seconds, expiration, state_id, sender_id, gas_payer_id, gas_target_id) \
             VALUES (900001, 'ORGTESTTX', 0, 900001, 1700000100, 0, 900001, 900001, 900001, 900001)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO contracts (id, name, hash, chain_id, last_updated_unix_seconds) \
             VALUES (900001, 'entry', 'entry-hash', 1, 0)",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO events (id, timestamp_unix_seconds, event_index, address_id, chain_id, contract_id, transaction_id, event_kind_id, payload_json) \
             SELECT 900001, 1700000100, 7, 900001, 1, 900001, 900001, ek.id, \
                    '{\"event_kind\": \"OrganizationCreate\", \"string_event\": {\"string_value\": \"Alpha\"}}'::jsonb \
             FROM event_kinds ek WHERE ek.name = 'OrganizationCreate'",
        )
        .execute(&mut *tx)
        .await?;

        let with_event = list_organizations(
            &mut *tx,
            &OrganizationFilter {
                organization_name: Some("Alpha"),
                with_creation_event: true,
                ..OrganizationFilter::default()
            },
            OrganizationOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(with_event.len(), 1);
        let alpha = &with_event[0];
        assert_eq!(
            alpha.create_event_id,
            Some(900001),
            "event resolved by name"
        );
        assert_eq!(
            alpha.create_event_kind.as_deref(),
            Some("OrganizationCreate")
        );
        assert_eq!(alpha.create_block_hash.as_deref(), Some("ORGTESTBLOCK"));
        assert_eq!(alpha.create_transaction_hash.as_deref(), Some("ORGTESTTX"));
        assert_eq!(alpha.create_timestamp_unix_seconds, Some(1_700_000_100));
        assert_eq!(alpha.create_contract_name.as_deref(), Some("entry"));

        // Mutation check: without the flag the same organization returns no
        // creation-event columns, and Beta (no event at all) stays NULL either way.
        let without_flag = list_organizations(
            &mut *tx,
            &OrganizationFilter {
                organization_name: Some("Alpha"),
                ..OrganizationFilter::default()
            },
            OrganizationOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(
            without_flag[0].create_event_id, None,
            "flag off keeps NULLs"
        );
        let beta_with_flag = list_organizations(
            &mut *tx,
            &OrganizationFilter {
                organization_name: Some("Beta"),
                with_creation_event: true,
                ..OrganizationFilter::default()
            },
            OrganizationOrderBy::Name,
            SortDirection::Asc,
            100,
            0,
        )
        .await?;
        assert_eq!(
            beta_with_flag[0].create_event_id, None,
            "no OrganizationCreate event -> no embed"
        );

        // `tx` dropped without commit -> rollback, leaving the tables untouched.
        Ok(())
    }
}
