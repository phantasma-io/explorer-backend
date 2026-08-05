//! Events read model (the `/events` endpoint plus transaction-detail event
//! hydration).
//!
//! The list path pages with a seek cursor (the active sort key + id) and has two SQL
//! variants: an address-less query scoped to the configured chain, and an
//! address-scoped query that also matches the event target address. The db read
//! fns own the SQL and return rows; the API enriches/maps them with
//! `events_from_rows`/`event_from_row` and keeps the cursor trimming. See the
//! design note in [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable keys for the events list. The seek cursor keys on the selected sort
/// column plus `event.id`, so paging stays consistent for every order.
#[derive(Debug, Clone, Copy)]
pub enum EventOrderBy {
    Id,
    Date,
    BlockHeight,
}

impl EventOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "date" => Some(Self::Date),
            "block_height" => Some(Self::BlockHeight),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Id => "event.id",
            Self::Date => "event.timestamp_unix_seconds",
            Self::BlockHeight => "block.height",
        }
    }
}

/// Order + seek + limit for an events list page. `cursor_sort_value` is the
/// previous page's last value of the active sort column; `cursor_id` is its
/// `event.id` tie-break (both `None` on the first page).
#[derive(Debug, Clone, Copy)]
pub struct EventPage {
    pub order_by: EventOrderBy,
    pub direction: SortDirection,
    pub cursor_sort_value: Option<i64>,
    pub cursor_id: Option<i32>,
    pub limit: i64,
}

/// Filters shared by the global and address-scoped event lists. `q` is the raw
/// free-text value (the read fn derives the numeric/substring forms).
#[derive(Debug, Default, Clone, Copy)]
pub struct EventFilter<'a> {
    pub transaction_hash: Option<&'a str>,
    pub block_height: Option<i64>,
    /// The kind id the event must have, resolved from the requested name (see
    /// [`super::event_kind_id_by_name`]). `None` means no kind filter; a name that
    /// matches no kind never reaches here — the handler answers it without a query.
    pub event_kind_id: Option<i32>,
    pub event_source: Option<&'a str>,
    pub contract: Option<&'a str>,
    pub q: Option<&'a str>,
    pub event_id: Option<i32>,
    /// Kept for the public `with_nsfw` / `with_blacklisted` query params. The flags now
    /// live only on `nfts`, where they belong: `events` carried a copy that no row ever
    /// set in 76M events, and every event of a flagged NFT is reachable through its
    /// `nft_id`.
    pub show_nsfw: bool,
    pub show_blacklisted: bool,
    pub token_id: Option<&'a str>,
    pub block_hash: Option<&'a str>,
    pub date_less: Option<i64>,
    pub date_greater: Option<i64>,
    pub date_day: Option<i64>,
    /// `%value%` LIKE forms for the partial filters (C# `.Contains`).
    pub event_kind_partial_ids: Option<&'a [i32]>,
    pub nft_name_partial: Option<&'a str>,
    pub nft_description_partial: Option<&'a str>,
    pub address_partial: Option<&'a str>,
    /// Restrict address-scoped events to this chain (None = all chains for the id).
    pub chain_id: Option<i32>,
}

// Derive the substring (`q_like`) and numeric (`q_height`) forms of the
// free-text `q` for the event lists. Unlike the transaction lists, the substring
// form is always present when `q` is set (a numeric `q` matches both height and
// substring), matching the previous in-handler logic.
fn event_q_forms(q: Option<&str>) -> (Option<String>, Option<i64>) {
    let q_like = q.map(|value| format!("%{value}%"));
    let q_height = q.and_then(|value| value.parse::<i64>().ok());
    (q_like, q_height)
}

/// Columns every event-list answer projects, after `event.id` and the cursor value.
/// Shared by both list variants so they cannot drift apart.
const EVENT_LIST_PROJECTION: &str = r#"            event.event_index,
            'legacy'::text AS event_source,
            COALESCE(chain.name, $2::text) AS chain_name,
            event.timestamp_unix_seconds,
            block.hash AS block_hash,
            tx.hash AS transaction_hash,
            event_kind.name AS event_kind,
            COALESCE(event.event_name, event_kind.name) AS event_name,
            address.address,
            address.address_name,
            contract.hash AS contract_hash,
            contract.name AS contract_name,
            contract.symbol AS contract_symbol,
            contract.hash AS raw_contract,
            event.token_id,
            event.payload_json,
            event.raw_data,
            CASE WHEN nft.id IS NOT NULL THEN jsonb_build_object(
                'description', nft.description, 'name', nft.name,
                'imageURL', nft.image, 'videoURL', nft.video, 'infoURL', nft.info_url,
                'rom', nft.rom, 'ram', nft.ram,
                'mint_date', nft.mint_date_unix_seconds::text,
                'mint_number', nft.mint_number::text, 'metadata', nft.metadata
            ) END AS nft_metadata_json,
            CASE WHEN series.id IS NOT NULL THEN jsonb_build_object(
                'id', series.id, 'series_id', series.series_id, 'creator', series_creator.address,
                'created_unix_seconds', series.series_created_unix_seconds,
                'current_supply', series.current_supply, 'max_supply', series.max_supply,
                'mode_name', series_mode.mode_name, 'name', series.name,
                'description', series.description, 'image', series.image,
                'royalties', series.royalties::text, 'type', series.type,
                'attr_type_1', series.attr_type_1, 'attr_value_1', series.attr_value_1,
                'attr_type_2', series.attr_type_2, 'attr_value_2', series.attr_value_2,
                'attr_type_3', series.attr_type_3, 'attr_value_3', series.attr_value_3,
                'metadata', series.metadata
            ) END AS series_json
"#;

/// The presentation joins behind that projection.
const EVENT_LIST_JOINS: &str = r#"        FROM events event
        JOIN transactions tx ON tx.id = event.transaction_id
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = event.chain_id
        JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
        LEFT JOIN addresses address ON address.id = event.address_id
        LEFT JOIN contracts contract ON contract.id = event.contract_id
        LEFT JOIN nfts nft ON nft.id = event.nft_id
        LEFT JOIN series series ON series.id = nft.series_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        LEFT JOIN addresses series_creator ON series_creator.id = series.creator_address_id
"#;

/// Renders the exact event-kind predicate.
///
/// When a kind is requested the planner sees a bare `event.event_kind_id = $n`, which it
/// can drive `IX_Events_EventKindId_Timestamp_Id` with even in a GENERIC plan; the
/// `($n IS NULL OR …)` guard form cannot, because a plan built without the value has to
/// assume the filter might not apply. When no kind is requested the parameter is still
/// referenced — as a predicate that is simply true — so the placeholder numbering and the
/// bind count stay the same in both shapes.
fn event_kind_id_predicate(column: &str, placeholder: &str, kind_id: Option<i32>) -> String {
    match kind_id {
        Some(_) => format!("          AND {column} = {placeholder}\n"),
        None => format!("          AND ({placeholder}::integer IS NULL)\n"),
    }
}

/// Renders the SUBSTRING event-kind predicate with the ids inlined as literals.
///
/// The ids come from our own `event_kinds` table (i32, resolved by
/// [`super::event_kind_ids_by_name_like`]), so there is nothing to escape. They are literals
/// rather than a bound parameter on purpose, and this is the whole point of the fragment:
/// a bound `event_kind_id = ANY($n)` is invisible to the planner when PostgreSQL builds a
/// GENERIC plan for a cached statement, so it stops seeking the kind index
/// and walks the ordering backwards instead — which never finishes for a kind with fewer
/// rows than one page. Measured on the live database under `force_generic_plan`: the
/// bound array times out, an OR of bound scalars times out, and the inlined form answers
/// in 16 ms off an index-only scan. Dropping the `IS NULL` guard alone does not help.
///
/// `Some(&[])` means the requested name matched no kind, which must match no rows — not
/// every row.
fn event_kind_predicate(column: &str, ids: Option<&[i32]>) -> String {
    match ids {
        None => String::new(),
        Some([]) => "          AND FALSE\n".to_owned(),
        Some(ids) => {
            let list = ids.iter().map(i32::to_string).collect::<Vec<_>>().join(",");
            format!("          AND {column} = ANY('{{{list}}}'::integer[])\n")
        }
    }
}

/// Global event list scoped to one chain (no address filter), seek-paged.
/// Fetches `limit + 1` rows so the API can detect a following page.
pub async fn list_events_global(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: Option<i32>,
    chain_name: &str,
    filter: &EventFilter<'_>,
    page: &EventPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let (q_like, q_height) = event_q_forms(filter.q);
    let sql = format!(
        r#"
        SELECT
            event.id,
            {column}::bigint AS cursor_sort_value,
{projection}
{joins}
        WHERE ($1::integer IS NULL OR event.chain_id = $1)
{kind_predicate}{kind_partial_predicate}          AND ($3::text IS NULL OR tx.hash = $3)
          AND ($4::bigint IS NULL OR block.height = $4)
          AND ($5::text IS NULL OR $5 = 'legacy')
          AND ($6::text IS NULL OR contract.hash = $6 OR contract.name = $6 OR contract.symbol = $6)
          AND ($10::integer IS NULL OR event.id = $10)
          AND ($11::text IS NULL OR tx.hash ILIKE $11 OR block.hash ILIKE $11 OR block.height = $12 OR event_kind.name ILIKE $11 OR address.address ILIKE $11 OR address.address_name ILIKE $11 OR contract.hash ILIKE $11 OR contract.name ILIKE $11 OR contract.symbol ILIKE $11 OR event.token_id ILIKE $11)
          AND ($15::text IS NULL OR event.token_id = $15)
          AND ($16::text IS NULL OR block.hash = $16)
          AND ($17::bigint IS NULL OR event.timestamp_unix_seconds <= $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds >= $18)
          AND (
              $19::bigint IS NULL
              OR (event.timestamp_unix_seconds >= $19
                  AND event.timestamp_unix_seconds < $19 + 86400)
          )
          AND ($20::text IS NULL OR nft.name ILIKE $20)
          AND ($21::text IS NULL OR nft.description ILIKE $21)
          AND ($22::text IS NULL OR address.address ILIKE $22 OR address.address_name ILIKE $22)
          AND (
              $7::bigint IS NULL
              OR {column} {op} $7
              OR ({column} = $7 AND event.id {op} $8)
          )
        ORDER BY {column} {dir}, event.id {dir}
        LIMIT $9
        "#,
        column = page.order_by.column(),
        projection = EVENT_LIST_PROJECTION,
        joins = EVENT_LIST_JOINS,
        kind_predicate =
            event_kind_id_predicate("event.event_kind_id", "$23", filter.event_kind_id),
        kind_partial_predicate =
            event_kind_predicate("event.event_kind_id", filter.event_kind_partial_ids),
    );
    // Not cached as a prepared statement, so PostgreSQL plans it with the real parameter
    // values every time. A cached statement eventually gets a GENERIC plan — built
    // without knowing any parameter — and these queries carry ~25 optional filters as
    // `($n IS NULL OR <predicate>)` guards that a generic plan cannot reason about.
    // Measured under `force_generic_plan` on the live database: the same statement is
    // fast for a rare kind and takes 20-30 s for a high-volume one, purely from losing
    // the cursor and limit values at plan time. Re-planning costs about a millisecond
    // here. A pool-wide `plan_cache_mode = force_custom_plan` was tried in June 2026 and
    // reverted: ineffective on sqlx pools, and it made `events?address` take five minutes.
    let rows = sqlx::query(&sql)
        .persistent(false)
        .bind(chain_id)
        .bind(chain_name)
        .bind(filter.transaction_hash)
        .bind(filter.block_height)
        .bind(filter.event_source)
        .bind(filter.contract)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(page.limit + 1)
        .bind(filter.event_id)
        .bind(q_like.as_deref())
        .bind(q_height)
        .bind(false)
        .bind(false)
        .bind(filter.token_id)
        .bind(filter.block_hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.date_day)
        .bind(filter.nft_name_partial)
        .bind(filter.nft_description_partial)
        .bind(filter.address_partial)
        .bind(filter.event_kind_id)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Address-scoped event list: matches the event's address or target address by
/// id, across all chains. The caller resolves the address string to its id
/// (see [`address_id_by_address`]) so the match can use the `event.address_id`
/// and `event.target_address_id` indexes instead of scanning and filtering on a
/// joined address string.
pub async fn list_events_by_address(
    executor: impl sqlx::PgExecutor<'_>,
    chain_name: &str,
    address_id: i32,
    filter: &EventFilter<'_>,
    page: &EventPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let (q_like, q_height) = event_q_forms(filter.q);
    let sql = format!(
        r#"
        SELECT
            event.id,
            {column}::bigint AS cursor_sort_value,
            event.event_index,
            'legacy'::text AS event_source,
            chain.name AS chain_name,
            event.timestamp_unix_seconds,
            block.hash AS block_hash,
            tx.hash AS transaction_hash,
            event_kind.name AS event_kind,
            COALESCE(event.event_name, event_kind.name) AS event_name,
            address.address,
            address.address_name,
            contract.hash AS contract_hash,
            contract.name AS contract_name,
            contract.symbol AS contract_symbol,
            contract.hash AS raw_contract,
            event.token_id,
            event.payload_json,
            event.raw_data,
            CASE WHEN nft.id IS NOT NULL THEN jsonb_build_object(
                'description', nft.description, 'name', nft.name,
                'imageURL', nft.image, 'videoURL', nft.video, 'infoURL', nft.info_url,
                'rom', nft.rom, 'ram', nft.ram,
                'mint_date', nft.mint_date_unix_seconds::text,
                'mint_number', nft.mint_number::text, 'metadata', nft.metadata
            ) END AS nft_metadata_json,
            CASE WHEN series.id IS NOT NULL THEN jsonb_build_object(
                'id', series.id, 'series_id', series.series_id, 'creator', series_creator.address,
                'created_unix_seconds', series.series_created_unix_seconds,
                'current_supply', series.current_supply, 'max_supply', series.max_supply,
                'mode_name', series_mode.mode_name, 'name', series.name,
                'description', series.description, 'image', series.image,
                'royalties', series.royalties::text, 'type', series.type,
                'attr_type_1', series.attr_type_1, 'attr_value_1', series.attr_value_1,
                'attr_type_2', series.attr_type_2, 'attr_value_2', series.attr_value_2,
                'attr_type_3', series.attr_type_3, 'attr_value_3', series.attr_value_3,
                'metadata', series.metadata
            ) END AS series_json
        FROM events event
        JOIN transactions tx ON tx.id = event.transaction_id
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = event.chain_id
        JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
        LEFT JOIN addresses address ON address.id = event.address_id
        LEFT JOIN addresses target_address ON target_address.id = event.target_address_id
        LEFT JOIN contracts contract ON contract.id = event.contract_id
        LEFT JOIN nfts nft ON nft.id = event.nft_id
        LEFT JOIN series series ON series.id = nft.series_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        LEFT JOIN addresses series_creator ON series_creator.id = series.creator_address_id
        WHERE ($5::integer IS NOT NULL OR chain.name = $1)
{kind_predicate}{kind_partial_predicate}          AND ($2::text IS NULL OR tx.hash = $2)
          AND ($3::bigint IS NULL OR block.height = $3)
          AND ($4::text IS NULL OR $4 = 'legacy')
          AND ($5::integer IS NULL OR event.address_id = $5 OR event.target_address_id = $5)
          AND ($23::integer IS NULL OR event.chain_id = $23)
          AND ($6::text IS NULL OR contract.hash = $6 OR contract.name = $6 OR contract.symbol = $6)
          AND ($10::integer IS NULL OR event.id = $10)
          AND ($11::text IS NULL OR tx.hash ILIKE $11 OR block.hash ILIKE $11 OR block.height = $12 OR event_kind.name ILIKE $11 OR address.address ILIKE $11 OR target_address.address ILIKE $11 OR address.address_name ILIKE $11 OR contract.hash ILIKE $11 OR contract.name ILIKE $11 OR contract.symbol ILIKE $11 OR event.token_id ILIKE $11)
          AND ($15::text IS NULL OR event.token_id = $15)
          AND ($16::text IS NULL OR block.hash = $16)
          AND ($17::bigint IS NULL OR event.timestamp_unix_seconds <= $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds >= $18)
          AND (
              $19::bigint IS NULL
              OR (event.timestamp_unix_seconds >= $19
                  AND event.timestamp_unix_seconds < $19 + 86400)
          )
          AND ($20::text IS NULL OR nft.name ILIKE $20)
          AND ($21::text IS NULL OR nft.description ILIKE $21)
          AND ($22::text IS NULL OR address.address ILIKE $22 OR address.address_name ILIKE $22)
          AND (
              $7::bigint IS NULL
              OR {column} {op} $7
              OR ({column} = $7 AND event.id {op} $8)
          )
        ORDER BY {column} {dir}, event.id {dir}
        LIMIT $9
        "#,
        column = page.order_by.column(),
        kind_predicate =
            event_kind_id_predicate("event.event_kind_id", "$24", filter.event_kind_id),
        kind_partial_predicate =
            event_kind_predicate("event.event_kind_id", filter.event_kind_partial_ids),
    );
    // Not cached as a prepared statement, so PostgreSQL plans it with the real parameter
    // values every time. A cached statement eventually gets a GENERIC plan — built
    // without knowing any parameter — and these queries carry ~25 optional filters as
    // `($n IS NULL OR <predicate>)` guards that a generic plan cannot reason about.
    // Measured under `force_generic_plan` on the live database: the same statement is
    // fast for a rare kind and takes 20-30 s for a high-volume one, purely from losing
    // the cursor and limit values at plan time. Re-planning costs about a millisecond
    // here. A pool-wide `plan_cache_mode = force_custom_plan` was tried in June 2026 and
    // reverted: ineffective on sqlx pools, and it made `events?address` take five minutes.
    let rows = sqlx::query(&sql)
        .persistent(false)
        .bind(chain_name)
        .bind(filter.transaction_hash)
        .bind(filter.block_height)
        .bind(filter.event_source)
        .bind(address_id)
        .bind(filter.contract)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(page.limit + 1)
        .bind(filter.event_id)
        .bind(q_like.as_deref())
        .bind(q_height)
        .bind(false)
        .bind(false)
        .bind(filter.token_id)
        .bind(filter.block_hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.date_day)
        .bind(filter.nft_name_partial)
        .bind(filter.nft_description_partial)
        .bind(filter.address_partial)
        .bind(filter.chain_id)
        .bind(filter.event_kind_id)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Load every event belonging to the given transaction ids, ordered for
/// grouping by transaction then event index. The API groups/maps the rows.
pub async fn list_events_by_transaction_ids(
    executor: impl sqlx::PgExecutor<'_>,
    transaction_ids: &[i32],
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            event.transaction_id AS event_transaction_id,
            event.id,
            event.event_index,
            'legacy'::text AS event_source,
            chain.name AS chain_name,
            event.timestamp_unix_seconds,
            block.hash AS block_hash,
            tx.hash AS transaction_hash,
            event_kind.name AS event_kind,
            COALESCE(event.event_name, event_kind.name) AS event_name,
            address.address,
            address.address_name,
            contract.hash AS contract_hash,
            contract.name AS contract_name,
            contract.symbol AS contract_symbol,
            contract.hash AS raw_contract,
            event.token_id,
            event.payload_json,
            event.raw_data,
            CASE WHEN nft.id IS NOT NULL THEN jsonb_build_object(
                'description', nft.description, 'name', nft.name,
                'imageURL', nft.image, 'videoURL', nft.video, 'infoURL', nft.info_url,
                'rom', nft.rom, 'ram', nft.ram,
                'mint_date', nft.mint_date_unix_seconds::text,
                'mint_number', nft.mint_number::text, 'metadata', nft.metadata
            ) END AS nft_metadata_json,
            CASE WHEN series.id IS NOT NULL THEN jsonb_build_object(
                'id', series.id, 'series_id', series.series_id, 'creator', series_creator.address,
                'created_unix_seconds', series.series_created_unix_seconds,
                'current_supply', series.current_supply, 'max_supply', series.max_supply,
                'mode_name', series_mode.mode_name, 'name', series.name,
                'description', series.description, 'image', series.image,
                'royalties', series.royalties::text, 'type', series.type,
                'attr_type_1', series.attr_type_1, 'attr_value_1', series.attr_value_1,
                'attr_type_2', series.attr_type_2, 'attr_value_2', series.attr_value_2,
                'attr_type_3', series.attr_type_3, 'attr_value_3', series.attr_value_3,
                'metadata', series.metadata
            ) END AS series_json
        FROM events event
        JOIN transactions tx ON tx.id = event.transaction_id
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = event.chain_id
        JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
        LEFT JOIN addresses address ON address.id = event.address_id
        LEFT JOIN contracts contract ON contract.id = event.contract_id
        LEFT JOIN nfts nft ON nft.id = event.nft_id
        LEFT JOIN series series ON series.id = nft.series_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        LEFT JOIN addresses series_creator ON series_creator.id = series.creator_address_id
        WHERE event.transaction_id = ANY($1)
        ORDER BY event.transaction_id ASC, event.event_index ASC
        "#,
    )
    .bind(transaction_ids)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// USD prices for market-event fiat enrichment (C# parity): the stored per-day
/// price for each `(symbol, day-start)` pair on the chain, plus each symbol's
/// current spot price as the symbol-only fallback. The API picks daily-first,
/// spot-fallback per event.
pub async fn market_event_usd_prices(
    pool: &sqlx::PgPool,
    chain_id: i32,
    symbols: &[String],
    days: &[i64],
) -> Result<(Vec<(String, i64, f64)>, Vec<(String, i32, Option<f64>)>), DbError> {
    let daily = sqlx::query_as::<_, (String, i64, f64)>(
        r#"
        SELECT t.symbol, tdp.date_unix_seconds, tdp.price_usd::float8
        FROM token_daily_prices tdp
        JOIN tokens t ON t.id = tdp.token_id
        WHERE t.chain_id = $1 AND t.symbol = ANY($2) AND tdp.date_unix_seconds = ANY($3)
        "#,
    )
    .bind(chain_id)
    .bind(symbols)
    .bind(days)
    .fetch_all(pool)
    .await?;
    let token_meta = sqlx::query_as::<_, (String, i32, Option<f64>)>(
        "SELECT symbol, decimals, price_usd::float8 FROM tokens WHERE chain_id = $1 AND symbol = ANY($2)",
    )
    .bind(chain_id)
    .bind(symbols)
    .fetch_all(pool)
    .await?;
    Ok((daily, token_meta))
}

/// Load the token rows used to enrich event payloads, by uppercase symbol. The
/// API builds the per-symbol JSON map from these rows.
/// Reads one event's stored payload, for the endpoints that serve a part of it on
/// demand instead of shipping the whole thing with every event row.
pub async fn event_payload_by_id(
    executor: impl sqlx::PgExecutor<'_>,
    event_id: i32,
) -> Result<Option<Value>, DbError> {
    let payload = sqlx::query_scalar::<_, Option<Value>>(
        r#"
        SELECT event.payload_json
        FROM events event
        WHERE event.id = $1
        "#,
    )
    .bind(event_id)
    .fetch_optional(executor)
    .await?;

    Ok(payload.flatten())
}

pub async fn list_event_tokens_by_symbols(
    executor: impl sqlx::PgExecutor<'_>,
    symbols: &[String],
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            token.symbol,
            token.fungible,
            token.transferable,
            token.divisible,
            token.fuel,
            token.stakable,
            token.fiat,
            token.swappable,
            token.burnable,
            token.mintable,
            token.decimals,
            token.max_supply_raw::text AS max_supply_raw
        FROM tokens token
        WHERE token.symbol = ANY($1)
        "#,
    )
    .bind(symbols)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // Address-scoped event list round-trip: seed one transaction with two events for
    // a fresh address, then list them by the resolved address id — unfiltered (both
    // events) and narrowed to one kind (one event). Guards the bind list of
    // `list_events_by_address`: the statement references a placeholder per optional
    // filter even when unused, so a missing `.bind()` fails EVERY address-scoped
    // request, which is exactly how the kind filter shipped broken once. Runs inside
    // a rolled-back transaction.
    #[tokio::test]
    async fn address_scoped_events_list_and_kind_filter() -> Result<(), Box<dyn std::error::Error>>
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
        let suffix = Uuid::now_v7().simple().to_string();
        let actor = format!("PTESTEVADDR{suffix}");

        let block = upsert_block(
            &mut tx,
            &mut crate::ProjectionDimensionCache::new(),
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_300_000),
                hash: format!("TESTEVADDRBLOCK{suffix}"),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: None,
                timestamp_unix_seconds: 1_800_300_000,
                reward: None,
            },
        )
        .await?;
        let seeded_tx = upsert_transaction(
            &mut tx,
            TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTEVADDRTX{suffix}"),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee_raw: None,
                gas_price_raw: None,
                gas_limit_raw: None,
                sender: Some(actor.clone()),
                gas_payer: Some(actor.clone()),
                gas_target: Some(actor.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;
        let events = vec![
            EventUpsert {
                transaction_id: seeded_tx.id,
                chain_id,
                event_index: 1,
                event_kind: "TokenSend".to_owned(),
                event_name: None,
                address: Some(actor.clone()),
                target_address: None,
                contract: Some("SOUL".to_owned()),
                token_id: None,
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
            },
            EventUpsert {
                transaction_id: seeded_tx.id,
                chain_id,
                event_index: 2,
                event_kind: "TokenReceive".to_owned(),
                event_name: None,
                address: Some(actor.clone()),
                target_address: None,
                contract: Some("SOUL".to_owned()),
                token_id: None,
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
            },
        ];
        replace_events(&mut tx, seeded_tx.id, &events).await?;

        let address_id = sqlx::query_scalar::<_, i32>(
            "SELECT id FROM addresses WHERE chain_id = $1 AND address = $2",
        )
        .bind(chain_id)
        .bind(&actor)
        .fetch_one(&mut *tx)
        .await?;
        let send_kind_id =
            sqlx::query_scalar::<_, i32>("SELECT id FROM event_kinds WHERE name = 'TokenSend'")
                .fetch_one(&mut *tx)
                .await?;
        let page = EventPage {
            order_by: EventOrderBy::Id,
            direction: SortDirection::Desc,
            cursor_sort_value: None,
            cursor_id: None,
            limit: 10,
        };

        let unfiltered =
            list_events_by_address(&mut *tx, "main", address_id, &EventFilter::default(), &page)
                .await?;
        assert_eq!(unfiltered.len(), 2);

        let kind_filtered = list_events_by_address(
            &mut *tx,
            "main",
            address_id,
            &EventFilter {
                event_kind_id: Some(send_kind_id),
                ..EventFilter::default()
            },
            &page,
        )
        .await?;
        assert_eq!(kind_filtered.len(), 1);
        assert_eq!(kind_filtered[0].get::<String, _>("event_kind"), "TokenSend");

        Ok(())
    }
}
