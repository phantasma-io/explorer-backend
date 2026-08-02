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
    /// Kind ids the event must have, already resolved from the requested name (see
    /// [`super::event_kind_ids_by_name`]). An empty slice matches nothing, which is the
    /// honest answer for a name no chain defines.
    pub event_kind_ids: Option<&'a [i32]>,
    pub event_source: Option<&'a str>,
    pub contract: Option<&'a str>,
    pub q: Option<&'a str>,
    pub event_id: Option<i32>,
    /// Show NSFW / blacklisted events. Default false → excluded (C# parity: the
    /// `with_nsfw`/`with_blacklisted` toggles default to 0 = hide).
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
/// Shared so the general list and the kind-seek path below cannot drift apart.
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

/// The presentation joins behind that projection. `events` is the driving table in the
/// general list; the kind-seek path joins it back by id after the rows are chosen.
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

/// Renders the event-kind predicate with the ids INLINED as literals.
///
/// The ids come from our own `event_kinds` table (i32, resolved by
/// [`super::event_kind_ids_by_name`]), so there is nothing to escape. They are literals
/// rather than a bound parameter on purpose, and this is the whole point of the fragment:
/// a bound `event_kind_id = ANY($n)` is invisible to the planner when PostgreSQL builds a
/// GENERIC plan for a cached statement, so it stops seeking `IX_Events_EventKindId_ID`
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
          AND ($13::bool OR NOT event.nsfw)
          AND ($14::bool OR NOT event.blacklisted)
          AND ($15::text IS NULL OR event.token_id = $15)
          AND ($16::text IS NULL OR block.hash = $16)
          AND ($17::bigint IS NULL OR event.timestamp_unix_seconds <= $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds >= $18)
          AND ($19::bigint IS NULL OR event.date_unix_seconds = $19)
          AND ($20::text IS NULL OR nft.name ILIKE $20)
          AND ($21::text IS NULL OR nft.description ILIKE $21)
          AND ($22::text IS NULL OR address.address ILIKE $22 OR address.address_name ILIKE $22 OR address.user_name ILIKE $22)
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
        kind_predicate = event_kind_predicate("event.event_kind_id", filter.event_kind_ids),
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
        .bind(filter.show_nsfw)
        .bind(filter.show_blacklisted)
        .bind(filter.token_id)
        .bind(filter.block_hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.date_day)
        .bind(filter.nft_name_partial)
        .bind(filter.nft_description_partial)
        .bind(filter.address_partial)
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
          AND ($13::bool OR NOT event.nsfw)
          AND ($14::bool OR NOT event.blacklisted)
          AND ($15::text IS NULL OR event.token_id = $15)
          AND ($16::text IS NULL OR block.hash = $16)
          AND ($17::bigint IS NULL OR event.timestamp_unix_seconds <= $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds >= $18)
          AND ($19::bigint IS NULL OR event.date_unix_seconds = $19)
          AND ($20::text IS NULL OR nft.name ILIKE $20)
          AND ($21::text IS NULL OR nft.description ILIKE $21)
          AND ($22::text IS NULL OR address.address ILIKE $22 OR address.address_name ILIKE $22 OR address.user_name ILIKE $22)
          AND (
              $7::bigint IS NULL
              OR {column} {op} $7
              OR ({column} = $7 AND event.id {op} $8)
          )
        ORDER BY {column} {dir}, event.id {dir}
        LIMIT $9
        "#,
        column = page.order_by.column(),
        kind_predicate = event_kind_predicate("event.event_kind_id", filter.event_kind_ids),
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
        .bind(filter.show_nsfw)
        .bind(filter.show_blacklisted)
        .bind(filter.token_id)
        .bind(filter.block_hash)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.date_day)
        .bind(filter.nft_name_partial)
        .bind(filter.nft_description_partial)
        .bind(filter.address_partial)
        .bind(filter.chain_id)
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
            token.max_supply_raw
        FROM tokens token
        WHERE token.symbol = ANY($1)
        "#,
    )
    .bind(symbols)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Whether the kind-seek path below can answer this filter.
///
/// It pre-selects rows from `events` alone, so every active filter must live on that
/// table; a filter that needs one of the presentation joins (a transaction hash, a
/// contract, `q`, an NFT name) would be applied only AFTER each branch took its page,
/// and the answer could come back short. Ordering by block height is excluded for the
/// same reason: the sort key itself comes from a join.
impl EventFilter<'_> {
    pub fn kind_seek_applies(&self, order_by: EventOrderBy) -> bool {
        let kind_ids_are_selective = matches!(self.event_kind_ids, Some(ids) if !ids.is_empty());
        let joined_filters_absent = self.transaction_hash.is_none()
            && self.block_height.is_none()
            && self.block_hash.is_none()
            && self.contract.is_none()
            && self.q.is_none()
            && self.nft_name_partial.is_none()
            && self.nft_description_partial.is_none()
            && self.address_partial.is_none()
            && self.event_kind_partial_ids.is_none();

        kind_ids_are_selective
            && joined_filters_absent
            && !matches!(order_by, EventOrderBy::BlockHeight)
    }
}

/// Event list for one or more kind ids, seeking each kind separately.
///
/// The general list walks the `event.id` (or timestamp) ordering and keeps rows whose
/// kind matches. That works only while matches are near where the walk starts, and on
/// this chain they are not: `event_kind_id` is strongly clustered because gen1, gen2 and
/// gen3 wrote different kinds in different id ranges. Measured on the live database,
/// `CrownRewards` — 6.7M rows, none of them in the newest 13M ids — takes 7 s descending
/// while its ascending page answers in 17 ms, and a kind with a hundred rows can die in
/// the ascending direction for the mirror-image reason. The planner is not wrong about
/// how many rows match, it is wrong about where they are, so no amount of statistics or
/// inlining fixes it.
///
/// This takes one page per kind id through `IX_Events_EventKindId_ID` /
/// `IX_Events_EventKind_Chain_Timestamp_Id`, which are ordered by exactly what each
/// branch asks for, then merges and re-limits. A row in the true page must be in its own
/// kind's page, so the merged result is identical to the general query's; the work is
/// bounded by `limit + 1` per kind whatever the direction and wherever the rows live.
pub async fn list_events_by_kind_seek(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: Option<i32>,
    filter: &EventFilter<'_>,
    page: &EventPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let sort_column = match page.order_by {
        EventOrderBy::Id => "candidate.id",
        EventOrderBy::Date => "candidate.timestamp_unix_seconds",
        // Excluded by `kind_seek_applies`; the sort key would come from a join.
        EventOrderBy::BlockHeight => "candidate.id",
    };
    let branches = filter
        .event_kind_ids
        .unwrap_or_default()
        .iter()
        .map(|kind_id| {
            format!(
                r#"
        (
            SELECT candidate.id, {sort_column}::bigint AS sort_value
            FROM events candidate
            WHERE candidate.event_kind_id = {kind_id}
              AND ($1::integer IS NULL OR candidate.chain_id = $1)
              AND ($4::integer IS NULL OR candidate.id = $4)
              AND ($5::bool OR NOT candidate.nsfw)
              AND ($6::bool OR NOT candidate.blacklisted)
              AND ($7::text IS NULL OR candidate.token_id = $7)
              AND ($8::bigint IS NULL OR candidate.timestamp_unix_seconds <= $8)
              AND ($9::bigint IS NULL OR candidate.timestamp_unix_seconds >= $9)
              AND ($10::bigint IS NULL OR candidate.date_unix_seconds = $10)
              AND (
                  $2::bigint IS NULL
                  OR {sort_column} {op} $2
                  OR ({sort_column} = $2 AND candidate.id {op} $3)
              )
            ORDER BY {sort_column} {dir}, candidate.id {dir}
            LIMIT $11
        )"#
            )
        })
        .collect::<Vec<_>>()
        .join("\n        UNION ALL");

    let sql = format!(
        r#"
        WITH picked AS ({branches})
        SELECT
            event.id,
            picked.sort_value AS cursor_sort_value,
{projection}
        FROM picked
        JOIN events event ON event.id = picked.id
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
        ORDER BY picked.sort_value {dir}, event.id {dir}
        LIMIT $11
        "#,
        projection = EVENT_LIST_PROJECTION,
    );

    let rows = sqlx::query(&sql)
        .persistent(false)
        .bind(chain_id)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(filter.event_id)
        .bind(filter.show_nsfw)
        .bind(filter.show_blacklisted)
        .bind(filter.token_id)
        .bind(filter.date_less)
        .bind(filter.date_greater)
        .bind(filter.date_day)
        .bind(page.limit + 1)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

#[cfg(test)]
mod kind_seek_tests {
    use super::*;

    fn kind_filter(ids: &[i32]) -> EventFilter<'_> {
        EventFilter {
            event_kind_ids: Some(ids),
            ..EventFilter::default()
        }
    }

    #[test]
    fn kind_seek_needs_kind_ids_to_seek_with() {
        // Without a kind filter there is nothing to seek per kind, and an empty id set is
        // already answered by `AND FALSE` in the general query.
        assert!(kind_filter(&[30, 92]).kind_seek_applies(EventOrderBy::Id));
        assert!(!kind_filter(&[]).kind_seek_applies(EventOrderBy::Id));
        assert!(!EventFilter::default().kind_seek_applies(EventOrderBy::Id));
    }

    #[test]
    fn kind_seek_refuses_filters_it_cannot_apply_before_the_limit() {
        // The seek path takes `limit + 1` rows per kind from `events` alone. A filter that
        // needs one of the presentation joins would only be applied after that cut, so the
        // page could come back short — those must fall back to the general query.
        let ids = [30, 92];
        for (name, filter) in [
            (
                "transaction_hash",
                EventFilter {
                    transaction_hash: Some("TX"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "block_height",
                EventFilter {
                    block_height: Some(1),
                    ..kind_filter(&ids)
                },
            ),
            (
                "block_hash",
                EventFilter {
                    block_hash: Some("B"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "contract",
                EventFilter {
                    contract: Some("SOUL"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "q",
                EventFilter {
                    q: Some("SOUL"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "nft_name",
                EventFilter {
                    nft_name_partial: Some("%a%"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "nft_description",
                EventFilter {
                    nft_description_partial: Some("%a%"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "address_partial",
                EventFilter {
                    address_partial: Some("%P%"),
                    ..kind_filter(&ids)
                },
            ),
            (
                "kind_partial",
                EventFilter {
                    event_kind_partial_ids: Some(&ids),
                    ..kind_filter(&ids)
                },
            ),
        ] {
            assert!(
                !filter.kind_seek_applies(EventOrderBy::Id),
                "{name} needs a join, so the seek path must not claim it"
            );
        }
    }

    #[test]
    fn kind_seek_refuses_ordering_that_lives_on_a_join() {
        // Block height comes from `blocks`, so a per-kind index cannot deliver that order.
        assert!(kind_filter(&[30]).kind_seek_applies(EventOrderBy::Id));
        assert!(kind_filter(&[30]).kind_seek_applies(EventOrderBy::Date));
        assert!(!kind_filter(&[30]).kind_seek_applies(EventOrderBy::BlockHeight));
    }

    #[test]
    fn kind_seek_keeps_filters_that_live_on_events() {
        // These are inside the per-kind branch, so they must NOT push the query onto the
        // general path — that is where the timeouts live.
        let ids = [30];
        let filter = EventFilter {
            event_id: Some(7),
            token_id: Some("1"),
            show_nsfw: true,
            show_blacklisted: true,
            date_less: Some(2),
            date_greater: Some(1),
            date_day: Some(3),
            chain_id: Some(1),
            ..kind_filter(&ids)
        };
        assert!(filter.kind_seek_applies(EventOrderBy::Date));
    }
}
