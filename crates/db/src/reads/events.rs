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
    pub event_kind: Option<&'a str>,
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
    pub event_kind_partial: Option<&'a str>,
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

/// Global event list scoped to one chain (no address filter), seek-paged.
/// Fetches `limit + 1` rows so the API can detect a following page.
pub async fn list_events_global(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: i32,
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
            event.event_index,
            'legacy'::text AS event_source,
            $2::text AS chain_name,
            event.timestamp_unix_seconds,
            block.hash AS block_hash,
            tx.hash AS transaction_hash,
            event_kind.name AS event_kind,
            event_kind.name AS event_name,
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
        JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
        LEFT JOIN addresses address ON address.id = event.address_id
        LEFT JOIN contracts contract ON contract.id = event.contract_id
        LEFT JOIN nfts nft ON nft.id = event.nft_id
        LEFT JOIN series series ON series.id = nft.series_id
        LEFT JOIN series_modes series_mode ON series_mode.id = series.series_mode_id
        LEFT JOIN addresses series_creator ON series_creator.id = series.creator_address_id
        WHERE event.chain_id = $1
          AND ($3::text IS NULL OR tx.hash = $3)
          AND ($4::bigint IS NULL OR block.height = $4)
          AND ($5::text IS NULL OR event_kind.name = $5)
          AND ($6::text IS NULL OR $6 = 'legacy')
          AND ($7::text IS NULL OR contract.hash = $7 OR contract.name = $7 OR contract.symbol = $7)
          AND ($11::integer IS NULL OR event.id = $11)
          AND ($12::text IS NULL OR tx.hash ILIKE $12 OR block.hash ILIKE $12 OR block.height = $13 OR event_kind.name ILIKE $12 OR address.address ILIKE $12 OR address.address_name ILIKE $12 OR contract.hash ILIKE $12 OR contract.name ILIKE $12 OR contract.symbol ILIKE $12 OR event.token_id ILIKE $12)
          AND ($14::bool OR NOT event.nsfw)
          AND ($15::bool OR NOT event.blacklisted)
          AND ($16::text IS NULL OR event.token_id = $16)
          AND ($17::text IS NULL OR block.hash = $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds <= $18)
          AND ($19::bigint IS NULL OR event.timestamp_unix_seconds >= $19)
          AND ($20::bigint IS NULL OR event.date_unix_seconds = $20)
          AND ($21::text IS NULL OR event_kind.name ILIKE $21)
          AND ($22::text IS NULL OR nft.name ILIKE $22)
          AND ($23::text IS NULL OR nft.description ILIKE $23)
          AND ($24::text IS NULL OR address.address ILIKE $24 OR address.address_name ILIKE $24 OR address.user_name ILIKE $24)
          AND (
              $8::bigint IS NULL
              OR {column} {op} $8
              OR ({column} = $8 AND event.id {op} $9)
          )
        ORDER BY {column} {dir}, event.id {dir}
        LIMIT $10
        "#,
        column = page.order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(chain_id)
        .bind(chain_name)
        .bind(filter.transaction_hash)
        .bind(filter.block_height)
        .bind(filter.event_kind)
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
        .bind(filter.event_kind_partial)
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
            event_kind.name AS event_name,
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
        WHERE ($6::integer IS NOT NULL OR chain.name = $1)
          AND ($2::text IS NULL OR tx.hash = $2)
          AND ($3::bigint IS NULL OR block.height = $3)
          AND ($4::text IS NULL OR event_kind.name = $4)
          AND ($5::text IS NULL OR $5 = 'legacy')
          AND ($6::integer IS NULL OR event.address_id = $6 OR event.target_address_id = $6)
          AND ($25::integer IS NULL OR event.chain_id = $25)
          AND ($7::text IS NULL OR contract.hash = $7 OR contract.name = $7 OR contract.symbol = $7)
          AND ($11::integer IS NULL OR event.id = $11)
          AND ($12::text IS NULL OR tx.hash ILIKE $12 OR block.hash ILIKE $12 OR block.height = $13 OR event_kind.name ILIKE $12 OR address.address ILIKE $12 OR target_address.address ILIKE $12 OR address.address_name ILIKE $12 OR contract.hash ILIKE $12 OR contract.name ILIKE $12 OR contract.symbol ILIKE $12 OR event.token_id ILIKE $12)
          AND ($14::bool OR NOT event.nsfw)
          AND ($15::bool OR NOT event.blacklisted)
          AND ($16::text IS NULL OR event.token_id = $16)
          AND ($17::text IS NULL OR block.hash = $17)
          AND ($18::bigint IS NULL OR event.timestamp_unix_seconds <= $18)
          AND ($19::bigint IS NULL OR event.timestamp_unix_seconds >= $19)
          AND ($20::bigint IS NULL OR event.date_unix_seconds = $20)
          AND ($21::text IS NULL OR event_kind.name ILIKE $21)
          AND ($22::text IS NULL OR nft.name ILIKE $22)
          AND ($23::text IS NULL OR nft.description ILIKE $23)
          AND ($24::text IS NULL OR address.address ILIKE $24 OR address.address_name ILIKE $24 OR address.user_name ILIKE $24)
          AND (
              $8::bigint IS NULL
              OR {column} {op} $8
              OR ({column} = $8 AND event.id {op} $9)
          )
        ORDER BY {column} {dir}, event.id {dir}
        LIMIT $10
        "#,
        column = page.order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(chain_name)
        .bind(filter.transaction_hash)
        .bind(filter.block_height)
        .bind(filter.event_kind)
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
        .bind(filter.event_kind_partial)
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
            event_kind.name AS event_name,
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

/// Load the token rows used to enrich event payloads, by uppercase symbol. The
/// API builds the per-symbol JSON map from these rows.
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
