//! Transactions read model (the `/transactions`, `/transaction`,
//! `/transactions/{hash}` and `/blocks/{h}/transactions/{i}` endpoints).
//!
//! The list paths page with a seek cursor (the active sort key + row id); the API
//! parses the cursor and passes the two seek values plus a typed order/limit via
//! [`TransactionPage`]. The db read fns own all the SQL and return rows; the API
//! maps them with `transaction_from_row`/`transaction_occurrence_from_row` and
//! keeps the orchestration (which list variant to run, cursor trimming, event
//! attachment, neighbour hydration). See the design note in [`super::tokens`].
use crate::*;
use sqlx::postgres::PgRow;

/// Sortable keys for the transactions list. The seek cursor keys on the selected
/// sort column plus `tx.id` (or `address_tx.id`), so paging stays consistent for
/// every order.
#[derive(Debug, Clone, Copy)]
pub enum TransactionOrderBy {
    Date,
    BlockHeight,
}

impl TransactionOrderBy {
    /// Parse the public `order_by` query param. `id` and `date` both sort by the
    /// transaction timestamp; the default is `date`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("date") {
            "id" | "date" => Some(Self::Date),
            "block_height" => Some(Self::BlockHeight),
            _ => None,
        }
    }

    /// The SQL column for this key (a fixed literal, never user input).
    fn column(self) -> &'static str {
        match self {
            Self::Date => "tx.timestamp_unix_seconds",
            Self::BlockHeight => "block.height",
        }
    }

    /// The SQL sort column for the address-scoped lists. `Date` reads the
    /// timestamp denormalized onto `address_transactions`, so the
    /// `(address_id, timestamp_unix_seconds, id)` index orders the page without
    /// sorting the joined transactions of a high-activity address.
    fn address_column(self) -> &'static str {
        match self {
            Self::Date => "address_tx.timestamp_unix_seconds",
            Self::BlockHeight => "block.height",
        }
    }
}

/// Order + seek + limit for a transaction list page. `cursor_sort_value` (the
/// previous page's last value of the active sort column) and `cursor_id` are the
/// parsed seek key from the previous page (both `None` on the first page).
#[derive(Debug, Clone, Copy)]
pub struct TransactionPage {
    pub order_by: TransactionOrderBy,
    pub direction: SortDirection,
    pub cursor_sort_value: Option<i64>,
    pub cursor_id: Option<i32>,
    pub limit: i64,
}

/// Filters shared by the global and filtered-address transaction lists. `q` is
/// the raw free-text value (the read fn derives the numeric/substring forms).
#[derive(Debug, Default, Clone, Copy)]
pub struct TransactionFilter<'a> {
    pub hash: Option<&'a str>,
    pub hash_partial: Option<&'a str>,
    pub block_height: Option<i64>,
    pub block_hash: Option<&'a str>,
    pub chain_id: Option<i32>,
    pub state_id: Option<i32>,
    pub q: Option<&'a str>,
    pub date_greater: Option<i64>,
    pub date_less: Option<i64>,
}

// Derive the numeric (`q_height`) and substring (`q_like`) forms of the free-text
// `q` for the transaction lists: a purely-numeric `q` is treated as a height and
// suppresses the substring match (matching the previous in-handler logic).
fn transaction_q_forms(q: Option<&str>) -> (Option<i64>, Option<String>) {
    let q_height = q.and_then(|value| value.parse::<i64>().ok());
    let q_like = if q_height.is_none() {
        q.map(|value| format!("%{value}%"))
    } else {
        None
    };
    (q_height, q_like)
}

/// The shared transaction projection with a caller-supplied WHERE clause. The
/// clause is built from fixed literals in this module, never user input.
fn transaction_select_sql(where_clause: &str) -> String {
    format!(
        r#"
        SELECT
            tx.id,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds,
            tx.fee,
            tx.fee_raw,
            tx.script_raw,
            tx.result,
            tx.debug_comment,
            tx.payload,
            tx.expiration AS expiration_unix_seconds,
            tx.gas_price,
            tx.gas_price_raw,
            tx.gas_limit,
            tx.gas_limit_raw,
            state_row.name AS state,
            tx.carbon_tx_type,
            tx.carbon_tx_data,
            sender.address AS sender_address,
            sender.address_name AS sender_address_name,
            gas_payer.address AS gas_payer_address,
            gas_payer.address_name AS gas_payer_address_name,
            gas_target.address AS gas_target_address,
            gas_target.address_name AS gas_target_address_name
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        JOIN transaction_states state_row ON state_row.id = tx.state_id
        LEFT JOIN addresses sender ON sender.id = tx.sender_id
        LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
        LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
        {where_clause}
        "#
    )
}

/// Resolve a transaction state name to its id (case-insensitive). Returns `None`
/// when the state is unknown; the API maps that to a 400.
pub async fn transaction_state_id_by_name(
    executor: impl sqlx::PgExecutor<'_>,
    state: &str,
) -> Result<Option<i32>, DbError> {
    let state_id = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT id
        FROM transaction_states
        WHERE lower(name) = lower($1)
        ORDER BY id
        LIMIT 1
        "#,
    )
    .bind(state)
    .fetch_optional(executor)
    .await?;

    Ok(state_id)
}

/// Global transaction list (no address scope), seek-paged. Fetches `limit + 1`
/// rows so the API can detect a following page.
pub async fn list_transactions_global(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &TransactionFilter<'_>,
    page: &TransactionPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let (q_height, q_like) = transaction_q_forms(filter.q);
    let sql = format!(
        r#"
        SELECT
            tx.id,
            tx.id AS cursor_id,
            {column}::bigint AS cursor_sort_value,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds,
            tx.fee,
            tx.fee_raw,
            tx.script_raw,
            tx.result,
            tx.debug_comment,
            tx.payload,
            tx.expiration AS expiration_unix_seconds,
            tx.gas_price,
            tx.gas_price_raw,
            tx.gas_limit,
            tx.gas_limit_raw,
            state_row.name AS state,
            tx.carbon_tx_type,
            tx.carbon_tx_data,
            sender.address AS sender_address,
            sender.address_name AS sender_address_name,
            gas_payer.address AS gas_payer_address,
            gas_payer.address_name AS gas_payer_address_name,
            gas_target.address AS gas_target_address,
            gas_target.address_name AS gas_target_address_name
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        JOIN transaction_states state_row ON state_row.id = tx.state_id
        LEFT JOIN addresses sender ON sender.id = tx.sender_id
        LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
        LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
        WHERE ($1::integer IS NULL OR block.chain_id = $1)
          AND ($2::text IS NULL OR tx.hash = $2)
          AND ($3::text IS NULL OR tx.hash ILIKE $3)
          AND ($4::bigint IS NULL OR block.height = $4)
          AND ($5::text IS NULL OR block.hash = $5)
          AND ($6::integer IS NULL OR tx.state_id = $6)
          AND ($7::bigint IS NULL OR tx.timestamp_unix_seconds >= $7)
          AND ($8::bigint IS NULL OR tx.timestamp_unix_seconds <= $8)
          AND (
              ($12::text IS NULL AND $13::bigint IS NULL)
              OR ($12::text IS NOT NULL AND (tx.hash ILIKE $12 OR block.hash ILIKE $12))
              OR ($13::bigint IS NOT NULL AND block.height = $13)
          )
          AND (
              $9::bigint IS NULL
              OR {column} {op} $9
              OR ({column} = $9 AND tx.id {op} $10)
          )
        ORDER BY {column} {dir}, tx.id {dir}
        LIMIT $11
        "#,
        column = page.order_by.column(),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.hash)
        .bind(filter.hash_partial)
        .bind(filter.block_height)
        .bind(filter.block_hash)
        .bind(filter.state_id)
        .bind(filter.date_greater)
        .bind(filter.date_less)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(page.limit + 1)
        .bind(q_like.as_deref())
        .bind(q_height)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Address-only transaction timeline: pages by the address's activity rows
/// across every chain (ordered by `transaction_id`), then joins transaction
/// facts. The caller resolves the address string to its id.
pub async fn list_transactions_for_address_timeline(
    executor: impl sqlx::PgExecutor<'_>,
    address_id: i32,
    page: &TransactionPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let column = page.order_by.address_column();
    // `block.height` ordering needs the block join inside the page CTE; the
    // default `date` ordering reads the denormalized timestamp straight from
    // `address_transactions` and stays on the covering index.
    let cte_block_join = match page.order_by {
        TransactionOrderBy::Date => "",
        TransactionOrderBy::BlockHeight => {
            "JOIN transactions tx ON tx.id = address_tx.transaction_id \
             JOIN blocks block ON block.id = tx.block_id"
        }
    };
    let sql = format!(
        r#"
        WITH address_page AS MATERIALIZED (
            SELECT
                address_tx.id AS cursor_id,
                address_tx.transaction_id,
                {column}::bigint AS cursor_sort_value
            FROM address_transactions address_tx
            {cte_block_join}
            WHERE address_tx.address_id = $1
              AND (
                  $2::bigint IS NULL
                  OR {column} {op} $2
                  OR ({column} = $2 AND address_tx.id {op} $3)
              )
            ORDER BY {column} {dir}, address_tx.id {dir}
            LIMIT $4
        )
        SELECT
            tx.id,
            address_page.cursor_id,
            address_page.cursor_sort_value,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds,
            tx.fee,
            tx.fee_raw,
            tx.script_raw,
            tx.result,
            tx.debug_comment,
            tx.payload,
            tx.expiration AS expiration_unix_seconds,
            tx.gas_price,
            tx.gas_price_raw,
            tx.gas_limit,
            tx.gas_limit_raw,
            state_row.name AS state,
            tx.carbon_tx_type,
            tx.carbon_tx_data,
            sender.address AS sender_address,
            sender.address_name AS sender_address_name,
            gas_payer.address AS gas_payer_address,
            gas_payer.address_name AS gas_payer_address_name,
            gas_target.address AS gas_target_address,
            gas_target.address_name AS gas_target_address_name
        FROM address_page
        JOIN transactions tx ON tx.id = address_page.transaction_id
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        JOIN transaction_states state_row ON state_row.id = tx.state_id
        LEFT JOIN addresses sender ON sender.id = tx.sender_id
        LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
        LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
        ORDER BY address_page.cursor_sort_value {dir}, address_page.cursor_id {dir}
        "#,
    );
    let rows = sqlx::query(&sql)
        .bind(address_id)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(page.limit + 1)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Filtered address timeline: scopes by address id (paging by `transaction_id`)
/// while the other transaction filters narrow the selected activity rows. The
/// caller resolves the address string to its id.
pub async fn list_transactions_for_filtered_address(
    executor: impl sqlx::PgExecutor<'_>,
    address_id: i32,
    filter: &TransactionFilter<'_>,
    page: &TransactionPage,
) -> Result<Vec<PgRow>, DbError> {
    let dir = page.direction.as_sql();
    let op = page.direction.cursor_operator();
    let (q_height, q_like) = transaction_q_forms(filter.q);
    let column = page.order_by.address_column();
    let sql = format!(
        r#"
        SELECT
            tx.id,
            address_tx.id AS cursor_id,
            {column}::bigint AS cursor_sort_value,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds,
            tx.fee,
            tx.fee_raw,
            tx.script_raw,
            tx.result,
            tx.debug_comment,
            tx.payload,
            tx.expiration AS expiration_unix_seconds,
            tx.gas_price,
            tx.gas_price_raw,
            tx.gas_limit,
            tx.gas_limit_raw,
            state_row.name AS state,
            tx.carbon_tx_type,
            tx.carbon_tx_data,
            sender.address AS sender_address,
            sender.address_name AS sender_address_name,
            gas_payer.address AS gas_payer_address,
            gas_payer.address_name AS gas_payer_address_name,
            gas_target.address AS gas_target_address,
            gas_target.address_name AS gas_target_address_name
        FROM address_transactions address_tx
        JOIN transactions tx ON tx.id = address_tx.transaction_id
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        JOIN transaction_states state_row ON state_row.id = tx.state_id
        LEFT JOIN addresses sender ON sender.id = tx.sender_id
        LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
        LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
        WHERE address_tx.address_id = $1
          AND ($2::text IS NULL OR tx.hash = $2)
          AND ($3::text IS NULL OR tx.hash ILIKE $3)
          AND ($4::bigint IS NULL OR block.height = $4)
          AND ($5::text IS NULL OR block.hash = $5)
          AND ($6::integer IS NULL OR block.chain_id = $6)
          AND ($7::integer IS NULL OR tx.state_id = $7)
          AND ($8::bigint IS NULL OR tx.timestamp_unix_seconds >= $8)
          AND ($9::bigint IS NULL OR tx.timestamp_unix_seconds <= $9)
          AND (
              ($13::text IS NULL AND $14::bigint IS NULL)
              OR ($13::text IS NOT NULL AND (tx.hash ILIKE $13 OR block.hash ILIKE $13))
              OR ($14::bigint IS NOT NULL AND block.height = $14)
          )
          AND (
              $10::bigint IS NULL
              OR {column} {op} $10
              OR ({column} = $10 AND address_tx.id {op} $11)
          )
        ORDER BY {column} {dir}, address_tx.id {dir}
        LIMIT $12
        "#,
    );
    let rows = sqlx::query(&sql)
        .bind(address_id)
        .bind(filter.hash)
        .bind(filter.hash_partial)
        .bind(filter.block_height)
        .bind(filter.block_hash)
        .bind(filter.chain_id)
        .bind(filter.state_id)
        .bind(filter.date_greater)
        .bind(filter.date_less)
        .bind(page.cursor_sort_value)
        .bind(page.cursor_id)
        .bind(page.limit + 1)
        .bind(q_like.as_deref())
        .bind(q_height)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}

/// Load all transactions belonging to the given block ids (for block-list
/// hydration). Returns the rows ordered by block height, index, id; the API
/// groups them by `tx_block_id`.
pub async fn list_transactions_by_block_ids(
    executor: impl sqlx::PgExecutor<'_>,
    block_ids: &[i32],
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            tx.block_id AS tx_block_id,
            tx.id,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds,
            tx.fee,
            tx.fee_raw,
            tx.script_raw,
            tx.result,
            tx.debug_comment,
            tx.payload,
            tx.expiration AS expiration_unix_seconds,
            tx.gas_price,
            tx.gas_price_raw,
            tx.gas_limit,
            tx.gas_limit_raw,
            state_row.name AS state,
            tx.carbon_tx_type,
            tx.carbon_tx_data,
            sender.address AS sender_address,
            sender.address_name AS sender_address_name,
            gas_payer.address AS gas_payer_address,
            gas_payer.address_name AS gas_payer_address_name,
            gas_target.address AS gas_target_address,
            gas_target.address_name AS gas_target_address_name
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        JOIN transaction_states state_row ON state_row.id = tx.state_id
        LEFT JOIN addresses sender ON sender.id = tx.sender_id
        LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
        LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
        WHERE tx.block_id = ANY($1)
        ORDER BY block.height ASC, tx.tx_index ASC, tx.id ASC
        "#,
    )
    .bind(block_ids)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// Single transaction occurrence by block height + index within a chain.
pub async fn transaction_row_by_block_index(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
    block_height: i64,
    index: i32,
) -> Result<Option<PgRow>, DbError> {
    let sql = transaction_select_sql(
        r#"
        WHERE chain.name = $1
          AND block.height = $2
          AND tx.tx_index = $3
        "#,
    );
    let row = sqlx::query(&sql)
        .bind(chain)
        .bind(block_height)
        .bind(index)
        .fetch_optional(executor)
        .await?;

    Ok(row)
}

/// Single transaction occurrence disambiguated by hash + block height + index.
pub async fn transaction_by_hash_block_index(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
    hash: &str,
    block_height: i64,
    index: i32,
) -> Result<Option<PgRow>, DbError> {
    let sql = transaction_select_sql(
        r#"
        WHERE chain.name = $1
          AND tx.hash = $2
          AND block.height = $3
          AND tx.tx_index = $4
        "#,
    );
    let row = sqlx::query(&sql)
        .bind(chain)
        .bind(hash)
        .bind(block_height)
        .bind(index)
        .fetch_optional(executor)
        .await?;

    Ok(row)
}

/// The single transaction occurrence for a hash on a chain (caller has already
/// established the hash resolves to exactly one occurrence).
pub async fn single_transaction_by_hash(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
    hash: &str,
) -> Result<Option<PgRow>, DbError> {
    let sql = transaction_select_sql(
        r#"
        WHERE chain.name = $1
          AND tx.hash = $2
        "#,
    );
    let row = sqlx::query(&sql)
        .bind(chain)
        .bind(hash)
        .fetch_optional(executor)
        .await?;

    Ok(row)
}

/// Count how many occurrences a hash has on a chain (legacy history can have
/// duplicate hashes across blocks).
pub async fn transaction_occurrence_count(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
    hash: &str,
) -> Result<i64, DbError> {
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        WHERE chain.name = $1
          AND tx.hash = $2
        "#,
    )
    .bind(chain)
    .bind(hash)
    .fetch_one(executor)
    .await?;

    Ok(count)
}

/// List the occurrence rows for an ambiguous hash (capped at 100), for the
/// 409 disambiguation response.
pub async fn list_transaction_occurrences(
    executor: impl sqlx::PgExecutor<'_>,
    chain: &str,
    hash: &str,
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            tx.id,
            tx.hash,
            block.hash AS block_hash,
            block.height AS block_height,
            chain.name AS chain_name,
            NULL::text AS previous_hash,
            NULL::text AS next_hash,
            tx.tx_index,
            tx.timestamp_unix_seconds
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        WHERE chain.name = $1
          AND tx.hash = $2
        ORDER BY block.height ASC, tx.tx_index ASC
        LIMIT 100
        "#,
    )
    .bind(chain)
    .bind(hash)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}

/// The previous/next transaction hashes around a transaction (by timestamp+id),
/// optionally scoped to a chain. Runs two seek queries, so it takes the pool.
pub async fn transaction_neighbors(
    pool: &PgPool,
    transaction_id: i32,
    timestamp_unix_seconds: i64,
    chain_id: Option<i32>,
) -> Result<(Option<String>, Option<String>), DbError> {
    let previous_hash = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT tx.hash
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        WHERE ($1::integer IS NULL OR block.chain_id = $1)
          AND (tx.timestamp_unix_seconds, tx.id) < ($2, $3)
        ORDER BY tx.timestamp_unix_seconds DESC, tx.id DESC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(timestamp_unix_seconds)
    .bind(transaction_id)
    .fetch_optional(pool)
    .await?
    .flatten();

    let next_hash = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT tx.hash
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        WHERE ($1::integer IS NULL OR block.chain_id = $1)
          AND (tx.timestamp_unix_seconds, tx.id) > ($2, $3)
        ORDER BY tx.timestamp_unix_seconds ASC, tx.id ASC
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .bind(timestamp_unix_seconds)
    .bind(transaction_id)
    .fetch_optional(pool)
    .await?
    .flatten();

    Ok((previous_hash, next_hash))
}

/// List a transaction's signatures (with a 0-based signature index). The API
/// maps the rows to `SignatureResponse`.
pub async fn list_signatures(
    executor: impl sqlx::PgExecutor<'_>,
    transaction_id: i32,
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            (row_number() OVER (ORDER BY signature.id) - 1)::integer AS signature_index,
            kind.name AS kind,
            signature.data
        FROM signatures signature
        JOIN signature_kinds kind ON kind.id = signature.signature_kind_id
        WHERE signature.transaction_id = $1
        ORDER BY signature.id ASC
        "#,
    )
    .bind(transaction_id)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}
