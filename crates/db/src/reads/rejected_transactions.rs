//! Rejected-transaction candidates read model (`/rejected-transactions`).
//!
//! A rejected-transaction candidate is shown only when the hash is NOT already a
//! canonical transaction on the chain. The API layer first checks
//! [`rejected_transaction_canonical_exists`]; if false, it lists the captured
//! candidates with [`list_rejected_transaction_candidates`].
use crate::*;

/// Whether a canonical (accepted) transaction with this hash exists on the
/// named chain. When true, there is nothing rejected to show.
pub async fn rejected_transaction_canonical_exists(
    executor: impl sqlx::PgExecutor<'_>,
    hash: &str,
    chain: &str,
) -> Result<bool, DbError> {
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM transactions tx
            JOIN blocks block ON block.id = tx.block_id
            JOIN chains chain ON chain.id = block.chain_id
            WHERE tx.hash = $1
              AND chain.name = $2
        )
        "#,
    )
    .bind(hash)
    .bind(chain)
    .fetch_one(executor)
    .await?;

    Ok(exists)
}

/// One captured rejected-transaction candidate row. The unix-seconds fields are
/// surfaced verbatim; the API layer renders them to strings.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RejectedTransactionRow {
    pub hash: String,
    pub nexus: String,
    pub chain: String,
    pub block_height: Option<i64>,
    pub block_hash: Option<String>,
    pub timestamp_unix_seconds: Option<i64>,
    pub state: Option<String>,
    pub result: Option<String>,
    pub debug_comment: Option<String>,
    pub payload: Option<String>,
    pub script_raw: Option<String>,
    pub fee_raw: Option<String>,
    pub expiration: Option<i64>,
    pub gas_price_raw: Option<String>,
    pub gas_limit_raw: Option<String>,
    pub sender: Option<String>,
    pub gas_payer: Option<String>,
    pub gas_target: Option<String>,
    pub canonical_status: Option<String>,
    pub rpc_response_json: Option<String>,
    pub block_response_json: Option<String>,
    pub captured_at_unix_seconds: i64,
    pub updated_at_unix_seconds: i64,
}

/// List captured rejected-transaction candidates for a hash on a chain, newest
/// first by last-seen time.
pub async fn list_rejected_transaction_candidates(
    executor: impl sqlx::PgExecutor<'_>,
    hash: &str,
    chain: &str,
) -> Result<Vec<RejectedTransactionRow>, DbError> {
    let rows = sqlx::query_as::<_, RejectedTransactionRow>(
        r#"
        SELECT
            hash,
            nexus,
            chain,
            block_height,
            block_hash,
            timestamp_unix_seconds,
            state,
            result,
            debug_comment,
            payload,
            script_raw,
            fee_raw,
            expiration,
            gas_price_raw,
            gas_limit_raw,
            sender,
            gas_payer,
            gas_target,
            canonical_status,
            rpc_response_json,
            block_response_json,
            captured_at_unix_seconds,
            updated_at_unix_seconds
        FROM rejected_transaction_candidates
        WHERE hash = $1
          AND chain = $2
        ORDER BY last_seen_at_unix_seconds DESC
        "#,
    )
    .bind(hash)
    .bind(chain)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}
