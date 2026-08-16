//! Rejected transaction candidates — the diagnostic capture behind the
//! `/rejected-transactions` endpoint, ported from the C# ExplorerBackend
//! (`RejectedTransactionCandidateMethods` + `EP.RejectedTransactions`).
//!
//! Rows land here only through the endpoint's on-demand capture, never through
//! the sync write path: the worker does not know these hashes exist. The table
//! is keyed by (nexus, chain, hash) so a re-capture refreshes the stored
//! snapshot in place while `captured_at_unix_seconds` keeps the first sighting.
use crate::*;

/// One stored rejected-transaction candidate, as captured from the node.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct RejectedCandidateRecord {
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
    pub last_seen_at_unix_seconds: i64,
}

/// The capture payload for [`upsert_rejected_candidate`]. Same fields as the
/// record minus the timestamps the upsert manages itself.
#[derive(Debug, Clone, Default)]
pub struct RejectedCandidateUpsert {
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
}

/// Whether the hash is already a canonical transaction on the named chain.
/// A canonical transaction can never be a rejected candidate, so the endpoint
/// answers empty without looking at (or writing to) the candidates table.
pub async fn canonical_transaction_exists(
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
            WHERE tx.hash = $1 AND chain.name = $2
        )
        "#,
    )
    .bind(hash)
    .bind(chain)
    .fetch_one(executor)
    .await?;
    Ok(exists)
}

/// Stored candidates for (hash, nexus, chain), newest sighting first.
pub async fn list_rejected_candidates(
    executor: impl sqlx::PgExecutor<'_>,
    hash: &str,
    nexus: &str,
    chain: &str,
) -> Result<Vec<RejectedCandidateRecord>, DbError> {
    let rows = sqlx::query_as::<_, RejectedCandidateRecord>(
        r#"
        SELECT hash, nexus, chain, block_height, block_hash, timestamp_unix_seconds,
               state, result, debug_comment, payload, script_raw, fee_raw, expiration,
               gas_price_raw, gas_limit_raw, sender, gas_payer, gas_target,
               canonical_status, rpc_response_json, block_response_json,
               captured_at_unix_seconds, updated_at_unix_seconds, last_seen_at_unix_seconds
        FROM rejected_transaction_candidates
        WHERE hash = $1 AND nexus = $2 AND chain = $3
        ORDER BY last_seen_at_unix_seconds DESC
        "#,
    )
    .bind(hash)
    .bind(nexus)
    .bind(chain)
    .fetch_all(executor)
    .await?;
    Ok(rows)
}

/// Insert or refresh a candidate. A conflict on (nexus, chain, hash) replaces
/// the captured snapshot and bumps `updated_at`/`last_seen_at`, but keeps the
/// original `captured_at` — the C# upsert preserved the first-capture time the
/// same way.
pub async fn upsert_rejected_candidate(
    executor: impl sqlx::PgExecutor<'_>,
    candidate: &RejectedCandidateUpsert,
    now_unix_seconds: i64,
) -> Result<RejectedCandidateRecord, DbError> {
    let row = sqlx::query_as::<_, RejectedCandidateRecord>(
        r#"
        INSERT INTO rejected_transaction_candidates (
            hash, nexus, chain, block_height, block_hash, timestamp_unix_seconds,
            state, result, debug_comment, payload, script_raw, fee_raw, expiration,
            gas_price_raw, gas_limit_raw, sender, gas_payer, gas_target,
            canonical_status, rpc_response_json, block_response_json,
            captured_at_unix_seconds, updated_at_unix_seconds, last_seen_at_unix_seconds
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21, $22, $22, $22
        )
        ON CONFLICT (nexus, chain, hash) DO UPDATE SET
            block_height = EXCLUDED.block_height,
            block_hash = EXCLUDED.block_hash,
            timestamp_unix_seconds = EXCLUDED.timestamp_unix_seconds,
            state = EXCLUDED.state,
            result = EXCLUDED.result,
            debug_comment = EXCLUDED.debug_comment,
            payload = EXCLUDED.payload,
            script_raw = EXCLUDED.script_raw,
            fee_raw = EXCLUDED.fee_raw,
            expiration = EXCLUDED.expiration,
            gas_price_raw = EXCLUDED.gas_price_raw,
            gas_limit_raw = EXCLUDED.gas_limit_raw,
            sender = EXCLUDED.sender,
            gas_payer = EXCLUDED.gas_payer,
            gas_target = EXCLUDED.gas_target,
            canonical_status = EXCLUDED.canonical_status,
            rpc_response_json = EXCLUDED.rpc_response_json,
            block_response_json = EXCLUDED.block_response_json,
            updated_at_unix_seconds = EXCLUDED.updated_at_unix_seconds,
            last_seen_at_unix_seconds = EXCLUDED.last_seen_at_unix_seconds
        RETURNING hash, nexus, chain, block_height, block_hash, timestamp_unix_seconds,
                  state, result, debug_comment, payload, script_raw, fee_raw, expiration,
                  gas_price_raw, gas_limit_raw, sender, gas_payer, gas_target,
                  canonical_status, rpc_response_json, block_response_json,
                  captured_at_unix_seconds, updated_at_unix_seconds, last_seen_at_unix_seconds
        "#,
    )
    .bind(&candidate.hash)
    .bind(&candidate.nexus)
    .bind(&candidate.chain)
    .bind(candidate.block_height)
    .bind(&candidate.block_hash)
    .bind(candidate.timestamp_unix_seconds)
    .bind(&candidate.state)
    .bind(&candidate.result)
    .bind(&candidate.debug_comment)
    .bind(&candidate.payload)
    .bind(&candidate.script_raw)
    .bind(&candidate.fee_raw)
    .bind(candidate.expiration)
    .bind(&candidate.gas_price_raw)
    .bind(&candidate.gas_limit_raw)
    .bind(&candidate.sender)
    .bind(&candidate.gas_payer)
    .bind(&candidate.gas_target)
    .bind(&candidate.canonical_status)
    .bind(&candidate.rpc_response_json)
    .bind(&candidate.block_response_json)
    .bind(now_unix_seconds)
    .fetch_one(executor)
    .await?;
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Behavior under test: capture -> re-capture of the same (nexus, chain, hash)
    // refreshes the snapshot fields and last_seen/updated, but keeps the original
    // captured_at; the list returns the stored row; the canonical check answers
    // false for an unknown hash. Runs inside a rolled-back transaction.
    #[tokio::test]
    async fn rejected_candidate_upsert_refreshes_but_keeps_first_capture()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut tx = pool.begin().await?;

        assert!(
            !canonical_transaction_exists(&mut *tx, "REJTESTHASH", "main").await?,
            "the test hash must not exist canonically"
        );

        let first = RejectedCandidateUpsert {
            hash: "REJTESTHASH".to_owned(),
            nexus: "testnexus".to_owned(),
            chain: "main".to_owned(),
            state: Some("Fault".to_owned()),
            canonical_status: Some("block_unavailable".to_owned()),
            ..RejectedCandidateUpsert::default()
        };
        let inserted = upsert_rejected_candidate(&mut *tx, &first, 1_700_000_000).await?;
        assert_eq!(inserted.captured_at_unix_seconds, 1_700_000_000);
        assert_eq!(inserted.state.as_deref(), Some("Fault"));

        let second = RejectedCandidateUpsert {
            state: Some("Break".to_owned()),
            canonical_status: Some("not_in_block_txs".to_owned()),
            block_height: Some(42),
            ..first.clone()
        };
        let updated = upsert_rejected_candidate(&mut *tx, &second, 1_700_000_500).await?;
        assert_eq!(
            updated.captured_at_unix_seconds, 1_700_000_000,
            "re-capture keeps the first-capture time"
        );
        assert_eq!(updated.last_seen_at_unix_seconds, 1_700_000_500);
        assert_eq!(updated.updated_at_unix_seconds, 1_700_000_500);
        assert_eq!(updated.state.as_deref(), Some("Break"));
        assert_eq!(updated.block_height, Some(42));

        let listed = list_rejected_candidates(&mut *tx, "REJTESTHASH", "testnexus", "main").await?;
        assert_eq!(listed.len(), 1, "the upsert never duplicates the key");
        assert_eq!(
            listed[0].canonical_status.as_deref(),
            Some("not_in_block_txs")
        );
        let missing =
            list_rejected_candidates(&mut *tx, "REJTESTHASH", "othernexus", "main").await?;
        assert!(missing.is_empty(), "nexus is part of the key");

        Ok(())
    }
}
