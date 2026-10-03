//! Writes and reads the table `transaction_summaries`. It has one row per transaction,
//! event kind and token. A row holds what the description of a transaction needs: the
//! number of events, the number of addresses, the address when there is only one, the
//! sum of a fungible token's values and the part of the transaction sender.
//!
//! The worker writes the rows of a block right after the block's events. The API reads
//! the rows of a transaction through the unique index, so it never needs the events.
use crate::*;

/// The statement that computes the summary rows of the transactions in `$1`.
/// Migration 202610020001 ran the same statement for every stored transaction. If
/// this statement changes, a new migration must compute the stored rows again.
///
/// For a fungible token, `events.token_id` holds the value of the event as a plain
/// integer. Other events keep an NFT id or text there, so the CASE turns the value
/// into a number for fungible tokens only.
const INSERT_SUMMARIES_FOR_TRANSACTIONS: &str = r#"
    INSERT INTO transaction_summaries (
        transaction_id,
        event_kind_id,
        token_symbol,
        event_count,
        address_count,
        address_id,
        sender_event_count,
        amount_raw,
        sender_amount_raw,
        first_event_index
    )
    SELECT
        event.transaction_id,
        event.event_kind_id,
        event.payload_json->'token_event'->>'token',
        count(*),
        count(DISTINCT event.address_id),
        CASE WHEN count(DISTINCT event.address_id) = 1 THEN min(event.address_id) END,
        count(*) FILTER (WHERE event.address_id = tx.sender_id),
        sum(CASE WHEN token.fungible THEN event.token_id::numeric END),
        CASE WHEN bool_and(token.fungible) THEN COALESCE(sum(
            CASE WHEN token.fungible AND event.address_id = tx.sender_id
                 THEN event.token_id::numeric END
        ), 0) END,
        min(event.event_index)
    FROM events event
    JOIN transactions tx ON tx.id = event.transaction_id
    LEFT JOIN tokens token ON token.symbol = event.payload_json->'token_event'->>'token'
    WHERE event.transaction_id = ANY($1)
    GROUP BY event.transaction_id, event.event_kind_id, event.payload_json->'token_event'->>'token'
"#;

/// Replaces the summary rows of the given transactions with rows computed from
/// their stored events, and returns how many rows were written. The caller runs it
/// after the events are written and in the same database transaction, so a reader
/// never sees events without their summary.
pub async fn replace_transaction_summaries(
    conn: &mut PgConnection,
    transaction_ids: &[i32],
) -> Result<u64, DbError> {
    if transaction_ids.is_empty() {
        return Ok(0);
    }
    sqlx::query("DELETE FROM transaction_summaries WHERE transaction_id = ANY($1)")
        .bind(transaction_ids)
        .execute(&mut *conn)
        .await?;
    let result = sqlx::query(INSERT_SUMMARIES_FOR_TRANSACTIONS)
        .bind(transaction_ids)
        .execute(&mut *conn)
        .await?;
    Ok(result.rows_affected())
}

/// One summary row as the API serves it: the kind by name, the address with its
/// name, the sums as decimal text.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TransactionSummaryRecord {
    pub transaction_id: i32,
    pub event_kind: String,
    pub token_symbol: Option<String>,
    pub event_count: i32,
    pub address_count: i32,
    /// The only address of these events; `None` when there are several.
    pub address: Option<String>,
    pub address_name: Option<String>,
    pub sender_event_count: i32,
    /// Set only for a fungible token.
    pub amount_raw: Option<String>,
    pub sender_amount_raw: Option<String>,
    pub first_event_index: i32,
}

/// Returns the summary rows of the given transactions, ordered by transaction and
/// then by the position of each row's first event, which is the order the events
/// happened in.
pub async fn list_transaction_summaries(
    executor: impl sqlx::PgExecutor<'_>,
    transaction_ids: &[i32],
) -> Result<Vec<TransactionSummaryRecord>, DbError> {
    let records = sqlx::query_as::<_, TransactionSummaryRecord>(
        r#"
        SELECT
            summary.transaction_id,
            event_kind.name AS event_kind,
            summary.token_symbol,
            summary.event_count,
            summary.address_count,
            address.address,
            address.address_name,
            summary.sender_event_count,
            summary.amount_raw::text AS amount_raw,
            summary.sender_amount_raw::text AS sender_amount_raw,
            summary.first_event_index
        FROM transaction_summaries summary
        JOIN event_kinds event_kind ON event_kind.id = summary.event_kind_id
        LEFT JOIN addresses address ON address.id = summary.address_id
        WHERE summary.transaction_id = ANY($1)
        ORDER BY summary.transaction_id, summary.first_event_index
        "#,
    )
    .bind(transaction_ids)
    .fetch_all(executor)
    .await?;
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_rows::insert_token;

    fn token_event(
        transaction_id: i32,
        chain_id: i32,
        event_index: i32,
        event_kind: &str,
        address: &str,
        symbol: &str,
        value: &str,
    ) -> EventUpsert {
        EventUpsert {
            transaction_id,
            chain_id,
            event_index,
            event_kind: event_kind.to_owned(),
            event_name: None,
            address: Some(address.to_owned()),
            target_address: None,
            contract: Some(symbol.to_owned()),
            token_id: Some(value.to_owned()),
            raw_data: None,
            payload_format: Some("live.v1".to_owned()),
            payload_json: Some(serde_json::json!({
                "token_event": {
                    "token": symbol,
                    "value": value,
                    "value_raw": value,
                    "chain_name": "main"
                }
            })),
            timestamp_unix_seconds: 1_800_800_000,
            burned: None,
        }
    }

    fn gas_event(
        transaction_id: i32,
        chain_id: i32,
        event_index: i32,
        event_kind: &str,
        address: &str,
    ) -> EventUpsert {
        EventUpsert {
            transaction_id,
            chain_id,
            event_index,
            event_kind: event_kind.to_owned(),
            event_name: None,
            address: Some(address.to_owned()),
            target_address: None,
            contract: Some("gas".to_owned()),
            token_id: None,
            raw_data: None,
            payload_format: Some("live.v1".to_owned()),
            payload_json: None,
            timestamp_unix_seconds: 1_800_800_000,
            burned: None,
        }
    }

    // Checks the summary rows of one transaction against its events: one row per event
    // kind and token, with the right counts, addresses, sums and first event. The
    // counts must add up to the number of events, gas events included. After the
    // events change, a second call must remove the rows of the events that are gone.
    #[tokio::test]
    async fn summary_rows_follow_the_events_of_a_transaction()
    -> Result<(), Box<dyn std::error::Error>> {
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
        let sender = format!("PTESTSUMA{suffix}");
        let first_recipient = format!("PTESTSUMB{suffix}");
        let second_recipient = format!("PTESTSUMC{suffix}");
        let fungible = format!("RSTSUMF{}", &suffix[..8]).to_uppercase();
        let nft = format!("RSTSUMN{}", &suffix[..8]).to_uppercase();
        let owner_id = upsert_address_id(&mut tx, chain_id, &sender).await?;
        insert_token(&mut tx, chain_id, &fungible, true, owner_id).await?;
        insert_token(&mut tx, chain_id, &nft, false, owner_id).await?;

        let block = upsert_block(
            &mut tx,
            &mut ProjectionDimensionCache::new(),
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_800_000),
                hash: format!("TESTSUMBLOCK{suffix}").to_uppercase(),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: None,
                timestamp_unix_seconds: 1_800_800_000,
                reward: None,
            },
        )
        .await?;
        let transaction = upsert_transaction(
            &mut tx,
            TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTSUMTX{suffix}").to_uppercase(),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee_raw: None,
                gas_price_raw: None,
                gas_limit_raw: None,
                sender: Some(sender.clone()),
                gas_payer: Some(sender.clone()),
                gas_target: Some(sender.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;
        let id = transaction.id;

        // Two fungible transfers to two recipients, two NFTs to one recipient, and
        // a gap in the event indexes before the gas payment, as real data has.
        let events = vec![
            gas_event(id, chain_id, 1, "GasEscrow", &sender),
            token_event(id, chain_id, 2, "TokenSend", &sender, &fungible, "100"),
            token_event(
                id,
                chain_id,
                3,
                "TokenReceive",
                &first_recipient,
                &fungible,
                "100",
            ),
            token_event(id, chain_id, 4, "TokenSend", &sender, &fungible, "50"),
            token_event(
                id,
                chain_id,
                5,
                "TokenReceive",
                &second_recipient,
                &fungible,
                "50",
            ),
            token_event(id, chain_id, 6, "TokenSend", &sender, &nft, "111"),
            token_event(
                id,
                chain_id,
                7,
                "TokenReceive",
                &first_recipient,
                &nft,
                "111",
            ),
            token_event(id, chain_id, 8, "TokenSend", &sender, &nft, "222"),
            token_event(
                id,
                chain_id,
                9,
                "TokenReceive",
                &first_recipient,
                &nft,
                "222",
            ),
            gas_event(id, chain_id, 12, "GasPayment", &sender),
        ];
        replace_events(&mut tx, id, &events).await?;
        let written = replace_transaction_summaries(&mut tx, &[id]).await?;
        assert_eq!(written, 6, "one row per event kind and token");

        let rows = list_transaction_summaries(&mut *tx, &[id]).await?;
        let shape = rows
            .iter()
            .map(|row| {
                (
                    row.event_kind.as_str(),
                    row.token_symbol.as_deref(),
                    row.event_count,
                    row.first_event_index,
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            shape,
            vec![
                ("GasEscrow", None, 1, 1),
                ("TokenSend", Some(fungible.as_str()), 2, 2),
                ("TokenReceive", Some(fungible.as_str()), 2, 3),
                ("TokenSend", Some(nft.as_str()), 2, 6),
                ("TokenReceive", Some(nft.as_str()), 2, 7),
                ("GasPayment", None, 1, 12),
            ],
            "rows come in the order their first events happened"
        );
        let total: i32 = rows.iter().map(|row| row.event_count).sum();
        assert_eq!(total, 10, "the counts add up to every event, gas included");

        let fungible_send = &rows[1];
        assert_eq!(fungible_send.address.as_deref(), Some(sender.as_str()));
        assert_eq!(fungible_send.amount_raw.as_deref(), Some("150"));
        assert_eq!(fungible_send.sender_event_count, 2);
        assert_eq!(fungible_send.sender_amount_raw.as_deref(), Some("150"));

        let fungible_receive = &rows[2];
        assert_eq!(fungible_receive.address_count, 2);
        assert_eq!(
            fungible_receive.address, None,
            "two recipients have no single address"
        );
        assert_eq!(fungible_receive.amount_raw.as_deref(), Some("150"));
        assert_eq!(fungible_receive.sender_event_count, 0);
        assert_eq!(
            fungible_receive.sender_amount_raw.as_deref(),
            Some("0"),
            "a fungible row always has the sender's part, zero included"
        );

        let nft_receive = &rows[4];
        assert_eq!(
            nft_receive.address.as_deref(),
            Some(first_recipient.as_str())
        );
        assert_eq!(
            nft_receive.amount_raw, None,
            "NFT ids are not amounts and are never summed"
        );
        assert_eq!(nft_receive.sender_amount_raw, None);

        // The events change (the NFT transfers and one fungible transfer go away):
        // a replace must drop the rows whose events are gone and recompute the rest.
        let remaining = vec![
            events[0].clone(),
            events[1].clone(),
            events[2].clone(),
            events[9].clone(),
        ];
        replace_events(&mut tx, id, &remaining).await?;
        replace_transaction_summaries(&mut tx, &[id]).await?;
        let rows = list_transaction_summaries(&mut *tx, &[id]).await?;
        let shape = rows
            .iter()
            .map(|row| {
                (
                    row.event_kind.as_str(),
                    row.event_count,
                    row.amount_raw.as_deref(),
                    row.address.as_deref(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            shape,
            vec![
                ("GasEscrow", 1, None, Some(sender.as_str())),
                ("TokenSend", 1, Some("100"), Some(sender.as_str())),
                (
                    "TokenReceive",
                    1,
                    Some("100"),
                    Some(first_recipient.as_str())
                ),
                ("GasPayment", 1, None, Some(sender.as_str())),
            ]
        );

        tx.rollback().await?;
        Ok(())
    }
}
