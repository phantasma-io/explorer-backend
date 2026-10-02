//! Adds `summary` and `event_count` to transaction responses. Both come from the table
//! `transaction_summaries`, which the worker writes with every block. The frontend
//! builds the description of a transaction from these rows, so neither the API nor the
//! frontend reads all the events of a transaction.
use crate::*;

/// Sets `event_count` on every transaction from its stored summary, and also the
/// summary rows when `with_summary` is set. Costs one query for the rows and, with
/// `with_summary`, one more for the first event of each row.
pub(crate) async fn attach_transaction_summaries(
    pool: &PgPool,
    transactions: &mut [TransactionResponse],
    with_summary: bool,
) -> Result<(), ApiError> {
    let ids = transactions
        .iter()
        .map(|transaction| parse_i32_id("transaction_id", &transaction.transaction_id))
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Ok(());
    }
    let records = list_transaction_summaries(pool, &ids).await?;
    let mut event_counts: HashMap<i32, i64> = HashMap::new();
    for record in &records {
        *event_counts.entry(record.transaction_id).or_default() += i64::from(record.event_count);
    }
    let mut rows = if with_summary {
        summary_rows_by_transaction(pool, records).await?
    } else {
        HashMap::new()
    };
    for (transaction, id) in transactions.iter_mut().zip(ids) {
        // A transaction without events has no summary rows, so its count is zero.
        transaction.event_count = Some(event_counts.get(&id).copied().unwrap_or(0));
        if with_summary {
            transaction.summary = Some(rows.remove(&id).unwrap_or_default());
        }
    }
    Ok(())
}

/// Builds the response rows of the given records, grouped by transaction and kept
/// in the order of the records.
async fn summary_rows_by_transaction(
    pool: &PgPool,
    records: Vec<TransactionSummaryRecord>,
) -> Result<HashMap<i32, Vec<TransactionSummaryRowResponse>>, ApiError> {
    let symbols = records
        .iter()
        .filter_map(|record| record.token_symbol.as_deref())
        .map(str::to_uppercase)
        .collect::<HashSet<_>>();
    let tokens = load_event_tokens_by_symbols(pool, symbols).await?;
    let mut first_events = load_first_events(pool, &records).await?;
    let mut grouped: HashMap<i32, Vec<TransactionSummaryRowResponse>> = HashMap::new();
    for record in records {
        let first_event = first_events.remove(&(record.transaction_id, record.first_event_index));
        grouped
            .entry(record.transaction_id)
            .or_default()
            .push(summary_row_response(record, &tokens, first_event));
    }
    Ok(grouped)
}

/// Loads the first event of every record with its event data, NFT metadata and
/// series, keyed by (transaction id, event index).
async fn load_first_events(
    pool: &PgPool,
    records: &[TransactionSummaryRecord],
) -> Result<HashMap<(i32, i32), EventResponse>, ApiError> {
    let transaction_ids = records
        .iter()
        .map(|record| record.transaction_id)
        .collect::<Vec<_>>();
    let event_indexes = records
        .iter()
        .map(|record| record.first_event_index)
        .collect::<Vec<_>>();
    let rows = list_events_at_positions(pool, &transaction_ids, &event_indexes).await?;
    let positions = rows
        .iter()
        .map(|row| {
            (
                row.get::<i32, _>("event_transaction_id"),
                row.get::<i32, _>("event_index"),
            )
        })
        .collect::<Vec<_>>();
    let events = events_from_rows(pool, &rows, true, true, true, false).await?;
    Ok(positions.into_iter().zip(events).collect())
}

/// Maps one stored row to its response: the token object of the events, the
/// address with its name, and the sums formatted with the token decimals. A symbol
/// with no token row keeps the bare symbol and unformatted sums.
fn summary_row_response(
    record: TransactionSummaryRecord,
    tokens: &HashMap<String, Value>,
    first_event: Option<EventResponse>,
) -> TransactionSummaryRowResponse {
    let token = record.token_symbol.as_deref().map(|symbol| {
        tokens
            .get(&symbol.to_uppercase())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({ "symbol": symbol }))
    });
    let decimals = token
        .as_ref()
        .and_then(|token| token.get("decimals"))
        .and_then(Value::as_u64)
        .and_then(|decimals| usize::try_from(decimals).ok())
        .unwrap_or_default();
    let format = |raw: Option<&str>| raw.map(|raw| format_token_amount(raw, decimals));
    TransactionSummaryRowResponse {
        amount: format(record.amount_raw.as_deref()),
        sender_amount: format(record.sender_amount_raw.as_deref()),
        event_kind: record.event_kind,
        token,
        event_count: i64::from(record.event_count),
        address_count: i64::from(record.address_count),
        address: address_ref(record.address, record.address_name),
        sender_event_count: i64::from(record.sender_event_count),
        amount_raw: record.amount_raw,
        sender_amount_raw: record.sender_amount_raw,
        first_event,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(event_kind: &str, token_symbol: Option<&str>) -> TransactionSummaryRecord {
        TransactionSummaryRecord {
            transaction_id: 7,
            event_kind: event_kind.to_owned(),
            token_symbol: token_symbol.map(str::to_owned),
            event_count: 3,
            address_count: 1,
            address: Some("P2KSENDER".to_owned()),
            address_name: Some("sender".to_owned()),
            sender_event_count: 3,
            amount_raw: None,
            sender_amount_raw: None,
            first_event_index: 2,
        }
    }

    fn tokens() -> HashMap<String, Value> {
        HashMap::from([
            (
                "SOUL".to_owned(),
                serde_json::json!({ "symbol": "SOUL", "fungible": true, "decimals": 8 }),
            ),
            (
                "BRC".to_owned(),
                serde_json::json!({ "symbol": "BRC", "fungible": false, "decimals": 0 }),
            ),
        ])
    }

    // A fungible row carries each sum twice: as stored, and already divided by the
    // token's decimals. The page shows the second form as it is, for example "1.5 SOUL".
    // The token is the full token object of an event, and the only address keeps its
    // name.
    #[test]
    fn fungible_row_formats_its_sums_with_the_token_decimals() {
        let mut soul = record("TokenSend", Some("SOUL"));
        soul.amount_raw = Some("150000000".to_owned());
        soul.sender_amount_raw = Some("0".to_owned());

        let row = summary_row_response(soul, &tokens(), None);

        assert_eq!(row.amount.as_deref(), Some("1.5"));
        assert_eq!(row.amount_raw.as_deref(), Some("150000000"));
        assert_eq!(row.sender_amount.as_deref(), Some("0"));
        assert_eq!(
            row.token.as_ref().and_then(|token| token.get("fungible")),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            row.address.as_ref().map(|address| address.address.as_str()),
            Some("P2KSENDER")
        );
        assert_eq!(
            row.address
                .as_ref()
                .and_then(|address| address.address_name.as_deref()),
            Some("sender")
        );
        assert_eq!(row.event_count, 3);
    }

    // An NFT row has no sums (its values are ids), a row without a token has no
    // token object, and a symbol with no token row still names its token.
    #[test]
    fn rows_without_sums_or_without_a_known_token_stay_readable() {
        let nft = summary_row_response(record("TokenReceive", Some("BRC")), &tokens(), None);
        assert_eq!(nft.amount, None);
        assert_eq!(nft.sender_amount, None);
        assert_eq!(
            nft.token.as_ref().and_then(|token| token.get("fungible")),
            Some(&Value::Bool(false))
        );

        let gas = summary_row_response(record("GasPayment", None), &tokens(), None);
        assert!(gas.token.is_none());

        let mut unknown = record("TokenMint", Some("NEWTOKEN"));
        unknown.amount_raw = Some("42".to_owned());
        let unknown = summary_row_response(unknown, &tokens(), None);
        assert_eq!(
            unknown.token,
            Some(serde_json::json!({ "symbol": "NEWTOKEN" }))
        );
        assert_eq!(unknown.amount.as_deref(), Some("42"));
    }
}
