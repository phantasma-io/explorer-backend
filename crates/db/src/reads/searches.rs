//! Search read model (the `/searches` endpoint).
//!
//! Checks whether a search value exists as each kind of entity. The API layer
//! validates/normalizes the value and turns these booleans into the endpoint's
//! per-target result list.
use crate::*;

/// Whether the search value matches an entity of each kind. `contracts`/`tokens`
/// match case-insensitively (against the lowercased value), and the block and
/// transaction hashes against the upper-cased one (that is how hashes are stored).
#[derive(Debug, Clone, Copy)]
pub struct SearchExistence {
    pub addresses: bool,
    pub blocks: bool,
    pub chains: bool,
    pub contracts: bool,
    pub organizations: bool,
    pub platforms: bool,
    pub tokens: bool,
    pub transactions: bool,
}

/// Probe each entity kind for the search value. `value` is the trimmed input;
/// `value_lower` and `value_upper` are its case-folded forms. Hashes are stored
/// upper-cased, so the block and transaction probes take `value_upper` — the
/// comparison stays `hash = $1` and keeps using the unique hash index, which
/// `upper(hash) = $1` would not.
pub async fn search_existence(
    conn: &mut PgConnection,
    value: &str,
    value_lower: &str,
    value_upper: &str,
) -> Result<SearchExistence, DbError> {
    let addresses = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM addresses WHERE address = $1 OR address_name = $1)",
    )
    .bind(value)
    .fetch_one(&mut *conn)
    .await?;
    // A block search value is either a hash or a height; they never overlap in
    // shape (a height parses as a non-negative integer, a hash does not). Probe
    // the matching column directly so each branch stays sargable: `hash = $1`
    // uses the unique hash index and `height = $1` the height index. The former
    // `hash = $1 OR height::text = $1` formulation forced a full table scan.
    let blocks = match value.parse::<i64>() {
        Ok(height) if height >= 0 => {
            sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM blocks WHERE height = $1)")
                .bind(height)
                .fetch_one(&mut *conn)
                .await?
        }
        _ => block_hash_exists(&mut *conn, value_upper).await?,
    };
    let chains =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM chains WHERE name = $1)")
            .bind(value)
            .fetch_one(&mut *conn)
            .await?;
    let contracts = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM contracts WHERE lower(hash) = $1)",
    )
    .bind(value_lower)
    .fetch_one(&mut *conn)
    .await?;
    let organizations = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM organizations WHERE name = $1 OR organization_id = $1)",
    )
    .bind(value)
    .fetch_one(&mut *conn)
    .await?;
    let platforms =
        sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM platforms WHERE name = $1)")
            .bind(value)
            .fetch_one(&mut *conn)
            .await?;
    let tokens = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (SELECT 1 FROM tokens WHERE lower(symbol) = $1)",
    )
    .bind(value_lower)
    .fetch_one(&mut *conn)
    .await?;
    let transactions = transaction_hash_exists(&mut *conn, value_upper).await?;

    Ok(SearchExistence {
        addresses,
        blocks,
        chains,
        contracts,
        organizations,
        platforms,
        tokens,
        transactions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use uuid::Uuid;

    // `/searches` must find a block and a transaction from a hash typed in any
    // case: hashes are stored upper-cased and the probes compare with `=`, so the
    // upper-cased form of the input is what they take. Runs inside a rolled-back
    // transaction.
    #[tokio::test]
    async fn search_finds_a_hash_typed_in_lowercase() -> Result<(), Box<dyn std::error::Error>> {
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
        let block_hash = format!("{suffix}{suffix}").to_uppercase();
        let tx_hash = format!("{suffix}{suffix}").to_uppercase().replace('0', "F");

        let block = upsert_block(
            &mut tx,
            &mut crate::ProjectionDimensionCache::new(),
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_800_000),
                hash: block_hash.clone(),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: None,
                timestamp_unix_seconds: 1_800_800_000,
                reward: None,
            },
        )
        .await?;
        upsert_transaction(
            &mut tx,
            TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index: 0,
                hash: tx_hash.clone(),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee_raw: None,
                gas_price_raw: None,
                gas_limit_raw: None,
                sender: None,
                gas_payer: None,
                gas_target: None,
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;

        for hash in [&block_hash, &tx_hash] {
            let typed = hash.to_lowercase();
            let found = search_existence(&mut tx, &typed, &typed, &typed.to_uppercase()).await?;
            if hash == &block_hash {
                assert!(
                    found.blocks,
                    "the block must be found from a lowercase hash"
                );
            } else {
                assert!(
                    found.transactions,
                    "the transaction must be found from a lowercase hash"
                );
            }
        }

        tx.rollback().await?;
        Ok(())
    }
}
