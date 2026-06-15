use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;
use tracing::info;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Compare safe metadata between two Explorer databases"
)]
struct Args {
    #[arg(long, env = "EXPLORER_REFERENCE_DATABASE_URL")]
    reference_database_url: Option<String>,
    #[arg(long, env = "EXPLORER_CANDIDATE_DATABASE_URL")]
    candidate_database_url: Option<String>,
    #[command(subcommand)]
    command: ParityCommand,
}

#[derive(Debug, Subcommand)]
enum ParityCommand {
    /// Compare row counts for any table with identical names in both databases.
    Count {
        #[arg(long)]
        table: String,
    },
    /// Compare semantic block/transaction/event digests for a chain height range.
    BlockRange {
        #[arg(long, default_value = "main")]
        chain: String,
        #[arg(long)]
        from: i64,
        #[arg(long)]
        to: i64,
        #[arg(long, value_enum, default_value_t = BlockRangeMode::Semantic)]
        mode: BlockRangeMode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum BlockRangeMode {
    /// Compare blockchain/API-visible contents and ignore insertion-order surrogate IDs.
    Semantic,
    /// Include legacy surrogate IDs to verify strict C# insertion-order parity.
    StrictIds,
}

#[derive(Debug, Serialize)]
struct CountParityReport {
    table: String,
    reference_count: i64,
    candidate_count: i64,
    matches: bool,
}

#[derive(Debug, Serialize)]
struct BlockRangeParityReport {
    chain: String,
    from: i64,
    to: i64,
    mode: BlockRangeMode,
    tables: Vec<TableDigestParityReport>,
    matches: bool,
}

#[derive(Debug, Serialize)]
struct TableDigestParityReport {
    table: String,
    reference_count: i64,
    candidate_count: i64,
    reference_digest: String,
    candidate_digest: String,
    matches: bool,
}

#[derive(Debug)]
struct TableDigest {
    table: &'static str,
    row_count: i64,
    digest: String,
}

#[derive(Debug, Clone, Copy)]
enum ExplorerSchema {
    Renamed,
    LegacyCsharp,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    explorer_runtime::init_tracing();

    let args = Args::parse();

    match args.command {
        ParityCommand::Count { table } => {
            let (reference, candidate) = connect_pair(
                args.reference_database_url.as_deref(),
                args.candidate_database_url.as_deref(),
            )
            .await?;
            let reference_count = table_count(&reference, &table).await?;
            let candidate_count = table_count(&candidate, &table).await?;
            let report = CountParityReport {
                table,
                reference_count,
                candidate_count,
                matches: reference_count == candidate_count,
            };

            info!(?report, "count parity completed");
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        ParityCommand::BlockRange {
            chain,
            from,
            to,
            mode,
        } => {
            let (reference, candidate) = connect_pair(
                args.reference_database_url.as_deref(),
                args.candidate_database_url.as_deref(),
            )
            .await?;
            let reference_tables = block_range_digests(&reference, &chain, from, to, mode).await?;
            let candidate_tables = block_range_digests(&candidate, &chain, from, to, mode).await?;
            let tables = reference_tables
                .iter()
                .zip(candidate_tables.iter())
                .map(|(reference, candidate)| {
                    let matches = reference.row_count == candidate.row_count
                        && reference.digest == candidate.digest;
                    TableDigestParityReport {
                        table: reference.table.to_owned(),
                        reference_count: reference.row_count,
                        candidate_count: candidate.row_count,
                        reference_digest: reference.digest.clone(),
                        candidate_digest: candidate.digest.clone(),
                        matches,
                    }
                })
                .collect::<Vec<_>>();
            let matches = tables.iter().all(|table| table.matches);
            let report = BlockRangeParityReport {
                chain,
                from,
                to,
                mode,
                tables,
                matches,
            };

            info!(?report, "block range parity completed");
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
    }

    Ok(())
}

async fn connect_pair(
    reference_database_url: Option<&str>,
    candidate_database_url: Option<&str>,
) -> anyhow::Result<(sqlx::PgPool, sqlx::PgPool)> {
    let reference_database_url = reference_database_url
        .context("missing --reference-database-url for this parity command")?;
    let candidate_database_url = candidate_database_url
        .context("missing --candidate-database-url for this parity command")?;
    let reference = PgPoolOptions::new()
        .max_connections(2)
        .connect(reference_database_url)
        .await?;
    let candidate = PgPoolOptions::new()
        .max_connections(2)
        .connect(candidate_database_url)
        .await?;

    Ok((reference, candidate))
}

async fn table_count(pool: &sqlx::PgPool, table: &str) -> anyhow::Result<i64> {
    let sql = format!("SELECT COUNT(*)::bigint FROM {}", quote_identifier(table)?);
    Ok(sqlx::query_scalar(&sql).fetch_one(pool).await?)
}

async fn block_range_digests(
    pool: &sqlx::PgPool,
    chain: &str,
    from: i64,
    to: i64,
    mode: BlockRangeMode,
) -> anyhow::Result<Vec<TableDigest>> {
    let schema = detect_schema(pool).await?;
    let queries = match (schema, mode) {
        (ExplorerSchema::Renamed, BlockRangeMode::Semantic) => BlockRangeDigestQueries {
            blocks: SEMANTIC_BLOCKS_RANGE_DIGEST_SQL,
            transactions: SEMANTIC_TRANSACTIONS_RANGE_DIGEST_SQL,
            events: SEMANTIC_EVENTS_RANGE_DIGEST_SQL,
            address_transactions: SEMANTIC_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL,
        },
        (ExplorerSchema::Renamed, BlockRangeMode::StrictIds) => BlockRangeDigestQueries {
            blocks: STRICT_BLOCKS_RANGE_DIGEST_SQL,
            transactions: STRICT_TRANSACTIONS_RANGE_DIGEST_SQL,
            events: STRICT_EVENTS_RANGE_DIGEST_SQL,
            address_transactions: STRICT_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL,
        },
        (ExplorerSchema::LegacyCsharp, BlockRangeMode::Semantic) => BlockRangeDigestQueries {
            blocks: LEGACY_CSHARP_SEMANTIC_BLOCKS_RANGE_DIGEST_SQL,
            transactions: LEGACY_CSHARP_SEMANTIC_TRANSACTIONS_RANGE_DIGEST_SQL,
            events: LEGACY_CSHARP_SEMANTIC_EVENTS_RANGE_DIGEST_SQL,
            address_transactions: LEGACY_CSHARP_SEMANTIC_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL,
        },
        (ExplorerSchema::LegacyCsharp, BlockRangeMode::StrictIds) => BlockRangeDigestQueries {
            blocks: LEGACY_CSHARP_STRICT_BLOCKS_RANGE_DIGEST_SQL,
            transactions: LEGACY_CSHARP_STRICT_TRANSACTIONS_RANGE_DIGEST_SQL,
            events: LEGACY_CSHARP_STRICT_EVENTS_RANGE_DIGEST_SQL,
            address_transactions: LEGACY_CSHARP_STRICT_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL,
        },
    };

    // Semantic mode ignores insertion-order surrogate IDs so parallel candidate
    // sync can prove blockchain-visible parity. Strict mode keeps those IDs for
    // legacy C# insertion-order parity checks.
    Ok(vec![
        digest_query(pool, "blocks", queries.blocks, chain, from, to).await?,
        digest_query(pool, "transactions", queries.transactions, chain, from, to).await?,
        digest_query(pool, "events", queries.events, chain, from, to).await?,
        digest_query(
            pool,
            "address_transactions",
            queries.address_transactions,
            chain,
            from,
            to,
        )
        .await?,
    ])
}

struct BlockRangeDigestQueries {
    blocks: &'static str,
    transactions: &'static str,
    events: &'static str,
    address_transactions: &'static str,
}

async fn detect_schema(pool: &sqlx::PgPool) -> anyhow::Result<ExplorerSchema> {
    let row = sqlx::query(
        r#"
        SELECT
            to_regclass('public.blocks') IS NOT NULL AS renamed,
            to_regclass('public."Blocks"') IS NOT NULL AS legacy_csharp
        "#,
    )
    .fetch_one(pool)
    .await?;
    let renamed: bool = row.try_get("renamed")?;
    let legacy_csharp: bool = row.try_get("legacy_csharp")?;

    match (renamed, legacy_csharp) {
        (true, false) => Ok(ExplorerSchema::Renamed),
        (false, true) => Ok(ExplorerSchema::LegacyCsharp),
        (true, true) => anyhow::bail!("database exposes both renamed and legacy C# block tables"),
        (false, false) => anyhow::bail!("database is not a recognized Explorer schema"),
    }
}

async fn digest_query(
    pool: &sqlx::PgPool,
    table: &'static str,
    sql: &str,
    chain: &str,
    from: i64,
    to: i64,
) -> anyhow::Result<TableDigest> {
    let row = sqlx::query(sql)
        .bind(chain)
        .bind(from)
        .bind(to)
        .fetch_one(pool)
        .await
        .with_context(|| format!("failed to digest table {table} for {chain} {from}-{to}"))?;

    Ok(TableDigest {
        table,
        row_count: row.try_get("row_count")?,
        digest: row.try_get("digest")?,
    })
}

const SEMANTIC_BLOCKS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block.height,
        ARRAY[
            block.height::text,
            block.hash,
            COALESCE(block.previous_hash, '<NULL>'),
            block.timestamp_unix_seconds::text,
            block.protocol::text,
            COALESCE(chain_address.address, '<NULL>'),
            COALESCE(validator_address.address, '<NULL>'),
            COALESCE(block.reward, '<NULL>')
        ] AS fields
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
    LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height), '')) AS digest
FROM rows
"#;

const SEMANTIC_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block.height,
        tx.tx_index,
        ARRAY[
            block.height::text,
            tx.tx_index::text,
            tx.hash,
            tx.timestamp_unix_seconds::text,
            COALESCE(tx.payload, '<NULL>'),
            COALESCE(tx.script_raw, '<NULL>'),
            COALESCE(tx.result, '<NULL>'),
            COALESCE(tx.fee, '<NULL>'),
            COALESCE(tx.fee_raw, '<NULL>'),
            tx.expiration::text,
            state.name,
            COALESCE(tx.gas_price, '<NULL>'),
            COALESCE(tx.gas_price_raw, '<NULL>'),
            COALESCE(tx.gas_limit, '<NULL>'),
            COALESCE(tx.gas_limit_raw, '<NULL>'),
            COALESCE(sender.address, '<NULL>'),
            COALESCE(gas_payer.address, '<NULL>'),
            COALESCE(gas_target.address, '<NULL>'),
            COALESCE(tx.carbon_tx_type::text, '<NULL>'),
            COALESCE(tx.carbon_tx_data, '<NULL>'),
            COALESCE(tx.debug_comment, '<NULL>')
        ] AS fields
    FROM transactions tx
    JOIN blocks block ON block.id = tx.block_id
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transaction_states state ON state.id = tx.state_id
    LEFT JOIN addresses sender ON sender.id = tx.sender_id
    LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
    LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index), '')) AS digest
FROM rows
"#;

const SEMANTIC_EVENTS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx.id,
        tx.tx_index,
        block.height
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transactions tx ON tx.block_id = block.id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        event.event_index,
        ARRAY[
            range_tx.height::text,
            range_tx.tx_index::text,
            event.event_index::text,
            event.timestamp_unix_seconds::text,
            event.date_unix_seconds::text,
            event_kind.name,
            COALESCE(address.address, '<NULL>'),
            COALESCE(target_address.address, '<NULL>'),
            COALESCE(contract.hash, '<NULL>'),
            COALESCE(event.token_id, '<NULL>'),
            event.nsfw::text,
            event.blacklisted::text,
            COALESCE(event.payload_format, '<NULL>'),
            COALESCE(event.payload_json::text, '<NULL>'),
            COALESCE(event.raw_data, '<NULL>')
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM events event_lookup
        WHERE event_lookup.transaction_id = range_tx.id
        OFFSET 0
    ) event ON TRUE
    JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
                                   AND event_kind.chain_id = event.chain_id
    LEFT JOIN addresses address ON address.id = event.address_id
    LEFT JOIN addresses target_address ON target_address.id = event.target_address_id
    LEFT JOIN contracts contract ON contract.id = event.contract_id
                                AND contract.chain_id = event.chain_id
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, event_index), '')) AS digest
FROM rows
"#;

const SEMANTIC_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx.id,
        tx.tx_index,
        tx.hash,
        block.height
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transactions tx ON tx.block_id = block.id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        address.address,
        ARRAY[
            range_tx.height::text,
            range_tx.tx_index::text,
            range_tx.hash,
            address.address
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM address_transactions address_tx_lookup
        WHERE address_tx_lookup.transaction_id = range_tx.id
        OFFSET 0
    ) address_tx ON TRUE
    JOIN addresses address ON address.id = address_tx.address_id
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, address), '')) AS digest
FROM rows
"#;

const STRICT_BLOCKS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block.id,
        block.height,
        ARRAY[
            block.id::text,
            block.height::text,
            block.hash,
            COALESCE(block.previous_hash, '<NULL>'),
            block.timestamp_unix_seconds::text,
            block.protocol::text,
            COALESCE(chain_address.address, '<NULL>'),
            COALESCE(validator_address.address, '<NULL>'),
            COALESCE(block.reward, '<NULL>')
        ] AS fields
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    LEFT JOIN addresses chain_address ON chain_address.id = block.chain_address_id
    LEFT JOIN addresses validator_address ON validator_address.id = block.validator_address_id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, id), '')) AS digest
FROM rows
"#;

const STRICT_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block.height,
        tx.tx_index,
        tx.id,
        ARRAY[
            tx.id::text,
            block.height::text,
            tx.tx_index::text,
            tx.hash,
            tx.timestamp_unix_seconds::text,
            COALESCE(tx.payload, '<NULL>'),
            COALESCE(tx.script_raw, '<NULL>'),
            COALESCE(tx.result, '<NULL>'),
            COALESCE(tx.fee, '<NULL>'),
            COALESCE(tx.fee_raw, '<NULL>'),
            tx.expiration::text,
            state.name,
            COALESCE(tx.gas_price, '<NULL>'),
            COALESCE(tx.gas_price_raw, '<NULL>'),
            COALESCE(tx.gas_limit, '<NULL>'),
            COALESCE(tx.gas_limit_raw, '<NULL>'),
            COALESCE(sender.address, '<NULL>'),
            COALESCE(gas_payer.address, '<NULL>'),
            COALESCE(gas_target.address, '<NULL>'),
            COALESCE(tx.carbon_tx_type::text, '<NULL>'),
            COALESCE(tx.carbon_tx_data, '<NULL>'),
            COALESCE(tx.debug_comment, '<NULL>')
        ] AS fields
    FROM transactions tx
    JOIN blocks block ON block.id = tx.block_id
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transaction_states state ON state.id = tx.state_id
    LEFT JOIN addresses sender ON sender.id = tx.sender_id
    LEFT JOIN addresses gas_payer ON gas_payer.id = tx.gas_payer_id
    LEFT JOIN addresses gas_target ON gas_target.id = tx.gas_target_id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, id), '')) AS digest
FROM rows
"#;

const STRICT_EVENTS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx.id,
        tx.tx_index,
        block.height
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transactions tx ON tx.block_id = block.id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        event.event_index,
        event.id,
        ARRAY[
            event.id::text,
            range_tx.height::text,
            range_tx.tx_index::text,
            event.event_index::text,
            event.timestamp_unix_seconds::text,
            event.date_unix_seconds::text,
            event_kind.name,
            COALESCE(address.address, '<NULL>'),
            COALESCE(target_address.address, '<NULL>'),
            COALESCE(contract.hash, '<NULL>'),
            COALESCE(event.token_id, '<NULL>'),
            COALESCE(event.burned::text, '<NULL>'),
            event.nsfw::text,
            event.blacklisted::text,
            COALESCE(event.payload_format, '<NULL>'),
            COALESCE(event.payload_json::text, '<NULL>'),
            COALESCE(event.raw_data, '<NULL>')
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM events event_lookup
        WHERE event_lookup.transaction_id = range_tx.id
        OFFSET 0
    ) event ON TRUE
    JOIN event_kinds event_kind ON event_kind.id = event.event_kind_id
                                   AND event_kind.chain_id = event.chain_id
    LEFT JOIN addresses address ON address.id = event.address_id
    LEFT JOIN addresses target_address ON target_address.id = event.target_address_id
    LEFT JOIN contracts contract ON contract.id = event.contract_id
                                AND contract.chain_id = event.chain_id
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, event_index, id), '')) AS digest
FROM rows
"#;

const STRICT_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx.id,
        tx.tx_index,
        tx.hash,
        block.height
    FROM blocks block
    JOIN chains chain ON chain.id = block.chain_id
    JOIN transactions tx ON tx.block_id = block.id
    WHERE chain.name = $1
      AND block.height BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        address_tx.id,
        address.address,
        ARRAY[
            address_tx.id::text,
            range_tx.height::text,
            range_tx.tx_index::text,
            range_tx.hash,
            address.address
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM address_transactions address_tx_lookup
        WHERE address_tx_lookup.transaction_id = range_tx.id
        OFFSET 0
    ) address_tx ON TRUE
    JOIN addresses address ON address.id = address_tx.address_id
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, address, id), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_SEMANTIC_BLOCKS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block."HEIGHT" AS height,
        ARRAY[
            block."HEIGHT"::text,
            block."HASH",
            COALESCE(block."PREVIOUS_HASH", '<NULL>'),
            block."TIMESTAMP_UNIX_SECONDS"::text,
            block."PROTOCOL"::text,
            COALESCE(chain_address."ADDRESS", '<NULL>'),
            COALESCE(validator_address."ADDRESS", '<NULL>'),
            COALESCE(block."REWARD", '<NULL>')
        ] AS fields
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    LEFT JOIN "Addresses" chain_address ON chain_address."ID" = block."ChainAddressId"
    LEFT JOIN "Addresses" validator_address ON validator_address."ID" = block."ValidatorAddressId"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_SEMANTIC_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block."HEIGHT" AS height,
        tx."INDEX" AS tx_index,
        ARRAY[
            block."HEIGHT"::text,
            tx."INDEX"::text,
            tx."HASH",
            tx."TIMESTAMP_UNIX_SECONDS"::text,
            COALESCE(tx."PAYLOAD", '<NULL>'),
            COALESCE(tx."SCRIPT_RAW", '<NULL>'),
            COALESCE(tx."RESULT", '<NULL>'),
            COALESCE(tx."FEE", '<NULL>'),
            COALESCE(tx."FEE_RAW", '<NULL>'),
            tx."EXPIRATION"::text,
            state."NAME",
            COALESCE(tx."GAS_PRICE", '<NULL>'),
            COALESCE(tx."GAS_PRICE_RAW", '<NULL>'),
            COALESCE(tx."GAS_LIMIT", '<NULL>'),
            COALESCE(tx."GAS_LIMIT_RAW", '<NULL>'),
            COALESCE(sender."ADDRESS", '<NULL>'),
            COALESCE(gas_payer."ADDRESS", '<NULL>'),
            COALESCE(gas_target."ADDRESS", '<NULL>'),
            COALESCE(tx."CARBON_TX_TYPE"::text, '<NULL>'),
            COALESCE(tx."CARBON_TX_DATA", '<NULL>'),
            COALESCE(tx."DEBUG_COMMENT", '<NULL>')
        ] AS fields
    FROM "Transactions" tx
    JOIN "Blocks" block ON block."ID" = tx."BlockId"
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "TransactionStates" state ON state."ID" = tx."StateId"
    LEFT JOIN "Addresses" sender ON sender."ID" = tx."SenderId"
    LEFT JOIN "Addresses" gas_payer ON gas_payer."ID" = tx."GasPayerId"
    LEFT JOIN "Addresses" gas_target ON gas_target."ID" = tx."GasTargetId"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_SEMANTIC_EVENTS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx."ID" AS id,
        tx."INDEX" AS tx_index,
        block."HEIGHT" AS height
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "Transactions" tx ON tx."BlockId" = block."ID"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        event."INDEX" AS event_index,
        ARRAY[
            range_tx.height::text,
            range_tx.tx_index::text,
            event."INDEX"::text,
            event."TIMESTAMP_UNIX_SECONDS"::text,
            event."DATE_UNIX_SECONDS"::text,
            event_kind."NAME",
            COALESCE(address."ADDRESS", '<NULL>'),
            COALESCE(target_address."ADDRESS", '<NULL>'),
            COALESCE(contract."HASH", '<NULL>'),
            COALESCE(event."TOKEN_ID", '<NULL>'),
            event."NSFW"::text,
            event."BLACKLISTED"::text,
            COALESCE(event."PAYLOAD_FORMAT", '<NULL>'),
            COALESCE(event."PAYLOAD_JSON"::text, '<NULL>'),
            COALESCE(event."RAW_DATA", '<NULL>')
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM "Events" event_lookup
        WHERE event_lookup."TransactionId" = range_tx.id
        OFFSET 0
    ) event ON TRUE
    JOIN "EventKinds" event_kind ON event_kind."ID" = event."EventKindId"
                                  AND event_kind."ChainId" = event."ChainId"
    LEFT JOIN "Addresses" address ON address."ID" = event."AddressId"
    LEFT JOIN "Addresses" target_address ON target_address."ID" = event."TargetAddressId"
    LEFT JOIN "Contracts" contract ON contract."ID" = event."ContractId"
                                  AND contract."ChainId" = event."ChainId"
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, event_index), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_SEMANTIC_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx."ID" AS id,
        tx."INDEX" AS tx_index,
        tx."HASH" AS hash,
        block."HEIGHT" AS height
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "Transactions" tx ON tx."BlockId" = block."ID"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        address."ADDRESS" AS address,
        ARRAY[
            range_tx.height::text,
            range_tx.tx_index::text,
            range_tx.hash,
            address."ADDRESS"
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM "AddressTransactions" address_tx_lookup
        WHERE address_tx_lookup."TransactionId" = range_tx.id
        OFFSET 0
    ) address_tx ON TRUE
    JOIN "Addresses" address ON address."ID" = address_tx."AddressId"
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, address), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_STRICT_BLOCKS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block."ID" AS id,
        block."HEIGHT" AS height,
        ARRAY[
            block."ID"::text,
            block."HEIGHT"::text,
            block."HASH",
            COALESCE(block."PREVIOUS_HASH", '<NULL>'),
            block."TIMESTAMP_UNIX_SECONDS"::text,
            block."PROTOCOL"::text,
            COALESCE(chain_address."ADDRESS", '<NULL>'),
            COALESCE(validator_address."ADDRESS", '<NULL>'),
            COALESCE(block."REWARD", '<NULL>')
        ] AS fields
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    LEFT JOIN "Addresses" chain_address ON chain_address."ID" = block."ChainAddressId"
    LEFT JOIN "Addresses" validator_address ON validator_address."ID" = block."ValidatorAddressId"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, id), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_STRICT_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH rows AS (
    SELECT
        block."HEIGHT" AS height,
        tx."INDEX" AS tx_index,
        tx."ID" AS id,
        ARRAY[
            tx."ID"::text,
            block."HEIGHT"::text,
            tx."INDEX"::text,
            tx."HASH",
            tx."TIMESTAMP_UNIX_SECONDS"::text,
            COALESCE(tx."PAYLOAD", '<NULL>'),
            COALESCE(tx."SCRIPT_RAW", '<NULL>'),
            COALESCE(tx."RESULT", '<NULL>'),
            COALESCE(tx."FEE", '<NULL>'),
            COALESCE(tx."FEE_RAW", '<NULL>'),
            tx."EXPIRATION"::text,
            state."NAME",
            COALESCE(tx."GAS_PRICE", '<NULL>'),
            COALESCE(tx."GAS_PRICE_RAW", '<NULL>'),
            COALESCE(tx."GAS_LIMIT", '<NULL>'),
            COALESCE(tx."GAS_LIMIT_RAW", '<NULL>'),
            COALESCE(sender."ADDRESS", '<NULL>'),
            COALESCE(gas_payer."ADDRESS", '<NULL>'),
            COALESCE(gas_target."ADDRESS", '<NULL>'),
            COALESCE(tx."CARBON_TX_TYPE"::text, '<NULL>'),
            COALESCE(tx."CARBON_TX_DATA", '<NULL>'),
            COALESCE(tx."DEBUG_COMMENT", '<NULL>')
        ] AS fields
    FROM "Transactions" tx
    JOIN "Blocks" block ON block."ID" = tx."BlockId"
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "TransactionStates" state ON state."ID" = tx."StateId"
    LEFT JOIN "Addresses" sender ON sender."ID" = tx."SenderId"
    LEFT JOIN "Addresses" gas_payer ON gas_payer."ID" = tx."GasPayerId"
    LEFT JOIN "Addresses" gas_target ON gas_target."ID" = tx."GasTargetId"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, id), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_STRICT_EVENTS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx."ID" AS id,
        tx."INDEX" AS tx_index,
        block."HEIGHT" AS height
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "Transactions" tx ON tx."BlockId" = block."ID"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        event."INDEX" AS event_index,
        event."ID" AS id,
        ARRAY[
            event."ID"::text,
            range_tx.height::text,
            range_tx.tx_index::text,
            event."INDEX"::text,
            event."TIMESTAMP_UNIX_SECONDS"::text,
            event."DATE_UNIX_SECONDS"::text,
            event_kind."NAME",
            COALESCE(address."ADDRESS", '<NULL>'),
            COALESCE(target_address."ADDRESS", '<NULL>'),
            COALESCE(contract."HASH", '<NULL>'),
            COALESCE(event."TOKEN_ID", '<NULL>'),
            COALESCE(event."BURNED"::text, '<NULL>'),
            event."NSFW"::text,
            event."BLACKLISTED"::text,
            COALESCE(event."PAYLOAD_FORMAT", '<NULL>'),
            COALESCE(event."PAYLOAD_JSON"::text, '<NULL>'),
            COALESCE(event."RAW_DATA", '<NULL>')
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM "Events" event_lookup
        WHERE event_lookup."TransactionId" = range_tx.id
        OFFSET 0
    ) event ON TRUE
    JOIN "EventKinds" event_kind ON event_kind."ID" = event."EventKindId"
                                  AND event_kind."ChainId" = event."ChainId"
    LEFT JOIN "Addresses" address ON address."ID" = event."AddressId"
    LEFT JOIN "Addresses" target_address ON target_address."ID" = event."TargetAddressId"
    LEFT JOIN "Contracts" contract ON contract."ID" = event."ContractId"
                                  AND contract."ChainId" = event."ChainId"
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, event_index, id), '')) AS digest
FROM rows
"#;

const LEGACY_CSHARP_STRICT_ADDRESS_TRANSACTIONS_RANGE_DIGEST_SQL: &str = r#"
WITH range_transactions AS MATERIALIZED (
    SELECT
        tx."ID" AS id,
        tx."INDEX" AS tx_index,
        tx."HASH" AS hash,
        block."HEIGHT" AS height
    FROM "Blocks" block
    JOIN "Chains" chain ON chain."ID" = block."ChainId"
    JOIN "Transactions" tx ON tx."BlockId" = block."ID"
    WHERE chain."NAME" = $1
      AND block."HEIGHT" BETWEEN $2 AND $3
),
rows AS (
    SELECT
        range_tx.height,
        range_tx.tx_index,
        address_tx."ID" AS id,
        address."ADDRESS" AS address,
        ARRAY[
            address_tx."ID"::text,
            range_tx.height::text,
            range_tx.tx_index::text,
            range_tx.hash,
            address."ADDRESS"
        ] AS fields
    FROM range_transactions range_tx
    JOIN LATERAL (
        SELECT *
        FROM "AddressTransactions" address_tx_lookup
        WHERE address_tx_lookup."TransactionId" = range_tx.id
        OFFSET 0
    ) address_tx ON TRUE
    JOIN "Addresses" address ON address."ID" = address_tx."AddressId"
)
SELECT
    COUNT(*)::bigint AS row_count,
    md5(COALESCE(string_agg(array_to_string(fields, '|'), E'\n' ORDER BY height, tx_index, address, id), '')) AS digest
FROM rows
"#;

fn quote_identifier(identifier: &str) -> anyhow::Result<String> {
    if identifier.is_empty()
        || !identifier
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        anyhow::bail!("unsafe SQL identifier {identifier:?}");
    }

    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_postgres_identifier() -> anyhow::Result<()> {
        // Count parity interpolates table names, so only simple identifiers are
        // accepted before they are quoted into SQL.
        assert_eq!(quote_identifier("blocks")?, "\"blocks\"");
        assert_eq!(
            quote_identifier("address_transactions")?,
            "\"address_transactions\""
        );
        assert!(quote_identifier("blocks; drop table blocks").is_err());
        Ok(())
    }
}
