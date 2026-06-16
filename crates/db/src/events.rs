//! Event projection and its side effects. Replaces a transaction's
//! event rows and applies the C#-parity side effects: contract string-event
//! linking, token create/supply, NFT lifecycle, series lifecycle, infusions,
//! and burn markers.
use super::*;

pub async fn replace_events(
    conn: &mut PgConnection,
    transaction_id: i32,
    events: &[EventUpsert],
) -> Result<(), DbError> {
    let mut cache = ProjectionDimensionCache::new();
    replace_events_cached(conn, &mut cache, transaction_id, events).await
}

pub async fn replace_events_cached(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    transaction_id: i32,
    events: &[EventUpsert],
) -> Result<(), DbError> {
    let existing_event_ids = sqlx::query_as::<_, (i32, i32)>(
        "SELECT event_index, id FROM events WHERE transaction_id = $1",
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect::<HashMap<_, _>>();

    let desired_event_indexes = events
        .iter()
        .map(|event| event.event_index)
        .collect::<Vec<_>>();

    sqlx::query(
        r#"
        DELETE FROM events
        WHERE transaction_id = $1
          AND NOT (event_index = ANY($2))
        "#,
    )
    .bind(transaction_id)
    .bind(&desired_event_indexes)
    .execute(&mut *conn)
    .await?;

    for event in events {
        upsert_event_cached(
            conn,
            cache,
            event,
            existing_event_ids.get(&event.event_index).copied(),
        )
        .await?;
    }
    apply_block_event_side_effects(conn, transaction_id, events).await?;

    Ok(())
}

/// Apply the C#-parity side effects for one transaction's events (token
/// create/supply, contract string-event linking, series lifecycle, NFT
/// lifecycle, token_mint_extended, infusions, burn markers). These read the
/// transaction's already-written events and depend on intra-block order, so they
/// run per transaction in transaction order — never batched or reordered across
/// transactions.
pub(crate) async fn apply_block_event_side_effects(
    conn: &mut PgConnection,
    transaction_id: i32,
    events: &[EventUpsert],
) -> Result<(), DbError> {
    upsert_tokens_for_transaction(conn, transaction_id).await?;
    if events.iter().any(|event| {
        matches!(
            event.event_kind.as_str(),
            "ContractDeploy" | "ContractUpgrade"
        )
    }) {
        apply_contract_string_event_side_effects_for_transaction(conn, transaction_id).await?;
    }
    if events
        .iter()
        .any(|event| event.event_kind == "TokenSeriesCreate")
    {
        upsert_series_for_transaction(conn, transaction_id).await?;
    }
    let has_token_mint_extended = events.iter().any(|event| {
        event
            .payload_json
            .as_ref()
            .is_some_and(|payload| payload.get("token_mint_extended").is_some())
    });
    let has_nft_side_effects = events
        .iter()
        .any(|event| event.token_id.is_some() && is_nft_side_effect_event_kind(&event.event_kind));
    let has_nft_side_effect_candidates = if has_nft_side_effects {
        has_nft_side_effect_candidates(conn, transaction_id).await?
    } else {
        false
    };
    if has_nft_side_effect_candidates {
        apply_nft_side_effects_for_transaction(conn, transaction_id).await?;
    }
    if has_token_mint_extended {
        apply_token_mint_extended_side_effects_for_transaction(conn, transaction_id).await?;
    }
    if events
        .iter()
        .any(|event| event.event_kind == "Infusion" && event.token_id.is_some())
    {
        apply_infusions_for_transaction(conn, transaction_id).await?;
    }
    if events.iter().any(|event| {
        event.token_id.is_some() && matches!(event.event_kind.as_str(), "TokenMint" | "TokenBurn")
    }) {
        apply_series_lifecycle_for_transaction(conn, transaction_id).await?;
    }
    if events.iter().any(|event| event.token_id.is_some()) {
        apply_burn_markers_for_transaction(conn, transaction_id).await?;
    }

    Ok(())
}

/// Write every event of a block, then apply each transaction's side effects in
/// transaction order. Mirrors the C# block flow: events are inserted set-based,
/// then the stateful per-transaction side effects run sequentially (they read the
/// already-written events and depend on intra-block order).
pub async fn project_block_events(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    batches: &[(i32, Vec<EventUpsert>)],
) -> Result<(), DbError> {
    insert_block_events(conn, cache, batches).await?;
    for (transaction_id, events) in batches {
        apply_block_event_side_effects(conn, *transaction_id, events).await?;
    }
    Ok(())
}

/// Insert the event rows for all of a block's transactions. On a fresh projection
/// (no pre-existing rows for these transactions) the rows are written in one
/// set-based `unnest` insert, in (transaction, event_index) order so the serial
/// event ids match the per-transaction insert order. A re-projection (some rows
/// already exist) falls back to the row-by-row upsert that reuses existing ids.
pub(crate) async fn insert_block_events(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    batches: &[(i32, Vec<EventUpsert>)],
) -> Result<(), DbError> {
    let transaction_ids: Vec<i32> = batches.iter().map(|(id, _)| *id).collect();
    if transaction_ids.is_empty() {
        return Ok(());
    }

    let existing_event_ids = sqlx::query_as::<_, (i32, i32, i32)>(
        "SELECT transaction_id, event_index, id FROM events WHERE transaction_id = ANY($1)",
    )
    .bind(&transaction_ids)
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|(transaction_id, event_index, id)| ((transaction_id, event_index), id))
    .collect::<HashMap<_, _>>();

    // Drop rows whose (transaction_id, event_index) is no longer desired.
    let desired_transaction_ids: Vec<i32> = batches
        .iter()
        .flat_map(|(transaction_id, events)| events.iter().map(move |_| *transaction_id))
        .collect();
    let desired_event_indexes: Vec<i32> = batches
        .iter()
        .flat_map(|(_, events)| events.iter().map(|event| event.event_index))
        .collect();
    sqlx::query(
        r#"
        DELETE FROM events
        WHERE transaction_id = ANY($1)
          AND NOT EXISTS (
              SELECT 1
              FROM unnest($2::int[], $3::int[]) AS desired(transaction_id, event_index)
              WHERE desired.transaction_id = events.transaction_id
                AND desired.event_index = events.event_index
          )
        "#,
    )
    .bind(&transaction_ids)
    .bind(&desired_transaction_ids)
    .bind(&desired_event_indexes)
    .execute(&mut *conn)
    .await?;

    if !existing_event_ids.is_empty() {
        // Re-projection: row-by-row upsert reusing existing ids.
        for (transaction_id, events) in batches {
            for event in events {
                let existing = existing_event_ids
                    .get(&(*transaction_id, event.event_index))
                    .copied();
                upsert_event_cached(conn, cache, event, existing).await?;
            }
        }
        return Ok(());
    }

    // Fresh projection: resolve dimensions (cached, in order) and insert all rows
    // in one pass. The id column is omitted so the serial default assigns ids in
    // `unnest` order, which is the (transaction, event_index) order.
    let mut timestamp = Vec::new();
    let mut date = Vec::new();
    let mut event_index = Vec::new();
    let mut token_id = Vec::new();
    let mut burned = Vec::new();
    let mut nsfw = Vec::new();
    let mut blacklisted = Vec::new();
    let mut address_id = Vec::new();
    let mut chain_id = Vec::new();
    let mut contract_id = Vec::new();
    let mut event_transaction_id = Vec::new();
    let mut event_kind_id = Vec::new();
    let mut target_address_id = Vec::new();
    let mut payload_format = Vec::new();
    let mut payload_json = Vec::new();
    let mut raw_data = Vec::new();
    for (_, events) in batches {
        for event in events {
            let kind_id = cache
                .event_kind_id(conn, event.chain_id, &event.event_kind)
                .await?;
            let resolved_address_id = cache
                .address_id(
                    conn,
                    event.chain_id,
                    event.address.as_deref().unwrap_or("NULL"),
                )
                .await?;
            let resolved_target_id = match event.target_address.as_deref().and_then(usable_address)
            {
                Some(address) => Some(cache.address_id(conn, event.chain_id, address).await?),
                None => None,
            };
            let resolved_contract_id = cache
                .contract_id(
                    conn,
                    event.chain_id,
                    event.contract.as_deref().unwrap_or("unknown"),
                )
                .await?;
            timestamp.push(event.timestamp_unix_seconds);
            date.push(event.date_unix_seconds);
            event_index.push(event.event_index);
            token_id.push(event.token_id.clone());
            burned.push(event.burned);
            nsfw.push(event.nsfw);
            blacklisted.push(event.blacklisted);
            address_id.push(resolved_address_id);
            chain_id.push(event.chain_id);
            contract_id.push(resolved_contract_id);
            event_transaction_id.push(event.transaction_id);
            event_kind_id.push(kind_id);
            target_address_id.push(resolved_target_id);
            payload_format.push(event.payload_format.clone());
            payload_json.push(event.payload_json.as_ref().map(|value| value.to_string()));
            raw_data.push(event.raw_data.clone());
        }
    }
    if event_index.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO events (
            dm_unix_seconds,
            timestamp_unix_seconds,
            date_unix_seconds,
            event_index,
            token_id,
            burned,
            nsfw,
            blacklisted,
            address_id,
            chain_id,
            contract_id,
            transaction_id,
            event_kind_id,
            target_address_id,
            payload_format,
            payload_json,
            raw_data
        )
        SELECT
            t.timestamp, t.timestamp, t.date, t.event_index, t.token_id, t.burned, t.nsfw,
            t.blacklisted, t.address_id, t.chain_id, t.contract_id, t.transaction_id,
            t.event_kind_id, t.target_address_id, t.payload_format, t.payload_json::jsonb,
            t.raw_data
        FROM unnest(
            $1::bigint[], $2::bigint[], $3::int[], $4::text[], $5::bool[], $6::bool[], $7::bool[],
            $8::int[], $9::int[], $10::int[], $11::int[], $12::int[], $13::int[], $14::text[],
            $15::text[], $16::text[]
        ) AS t(
            timestamp, date, event_index, token_id, burned, nsfw, blacklisted, address_id,
            chain_id, contract_id, transaction_id, event_kind_id, target_address_id,
            payload_format, payload_json, raw_data
        )
        ORDER BY t.transaction_id, t.event_index
        "#,
    )
    .bind(&timestamp)
    .bind(&date)
    .bind(&event_index)
    .bind(&token_id)
    .bind(&burned)
    .bind(&nsfw)
    .bind(&blacklisted)
    .bind(&address_id)
    .bind(&chain_id)
    .bind(&contract_id)
    .bind(&event_transaction_id)
    .bind(&event_kind_id)
    .bind(&target_address_id)
    .bind(&payload_format)
    .bind(&payload_json)
    .bind(&raw_data)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

pub async fn reconcile_contract_string_event_side_effects(
    conn: &mut PgConnection,
    chain_id: i32,
    min_block_height_exclusive: Option<BlockHeight>,
) -> Result<ContractStringEventSideEffectReport, DbError> {
    apply_contract_string_event_side_effects(conn, Some(chain_id), None, min_block_height_exclusive)
        .await
}

async fn apply_contract_string_event_side_effects_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<ContractStringEventSideEffectReport, DbError> {
    apply_contract_string_event_side_effects(conn, None, Some(transaction_id), None).await
}

async fn apply_contract_string_event_side_effects(
    conn: &mut PgConnection,
    chain_id: Option<i32>,
    transaction_id: Option<i32>,
    min_block_height_exclusive: Option<BlockHeight>,
) -> Result<ContractStringEventSideEffectReport, DbError> {
    let min_block_height = min_block_height_exclusive
        .map(block_height_to_i64)
        .transpose()?;
    let upserted_contracts = sqlx::query(
        r#"
        WITH string_contract_events AS (
            SELECT DISTINCT
                event.chain_id,
                NULLIF(BTRIM(event.payload_json #>> '{string_event,string_value}'), '') AS contract_hash
            FROM events event
            JOIN transactions tx ON tx.id = event.transaction_id
            JOIN blocks block ON block.id = tx.block_id
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            WHERE ($1::integer IS NULL OR event.chain_id = $1)
              AND ($2::integer IS NULL OR event.transaction_id = $2)
              AND ($3::bigint IS NULL OR block.height > $3)
              AND event_kind.name IN ('ContractDeploy', 'ContractUpgrade')
        )
        INSERT INTO contracts (name, hash, symbol, chain_id, last_updated_unix_seconds)
        SELECT contract_hash, contract_hash, contract_hash, chain_id, 0
        FROM string_contract_events
        WHERE contract_hash IS NOT NULL
        ON CONFLICT (chain_id, hash) DO UPDATE SET
            name = CASE
                WHEN contracts.name IS NULL OR contracts.name = '' THEN EXCLUDED.name
                ELSE contracts.name
            END,
            symbol = CASE
                WHEN contracts.symbol IS NULL OR contracts.symbol = '' THEN EXCLUDED.symbol
                ELSE contracts.symbol
            END
        "#,
    )
    .bind(chain_id)
    .bind(transaction_id)
    .bind(min_block_height)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    let linked_contract_creates = sqlx::query(
        r#"
        WITH deploy_events AS (
            SELECT DISTINCT ON (event.chain_id, contract_hash)
                event.chain_id,
                contract_hash,
                event.id AS event_id
            FROM (
                SELECT
                    event.id,
                    event.chain_id,
                    NULLIF(BTRIM(event.payload_json #>> '{string_event,string_value}'), '') AS contract_hash
                FROM events event
                JOIN transactions tx ON tx.id = event.transaction_id
                JOIN blocks block ON block.id = tx.block_id
                JOIN event_kinds event_kind
                  ON event_kind.id = event.event_kind_id
                 AND event_kind.chain_id = event.chain_id
                WHERE ($1::integer IS NULL OR event.chain_id = $1)
                  AND ($2::integer IS NULL OR event.transaction_id = $2)
                  AND ($3::bigint IS NULL OR block.height > $3)
                  AND event_kind.name = 'ContractDeploy'
            ) event
            WHERE contract_hash IS NOT NULL
            ORDER BY event.chain_id, contract_hash, event.id DESC
        )
        UPDATE contracts contract
        SET create_event_id = deploy_events.event_id
        FROM deploy_events
        WHERE contract.chain_id = deploy_events.chain_id
          AND contract.hash = deploy_events.contract_hash
          AND contract.create_event_id IS DISTINCT FROM deploy_events.event_id
        "#,
    )
    .bind(chain_id)
    .bind(transaction_id)
    .bind(min_block_height)
    .execute(&mut *conn)
    .await?
    .rows_affected();

    Ok(ContractStringEventSideEffectReport {
        upserted_contracts,
        linked_contract_creates,
    })
}

async fn has_nft_side_effect_candidates(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<bool, DbError> {
    let has_candidates = sqlx::query_scalar::<_, bool>(
        r#"
        WITH tx_events AS MATERIALIZED (
            SELECT
                chain_id,
                contract_id,
                token_id,
                event_kind_id
            FROM events
            WHERE transaction_id = $1
              AND token_id IS NOT NULL
        )
        SELECT EXISTS (
            SELECT 1
            FROM tx_events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens token
              ON token.chain_id = event.chain_id
             AND token.contract_id = event.contract_id
            WHERE token.fungible IS FALSE
              AND event_kind.name IN (
                  'TokenMint',
                  'TokenClaim',
                  'TokenBurn',
                  'TokenSend',
                  'TokenReceive',
                  'TokenStake',
                  'CrownRewards',
                  'Inflation',
                  'Infusion',
                  'OrderCancelled',
                  'OrderClosed',
                  'OrderCreated',
                  'OrderFilled',
                  'OrderBid'
              )
        )
        "#,
    )
    .bind(transaction_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(has_candidates)
}

pub(crate) async fn upsert_event_cached(
    conn: &mut PgConnection,
    cache: &mut ProjectionDimensionCache,
    event: &EventUpsert,
    id: Option<i32>,
) -> Result<(), DbError> {
    let event_kind_id = cache
        .event_kind_id(conn, event.chain_id, &event.event_kind)
        .await?;
    let address = event.address.as_deref().unwrap_or("NULL");
    let address_id = cache.address_id(conn, event.chain_id, address).await?;
    let target_address_id = match event.target_address.as_deref().and_then(usable_address) {
        Some(address) => Some(cache.address_id(conn, event.chain_id, address).await?),
        None => None,
    };
    let contract = event.contract.as_deref().unwrap_or("unknown");
    let contract_id = cache.contract_id(conn, event.chain_id, contract).await?;

    sqlx::query(
        r#"
        INSERT INTO events (
            id,
            dm_unix_seconds,
            timestamp_unix_seconds,
            date_unix_seconds,
            event_index,
            token_id,
            burned,
            nsfw,
            blacklisted,
            address_id,
            chain_id,
            contract_id,
            transaction_id,
            event_kind_id,
            target_address_id,
            payload_format,
            payload_json,
            raw_data
        )
        VALUES (
            COALESCE($1, nextval(pg_get_serial_sequence('events', 'id'))::integer),
            $2, $3, $4, $5, $6, $7, $8, $9,
            $10, $11, $12, $13, $14, $15, $16, $17, $18
        )
        ON CONFLICT (id) DO UPDATE SET
            dm_unix_seconds = EXCLUDED.dm_unix_seconds,
            timestamp_unix_seconds = EXCLUDED.timestamp_unix_seconds,
            date_unix_seconds = EXCLUDED.date_unix_seconds,
            event_index = EXCLUDED.event_index,
            token_id = EXCLUDED.token_id,
            burned = EXCLUDED.burned,
            nsfw = EXCLUDED.nsfw,
            blacklisted = EXCLUDED.blacklisted,
            address_id = EXCLUDED.address_id,
            chain_id = EXCLUDED.chain_id,
            contract_id = EXCLUDED.contract_id,
            transaction_id = EXCLUDED.transaction_id,
            event_kind_id = EXCLUDED.event_kind_id,
            nft_id = NULL,
            target_address_id = EXCLUDED.target_address_id,
            payload_format = EXCLUDED.payload_format,
            payload_json = EXCLUDED.payload_json,
            raw_data = EXCLUDED.raw_data
        "#,
    )
    .bind(id)
    .bind(event.timestamp_unix_seconds)
    .bind(event.timestamp_unix_seconds)
    .bind(event.date_unix_seconds)
    .bind(event.event_index)
    .bind(&event.token_id)
    .bind(event.burned)
    .bind(event.nsfw)
    .bind(event.blacklisted)
    .bind(address_id)
    .bind(event.chain_id)
    .bind(contract_id)
    .bind(event.transaction_id)
    .bind(event_kind_id)
    .bind(target_address_id)
    .bind(&event.payload_format)
    .bind(&event.payload_json)
    .bind(&event.raw_data)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn upsert_tokens_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    // C# TokenCreate processing has two side effects: it stores the event and
    // upserts the token/contract row linked back to that event. Keeping the
    // side effect here prevents event parity from hiding a broken token table.
    sqlx::query(
        r#"
        WITH token_create_events AS (
            SELECT
                event.id AS event_id,
                event.chain_id,
                event.address_id,
                event.payload_json->'token_create_event' AS payload
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            WHERE event.transaction_id = $1
              AND event_kind.name = 'TokenCreate'
              AND jsonb_typeof(event.payload_json->'token_create_event') = 'object'
        ),
        token_create_sources AS (
            SELECT
                event_id,
                chain_id,
                address_id,
                payload,
                COALESCE(
                    NULLIF(BTRIM(payload->>'flags'), ''),
                    NULLIF(BTRIM(payload #>> '{metadata,flags}'), ''),
                    NULLIF(BTRIM(payload #>> '{metadata,token_flags}'), '')
                ) AS flags_raw,
                COALESCE((payload->>'is_non_fungible')::boolean, FALSE) AS is_non_fungible
            FROM token_create_events
        ),
        token_create_values AS (
            SELECT
                event_id,
                chain_id,
                address_id,
                NULLIF(payload->>'symbol', '') AS symbol,
                CASE
                    WHEN COALESCE(payload->>'carbon_token_id', '') ~ '^[0-9]+$'
                    THEN (payload->>'carbon_token_id')::bigint
                    ELSE NULL
                END AS carbon_id,
                COALESCE(
                    NULLIF(payload->>'name', ''),
                    NULLIF(payload #>> '{metadata,name}', ''),
                    NULLIF(payload #>> '{metadata,token_name}', ''),
                    NULLIF(payload->>'symbol', '')
                ) AS name,
                (payload->>'decimals')::integer AS decimals,
                NULLIF(payload->>'max_supply', '') AS max_supply_raw,
                is_non_fungible,
                COALESCE(
                    (payload->>'fungible')::boolean,
                    CASE
                        WHEN flags_raw IS NOT NULL THEN flags_raw ~* '(^|[|,;[:space:]])Fungible($|[|,;[:space:]])'
                        ELSE NOT is_non_fungible
                    END
                ) AS fungible,
                COALESCE(
                    (payload->>'transferable')::boolean,
                    CASE
                        WHEN flags_raw IS NOT NULL THEN flags_raw ~* '(^|[|,;[:space:]])Transferable($|[|,;[:space:]])'
                        ELSE TRUE
                    END
                ) AS transferable,
                COALESCE(
                    (payload->>'finite')::boolean,
                    CASE
                        WHEN flags_raw IS NOT NULL THEN flags_raw ~* '(^|[|,;[:space:]])Finite($|[|,;[:space:]])'
                        ELSE COALESCE(NULLIF(payload->>'max_supply', ''), '0') ~ '[1-9]'
                    END
                ) AS finite,
                COALESCE(
                    (payload->>'divisible')::boolean,
                    CASE
                        WHEN flags_raw IS NOT NULL THEN flags_raw ~* '(^|[|,;[:space:]])Divisible($|[|,;[:space:]])'
                        ELSE NOT is_non_fungible
                    END
                ) AS divisible,
                COALESCE(
                    (payload->>'fuel')::boolean,
                    flags_raw IS NOT NULL AND flags_raw ~* '(^|[|,;[:space:]])Fuel($|[|,;[:space:]])'
                ) AS fuel,
                COALESCE(
                    (payload->>'stakable')::boolean,
                    flags_raw IS NOT NULL AND flags_raw ~* '(^|[|,;[:space:]])Stakable($|[|,;[:space:]])'
                ) AS stakable,
                COALESCE(
                    (payload->>'fiat')::boolean,
                    flags_raw IS NOT NULL AND flags_raw ~* '(^|[|,;[:space:]])Fiat($|[|,;[:space:]])'
                ) AS fiat,
                COALESCE(
                    (payload->>'swappable')::boolean,
                    flags_raw IS NOT NULL AND flags_raw ~* '(^|[|,;[:space:]])Swappable($|[|,;[:space:]])'
                ) AS swappable,
                COALESCE(
                    (payload->>'burnable')::boolean,
                    flags_raw IS NOT NULL AND flags_raw ~* '(^|[|,;[:space:]])Burnable($|[|,;[:space:]])'
                ) AS burnable,
                COALESCE(
                    (payload->>'mintable')::boolean,
                    CASE
                        WHEN flags_raw IS NOT NULL THEN flags_raw ~* '(^|[|,;[:space:]])Mintable($|[|,;[:space:]])'
                        ELSE TRUE
                    END
                ) AS mintable
            FROM token_create_sources
            WHERE NULLIF(payload->>'symbol', '') IS NOT NULL
              AND COALESCE(payload->>'decimals', '') ~ '^[0-9]+$'
              AND COALESCE(payload->>'max_supply', '') ~ '^[0-9]+$'
        ),
        token_schema_values AS (
            SELECT
                event_id,
                CASE
                    WHEN COALESCE(payload->>'carbon_token_schemas', '') ~ '^[0-9A-Fa-f]+$'
                    THEN decode(payload->>'carbon_token_schemas', 'hex')
                    ELSE decode('', 'hex')
                END AS carbon_token_schemas
            FROM token_create_events
        ),
        token_contracts AS (
            INSERT INTO contracts (name, hash, symbol, chain_id, last_updated_unix_seconds)
            SELECT DISTINCT symbol, symbol, symbol, chain_id, 0
            FROM token_create_values
            ON CONFLICT (chain_id, hash) DO UPDATE SET
                name = CASE
                    WHEN contracts.name IS NULL OR contracts.name = '' THEN EXCLUDED.name
                    ELSE contracts.name
                END,
                symbol = CASE
                    WHEN contracts.symbol IS NULL OR contracts.symbol = '' THEN EXCLUDED.symbol
                    ELSE contracts.symbol
                END
            RETURNING id, chain_id, symbol
        ),
        token_rows AS (
            SELECT
                token_create_values.*,
                token_contracts.id AS contract_id,
                token_schema_values.carbon_token_schemas,
                CASE
                    WHEN max_supply_raw = '0' OR decimals = 0 THEN max_supply_raw
                    WHEN length(max_supply_raw) <= decimals THEN
                        COALESCE(
                            NULLIF(
                                regexp_replace(
                                    trim(trailing '0' FROM '0.' || repeat('0', decimals - length(max_supply_raw)) || max_supply_raw),
                                    '\.$',
                                    ''
                                ),
                                ''
                            ),
                            '0'
                        )
                    ELSE
                        COALESCE(
                            NULLIF(
                                regexp_replace(
                                    trim(trailing '0' FROM left(max_supply_raw, length(max_supply_raw) - decimals) || '.' || right(max_supply_raw, decimals)),
                                    '\.$',
                                    ''
                                ),
                                ''
                            ),
                            '0'
                        )
                END AS max_supply
            FROM token_create_values
            JOIN token_contracts
              ON token_contracts.chain_id = token_create_values.chain_id
             AND lower(token_contracts.symbol) = lower(token_create_values.symbol)
            LEFT JOIN token_schema_values
              ON token_schema_values.event_id = token_create_values.event_id
        )
        INSERT INTO tokens (
            symbol,
            fungible,
            transferable,
            finite,
            divisible,
            fuel,
            stakable,
            fiat,
            swappable,
            burnable,
            decimals,
            current_supply,
            max_supply,
            burned_supply,
            script_raw,
            address_id,
            owner_id,
            price_usd,
            price_eur,
            price_gbp,
            price_jpy,
            price_cad,
            price_aud,
            price_cny,
            price_rub,
            chain_id,
            contract_id,
            create_event_id,
            burned_supply_raw,
            current_supply_raw,
            max_supply_raw,
            mintable,
            name,
            carbon_token_schemas,
            carbon_id
        )
        SELECT
            symbol,
            fungible,
            transferable,
            finite,
            divisible,
            fuel,
            stakable,
            fiat,
            swappable,
            burnable,
            decimals,
            '0',
            max_supply,
            '0',
            NULL,
            address_id,
            address_id,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            chain_id,
            contract_id,
            event_id,
            '0',
            '0',
            max_supply_raw,
            mintable,
            name,
            carbon_token_schemas,
            carbon_id
        FROM token_rows
        ON CONFLICT (chain_id, contract_id, symbol) DO UPDATE SET
            carbon_id = COALESCE(EXCLUDED.carbon_id, tokens.carbon_id),
            name = EXCLUDED.name,
            decimals = EXCLUDED.decimals,
            fungible = EXCLUDED.fungible,
            transferable = EXCLUDED.transferable,
            finite = EXCLUDED.finite,
            divisible = EXCLUDED.divisible,
            fuel = EXCLUDED.fuel,
            stakable = EXCLUDED.stakable,
            fiat = EXCLUDED.fiat,
            swappable = EXCLUDED.swappable,
            burnable = EXCLUDED.burnable,
            mintable = EXCLUDED.mintable,
            address_id = EXCLUDED.address_id,
            owner_id = EXCLUDED.owner_id,
            max_supply = EXCLUDED.max_supply,
            max_supply_raw = EXCLUDED.max_supply_raw,
            create_event_id = EXCLUDED.create_event_id,
            carbon_token_schemas = CASE
                WHEN EXCLUDED.carbon_token_schemas IS NULL THEN tokens.carbon_token_schemas
                WHEN octet_length(EXCLUDED.carbon_token_schemas) > 0
                THEN EXCLUDED.carbon_token_schemas
                WHEN tokens.carbon_token_schemas IS NULL THEN EXCLUDED.carbon_token_schemas
                ELSE tokens.carbon_token_schemas
            END
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn upsert_series_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        WITH series_events AS MATERIALIZED (
            SELECT
                event.chain_id,
                event.contract_id,
                event.address_id,
                event.timestamp_unix_seconds,
                event.payload_json->'token_series_event' AS payload
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            WHERE event.transaction_id = $1
              AND event_kind.name = 'TokenSeriesCreate'
              AND jsonb_typeof(event.payload_json->'token_series_event') = 'object'
        ),
        series_metadata AS (
            SELECT
                series_events.*,
                metadata.metadata_json
            FROM series_events
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(entry.key, entry.value #>> '{}') AS metadata_json
                FROM jsonb_each(
                    CASE
                        WHEN jsonb_typeof(series_events.payload->'metadata') = 'object'
                        THEN series_events.payload->'metadata'
                        ELSE '{}'::jsonb
                    END
                ) AS entry(key, value)
                WHERE NULLIF(entry.value #>> '{}', '') IS NOT NULL
            ) metadata ON TRUE
        ),
        series_values AS (
            SELECT
                contract_id,
                address_id,
                chain_id,
                timestamp_unix_seconds,
                NULLIF(payload->>'series_id', '') AS series_id,
                NULLIF(payload->>'token', '') AS token_symbol,
                NULLIF(payload->>'owner', '') AS owner_address,
                metadata_json,
                CASE
                    WHEN COALESCE(payload->>'max_supply', '') ~ '^[0-9]+$'
                    THEN LEAST((payload->>'max_supply')::numeric, 2147483647)::integer
                    ELSE 0
                END AS max_supply,
                NULLIF(payload->>'carbon_series_id', '') AS carbon_series_id,
                NULLIF(COALESCE(metadata_json->>'mode', metadata_json->>'seriesMode'), '') AS mode_raw
            FROM series_metadata
            WHERE NULLIF(payload->>'series_id', '') IS NOT NULL
        ),
        normalized_series_values AS (
            SELECT
                *,
                CASE
                    WHEN mode_raw ~ '^[+-]?[0-9]+$'
                    THEN CASE WHEN mode_raw::numeric = 0 THEN 'Unique' ELSE 'Duplicated' END
                    ELSE mode_raw
                END AS mode_name,
                CASE
                    WHEN carbon_series_id ~ '^[0-9]+$'
                         AND carbon_series_id::numeric > 0
                         AND token_symbol IS NOT NULL
                    THEN 'Series #' || carbon_series_id || ' for ' || token_symbol
                    ELSE NULL
                END AS default_name
            FROM series_values
        ),
        upserted_modes AS (
            INSERT INTO series_modes (mode_name)
            SELECT DISTINCT mode_name
            FROM normalized_series_values
            WHERE mode_name IS NOT NULL
            ON CONFLICT (mode_name) DO UPDATE SET
                mode_name = EXCLUDED.mode_name
            RETURNING id, mode_name
        ),
        upserted_creators AS (
            INSERT INTO addresses (
                address,
                chain_id,
                name_last_updated_unix_seconds,
                stake_timestamp,
                storage_available,
                storage_used,
                total_soul_amount,
                balance_dirty_block
            )
            SELECT DISTINCT
                owner_address,
                chain_id,
                0,
                0,
                0,
                0,
                0,
                0
            FROM normalized_series_values
            WHERE owner_address IS NOT NULL
              AND owner_address <> 'NULL'
            ON CONFLICT (chain_id, address) DO UPDATE SET
                address = addresses.address
            RETURNING id, chain_id, address
        )
        INSERT INTO series (
            contract_id,
            series_id,
            current_supply,
            max_supply,
            series_mode_id,
            name,
            royalties,
            type,
            has_locked,
            nsfw,
            blacklisted,
            dm_unix_seconds,
            creator_address_id,
            metadata,
            series_created_unix_seconds
        )
        SELECT
            series_values.contract_id,
            series_values.series_id,
            0,
            series_values.max_supply,
            upserted_modes.id,
            series_values.default_name,
            0,
            0,
            FALSE,
            NULL,
            NULL,
            0,
            upserted_creators.id,
            series_values.metadata_json,
            series_values.timestamp_unix_seconds
        FROM normalized_series_values series_values
        LEFT JOIN upserted_modes
          ON upserted_modes.mode_name = series_values.mode_name
        LEFT JOIN upserted_creators
          ON upserted_creators.chain_id = series_values.chain_id
         AND upserted_creators.address = series_values.owner_address
        ON CONFLICT (contract_id, series_id) DO UPDATE SET
            max_supply = EXCLUDED.max_supply,
            series_mode_id = COALESCE(EXCLUDED.series_mode_id, series.series_mode_id),
            creator_address_id = COALESCE(EXCLUDED.creator_address_id, series.creator_address_id),
            name = CASE
                WHEN (series.name IS NULL OR series.name = '') AND EXCLUDED.name IS NOT NULL
                THEN EXCLUDED.name
                ELSE series.name
            END,
            metadata = CASE
                WHEN EXCLUDED.metadata IS NOT NULL
                THEN COALESCE(series.metadata, '{}'::jsonb) || EXCLUDED.metadata
                ELSE series.metadata
            END,
            nsfw = NULLIF(series.nsfw, FALSE),
            blacklisted = NULLIF(series.blacklisted, FALSE),
            series_created_unix_seconds = CASE
                WHEN EXCLUDED.series_created_unix_seconds > 0
                     AND (
                         series.series_created_unix_seconds <= 0
                         OR EXCLUDED.series_created_unix_seconds < series.series_created_unix_seconds
                     )
                THEN EXCLUDED.series_created_unix_seconds
                ELSE series.series_created_unix_seconds
            END
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn apply_token_mint_extended_side_effects_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    let dm_unix_seconds = Utc::now().timestamp();

    sqlx::query(
        r#"
        WITH mint_values AS MATERIALIZED (
            SELECT
                event.nft_id,
                event.contract_id,
                event.timestamp_unix_seconds,
                NULLIF(event.payload_json #>> '{token_mint_extended,series_id}', '') AS series_id,
                CASE
                    WHEN COALESCE(event.payload_json #>> '{token_mint_extended,mint_number}', '') ~ '^[0-9]+$'
                    THEN LEAST((event.payload_json #>> '{token_mint_extended,mint_number}')::numeric, 2147483647)::integer
                    ELSE NULL
                END AS mint_number
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens token
              ON token.chain_id = event.chain_id
             AND token.contract_id = event.contract_id
            WHERE event.transaction_id = $1
              AND event_kind.name = 'TokenMint'
              AND event.nft_id IS NOT NULL
              AND token.fungible IS FALSE
              AND jsonb_typeof(event.payload_json->'token_mint_extended') = 'object'
              AND NULLIF(event.payload_json #>> '{token_mint_extended,series_id}', '') IS NOT NULL
        ),
        upserted_series AS (
            INSERT INTO series (
                contract_id,
                series_id,
                current_supply,
                max_supply,
                royalties,
                type,
                has_locked,
                nsfw,
                blacklisted,
                dm_unix_seconds,
                series_created_unix_seconds
            )
            SELECT
                contract_id,
                series_id,
                0,
                0,
                0,
                0,
                FALSE,
                FALSE,
                FALSE,
                0,
                MIN(timestamp_unix_seconds)
            FROM mint_values
            GROUP BY contract_id, series_id
            ON CONFLICT (contract_id, series_id) DO UPDATE SET
                series_created_unix_seconds = CASE
                    WHEN EXCLUDED.series_created_unix_seconds > 0
                         AND (
                             series.series_created_unix_seconds <= 0
                             OR EXCLUDED.series_created_unix_seconds < series.series_created_unix_seconds
                         )
                    THEN EXCLUDED.series_created_unix_seconds
                    ELSE series.series_created_unix_seconds
                END
            RETURNING id, contract_id, series_id
        )
        UPDATE nfts nft
        SET
            series_id = upserted_series.id,
            mint_number = CASE
                WHEN mint_values.mint_number IS NOT NULL AND mint_values.mint_number > 0
                THEN mint_values.mint_number
                ELSE nft.mint_number
            END,
            dm_unix_seconds = $2
        FROM mint_values
        JOIN upserted_series
          ON upserted_series.contract_id = mint_values.contract_id
         AND upserted_series.series_id = mint_values.series_id
        WHERE nft.id = mint_values.nft_id
          AND (
              nft.series_id IS DISTINCT FROM upserted_series.id
              OR (
                  mint_values.mint_number IS NOT NULL
                  AND mint_values.mint_number > 0
                  AND nft.mint_number IS DISTINCT FROM mint_values.mint_number
              )
          )
        "#,
    )
    .bind(transaction_id)
    .bind(dm_unix_seconds)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn apply_nft_side_effects_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    let dm_unix_seconds = Utc::now().timestamp();

    sqlx::query(
        r#"
        WITH nft_event_candidates AS MATERIALIZED (
            SELECT
                event.chain_id,
                event.contract_id,
                event.token_id,
                event.timestamp_unix_seconds,
                event_kind.name AS event_kind
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens token
              ON token.chain_id = event.chain_id
             AND token.contract_id = event.contract_id
            WHERE event.transaction_id = $1
              AND event.token_id IS NOT NULL
              AND token.fungible IS FALSE
              AND event_kind.name IN (
                  'TokenMint',
                  'TokenClaim',
                  'TokenBurn',
                  'TokenSend',
                  'TokenReceive',
                  'TokenStake',
                  'CrownRewards',
                  'Inflation',
                  'Infusion',
                  'OrderCancelled',
                  'OrderClosed',
                  'OrderCreated',
                  'OrderFilled',
                  'OrderBid'
              )
        ),
        nft_rows AS (
            SELECT
                chain_id,
                contract_id,
                token_id,
                COALESCE(
                    MAX(timestamp_unix_seconds) FILTER (WHERE event_kind = 'TokenMint'),
                    0
                ) AS mint_date_unix_seconds
            FROM nft_event_candidates
            GROUP BY chain_id, contract_id, token_id
        )
        INSERT INTO nfts (
            dm_unix_seconds,
            token_id,
            token_uri,
            mint_date_unix_seconds,
            mint_number,
            burned,
            nsfw,
            blacklisted,
            chain_id,
            contract_id
        )
        SELECT
            $2,
            token_id,
            NULL,
            mint_date_unix_seconds,
            0,
            NULL,
            FALSE,
            FALSE,
            chain_id,
            contract_id
        FROM nft_rows
        ON CONFLICT (contract_id, token_id) DO UPDATE SET
            mint_date_unix_seconds = CASE
                WHEN EXCLUDED.mint_date_unix_seconds > 0 THEN EXCLUDED.mint_date_unix_seconds
                ELSE nfts.mint_date_unix_seconds
            END
        "#,
    )
    .bind(transaction_id)
    .bind(dm_unix_seconds)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        WITH nft_event_candidates AS MATERIALIZED (
            SELECT
                event.id AS event_id,
                event.chain_id,
                event.contract_id,
                event.token_id
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens token
              ON token.chain_id = event.chain_id
             AND token.contract_id = event.contract_id
            WHERE event.transaction_id = $1
              AND event.token_id IS NOT NULL
              AND token.fungible IS FALSE
              AND event_kind.name IN (
                  'TokenMint',
                  'TokenClaim',
                  'TokenBurn',
                  'TokenSend',
                  'TokenReceive',
                  'TokenStake',
                  'CrownRewards',
                  'Inflation',
                  'Infusion',
                  'OrderCancelled',
                  'OrderClosed',
                  'OrderCreated',
                  'OrderFilled',
                  'OrderBid'
              )
        )
        UPDATE events event
        SET nft_id = nft.id
        FROM nft_event_candidates candidate
        JOIN nfts nft
          ON nft.contract_id = candidate.contract_id
         AND nft.token_id = candidate.token_id
        WHERE event.id = candidate.event_id
          AND event.nft_id IS DISTINCT FROM nft.id
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        WITH tx_ownership_events AS MATERIALIZED (
            SELECT
                event.id AS event_id,
                event.chain_id,
                event.contract_id,
                event.token_id,
                event.address_id,
                event.timestamp_unix_seconds,
                event.event_index,
                event_kind.name AS event_kind
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            WHERE event.transaction_id = $1
              AND event.token_id IS NOT NULL
              AND event.address_id IS NOT NULL
              AND (
                  event_kind.name IN (
                      'TokenMint',
                      'TokenClaim',
                      'TokenBurn',
                      'TokenSend',
                      'TokenReceive',
                      'TokenStake',
                      'CrownRewards',
                      'Inflation'
                  )
                  OR event_kind.name = 'OrderFilled'
              )
        ),
        ownership_events AS MATERIALIZED (
            SELECT
                tx_event.event_id,
                nft.id AS nft_id,
                tx_event.address_id,
                tx_event.timestamp_unix_seconds,
                tx_event.event_index
            FROM tx_ownership_events tx_event
            JOIN LATERAL (
                SELECT nft.id
                FROM nfts nft
                WHERE nft.contract_id = tx_event.contract_id
                  AND nft.token_id = tx_event.token_id
                LIMIT 1
            ) nft ON TRUE
            WHERE EXISTS (
                SELECT 1
                FROM tokens token
                WHERE token.chain_id = tx_event.chain_id
                  AND token.contract_id = tx_event.contract_id
                  AND token.fungible IS FALSE
            )
        ),
        desired_ownership AS (
            SELECT DISTINCT ON (nft_id)
                nft_id,
                address_id,
                timestamp_unix_seconds
            FROM ownership_events
            ORDER BY
                nft_id,
                timestamp_unix_seconds DESC,
                event_index DESC,
                event_id DESC
        ),
        ownership_anchor AS (
            SELECT DISTINCT ON (ownership.nft_id)
                ownership.id,
                ownership.nft_id,
                ownership.last_change_unix_seconds
            FROM nft_ownerships ownership
            JOIN desired_ownership desired
              ON desired.nft_id = ownership.nft_id
            ORDER BY
                ownership.nft_id,
                ownership.last_change_unix_seconds,
                ownership.id
        ),
        updated_ownership AS (
            UPDATE nft_ownerships ownership
            SET
                address_id = desired.address_id,
                last_change_unix_seconds = desired.timestamp_unix_seconds
            FROM desired_ownership desired
            JOIN ownership_anchor anchor
              ON anchor.nft_id = desired.nft_id
            WHERE ownership.id = anchor.id
              AND desired.timestamp_unix_seconds >= anchor.last_change_unix_seconds
            RETURNING ownership.nft_id
        )
        INSERT INTO nft_ownerships (
            last_change_unix_seconds,
            amount,
            nft_id,
            address_id
        )
        SELECT
            desired.timestamp_unix_seconds,
            1,
            desired.nft_id,
            desired.address_id
        FROM desired_ownership desired
        WHERE NOT EXISTS (
            SELECT 1
            FROM ownership_anchor anchor
            WHERE anchor.nft_id = desired.nft_id
        )
        ON CONFLICT (address_id, nft_id) DO UPDATE SET
            last_change_unix_seconds = CASE
                WHEN EXCLUDED.last_change_unix_seconds >= nft_ownerships.last_change_unix_seconds
                THEN EXCLUDED.last_change_unix_seconds
                ELSE nft_ownerships.last_change_unix_seconds
            END,
            amount = EXCLUDED.amount
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn apply_infusions_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        WITH touched_fungible AS MATERIALIZED (
            SELECT DISTINCT
                event.nft_id,
                infused_token.id AS token_id,
                infused_token.symbol AS key
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens infused_token
              ON infused_token.chain_id = event.chain_id
             AND lower(infused_token.symbol) = lower(event.payload_json #>> '{infusion_event,infused_token}')
            WHERE event.transaction_id = $1
              AND event_kind.name = 'Infusion'
              AND event.nft_id IS NOT NULL
              AND jsonb_typeof(event.payload_json->'infusion_event') = 'object'
              AND infused_token.fungible IS TRUE
        ),
        fungible_totals AS (
            SELECT
                touched.nft_id,
                touched.token_id,
                touched.key,
                CASE
                    WHEN SUM((event.payload_json #>> '{infusion_event,infused_value}')::numeric / power(10::numeric, infused_token.decimals)) = 0
                    THEN '0'
                    ELSE regexp_replace(
                        regexp_replace(
                            SUM((event.payload_json #>> '{infusion_event,infused_value}')::numeric / power(10::numeric, infused_token.decimals))::text,
                            '0+$',
                            ''
                        ),
                        '\.$',
                        ''
                    )
                END AS value
            FROM touched_fungible touched
            JOIN tokens infused_token
              ON infused_token.id = touched.token_id
            JOIN events event
              ON event.nft_id = touched.nft_id
             AND lower(event.payload_json #>> '{infusion_event,infused_token}') = lower(touched.key)
             AND COALESCE(event.payload_json #>> '{infusion_event,infused_value}', '') ~ '^[0-9]+$'
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
             AND event_kind.name = 'Infusion'
            GROUP BY touched.nft_id, touched.token_id, touched.key
        ),
        updated_fungible AS (
            UPDATE infusions infusion
            SET
                value = fungible_totals.value,
                token_id = fungible_totals.token_id
            FROM fungible_totals
            WHERE infusion.nft_id = fungible_totals.nft_id
              AND infusion.key = fungible_totals.key
              AND (
                  infusion.token_id = fungible_totals.token_id
                  OR infusion.token_id IS NULL
              )
            RETURNING infusion.nft_id, infusion.key, infusion.token_id
        )
        INSERT INTO infusions (key, value, token_id, nft_id)
        SELECT
            fungible_totals.key,
            fungible_totals.value,
            fungible_totals.token_id,
            fungible_totals.nft_id
        FROM fungible_totals
        WHERE NOT EXISTS (
            SELECT 1
            FROM updated_fungible updated
            WHERE updated.nft_id = fungible_totals.nft_id
              AND updated.key = fungible_totals.key
              AND updated.token_id = fungible_totals.token_id
        )
          AND NOT EXISTS (
            SELECT 1
            FROM infusions infusion
            WHERE infusion.nft_id = fungible_totals.nft_id
              AND infusion.key = fungible_totals.key
              AND infusion.token_id = fungible_totals.token_id
        )
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        r#"
        WITH nonfungible_values AS MATERIALIZED (
            SELECT DISTINCT
                event.nft_id,
                infused_token.contract_id AS infused_contract_id,
                infused_token.symbol AS key,
                event.payload_json #>> '{infusion_event,infused_value}' AS value
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN tokens infused_token
              ON infused_token.chain_id = event.chain_id
             AND lower(infused_token.symbol) = lower(event.payload_json #>> '{infusion_event,infused_token}')
            WHERE event.transaction_id = $1
              AND event_kind.name = 'Infusion'
              AND event.nft_id IS NOT NULL
              AND jsonb_typeof(event.payload_json->'infusion_event') = 'object'
              AND infused_token.fungible IS FALSE
              AND NULLIF(event.payload_json #>> '{infusion_event,infused_value}', '') IS NOT NULL
        ),
        inserted_nonfungible AS (
            INSERT INTO infusions (key, value, nft_id)
            SELECT
                key,
                value,
                nft_id
            FROM nonfungible_values desired
            WHERE NOT EXISTS (
                SELECT 1
                FROM infusions infusion
                WHERE infusion.nft_id = desired.nft_id
                  AND infusion.key = desired.key
                  AND infusion.value = desired.value
            )
            RETURNING id
        )
        UPDATE nfts infused_nft
        SET infused_into_id = desired.nft_id
        FROM nonfungible_values desired
        WHERE infused_nft.contract_id = desired.infused_contract_id
          AND infused_nft.token_id = desired.value
          AND infused_nft.infused_into_id IS DISTINCT FROM desired.nft_id
        "#,
    )
    .bind(transaction_id)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

async fn apply_series_lifecycle_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    let lifecycle_events = sqlx::query(
        r#"
        SELECT
            event_kind.name AS event_kind,
            nft.id AS nft_id,
            nft.series_id AS series_id
        FROM events event
        JOIN event_kinds event_kind
          ON event_kind.id = event.event_kind_id
         AND event_kind.chain_id = event.chain_id
        JOIN nfts nft
          ON nft.id = event.nft_id
        WHERE event.transaction_id = $1
          AND event_kind.name IN ('TokenMint', 'TokenBurn')
          AND nft.series_id IS NOT NULL
        ORDER BY event.event_index, event.id
        "#,
    )
    .bind(transaction_id)
    .fetch_all(&mut *conn)
    .await?;

    for lifecycle_event in lifecycle_events {
        let event_kind: String = lifecycle_event.get("event_kind");
        let nft_id: i32 = lifecycle_event.get("nft_id");
        let series_id: i32 = lifecycle_event.get("series_id");

        match event_kind.as_str() {
            "TokenMint" => {
                let nft_update = sqlx::query(
                    r#"
                    UPDATE nfts
                    SET burned = FALSE
                    WHERE id = $1
                      AND burned IS DISTINCT FROM FALSE
                    "#,
                )
                .bind(nft_id)
                .execute(&mut *conn)
                .await?;

                if nft_update.rows_affected() > 0 {
                    sqlx::query(
                        r#"
                        UPDATE series
                        SET current_supply = current_supply + 1
                        WHERE id = $1
                        "#,
                    )
                    .bind(series_id)
                    .execute(&mut *conn)
                    .await?;
                }
            }
            "TokenBurn" => {
                let nft_update = sqlx::query(
                    r#"
                    UPDATE nfts
                    SET burned = TRUE
                    WHERE id = $1
                      AND burned IS DISTINCT FROM TRUE
                    "#,
                )
                .bind(nft_id)
                .execute(&mut *conn)
                .await?;

                if nft_update.rows_affected() > 0 {
                    sqlx::query(
                        r#"
                        UPDATE series
                        SET current_supply = CASE
                            WHEN current_supply > 0 THEN current_supply - 1
                            ELSE current_supply
                        END
                        WHERE id = $1
                        "#,
                    )
                    .bind(series_id)
                    .execute(&mut *conn)
                    .await?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

async fn apply_burn_markers_for_transaction(
    conn: &mut PgConnection,
    transaction_id: i32,
) -> Result<(), DbError> {
    // The legacy C# background plugin denormalized burn state by token pair and
    // excluded only KCAL burns. Keep that behavior for DB/API parity, including
    // historical burns that should mark newly ingested matching events.
    let burn_tokens = sqlx::query_as::<_, (i32, i32, String)>(
        r#"
        WITH tx_events AS MATERIALIZED (
            SELECT
                id,
                chain_id,
                contract_id,
                token_id,
                event_kind_id
            FROM events
            WHERE transaction_id = $1
              AND token_id IS NOT NULL
        ),
        token_burn_kinds AS MATERIALIZED (
            SELECT id, chain_id
            FROM event_kinds
            WHERE name = $2
        ),
        tx_non_kcal_events AS MATERIALIZED (
            SELECT tx_event.*
            FROM tx_events tx_event
            LEFT JOIN contracts tx_contract
              ON tx_contract.id = tx_event.contract_id
             AND tx_contract.chain_id = tx_event.chain_id
            WHERE tx_contract.symbol IS DISTINCT FROM $3
        )
        SELECT DISTINCT
            burned_token.chain_id,
            burned_token.contract_id,
            burned_token.token_id
        FROM (
            SELECT
                burn_event.chain_id,
                burn_event.contract_id,
                burn_event.token_id
            FROM tx_non_kcal_events burn_event
            JOIN token_burn_kinds burn_kind
              ON burn_kind.id = burn_event.event_kind_id
             AND burn_kind.chain_id = burn_event.chain_id

            UNION

            SELECT
                tx_event.chain_id,
                tx_event.contract_id,
                tx_event.token_id
            FROM tx_non_kcal_events tx_event
            WHERE EXISTS (
                SELECT 1
                FROM events burn_event
                JOIN token_burn_kinds burn_kind
                  ON burn_kind.id = burn_event.event_kind_id
                 AND burn_kind.chain_id = burn_event.chain_id
                WHERE burn_event.chain_id = tx_event.chain_id
                  AND burn_event.contract_id = tx_event.contract_id
                  AND burn_event.token_id = tx_event.token_id
            )
        ) burned_token
        "#,
    )
    .bind(transaction_id)
    .bind(LEGACY_TOKEN_BURN_EVENT_KIND)
    .bind("KCAL")
    .fetch_all(&mut *conn)
    .await?;

    for (chain_id, contract_id, token_id) in burn_tokens {
        sqlx::query(
            r#"
            UPDATE events
            SET burned = TRUE
            WHERE chain_id = $1
              AND contract_id = $2
              AND token_id = $3
              AND burned IS DISTINCT FROM TRUE
            "#,
        )
        .bind(chain_id)
        .bind(contract_id)
        .bind(&token_id)
        .execute(&mut *conn)
        .await?;

        sqlx::query(
            r#"
            UPDATE nfts
            SET burned = TRUE
            WHERE chain_id = $1
              AND contract_id = $2
              AND token_id = $3
              AND burned IS DISTINCT FROM TRUE
            "#,
        )
        .bind(chain_id)
        .bind(contract_id)
        .bind(&token_id)
        .execute(&mut *conn)
        .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn contract_string_events_upsert_contracts_and_create_links()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain = ChainName::new("main")?;
        let chain_id = resolve_chain_id(&mut transaction, &chain).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let owner = format!("PTESTCONTRACT{suffix}");
        let deployed_hash = format!("rstdep{}", &suffix[..8]);
        let upgraded_hash = format!("rstupg{}", &suffix[..8]);

        let block = upsert_block(
            &mut transaction,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_100_000),
                hash: format!("TESTCONTRACTBLOCK{suffix}"),
                previous_hash: None,
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                timestamp_unix_seconds: 1_800_100_000,
                reward: None,
            },
        )
        .await?;
        let tx = upsert_transaction(
            &mut transaction,
            TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTCONTRACTTX{suffix}"),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee: None,
                fee_raw: None,
                gas_price: None,
                gas_price_raw: None,
                gas_limit: None,
                gas_limit_raw: None,
                sender: Some(owner.clone()),
                gas_payer: Some(owner.clone()),
                gas_target: Some(owner.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;
        let events = vec![
            EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 1,
                event_kind: "ContractDeploy".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some("entry".to_owned()),
                token_id: None,
                raw_data: None,
                payload_format: Some("legacy.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "ContractDeploy",
                    "chain": "main",
                    "contract": "entry",
                    "address": owner,
                    "string_event": {
                        "string_value": deployed_hash.clone()
                    }
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 2,
                event_kind: "ContractUpgrade".to_owned(),
                address: None,
                target_address: None,
                contract: Some("entry".to_owned()),
                token_id: None,
                raw_data: None,
                payload_format: Some("legacy.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "ContractUpgrade",
                    "chain": "main",
                    "contract": "entry",
                    "string_event": {
                        "string_value": upgraded_hash.clone()
                    }
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
        ];

        replace_events(&mut transaction, tx.id, &events).await?;

        let rows = sqlx::query_as::<_, (String, String, Option<String>, Option<i32>)>(
            r#"
            SELECT contract.hash, contract.symbol, event_kind.name, event.event_index
            FROM contracts contract
            LEFT JOIN events event ON event.id = contract.create_event_id
            LEFT JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            WHERE contract.chain_id = $1
              AND contract.hash = ANY($2)
            ORDER BY contract.hash
            "#,
        )
        .bind(chain_id)
        .bind(vec![deployed_hash.clone(), upgraded_hash.clone()])
        .fetch_all(&mut *transaction)
        .await?;

        assert_eq!(
            rows,
            vec![
                (
                    deployed_hash.clone(),
                    deployed_hash,
                    Some("ContractDeploy".to_owned()),
                    Some(1)
                ),
                (upgraded_hash.clone(), upgraded_hash, None, None),
            ]
        );

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn nft_series_and_infusion_side_effects_smoke() -> Result<(), Box<dyn std::error::Error>>
    {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain = ChainName::new("main")?;
        let chain_id = resolve_chain_id(&mut transaction, &chain).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let owner = format!("PTESTOWNER{suffix}");
        let nft_symbol = format!("RSTNFT{}", &suffix[..8]);
        let fuel_symbol = format!("RSTFUEL{}", &suffix[..8]);
        let nft_token_id = "9000000000000000000001";
        let series_id = "7000000000000000000001";

        let owner_id = upsert_address_id(&mut transaction, chain_id, &owner).await?;
        let nft_contract_id = upsert_contract_id(&mut transaction, chain_id, &nft_symbol).await?;
        let fuel_contract_id = upsert_contract_id(&mut transaction, chain_id, &fuel_symbol).await?;

        insert_test_token(
            &mut transaction,
            chain_id,
            nft_contract_id,
            owner_id,
            &nft_symbol,
            false,
            0,
        )
        .await?;
        insert_test_token(
            &mut transaction,
            chain_id,
            fuel_contract_id,
            owner_id,
            &fuel_symbol,
            true,
            2,
        )
        .await?;

        let block = upsert_block(
            &mut transaction,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_000_000),
                hash: format!("TESTBLOCK{suffix}"),
                previous_hash: None,
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                timestamp_unix_seconds: 1_800_000_000,
                reward: None,
            },
        )
        .await?;
        let tx = upsert_transaction(
            &mut transaction,
            TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTTX{suffix}"),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee: None,
                fee_raw: None,
                gas_price: None,
                gas_price_raw: None,
                gas_limit: None,
                gas_limit_raw: None,
                sender: Some(owner.clone()),
                gas_payer: Some(owner.clone()),
                gas_target: Some(owner.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;

        let events = vec![
            EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 1,
                event_kind: "TokenSeriesCreate".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(nft_symbol.clone()),
                token_id: Some(series_id.to_owned()),
                raw_data: Some(String::new()),
                payload_format: Some("live.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "TokenSeriesCreate",
                    "chain": "main",
                    "contract": nft_symbol,
                    "address": owner,
                    "token_id": series_id,
                    "token_series_event": {
                        "token": nft_symbol,
                        "series_id": series_id,
                        "max_supply": "10",
                        "owner": owner,
                        "carbon_series_id": "1",
                        "metadata": {
                            "mode": "0",
                            "rom": "ABCD"
                        }
                    }
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 2,
                event_kind: "TokenMint".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(nft_symbol.clone()),
                token_id: Some(nft_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "TokenMint",
                    "chain": "main",
                    "contract": nft_symbol,
                    "address": owner,
                    "token_id": nft_token_id,
                    "token_event": {
                        "token": nft_symbol,
                        "value": nft_token_id,
                        "value_raw": nft_token_id,
                        "chain_name": "main"
                    },
                    "token_mint_extended": {
                        "token_id": nft_token_id,
                        "series_id": series_id,
                        "mint_number": "3",
                        "carbon_token_id": "7",
                        "carbon_series_id": "1",
                        "carbon_instance_id": "3",
                        "owner": owner
                    }
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 3,
                event_kind: "Infusion".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(nft_symbol.clone()),
                token_id: Some(nft_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "Infusion",
                    "chain": "main",
                    "contract": nft_symbol,
                    "address": owner,
                    "token_id": nft_token_id,
                    "infusion_event": {
                        "token_id": nft_token_id,
                        "base_token": nft_symbol,
                        "infused_token": fuel_symbol,
                        "infused_value": "12345"
                    }
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
        ];

        replace_events(&mut transaction, tx.id, &events).await?;

        let row = sqlx::query_as::<_, (i32, i32, String, i32, String)>(
            r#"
            SELECT
                series.current_supply,
                nft.mint_number,
                mode.mode_name,
                COUNT(infusion.id)::integer,
                COALESCE(MAX(infusion.value), '')
            FROM nfts nft
            JOIN series series ON series.id = nft.series_id
            LEFT JOIN series_modes mode ON mode.id = series.series_mode_id
            LEFT JOIN infusions infusion ON infusion.nft_id = nft.id
            WHERE nft.contract_id = $1
              AND nft.token_id = $2
              AND series.series_id = $3
            GROUP BY series.current_supply, nft.mint_number, mode.mode_name
            "#,
        )
        .bind(nft_contract_id)
        .bind(nft_token_id)
        .bind(series_id)
        .fetch_one(&mut *transaction)
        .await?;

        assert_eq!(row, (1, 3, "Unique".to_owned(), 1, "123.45".to_owned()));

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn burn_markers_follow_csharp_kcal_exclusion() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain = ChainName::new("main")?;
        let chain_id = resolve_chain_id(&mut transaction, &chain).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let owner = format!("PTESTOWNER{suffix}");
        let burned_symbol = format!("RSTBRN{}", &suffix[..8]);
        let burned_token_id = "424242";
        let kcal_token_id = "777";
        let height_seed = u64::from_str_radix(&suffix[..8], 16)?;
        let first_height = 9_800_000_000 + height_seed % 10_000_000;

        let owner_id = upsert_address_id(&mut transaction, chain_id, &owner).await?;
        let burned_contract_id =
            upsert_contract_id(&mut transaction, chain_id, &burned_symbol).await?;
        let kcal_contract_id = upsert_contract_id(&mut transaction, chain_id, "KCAL").await?;

        insert_test_token(
            &mut transaction,
            chain_id,
            burned_contract_id,
            owner_id,
            &burned_symbol,
            true,
            0,
        )
        .await?;
        insert_test_token(
            &mut transaction,
            chain_id,
            kcal_contract_id,
            owner_id,
            "KCAL",
            true,
            10,
        )
        .await?;

        let first_block = upsert_block(
            &mut transaction,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(first_height),
                hash: format!("TESTBURNBLOCK{suffix}A"),
                previous_hash: None,
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                timestamp_unix_seconds: 1_800_000_000,
                reward: None,
            },
        )
        .await?;
        let first_tx = upsert_transaction(
            &mut transaction,
            TransactionUpsert {
                block_id: first_block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTBURNTX{suffix}A"),
                timestamp_unix_seconds: first_block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee: None,
                fee_raw: None,
                gas_price: None,
                gas_price_raw: None,
                gas_limit: None,
                gas_limit_raw: None,
                sender: Some(owner.clone()),
                gas_payer: Some(owner.clone()),
                gas_target: Some(owner.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;
        let first_events = vec![
            EventUpsert {
                transaction_id: first_tx.id,
                chain_id,
                event_index: 1,
                event_kind: "TokenBurn".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(burned_symbol.clone()),
                token_id: Some(burned_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: first_block.timestamp_unix_seconds,
                date_unix_seconds: first_block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: first_tx.id,
                chain_id,
                event_index: 2,
                event_kind: "TokenBurn".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some("KCAL".to_owned()),
                token_id: Some(kcal_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: first_block.timestamp_unix_seconds,
                date_unix_seconds: first_block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
        ];
        replace_events(&mut transaction, first_tx.id, &first_events).await?;

        let second_block = upsert_block(
            &mut transaction,
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(first_height + 1),
                hash: format!("TESTBURNBLOCK{suffix}B"),
                previous_hash: None,
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                timestamp_unix_seconds: 1_800_000_001,
                reward: None,
            },
        )
        .await?;
        let second_tx = upsert_transaction(
            &mut transaction,
            TransactionUpsert {
                block_id: second_block.id,
                chain_id,
                tx_index: 0,
                hash: format!("TESTBURNTX{suffix}B"),
                timestamp_unix_seconds: second_block.timestamp_unix_seconds,
                state: "Halt".to_owned(),
                result: None,
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee: None,
                fee_raw: None,
                gas_price: None,
                gas_price_raw: None,
                gas_limit: None,
                gas_limit_raw: None,
                sender: Some(owner.clone()),
                gas_payer: Some(owner.clone()),
                gas_target: Some(owner.clone()),
                carbon_tx_type: None,
                carbon_tx_data: None,
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            },
        )
        .await?;
        let second_events = vec![
            EventUpsert {
                transaction_id: second_tx.id,
                chain_id,
                event_index: 1,
                event_kind: "TokenSend".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(burned_symbol.clone()),
                token_id: Some(burned_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: second_block.timestamp_unix_seconds,
                date_unix_seconds: second_block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: second_tx.id,
                chain_id,
                event_index: 2,
                event_kind: "TokenReceive".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some(burned_symbol.clone()),
                token_id: Some(burned_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: second_block.timestamp_unix_seconds,
                date_unix_seconds: second_block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            EventUpsert {
                transaction_id: second_tx.id,
                chain_id,
                event_index: 3,
                event_kind: "TokenSend".to_owned(),
                address: Some(owner.clone()),
                target_address: None,
                contract: Some("KCAL".to_owned()),
                token_id: Some(kcal_token_id.to_owned()),
                raw_data: None,
                payload_format: Some("live.v1".to_owned()),
                payload_json: None,
                timestamp_unix_seconds: second_block.timestamp_unix_seconds,
                date_unix_seconds: second_block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
        ];
        replace_events(&mut transaction, second_tx.id, &second_events).await?;

        let rows = sqlx::query_as::<_, (String, i32, Option<bool>)>(
            r#"
            SELECT contract.symbol, event.event_index, event.burned
            FROM events event
            JOIN contracts contract ON contract.id = event.contract_id
            WHERE event.transaction_id = $1
            ORDER BY event.event_index
            "#,
        )
        .bind(second_tx.id)
        .fetch_all(&mut *transaction)
        .await?;

        assert_eq!(
            rows,
            vec![
                (burned_symbol.clone(), 1, Some(true)),
                (burned_symbol, 2, Some(true)),
                ("KCAL".to_owned(), 3, None),
            ]
        );

        transaction.rollback().await?;
        Ok(())
    }

    async fn insert_test_token(
        conn: &mut PgConnection,
        chain_id: i32,
        contract_id: i32,
        owner_id: i32,
        symbol: &str,
        fungible: bool,
        decimals: i32,
    ) -> Result<(), DbError> {
        sqlx::query(
            r#"
            INSERT INTO tokens (
                symbol,
                fungible,
                transferable,
                finite,
                divisible,
                fuel,
                stakable,
                fiat,
                swappable,
                burnable,
                decimals,
                current_supply,
                max_supply,
                burned_supply,
                address_id,
                owner_id,
                price_usd,
                price_eur,
                price_gbp,
                price_jpy,
                price_cad,
                price_aud,
                price_cny,
                price_rub,
                chain_id,
                contract_id,
                burned_supply_raw,
                current_supply_raw,
                max_supply_raw,
                mintable,
                name
            )
            VALUES (
                $1,
                $2,
                TRUE,
                FALSE,
                $2,
                FALSE,
                FALSE,
                FALSE,
                FALSE,
                TRUE,
                $3,
                '0',
                '0',
                '0',
                $4,
                $4,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                $5,
                $6,
                '0',
                '0',
                '0',
                TRUE,
                $1
            )
            ON CONFLICT (chain_id, contract_id, symbol) DO UPDATE SET
                fungible = EXCLUDED.fungible,
                decimals = EXCLUDED.decimals
            "#,
        )
        .bind(symbol)
        .bind(fungible)
        .bind(decimals)
        .bind(owner_id)
        .bind(chain_id)
        .bind(contract_id)
        .execute(&mut *conn)
        .await?;

        Ok(())
    }
}
