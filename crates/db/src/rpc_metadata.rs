//! RPC-driven metadata hydration for contracts, NFTs, and series.
//! Fetches candidates from the DB and applies metadata read from the chain RPC
//! (contract ABI/upgrade methods, NFT RAM/ROM properties, series presentation).
use super::*;

pub async fn fetch_contract_rpc_metadata_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    stale_before_unix_seconds: i64,
    min_upgrade_block_height_exclusive: BlockHeight,
    batch_size: i64,
) -> Result<Vec<ContractRpcMetadataCandidate>, DbError> {
    let min_upgrade_block_height = block_height_to_i64(min_upgrade_block_height_exclusive)?;
    let rows = sqlx::query_as::<_, (i32, String, bool)>(
        r#"
        SELECT
            contract.id,
            COALESCE(NULLIF(contract.name, ''), contract.hash),
            NOT EXISTS (
                SELECT 1
                FROM events event
                JOIN event_kinds event_kind
                  ON event_kind.id = event.event_kind_id
                 AND event_kind.chain_id = event.chain_id
                JOIN transactions tx ON tx.id = event.transaction_id
                JOIN blocks block ON block.id = tx.block_id
                WHERE event.chain_id = contract.chain_id
                  AND event_kind.name = 'ContractUpgrade'
                  AND block.height > $3
                  AND NULLIF(BTRIM(event.payload_json #>> '{string_event,string_value}'), '') = contract.hash
            ) AS insert_current_method
        FROM contracts contract
        WHERE contract.chain_id = $1
          AND COALESCE(NULLIF(contract.name, ''), contract.hash) IS NOT NULL
          AND (
              contract.last_updated_unix_seconds = 0
              OR contract.last_updated_unix_seconds < $2
          )
        ORDER BY contract.last_updated_unix_seconds ASC, contract.id ASC
        LIMIT $4
        "#,
    )
    .bind(chain_id)
    .bind(stale_before_unix_seconds)
    .bind(min_upgrade_block_height)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(id, name, insert_current_method)| ContractRpcMetadataCandidate {
                id,
                name,
                insert_current_method,
            },
        )
        .collect())
}

pub async fn apply_contract_rpc_metadata(
    conn: &mut PgConnection,
    chain_id: i32,
    upsert: &ContractRpcMetadataUpsert,
) -> Result<ContractRpcMetadataUpsertResult, DbError> {
    let result = sqlx::query_as::<_, (bool, bool)>(
        r#"
        WITH upserted_address AS (
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
            SELECT $2, $6, 0, 0, 0, 0, 0, 0
            WHERE NULLIF(BTRIM($2), '') IS NOT NULL
              AND BTRIM($2) <> 'NULL'
            ON CONFLICT (chain_id, address) DO UPDATE SET
                address = addresses.address
            RETURNING id
        ),
        inserted_method AS (
            INSERT INTO contract_methods (contract_id, methods, timestamp_unix_seconds)
            SELECT $1, $4::jsonb, 0
            WHERE $7
              AND $4::jsonb IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM contracts
                  WHERE id = $1
                    AND chain_id = $6
                    AND contract_method_id IS NULL
              )
            RETURNING id
        ),
        updated_contract AS (
            UPDATE contracts contract
            SET
                address_id = COALESCE((SELECT id FROM upserted_address), contract.address_id),
                script_raw = CASE
                    WHEN NULLIF(BTRIM($3), '') IS NOT NULL THEN $3
                    ELSE contract.script_raw
                END,
                contract_method_id = COALESCE(
                    (SELECT id FROM inserted_method),
                    contract.contract_method_id
                ),
                last_updated_unix_seconds = $5
            WHERE contract.id = $1
              AND contract.chain_id = $6
            RETURNING id
        )
        SELECT
            EXISTS (SELECT 1 FROM updated_contract),
            EXISTS (SELECT 1 FROM inserted_method)
        "#,
    )
    .bind(upsert.contract_id)
    .bind(&upsert.address)
    .bind(&upsert.script_raw)
    .bind(&upsert.methods)
    .bind(upsert.last_updated_unix_seconds)
    .bind(chain_id)
    .bind(upsert.insert_current_method)
    .fetch_one(&mut *conn)
    .await?;

    Ok(ContractRpcMetadataUpsertResult {
        updated_contract: result.0,
        inserted_method: result.1,
    })
}

pub async fn fetch_contract_upgrade_method_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    min_block_height_exclusive: BlockHeight,
    batch_size: i64,
) -> Result<Vec<ContractUpgradeMethodCandidate>, DbError> {
    let min_block_height = block_height_to_i64(min_block_height_exclusive)?;
    let rows = sqlx::query_as::<_, (i32, String, i64)>(
        r#"
        WITH upgrade_events AS (
            SELECT
                contract.id AS contract_id,
                COALESCE(NULLIF(contract.name, ''), contract.hash) AS contract_name,
                block.timestamp_unix_seconds,
                block.height,
                tx.tx_index,
                event.event_index
            FROM events event
            JOIN event_kinds event_kind
              ON event_kind.id = event.event_kind_id
             AND event_kind.chain_id = event.chain_id
            JOIN transactions tx ON tx.id = event.transaction_id
            JOIN blocks block ON block.id = tx.block_id
            JOIN contracts contract
              ON contract.chain_id = event.chain_id
             AND contract.hash = NULLIF(BTRIM(event.payload_json #>> '{string_event,string_value}'), '')
            WHERE event.chain_id = $1
              AND event_kind.name = 'ContractUpgrade'
              AND block.height > $2
              AND NOT EXISTS (
                  SELECT 1
                  FROM contract_methods method
                  WHERE method.contract_id = contract.id
                    AND method.timestamp_unix_seconds = block.timestamp_unix_seconds
              )
        )
        SELECT contract_id, contract_name, timestamp_unix_seconds
        FROM upgrade_events
        ORDER BY height ASC, tx_index ASC, event_index ASC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(min_block_height)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(
            |(contract_id, name, timestamp_unix_seconds)| ContractUpgradeMethodCandidate {
                contract_id,
                name,
                timestamp_unix_seconds,
            },
        )
        .collect())
}

pub async fn apply_contract_upgrade_method(
    conn: &mut PgConnection,
    chain_id: i32,
    upsert: &ContractUpgradeMethodUpsert,
) -> Result<ContractUpgradeMethodUpsertResult, DbError> {
    let result = sqlx::query_as::<_, (bool, bool)>(
        r#"
        WITH existing_method AS (
            SELECT id
            FROM contract_methods
            WHERE contract_id = $1
              AND timestamp_unix_seconds = $3
            ORDER BY id DESC
            LIMIT 1
        ),
        inserted_method AS (
            INSERT INTO contract_methods (contract_id, methods, timestamp_unix_seconds)
            SELECT $1, $2::jsonb, $3
            WHERE NOT EXISTS (SELECT 1 FROM existing_method)
            RETURNING id
        ),
        target_method AS (
            SELECT id FROM inserted_method
            UNION ALL
            SELECT id FROM existing_method
            LIMIT 1
        ),
        linked_contract AS (
            UPDATE contracts contract
            SET contract_method_id = (SELECT id FROM target_method)
            WHERE contract.id = $1
              AND contract.chain_id = $4
              AND (SELECT id FROM target_method) IS NOT NULL
              AND contract.contract_method_id IS DISTINCT FROM (SELECT id FROM target_method)
            RETURNING id
        )
        SELECT
            EXISTS (SELECT 1 FROM inserted_method),
            EXISTS (SELECT 1 FROM linked_contract)
        "#,
    )
    .bind(upsert.contract_id)
    .bind(&upsert.methods)
    .bind(upsert.timestamp_unix_seconds)
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(ContractUpgradeMethodUpsertResult {
        inserted_method: result.0,
        linked_contract: result.1,
    })
}

pub async fn fetch_nft_rpc_metadata_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    min_mint_block_height: i64,
    batch_size: i64,
) -> Result<Vec<NftRpcMetadataCandidate>, DbError> {
    fetch_nft_rpc_metadata_candidates_impl(conn, chain_id, min_mint_block_height, batch_size, false)
        .await
}

pub async fn fetch_nft_rpc_metadata_repair_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    min_mint_block_height: i64,
    batch_size: i64,
) -> Result<Vec<NftRpcMetadataCandidate>, DbError> {
    fetch_nft_rpc_metadata_candidates_impl(conn, chain_id, min_mint_block_height, batch_size, true)
        .await
}

async fn fetch_nft_rpc_metadata_candidates_impl(
    conn: &mut PgConnection,
    chain_id: i32,
    min_mint_block_height: i64,
    batch_size: i64,
    include_stored_rpc_field_drift: bool,
) -> Result<Vec<NftRpcMetadataCandidate>, DbError> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT
            contract.symbol,
            nft.token_id
        FROM nfts nft
        JOIN contracts contract
          ON contract.id = nft.contract_id
         AND contract.chain_id = nft.chain_id
        WHERE nft.chain_id = $1
          AND NULLIF(nft.token_id, '') IS NOT NULL
          AND COALESCE(nft.burned, FALSE) IS FALSE
          AND (
              nft.chain_api_response IS NULL
              OR (
                  $4
                  AND nft.chain_api_response IS NOT NULL
                  AND nft.mint_date_unix_seconds = 0
                  AND EXISTS (
                      SELECT 1
                      FROM jsonb_to_recordset(
                          CASE
                              WHEN jsonb_typeof(nft.chain_api_response->'properties') = 'array'
                              THEN nft.chain_api_response->'properties'
                              ELSE '[]'::jsonb
                          END
                      ) AS prop(key text, value text)
                      WHERE lower(prop.key) IN ('created', 'mint_date')
                        AND prop.value ~ '^[0-9]+$'
                        AND prop.value::numeric > 0
                  )
              )
          )
          AND EXISTS (
              SELECT 1
              FROM events event
              JOIN event_kinds event_kind
                ON event_kind.id = event.event_kind_id
               AND event_kind.chain_id = event.chain_id
              JOIN transactions tx
                ON tx.id = event.transaction_id
              JOIN blocks block
                ON block.id = tx.block_id
               AND block.chain_id = event.chain_id
              WHERE event.nft_id = nft.id
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
                AND block.height > $2
          )
        ORDER BY nft.id ASC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(min_mint_block_height)
    .bind(batch_size)
    .bind(include_stored_rpc_field_drift)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, token_id)| NftRpcMetadataCandidate { symbol, token_id })
        .collect())
}

pub async fn fetch_nft_rpc_metadata_candidates_for_mint_block(
    conn: &mut PgConnection,
    chain_id: i32,
    mint_block_height: i64,
    batch_size: i64,
) -> Result<Vec<NftRpcMetadataCandidate>, DbError> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT DISTINCT
            contract.symbol,
            nft.token_id
        FROM nfts nft
        JOIN contracts contract
          ON contract.id = nft.contract_id
         AND contract.chain_id = nft.chain_id
        JOIN events event
          ON event.nft_id = nft.id
         AND event.chain_id = nft.chain_id
        JOIN event_kinds event_kind
          ON event_kind.id = event.event_kind_id
         AND event_kind.chain_id = event.chain_id
        JOIN transactions tx
          ON tx.id = event.transaction_id
        JOIN blocks block
          ON block.id = tx.block_id
         AND block.chain_id = event.chain_id
        WHERE nft.chain_id = $1
          AND NULLIF(nft.token_id, '') IS NOT NULL
          AND COALESCE(nft.burned, FALSE) IS FALSE
          AND nft.chain_api_response IS NULL
          AND event_kind.name = 'TokenMint'
          AND block.height = $2
        ORDER BY nft.token_id ASC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(mint_block_height)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, token_id)| NftRpcMetadataCandidate { symbol, token_id })
        .collect())
}

pub async fn apply_nft_rpc_metadata(
    conn: &mut PgConnection,
    chain_id: i32,
    upserts: &[NftRpcMetadataUpsert],
) -> Result<u64, DbError> {
    if upserts.is_empty() {
        return Ok(0);
    }

    let payloads = serde_json::to_value(upserts)?;
    let dm_unix_seconds = Utc::now().timestamp();

    let result = sqlx::query(
        r#"
        WITH payload_rows AS MATERIALIZED (
            SELECT *
            FROM jsonb_to_recordset($2::jsonb) AS payload(
                symbol text,
                token_id text,
                series_id text,
                creator_address text,
                mint_number integer,
                mint_date_unix_seconds bigint,
                rom text,
                ram text,
                name text,
                description text,
                image text,
                info_url text,
                metadata jsonb,
                chain_api_response jsonb
            )
            WHERE NULLIF(symbol, '') IS NOT NULL
              AND NULLIF(token_id, '') IS NOT NULL
        ),
        target_values AS MATERIALIZED (
            SELECT
                nft.id AS nft_id,
                nft.contract_id,
                nft.chain_id,
                NULLIF(payload.series_id, '') AS series_id,
                NULLIF(payload.creator_address, '') AS creator_address,
                payload.mint_number,
                payload.mint_date_unix_seconds,
                NULLIF(payload.rom, '') AS rom,
                NULLIF(payload.ram, '') AS ram,
                NULLIF(payload.name, '') AS name,
                NULLIF(payload.description, '') AS description,
                NULLIF(payload.image, '') AS image,
                NULLIF(payload.info_url, '') AS info_url,
                CASE
                    WHEN jsonb_typeof(payload.metadata) = 'object' THEN payload.metadata
                    ELSE '{}'::jsonb
                END AS metadata,
                payload.chain_api_response
            FROM payload_rows payload
            JOIN contracts contract
              ON contract.chain_id = $1
             AND lower(contract.symbol) = lower(payload.symbol)
            JOIN nfts nft
              ON nft.chain_id = $1
             AND nft.contract_id = contract.id
             AND nft.token_id = payload.token_id
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
                creator_address,
                chain_id,
                0,
                0,
                0,
                0,
                0,
                0
            FROM target_values
            WHERE creator_address IS NOT NULL
              AND creator_address <> 'NULL'
            ON CONFLICT (chain_id, address) DO UPDATE SET
                address = addresses.address
            RETURNING id, chain_id, address
        ),
        series_values AS MATERIALIZED (
            -- Some current RPC series responses do not expose presentation
            -- metadata, while getNFT/getNFTs does. Use only the already
            -- parsed RPC NFT properties, and only as a fallback for blank or
            -- generated series presentation fields.
            SELECT DISTINCT ON (contract_id, series_id)
                contract_id,
                series_id,
                name,
                description,
                image
            FROM target_values
            WHERE series_id IS NOT NULL
            ORDER BY
                contract_id,
                series_id,
                CASE WHEN mint_number IS NOT NULL AND mint_number > 0 THEN 0 ELSE 1 END,
                mint_number NULLS LAST,
                nft_id
        ),
        upserted_series AS (
            INSERT INTO series (
                contract_id,
                series_id,
                current_supply,
                max_supply,
                name,
                description,
                image,
                royalties,
                type,
                has_locked,
                nsfw,
                blacklisted,
                dm_unix_seconds
            )
            SELECT
                contract_id,
                series_id,
                0,
                0,
                name,
                description,
                image,
                0,
                0,
                FALSE,
                FALSE,
                FALSE,
                $3
            FROM series_values
            ON CONFLICT (contract_id, series_id) DO UPDATE SET
                name = CASE
                    WHEN EXCLUDED.name IS NOT NULL
                         AND (
                             series.name IS NULL
                             OR BTRIM(series.name) = ''
                             OR series.name LIKE 'Series #% for %'
                         )
                    THEN EXCLUDED.name
                    ELSE series.name
                END,
                description = CASE
                    WHEN EXCLUDED.description IS NOT NULL
                         AND (series.description IS NULL OR BTRIM(series.description) = '')
                    THEN EXCLUDED.description
                    ELSE series.description
                END,
                image = CASE
                    WHEN EXCLUDED.image IS NOT NULL
                         AND (series.image IS NULL OR BTRIM(series.image) = '')
                    THEN EXCLUDED.image
                    ELSE series.image
                END,
                dm_unix_seconds = CASE
                    WHEN (
                        EXCLUDED.name IS NOT NULL
                        AND (
                            series.name IS NULL
                            OR BTRIM(series.name) = ''
                            OR series.name LIKE 'Series #% for %'
                        )
                    )
                    OR (
                        EXCLUDED.description IS NOT NULL
                        AND (series.description IS NULL OR BTRIM(series.description) = '')
                    )
                    OR (
                        EXCLUDED.image IS NOT NULL
                        AND (series.image IS NULL OR BTRIM(series.image) = '')
                    )
                    THEN $3
                    ELSE series.dm_unix_seconds
                END
            RETURNING id, contract_id, series_id
        )
        UPDATE nfts nft
        SET
            creator_address_id = COALESCE(upserted_creators.id, nft.creator_address_id),
            series_id = COALESCE(upserted_series.id, nft.series_id),
            mint_number = CASE
                WHEN target_values.mint_number IS NOT NULL AND target_values.mint_number > 0
                THEN target_values.mint_number
                ELSE nft.mint_number
            END,
            mint_date_unix_seconds = CASE
                WHEN target_values.mint_date_unix_seconds IS NOT NULL
                     AND target_values.mint_date_unix_seconds > 0
                THEN target_values.mint_date_unix_seconds
                ELSE nft.mint_date_unix_seconds
            END,
            rom = COALESCE(target_values.rom, nft.rom),
            ram = COALESCE(target_values.ram, nft.ram),
            name = COALESCE(target_values.name, nft.name),
            description = COALESCE(target_values.description, nft.description),
            image = COALESCE(target_values.image, nft.image),
            info_url = COALESCE(target_values.info_url, nft.info_url),
            metadata = CASE
                WHEN target_values.metadata <> '{}'::jsonb
                THEN COALESCE(nft.metadata, '{}'::jsonb) || target_values.metadata
                ELSE nft.metadata
            END,
            chain_api_response = COALESCE(target_values.chain_api_response, nft.chain_api_response),
            dm_unix_seconds = $3
        FROM target_values
        LEFT JOIN upserted_creators
          ON upserted_creators.chain_id = target_values.chain_id
         AND upserted_creators.address = target_values.creator_address
        LEFT JOIN upserted_series
          ON upserted_series.contract_id = target_values.contract_id
         AND upserted_series.series_id = target_values.series_id
        WHERE nft.id = target_values.nft_id
        "#,
    )
    .bind(chain_id)
    .bind(payloads)
    .bind(dm_unix_seconds)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

pub async fn fetch_series_rpc_metadata_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    min_event_block_height: i64,
    batch_size: i64,
) -> Result<Vec<SeriesRpcMetadataCandidate>, DbError> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        WITH boundary AS (
            SELECT COALESCE((
                SELECT block.timestamp_unix_seconds
                FROM blocks block
                WHERE block.chain_id = $1
                  AND block.height = $2
                ORDER BY block.id
                LIMIT 1
            ), 0) AS timestamp_unix_seconds
        )
        SELECT
            contract.symbol,
            series.series_id
        FROM series series
        JOIN contracts contract
          ON contract.id = series.contract_id
         AND contract.chain_id = $1
        CROSS JOIN boundary
        WHERE NULLIF(series.series_id, '') IS NOT NULL
          AND series.chain_api_response IS NULL
          AND series.series_created_unix_seconds > boundary.timestamp_unix_seconds
        ORDER BY series.id ASC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(min_event_block_height)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, series_id)| SeriesRpcMetadataCandidate { symbol, series_id })
        .collect())
}

pub async fn fetch_series_rpc_metadata_repair_candidates(
    conn: &mut PgConnection,
    chain_id: i32,
    min_event_block_height: i64,
    batch_size: i64,
) -> Result<Vec<SeriesRpcMetadataCandidate>, DbError> {
    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        WITH boundary AS (
            SELECT COALESCE((
                SELECT block.timestamp_unix_seconds
                FROM blocks block
                WHERE block.chain_id = $1
                  AND block.height = $2
                ORDER BY block.id
                LIMIT 1
            ), 0) AS timestamp_unix_seconds
        )
        SELECT
            contract.symbol,
            series.series_id
        FROM series series
        JOIN contracts contract
          ON contract.id = series.contract_id
         AND contract.chain_id = $1
        LEFT JOIN addresses creator
          ON creator.id = series.creator_address_id
        CROSS JOIN boundary
        WHERE NULLIF(series.series_id, '') IS NOT NULL
          AND series.chain_api_response IS NOT NULL
          AND series.series_created_unix_seconds > boundary.timestamp_unix_seconds
          AND (
              (
                  series.chain_api_response->>'currentSupply' ~ '^[0-9]+$'
                  AND series.current_supply IS DISTINCT FROM LEAST(
                      (series.chain_api_response->>'currentSupply')::numeric,
                      2147483647
                  )::integer
              )
              OR (
                  series.chain_api_response->>'maxSupply' ~ '^[0-9]+$'
                  AND series.max_supply IS DISTINCT FROM LEAST(
                      (series.chain_api_response->>'maxSupply')::numeric,
                      2147483647
                  )::integer
              )
              OR (
                  NULLIF(series.chain_api_response->>'ownerAddress', '') IS NOT NULL
                  AND creator.address IS DISTINCT FROM series.chain_api_response->>'ownerAddress'
              )
          )
        ORDER BY series.id ASC
        LIMIT $3
        "#,
    )
    .bind(chain_id)
    .bind(min_event_block_height)
    .bind(batch_size)
    .fetch_all(&mut *conn)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(symbol, series_id)| SeriesRpcMetadataCandidate { symbol, series_id })
        .collect())
}

pub async fn apply_series_rpc_metadata(
    conn: &mut PgConnection,
    chain_id: i32,
    upserts: &[SeriesRpcMetadataUpsert],
) -> Result<u64, DbError> {
    if upserts.is_empty() {
        return Ok(0);
    }

    let payloads = serde_json::to_value(upserts)?;
    let dm_unix_seconds = Utc::now().timestamp();

    let result = sqlx::query(
        r#"
        WITH payload_rows AS MATERIALIZED (
            SELECT *
            FROM jsonb_to_recordset($2::jsonb) AS payload(
                symbol text,
                series_id text,
                current_supply integer,
                max_supply integer,
                mode text,
                creator_address text,
                name text,
                description text,
                image text,
                royalties integer,
                series_type integer,
                has_locked boolean,
                metadata jsonb,
                chain_api_response jsonb
            )
            WHERE NULLIF(symbol, '') IS NOT NULL
              AND NULLIF(series_id, '') IS NOT NULL
        ),
        target_values AS MATERIALIZED (
            SELECT
                series.id AS series_db_id,
                series.contract_id,
                contract.chain_id,
                NULLIF(payload.mode, '') AS mode_name,
                NULLIF(payload.creator_address, '') AS creator_address,
                payload.current_supply,
                payload.max_supply,
                NULLIF(payload.name, '') AS name,
                NULLIF(payload.description, '') AS description,
                NULLIF(payload.image, '') AS image,
                payload.royalties,
                payload.series_type,
                payload.has_locked,
                CASE
                    WHEN jsonb_typeof(payload.metadata) = 'object' THEN payload.metadata
                    ELSE '{}'::jsonb
                END AS metadata,
                payload.chain_api_response
            FROM payload_rows payload
            JOIN contracts contract
              ON contract.chain_id = $1
             AND lower(contract.symbol) = lower(payload.symbol)
            JOIN series series
              ON series.contract_id = contract.id
             AND series.series_id = payload.series_id
        ),
        upserted_modes AS (
            INSERT INTO series_modes (mode_name)
            SELECT DISTINCT mode_name
            FROM target_values
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
                creator_address,
                chain_id,
                0,
                0,
                0,
                0,
                0,
                0
            FROM target_values
            WHERE creator_address IS NOT NULL
              AND creator_address <> 'NULL'
            ON CONFLICT (chain_id, address) DO UPDATE SET
                address = addresses.address
            RETURNING id, chain_id, address
        )
        UPDATE series series
        SET
            current_supply = COALESCE(target_values.current_supply, series.current_supply),
            max_supply = COALESCE(target_values.max_supply, series.max_supply),
            series_mode_id = COALESCE(upserted_modes.id, series.series_mode_id),
            creator_address_id = COALESCE(upserted_creators.id, series.creator_address_id),
            name = COALESCE(target_values.name, series.name),
            description = COALESCE(target_values.description, series.description),
            image = COALESCE(target_values.image, series.image),
            royalties = COALESCE(target_values.royalties, series.royalties),
            type = COALESCE(target_values.series_type, series.type),
            has_locked = COALESCE(target_values.has_locked, series.has_locked),
            metadata = CASE
                WHEN target_values.metadata <> '{}'::jsonb
                THEN COALESCE(series.metadata, '{}'::jsonb) || target_values.metadata
                ELSE series.metadata
            END,
            chain_api_response = COALESCE(target_values.chain_api_response, series.chain_api_response),
            dm_unix_seconds = $3
        FROM target_values
        LEFT JOIN upserted_modes
          ON upserted_modes.mode_name = target_values.mode_name
        LEFT JOIN upserted_creators
          ON upserted_creators.chain_id = target_values.chain_id
         AND upserted_creators.address = target_values.creator_address
        WHERE series.id = target_values.series_db_id
        "#,
    )
    .bind(chain_id)
    .bind(payloads)
    .bind(dm_unix_seconds)
    .execute(&mut *conn)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nft_rpc_metadata_updates_nft_and_series_presentation_from_rpc()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let symbol = format!("RSTRPC{}", &suffix[..8]);
        let token_id = "515151515151515151";
        let series_id = "919191919191919191";
        let creator = format!("PTESTCREATOR{suffix}");
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &symbol).await?;

        sqlx::query(
            r#"
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
            VALUES (0, $1, NULL, 0, 0, FALSE, FALSE, FALSE, $2, $3)
            "#,
        )
        .bind(token_id)
        .bind(chain_id)
        .bind(contract_id)
        .execute(&mut *transaction)
        .await?;

        // NFT RPC metadata is allowed to seed series presentation because the
        // values come from getNFT/getNFTs properties, not local ROM/RAM decode.
        let updated = apply_nft_rpc_metadata(
            &mut transaction,
            chain_id,
            &[NftRpcMetadataUpsert {
                symbol: symbol.clone(),
                token_id: token_id.to_owned(),
                series_id: Some(series_id.to_owned()),
                creator_address: Some(creator.clone()),
                mint_number: Some(4),
                mint_date_unix_seconds: Some(1_800_333_444),
                rom: Some("AABB".to_owned()),
                ram: Some("CCDD".to_owned()),
                name: Some("RPC NFT".to_owned()),
                description: Some("RPC description".to_owned()),
                image: Some("https://cdn.example/nft.png".to_owned()),
                info_url: Some("https://example/nft".to_owned()),
                metadata: serde_json::json!({
                    "name": "RPC NFT",
                    "status": "Transferable",
                    "rom": "AABB"
                }),
                chain_api_response: serde_json::json!({
                    "id": token_id,
                    "series": series_id,
                    "creatorAddress": creator
                }),
            }],
        )
        .await?;

        assert_eq!(updated, 1);

        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                String,
                String,
                i32,
                i64,
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
            ),
        >(
            r#"
            SELECT
                nft.rom,
                nft.ram,
                nft.name,
                nft.description,
                nft.image,
                nft.info_url,
                nft.mint_number,
                nft.mint_date_unix_seconds,
                nft.metadata->>'name',
                nft.metadata->>'status',
                nft.chain_api_response->>'id',
                creator.address,
                series.series_id,
                series.name,
                series.description,
                series.image
            FROM nfts nft
            JOIN addresses creator ON creator.id = nft.creator_address_id
            JOIN series series ON series.id = nft.series_id
            WHERE nft.contract_id = $1
              AND nft.token_id = $2
            "#,
        )
        .bind(contract_id)
        .bind(token_id)
        .fetch_one(&mut *transaction)
        .await?;

        assert_eq!(row.0, "AABB");
        assert_eq!(row.1, "CCDD");
        assert_eq!(row.2, "RPC NFT");
        assert_eq!(row.3, "RPC description");
        assert_eq!(row.4, "https://cdn.example/nft.png");
        assert_eq!(row.5, "https://example/nft");
        assert_eq!(row.6, 4);
        assert_eq!(row.7, 1_800_333_444);
        assert_eq!(row.8, "RPC NFT");
        assert_eq!(row.9, "Transferable");
        assert_eq!(row.10, token_id);
        assert_eq!(row.11, creator);
        assert_eq!(row.12, series_id);
        assert_eq!(row.13.as_deref(), Some("RPC NFT"));
        assert_eq!(row.14.as_deref(), Some("RPC description"));
        assert_eq!(row.15.as_deref(), Some("https://cdn.example/nft.png"));

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn nft_rpc_metadata_mint_block_candidates_are_targeted()
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
        let symbol = format!("RSTMINT{}", &suffix[..8]);
        let token_id = format!("515151{}", &suffix[..16]);
        let owner = format!("PTESTMINT{suffix}");
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &symbol).await?;

        let block = upsert_block(
            &mut transaction,
            BlockUpsert {
                chain,
                height: BlockHeight::new(9_900_200_000),
                hash: format!("TESTMINTBLOCK{suffix}"),
                previous_hash: None,
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                timestamp_unix_seconds: 1_800_200_000,
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
                hash: format!("TESTMINTTX{suffix}"),
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
        let nft_id = sqlx::query_scalar::<_, i32>(
            r#"
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
            VALUES (0, $1, NULL, 0, 0, FALSE, FALSE, FALSE, $2, $3)
            RETURNING id
            "#,
        )
        .bind(&token_id)
        .bind(chain_id)
        .bind(contract_id)
        .fetch_one(&mut *transaction)
        .await?;

        crate::events::upsert_event(
            &mut transaction,
            &EventUpsert {
                transaction_id: tx.id,
                chain_id,
                event_index: 1,
                event_kind: "TokenMint".to_owned(),
                address: Some(owner),
                target_address: None,
                contract: Some(symbol.clone()),
                token_id: Some(token_id.clone()),
                raw_data: Some("AA".to_owned()),
                payload_format: Some("live.v1".to_owned()),
                payload_json: Some(serde_json::json!({
                    "event_kind": "TokenMint",
                    "contract": symbol,
                    "token_id": token_id
                })),
                timestamp_unix_seconds: block.timestamp_unix_seconds,
                date_unix_seconds: block.timestamp_unix_seconds,
                burned: None,
                nsfw: false,
                blacklisted: false,
            },
            None,
        )
        .await?;
        sqlx::query(
            r#"
            UPDATE events
            SET nft_id = $1
            WHERE transaction_id = $2
              AND event_index = 1
            "#,
        )
        .bind(nft_id)
        .bind(tx.id)
        .execute(&mut *transaction)
        .await?;

        let candidates = fetch_nft_rpc_metadata_candidates_for_mint_block(
            &mut transaction,
            chain_id,
            block.height,
            10,
        )
        .await?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].symbol, symbol);
        assert_eq!(candidates[0].token_id, token_id);

        let wrong_block_candidates = fetch_nft_rpc_metadata_candidates_for_mint_block(
            &mut transaction,
            chain_id,
            block.height + 1,
            10,
        )
        .await?;
        assert!(wrong_block_candidates.is_empty());

        sqlx::query(
            r#"
            UPDATE nfts
            SET chain_api_response = '{}'::jsonb
            WHERE id = $1
            "#,
        )
        .bind(nft_id)
        .execute(&mut *transaction)
        .await?;
        let already_synced_candidates = fetch_nft_rpc_metadata_candidates_for_mint_block(
            &mut transaction,
            chain_id,
            block.height,
            10,
        )
        .await?;
        assert!(already_synced_candidates.is_empty());

        // Repair mode deliberately revisits already-fetched RPC rows only when
        // a direct materialized field can be proven stale from stored RPC data.
        sqlx::query(
            r#"
            UPDATE nfts
            SET chain_api_response = jsonb_build_object(
                    'properties',
                    jsonb_build_array(jsonb_build_object('key', 'Created', 'value', '1800456789'))
                ),
                mint_date_unix_seconds = 0
            WHERE id = $1
            "#,
        )
        .bind(nft_id)
        .execute(&mut *transaction)
        .await?;
        let repair_candidates = fetch_nft_rpc_metadata_repair_candidates(
            &mut transaction,
            chain_id,
            block.height - 1,
            10,
        )
        .await?;
        assert_eq!(repair_candidates.len(), 1);
        assert_eq!(repair_candidates[0].symbol, symbol);
        assert_eq!(repair_candidates[0].token_id, token_id);

        sqlx::query(
            r#"
            UPDATE nfts
            SET mint_date_unix_seconds = 1800456789
            WHERE id = $1
            "#,
        )
        .bind(nft_id)
        .execute(&mut *transaction)
        .await?;
        let repaired_candidates = fetch_nft_rpc_metadata_repair_candidates(
            &mut transaction,
            chain_id,
            block.height - 1,
            10,
        )
        .await?;
        assert!(repaired_candidates.is_empty());

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn series_rpc_metadata_updates_only_direct_series_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let suffix = Uuid::now_v7().simple().to_string();
        let symbol = format!("RSTSRS{}", &suffix[..8]);
        let series_id = "818181818181818181";
        let creator = format!("PTESTSERIES{suffix}");
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &symbol).await?;

        sqlx::query(
            r#"
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
                dm_unix_seconds
            )
            VALUES ($1, $2, 0, 0, 0, 0, FALSE, FALSE, FALSE, 0)
            "#,
        )
        .bind(contract_id)
        .bind(series_id)
        .execute(&mut *transaction)
        .await?;

        let updated = apply_series_rpc_metadata(
            &mut transaction,
            chain_id,
            &[SeriesRpcMetadataUpsert {
                symbol: symbol.clone(),
                series_id: series_id.to_owned(),
                current_supply: Some(7),
                max_supply: Some(10),
                mode: Some("Duplicated".to_owned()),
                creator_address: Some(creator.clone()),
                name: Some("RPC Series".to_owned()),
                description: Some("RPC series description".to_owned()),
                image: Some("https://cdn.example/series.png".to_owned()),
                royalties: Some(250),
                series_type: Some(2),
                has_locked: Some(true),
                metadata: serde_json::json!({
                    "name": "RPC Series",
                    "mode": "Duplicated",
                    "rom": "AABB"
                }),
                chain_api_response: serde_json::json!({
                    "seriesId": series_id,
                    "ownerAddress": creator,
                    "currentSupply": "7"
                }),
            }],
        )
        .await?;

        assert_eq!(updated, 1);

        let row = sqlx::query_as::<
            _,
            (
                i32,
                i32,
                String,
                String,
                String,
                i32,
                i32,
                bool,
                String,
                String,
                String,
                String,
            ),
        >(
            r#"
            SELECT
                series.current_supply,
                series.max_supply,
                mode.mode_name,
                creator.address,
                series.name,
                series.royalties::integer,
                series.type::integer,
                series.has_locked,
                series.description,
                series.image,
                series.metadata->>'rom',
                series.chain_api_response->>'seriesId'
            FROM series series
            JOIN series_modes mode ON mode.id = series.series_mode_id
            JOIN addresses creator ON creator.id = series.creator_address_id
            WHERE series.contract_id = $1
              AND series.series_id = $2
            "#,
        )
        .bind(contract_id)
        .bind(series_id)
        .fetch_one(&mut *transaction)
        .await?;

        assert_eq!(
            row,
            (
                7,
                10,
                "Duplicated".to_owned(),
                creator,
                "RPC Series".to_owned(),
                250,
                2,
                true,
                "RPC series description".to_owned(),
                "https://cdn.example/series.png".to_owned(),
                "AABB".to_owned(),
                series_id.to_owned(),
            )
        );

        transaction.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn series_rpc_metadata_repair_candidates_find_drift_from_stored_rpc()
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
        let symbol = format!("RSTSREP{}", &suffix[..8]);
        let series_id = "717171717171717171";
        let owner = format!("PTESTSERIESREPAIR{suffix}");
        let contract_id = upsert_contract_id(&mut transaction, chain_id, &symbol).await?;

        sqlx::query(
            r#"
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
                series_created_unix_seconds,
                chain_api_response
            )
            VALUES ($1, $2, 9, 25, 0, 0, FALSE, FALSE, FALSE, 0, 1800300000, $3)
            "#,
        )
        .bind(contract_id)
        .bind(series_id)
        .bind(serde_json::json!({
            "seriesId": series_id,
            "currentSupply": "7",
            "maxSupply": "25",
            "ownerAddress": owner
        }))
        .execute(&mut *transaction)
        .await?;

        let boundary_height = i64::try_from(explorer_domain::MAIN_ZERO_STATE_BOUNDARY_HEIGHT)?;
        let candidates = fetch_series_rpc_metadata_repair_candidates(
            &mut transaction,
            chain_id,
            boundary_height,
            10,
        )
        .await?;
        assert!(
            candidates
                .iter()
                .any(|candidate| candidate.symbol == symbol && candidate.series_id == series_id)
        );

        let owner_id = upsert_address_id(&mut transaction, chain_id, &owner).await?;
        sqlx::query(
            r#"
            UPDATE series
            SET current_supply = 7,
                creator_address_id = $1
            WHERE contract_id = $2
              AND series_id = $3
            "#,
        )
        .bind(owner_id)
        .bind(contract_id)
        .bind(series_id)
        .execute(&mut *transaction)
        .await?;

        let repaired_candidates = fetch_series_rpc_metadata_repair_candidates(
            &mut transaction,
            chain_id,
            boundary_height,
            10,
        )
        .await?;
        assert!(
            !repaired_candidates
                .iter()
                .any(|candidate| candidate.symbol == symbol && candidate.series_id == series_id)
        );

        transaction.rollback().await?;
        Ok(())
    }
}
