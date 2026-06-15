//! Stake-snapshot projection: current-stake snapshot upsert plus the Soul-Masters
//! daily/monthly projector. The projector builds the daily/monthly series forward
//! from a captured per-address stake seed (`capture_stake_boundary_slice`); see
//! this module's tests.
use super::*;
// Calendar-math trait methods used by stake_snapshot_next_month_start. Imported explicitly
// so the helper compiles regardless of the db crate root's glob imports.
use chrono::{Datelike, TimeZone};

const STAKE_SNAPSHOT_PROJECTOR_SOURCE: &str = "staking-snapshot-projector.v1";
const STAKE_SNAPSHOT_SECONDS_PER_DAY: i64 = 86_400;
const STAKE_SNAPSHOT_MASTER_THRESHOLD_RAW: i64 = 5_000_000_000_000;

pub async fn upsert_current_stake_snapshots(
    conn: &mut PgConnection,
    chain_id: i32,
    now_unix_seconds: i64,
) -> Result<(), DbError> {
    sqlx::query(
        r#"
        WITH clock AS (
            SELECT
                ($2::bigint - MOD($2::bigint, 86400)) AS date_unix_seconds,
                EXTRACT(EPOCH FROM date_trunc('month', to_timestamp($2::double precision)))::bigint AS month_unix_seconds
        ),
        soul AS (
            SELECT COALESCE(NULLIF(current_supply_raw, '')::numeric, 0) AS supply_raw
            FROM tokens
            WHERE chain_id = $1
              AND symbol = 'SOUL'
            ORDER BY id
            LIMIT 1
        ),
        staked AS (
            SELECT COALESCE(SUM(COALESCE(NULLIF(staked_amount_raw, '')::numeric, 0)), 0) AS staked_raw
            FROM addresses
            WHERE chain_id = $1
              AND address <> 'NULL'
        ),
        counts AS (
            SELECT
                COUNT(*) FILTER (WHERE organization.name = 'stakers')::integer AS stakers_count,
                COUNT(*) FILTER (WHERE organization.name = 'masters')::integer AS masters_count
            FROM organization_addresses membership
            JOIN organizations organization ON organization.id = membership.organization_id
            JOIN addresses address ON address.id = membership.address_id
            WHERE address.chain_id = $1
              AND organization.name IN ('stakers', 'masters')
        ),
        metrics AS (
            SELECT
                clock.date_unix_seconds,
                clock.month_unix_seconds,
                staked.staked_raw,
                COALESCE(soul.supply_raw, 0) AS soul_supply_raw,
                counts.stakers_count,
                counts.masters_count,
                CASE
                    WHEN COALESCE(soul.supply_raw, 0) > 0
                    THEN staked.staked_raw / soul.supply_raw
                    ELSE 0
                END AS staking_ratio
            FROM clock
            CROSS JOIN staked
            CROSS JOIN counts
            LEFT JOIN soul ON TRUE
        ),
        upsert_daily AS (
            INSERT INTO staking_progress_dailies (
                chain_id,
                date_unix_seconds,
                staked_soul_raw,
                soul_supply_raw,
                stakers_count,
                masters_count,
                staking_ratio,
                captured_at_unix_seconds,
                source
            )
            SELECT
                $1,
                date_unix_seconds,
                staked_raw::text,
                soul_supply_raw::text,
                stakers_count,
                masters_count,
                staking_ratio,
                $2,
                'balance-sync.v1'
            FROM metrics
            ON CONFLICT (chain_id, date_unix_seconds) DO UPDATE SET
                staked_soul_raw = EXCLUDED.staked_soul_raw,
                soul_supply_raw = EXCLUDED.soul_supply_raw,
                stakers_count = EXCLUDED.stakers_count,
                masters_count = EXCLUDED.masters_count,
                staking_ratio = EXCLUDED.staking_ratio,
                captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
                source = EXCLUDED.source
            RETURNING id
        )
        INSERT INTO soul_masters_monthlies (
            chain_id,
            month_unix_seconds,
            masters_count,
            captured_at_unix_seconds,
            source
        )
        SELECT
            $1,
            month_unix_seconds,
            masters_count,
            $2,
            'balance-sync.v1'
        FROM metrics
        ON CONFLICT (chain_id, month_unix_seconds) DO UPDATE SET
            masters_count = EXCLUDED.masters_count,
            captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
            source = EXCLUDED.source
        "#,
    )
    .bind(chain_id)
    .bind(now_unix_seconds)
    .execute(&mut *conn)
    .await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct StakeSnapshotState {
    stakes_by_address: HashMap<String, BigInt>,
    total_staked_raw: BigInt,
    soul_supply_raw: BigInt,
    stakers_count: i32,
    masters_count: i32,
}

#[derive(Debug, Clone)]
struct StakeSnapshotEventRow {
    event_id: i32,
    tx_id: i32,
    kind: String,
    timestamp_unix_seconds: i64,
    payload_identity: String,
    token_symbol: Option<String>,
    value_raw: Option<BigInt>,
    address: Option<String>,
    market_quote_symbol: Option<String>,
    tx_has_stake_call: bool,
    tx_has_unstake_call: bool,
    tx_has_claim_call: bool,
    tx_apply_inflation_result_soul_delta_raw: Option<BigInt>,
}

#[derive(Debug, Clone)]
struct StakeSnapshotDailyPoint {
    date_unix_seconds: i64,
    staked_soul_raw: String,
    soul_supply_raw: String,
    stakers_count: i32,
    masters_count: i32,
    captured_at_unix_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StakeSnapshotTxKind {
    Normal,
    MarketEvent,
    SmReward,
    StakeReward,
}

/// Result of the one-time boundary-slice capture.
#[derive(Debug, Clone)]
pub struct StakeBoundarySliceReport {
    pub chain_id: i32,
    pub boundary_day_unix_seconds: i64,
    pub masters_count: i32,
    pub stakers_count: i32,
    pub staked_soul_raw: String,
    pub soul_supply_raw: String,
    pub addresses_written: usize,
}

/// One-time, offline computation of the per-address stake seed that bootstraps the forward
/// projector. The per-address SOUL stake at the seed day is not directly available (RPC
/// `getAccount` returns only current state), so it is derived once by walking the known current
/// state back over the stake events, then stored in `stake_boundary_state` +
/// `stake_boundary_balances`.
///
/// Run it once on a fully populated database; afterwards the worker builds the Soul-Masters series
/// forward from the stored seed (`project_stake_snapshots_forward`).
pub async fn capture_stake_boundary_slice(
    pool: &PgPool,
    chain_id: i32,
) -> Result<StakeBoundarySliceReport, DbError> {
    let mut transaction = pool.begin().await?;
    let report = capture_stake_boundary_slice_in_tx(&mut transaction, chain_id).await?;
    transaction.commit().await?;
    Ok(report)
}

async fn capture_stake_boundary_slice_in_tx(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<StakeBoundarySliceReport, DbError> {
    // Anchor on the start block itself (not a daily-gap heuristic): the seed day is the UTC day of
    // `main` block MAIN_ZERO_STATE_BOUNDARY_HEIGHT.
    let boundary_block_ts = load_boundary_block_timestamp(conn, chain_id).await?;
    let boundary_day = stake_snapshot_day_start(boundary_block_ts);

    let Some(cursor_timestamp) = load_stake_snapshot_cursor_timestamp(conn, chain_id).await? else {
        return Err(DbError::StakeSnapshotReplay {
            reason: "chain has no projected block timestamp; cannot capture the stake seed"
                .to_owned(),
        });
    };

    // Walk the stake events (day after the seed day .. cursor) back from the known current state
    // to the end of the seed day. `state` then holds the per-address stake at the seed day.
    let from_ts = boundary_day + STAKE_SNAPSHOT_SECONDS_PER_DAY;
    let events = load_stake_snapshot_events(conn, chain_id, from_ts, cursor_timestamp).await?;
    let mut state = load_current_stake_snapshot_state(conn, chain_id).await?;
    reverse_replay_stake_snapshot_events(&mut state, &events)?;

    // Freeze the slice. Replace any prior capture for this chain so re-running is idempotent.
    sqlx::query("DELETE FROM stake_boundary_balances WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *conn)
        .await?;
    let mut addresses_written = 0_usize;
    for (address, staked) in &state.stakes_by_address {
        if staked <= &BigInt::zero() {
            continue;
        }
        sqlx::query(
            "INSERT INTO stake_boundary_balances (chain_id, address, staked_amount_raw) VALUES ($1, $2, $3)",
        )
        .bind(chain_id)
        .bind(address)
        .bind(staked.to_string())
        .execute(&mut *conn)
        .await?;
        addresses_written += 1;
    }

    sqlx::query(
        r#"
        INSERT INTO stake_boundary_state
            (chain_id, boundary_day_unix_seconds, soul_supply_raw, masters_count,
             stakers_count, staked_soul_raw, captured_at_unix_seconds, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (chain_id) DO UPDATE SET
            boundary_day_unix_seconds = EXCLUDED.boundary_day_unix_seconds,
            soul_supply_raw = EXCLUDED.soul_supply_raw,
            masters_count = EXCLUDED.masters_count,
            stakers_count = EXCLUDED.stakers_count,
            staked_soul_raw = EXCLUDED.staked_soul_raw,
            captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
            source = EXCLUDED.source
        "#,
    )
    .bind(chain_id)
    .bind(boundary_day)
    .bind(state.soul_supply_raw.to_string())
    .bind(state.masters_count)
    .bind(state.stakers_count)
    .bind(state.total_staked_raw.to_string())
    .bind(cursor_timestamp)
    .bind("boundary-unwind.v1")
    .execute(&mut *conn)
    .await?;

    Ok(StakeBoundarySliceReport {
        chain_id,
        boundary_day_unix_seconds: boundary_day,
        masters_count: state.masters_count,
        stakers_count: state.stakers_count,
        staked_soul_raw: state.total_staked_raw.to_string(),
        soul_supply_raw: state.soul_supply_raw.to_string(),
        addresses_written,
    })
}

/// Reads the stored stake seed into a `StakeSnapshotState` plus its day. Returns `None` if no seed
/// has been captured for the chain. This is the forward builder's starting anchor.
async fn load_stake_boundary_slice(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<(i64, StakeSnapshotState)>, DbError> {
    let Some(header) = sqlx::query(
        "SELECT boundary_day_unix_seconds, soul_supply_raw FROM stake_boundary_state WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };
    let boundary_day: i64 = header.get("boundary_day_unix_seconds");
    let soul_supply_raw = parse_stake_snapshot_raw(
        "soul_supply_raw",
        &header.get::<String, _>("soul_supply_raw"),
    )?;

    let rows = sqlx::query(
        "SELECT address, staked_amount_raw FROM stake_boundary_balances WHERE chain_id = $1",
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;

    let threshold = stake_snapshot_master_threshold();
    let mut state = StakeSnapshotState {
        stakes_by_address: HashMap::with_capacity(rows.len()),
        total_staked_raw: BigInt::zero(),
        soul_supply_raw,
        stakers_count: 0,
        masters_count: 0,
    };
    for row in rows {
        let address: String = row.get("address");
        let staked = parse_stake_snapshot_raw(
            "staked_amount_raw",
            &row.get::<String, _>("staked_amount_raw"),
        )?;
        if staked <= BigInt::zero() {
            continue;
        }
        if staked >= threshold {
            state.masters_count += 1;
        }
        state.stakers_count += 1;
        state.total_staked_raw += &staked;
        state.stakes_by_address.insert(address, staked);
    }
    Ok(Some((boundary_day, state)))
}

async fn load_boundary_block_timestamp(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<i64, DbError> {
    let height = i64::try_from(explorer_domain::MAIN_ZERO_STATE_BOUNDARY_HEIGHT).map_err(|_| {
        DbError::StakeSnapshotReplay {
            reason: "start height overflows i64".to_owned(),
        }
    })?;
    sqlx::query_scalar::<_, i64>(
        "SELECT timestamp_unix_seconds FROM blocks WHERE chain_id = $1 AND height = $2",
    )
    .bind(chain_id)
    .bind(height)
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| DbError::StakeSnapshotReplay {
        reason: "start-height block is not present; cannot anchor the stake seed".to_owned(),
    })
}

/// Result of a forward Soul-Masters build.
#[derive(Debug, Clone, Serialize)]
pub struct StakeForwardBuildReport {
    pub chain_id: i32,
    pub boundary_day_unix_seconds: i64,
    pub boundary_masters_count: i32,
    pub validated: bool,
    pub daily_upserted: u64,
    pub monthly_upserted: u64,
    pub skipped_reason: Option<String>,
}

pub async fn project_stake_snapshots_forward(
    pool: &PgPool,
    chain_id: i32,
) -> Result<StakeForwardBuildReport, DbError> {
    let mut transaction = pool.begin().await?;
    let report = project_stake_snapshots_forward_in_tx(&mut transaction, chain_id).await?;
    transaction.commit().await?;
    Ok(report)
}

/// Builds the Soul-Masters daily+monthly series forward from the stored stake seed
/// (`stake_boundary_*`) across the stake events up to the projected tip: the per-address seed state
/// is read from `stake_boundary_*`, then `build_stake_snapshot_daily_points` advances it day by
/// day. Idempotent (safe every tick) and validated against the independent `balance-sync.v1` tip
/// dailies. The seed day and its month are never overwritten (the monthly rollup starts at the
/// month after the seed day).
async fn project_stake_snapshots_forward_in_tx(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<StakeForwardBuildReport, DbError> {
    let skip = |boundary_day: i64, boundary_masters: i32, reason: &str| StakeForwardBuildReport {
        chain_id,
        boundary_day_unix_seconds: boundary_day,
        boundary_masters_count: boundary_masters,
        validated: false,
        daily_upserted: 0,
        monthly_upserted: 0,
        skipped_reason: Some(reason.to_owned()),
    };

    let Some((boundary_day, boundary_state)) = load_stake_boundary_slice(conn, chain_id).await?
    else {
        return Ok(skip(
            0,
            0,
            "stake seed not captured; run capture_stake_boundary_slice first",
        ));
    };
    let boundary_masters = boundary_state.masters_count;

    let Some(cursor_timestamp) = load_stake_snapshot_cursor_timestamp(conn, chain_id).await? else {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "chain has no projected block timestamp",
        ));
    };
    // Build through (and including) the cursor's own day. That tip day is the only day the live
    // `balance-sync.v1` writer is guaranteed to have on a first sync, so it is the overlap the
    // validation gate needs; the write filter below still leaves that trusted tip day to
    // balance-sync.v1 and only persists the days before it.
    let target_exclusive_day =
        stake_snapshot_day_start(cursor_timestamp) + STAKE_SNAPSHOT_SECONDS_PER_DAY;
    let from_day = boundary_day + STAKE_SNAPSHOT_SECONDS_PER_DAY;
    if from_day >= target_exclusive_day {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "boundary is already at the tip; nothing to build",
        ));
    }

    // All stake events from the day after the seed day to the cursor.
    let events = load_stake_snapshot_events(conn, chain_id, from_day, cursor_timestamp).await?;
    let curve =
        build_stake_snapshot_daily_points(boundary_state, &events, from_day, target_exclusive_day)?;

    // Validate the forward curve against the independent live `balance-sync.v1` dailies; refuse to
    // write if any overlapping day disagrees or none overlap.
    //
    // Scope of this gate (important): the series is built forward from the stored seed and shares
    // no derivation with `balance-sync.v1` (which snapshots live RPC account state), so agreement
    // on the overlapping tip day(s) is a genuine end-to-end check, not a tautology. But only the
    // OVERLAP is checked per day; earlier days written below (before `first_trusted_day`) have no
    // `balance-sync.v1` row to compare against, so they are validated only by endpoint convergence,
    // not per day. Two compensating errors before the trusted window (a count miscounted up on one
    // day and back down on another) would still converge at the tip and be written with
    // `validated = true` — it is not a per-day proof of those earlier days.
    let trusted = load_trusted_stake_snapshot_dailies(conn, chain_id).await?;
    let mut validated_days = 0_usize;
    for point in &curve {
        let Some(expected) = trusted.get(&point.date_unix_seconds) else {
            continue;
        };
        if point.masters_count != expected.masters_count
            || point.stakers_count != expected.stakers_count
            || point.staked_soul_raw != expected.staked_soul_raw
        {
            return Ok(skip(
                boundary_day,
                boundary_masters,
                &format!(
                    "forward curve disagrees with balance-sync.v1 at day {}: masters {}/{}, stakers {}/{}; refusing to write",
                    point.date_unix_seconds,
                    point.masters_count,
                    expected.masters_count,
                    point.stakers_count,
                    expected.stakers_count,
                ),
            ));
        }
        validated_days += 1;
    }
    if validated_days == 0 {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "no balance-sync.v1 dailies overlap the built range; refusing to write blind",
        ));
    }

    // Write only the gap dailies (below the live balance-sync.v1 window) and the monthly rollup
    // starting the month after the seed day; never touch the seed day/month.
    let Some(first_trusted_day) = trusted.keys().copied().min() else {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "no balance-sync.v1 day after validation",
        ));
    };
    let daily_to_write = curve
        .into_iter()
        .filter(|point| point.date_unix_seconds < first_trusted_day)
        .collect::<Vec<_>>();
    let daily_upserted =
        upsert_stake_snapshot_daily_points(conn, chain_id, &daily_to_write).await?;
    let monthly_upserted = upsert_stake_snapshot_monthlies_from_daily(
        conn,
        chain_id,
        stake_snapshot_next_month_start(boundary_day),
        first_trusted_day,
    )
    .await?;

    Ok(StakeForwardBuildReport {
        chain_id,
        boundary_day_unix_seconds: boundary_day,
        boundary_masters_count: boundary_masters,
        validated: true,
        daily_upserted,
        monthly_upserted,
        skipped_reason: None,
    })
}

/// A trustworthy daily snapshot used to validate the projected series. These come from the live
/// `balance-sync.v1` writer (real-time DAO membership + on-chain stake), which is independent of
/// the projector, so an exact match proves the projected series is correct.
struct TrustedStakeSnapshotDaily {
    masters_count: i32,
    stakers_count: i32,
    staked_soul_raw: String,
}

async fn load_trusted_stake_snapshot_dailies(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<HashMap<i64, TrustedStakeSnapshotDaily>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT date_unix_seconds, staked_soul_raw, stakers_count, masters_count
        FROM staking_progress_dailies
        WHERE chain_id = $1
          AND source = 'balance-sync.v1'
        "#,
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;

    let mut map = HashMap::with_capacity(rows.len());
    for row in rows {
        let date_unix_seconds: i64 = row.get("date_unix_seconds");
        let staked_soul_raw: String = row
            .get::<Option<String>, _>("staked_soul_raw")
            .unwrap_or_default();
        map.insert(
            date_unix_seconds,
            TrustedStakeSnapshotDaily {
                masters_count: row.get("masters_count"),
                stakers_count: row.get("stakers_count"),
                staked_soul_raw,
            },
        );
    }
    Ok(map)
}

async fn load_stake_snapshot_cursor_timestamp(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<i64>, DbError> {
    let cursor_timestamp = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT block.timestamp_unix_seconds
        FROM chains chain
        JOIN LATERAL (
            SELECT block.timestamp_unix_seconds
            FROM blocks block
            WHERE block.chain_id = chain.id
              AND block.height <= chain.current_height
            ORDER BY block.height DESC
            LIMIT 1
        ) block ON TRUE
        WHERE chain.id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?;

    Ok(cursor_timestamp)
}

async fn load_current_stake_snapshot_state(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<StakeSnapshotState, DbError> {
    let soul_supply_raw = sqlx::query_scalar::<_, Option<String>>(
        r#"
        SELECT current_supply_raw
        FROM tokens
        WHERE chain_id = $1
          AND symbol = 'SOUL'
        ORDER BY id
        LIMIT 1
        "#,
    )
    .bind(chain_id)
    .fetch_one(&mut *conn)
    .await?
    .ok_or_else(|| DbError::TokenMissing {
        chain_id,
        symbol: "SOUL".to_owned(),
    })?;

    let rows = sqlx::query(
        r#"
        SELECT address, staked_amount_raw
        FROM addresses
        WHERE chain_id = $1
          AND address <> 'NULL'
          AND NULLIF(staked_amount_raw, '') IS NOT NULL
          AND NULLIF(staked_amount_raw, '')::numeric > 0
        "#,
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;

    let master_threshold = stake_snapshot_master_threshold();
    let mut state = StakeSnapshotState {
        stakes_by_address: HashMap::new(),
        total_staked_raw: BigInt::zero(),
        soul_supply_raw: parse_stake_snapshot_raw("current_supply_raw", &soul_supply_raw)?,
        stakers_count: 0,
        masters_count: 0,
    };

    for row in rows {
        let address: String = row.get("address");
        let staked_amount_raw: String = row.get("staked_amount_raw");
        let stake_raw = parse_stake_snapshot_raw("staked_amount_raw", &staked_amount_raw)?;
        if stake_raw <= BigInt::zero() {
            continue;
        }
        if stake_raw >= master_threshold {
            state.masters_count += 1;
        }
        state.stakers_count += 1;
        state.total_staked_raw += &stake_raw;
        state.stakes_by_address.insert(address, stake_raw);
    }

    Ok(state)
}

async fn load_stake_snapshot_events(
    conn: &mut PgConnection,
    chain_id: i32,
    from_ts: i64,
    to_ts: i64,
) -> Result<Vec<StakeSnapshotEventRow>, DbError> {
    let rows = sqlx::query(
        r#"
        SELECT
            event.id AS event_id,
            tx.id AS tx_id,
            event_kind.name AS kind,
            tx.timestamp_unix_seconds AS timestamp_unix_seconds,
            COALESCE(event.raw_data, event.payload_json::text, '') AS payload_identity,
            event.payload_json->'token_event'->>'token' AS token_symbol,
            event.payload_json->'token_event'->>'value_raw' AS value_raw,
            COALESCE(
                event.payload_json->>'address',
                event.payload_json->'token_event'->>'address'
            ) AS address,
            COALESCE(
                event.payload_json->'market_event'->>'quote_symbol',
                event.payload_json->'market_event'->>'quote_token'
            ) AS market_quote_symbol,
            (
                POSITION(DECODE('5374616B65', 'hex') IN DECODE(COALESCE(tx.carbon_tx_data, ''), 'hex')) > 0
                OR POSITION(DECODE('5374616B65', 'hex') IN DECODE(COALESCE(tx.script_raw, ''), 'hex')) > 0
            ) AS tx_has_stake_call,
            (
                POSITION(DECODE('556E7374616B65', 'hex') IN DECODE(COALESCE(tx.carbon_tx_data, ''), 'hex')) > 0
                OR POSITION(DECODE('556E7374616B65', 'hex') IN DECODE(COALESCE(tx.script_raw, ''), 'hex')) > 0
            ) AS tx_has_unstake_call,
            (
                POSITION(DECODE('436C61696D', 'hex') IN DECODE(COALESCE(tx.carbon_tx_data, ''), 'hex')) > 0
                OR POSITION(DECODE('436C61696D', 'hex') IN DECODE(COALESCE(tx.script_raw, ''), 'hex')) > 0
            ) AS tx_has_claim_call,
            LOWER(COALESCE(tx.carbon_tx_data, '')) = '0100000016000000080000000200000000000000'
                AS tx_is_soul_apply_inflation,
            tx.result AS tx_result
        FROM events event
        JOIN event_kinds event_kind
          ON event_kind.id = event.event_kind_id
         AND event_kind.chain_id = event.chain_id
        JOIN transactions tx
          ON tx.id = event.transaction_id
        JOIN blocks block
          ON block.id = tx.block_id
         AND block.chain_id = event.chain_id
        WHERE event.chain_id = $1
          AND tx.timestamp_unix_seconds >= $2
          AND tx.timestamp_unix_seconds <= $3
          AND event.payload_format IN ('legacy.backfill.v1', 'live.v1')
          AND (
              (
                  event_kind.name IN ('TokenStake', 'TokenClaim', 'TokenMint', 'TokenBurn')
                  AND UPPER(COALESCE(event.payload_json->'token_event'->>'token', '')) = 'SOUL'
              )
              OR (
                  event_kind.name = 'TokenMint'
                  AND UPPER(COALESCE(event.payload_json->'token_event'->>'token', '')) = 'KCAL'
                  AND LOWER(COALESCE(event.payload_json->>'contract', '')) = 'stake'
              )
              OR (
                  event_kind.name IN (
                      'OrderCreated',
                      'OrderCancelled',
                      'OrderFilled',
                      'OrderClosed',
                      'OrderBid'
                  )
                  AND UPPER(COALESCE(
                      event.payload_json->'market_event'->>'quote_symbol',
                      event.payload_json->'market_event'->>'quote_token',
                      ''
                  )) = 'SOUL'
              )
          )
        ORDER BY
            tx.timestamp_unix_seconds ASC,
            block.height ASC,
            tx.tx_index ASC,
            event.event_index ASC,
            event.id ASC
        "#,
    )
    .bind(chain_id)
    .bind(from_ts)
    .bind(to_ts)
    .fetch_all(&mut *conn)
    .await?;

    rows.into_iter()
        .map(|row| {
            let value_raw = row
                .get::<Option<String>, _>("value_raw")
                .map(|value| parse_stake_snapshot_raw("value_raw", &value))
                .transpose()?;
            let tx_is_soul_apply_inflation: bool = row.get("tx_is_soul_apply_inflation");
            let tx_apply_inflation_result_soul_delta_raw = if tx_is_soul_apply_inflation {
                let tx_result: Option<String> = row.get("tx_result");
                let tx_result =
                    tx_result
                        .as_deref()
                        .ok_or_else(|| DbError::StakeSnapshotReplay {
                            reason: format!(
                                "missing Token.ApplyInflation SOUL result in tx {}",
                                row.get::<i32, _>("tx_id")
                            ),
                        })?;
                Some(parse_carbon_intx_i64_raw("tx.result", tx_result)?)
            } else {
                None
            };
            Ok(StakeSnapshotEventRow {
                event_id: row.get("event_id"),
                tx_id: row.get("tx_id"),
                kind: row.get("kind"),
                timestamp_unix_seconds: row.get("timestamp_unix_seconds"),
                payload_identity: row.get("payload_identity"),
                token_symbol: row.get("token_symbol"),
                value_raw,
                address: row.get("address"),
                market_quote_symbol: row.get("market_quote_symbol"),
                tx_has_stake_call: row.get("tx_has_stake_call"),
                tx_has_unstake_call: row.get("tx_has_unstake_call"),
                tx_has_claim_call: row.get("tx_has_claim_call"),
                tx_apply_inflation_result_soul_delta_raw,
            })
        })
        .collect()
}

fn reverse_replay_stake_snapshot_events(
    state: &mut StakeSnapshotState,
    rows: &[StakeSnapshotEventRow],
) -> Result<(), DbError> {
    let mut tx_group_end = rows.len();
    while tx_group_end > 0 {
        let tx_id = rows[tx_group_end - 1].tx_id;
        let mut tx_group_start = tx_group_end - 1;
        while tx_group_start > 0 && rows[tx_group_start - 1].tx_id == tx_id {
            tx_group_start -= 1;
        }
        let tx_rows = deduplicate_stake_snapshot_tx_rows(&rows[tx_group_start..tx_group_end]);
        apply_stake_snapshot_transaction(state, &tx_rows, true)?;
        tx_group_end = tx_group_start;
    }
    Ok(())
}

fn build_stake_snapshot_daily_points(
    mut state: StakeSnapshotState,
    rows: &[StakeSnapshotEventRow],
    from_day: i64,
    to_exclusive_day: i64,
) -> Result<Vec<StakeSnapshotDailyPoint>, DbError> {
    let mut snapshots = Vec::new();
    let mut tx_group_start = 0;
    let mut day_cursor = from_day;

    while day_cursor < to_exclusive_day {
        let day_end = stake_snapshot_day_end(day_cursor);
        while tx_group_start < rows.len() {
            let tx_id = rows[tx_group_start].tx_id;
            let mut tx_group_end = tx_group_start + 1;
            while tx_group_end < rows.len() && rows[tx_group_end].tx_id == tx_id {
                tx_group_end += 1;
            }
            if rows[tx_group_start].timestamp_unix_seconds > day_end {
                break;
            }
            let tx_rows = deduplicate_stake_snapshot_tx_rows(&rows[tx_group_start..tx_group_end]);
            apply_stake_snapshot_transaction(&mut state, &tx_rows, false)?;
            tx_group_start = tx_group_end;
        }

        snapshots.push(StakeSnapshotDailyPoint {
            date_unix_seconds: day_cursor,
            staked_soul_raw: state.total_staked_raw.to_string(),
            soul_supply_raw: state.soul_supply_raw.to_string(),
            stakers_count: state.stakers_count,
            masters_count: state.masters_count,
            captured_at_unix_seconds: day_end,
        });
        day_cursor += STAKE_SNAPSHOT_SECONDS_PER_DAY;
    }

    Ok(snapshots)
}

fn deduplicate_stake_snapshot_tx_rows(
    rows: &[StakeSnapshotEventRow],
) -> Vec<StakeSnapshotEventRow> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(rows.len());
    for row in rows {
        let key = format!("{}|{}", row.kind, row.payload_identity);
        if seen.insert(key) {
            deduped.push(row.clone());
        }
    }
    deduped
}

fn apply_stake_snapshot_transaction(
    state: &mut StakeSnapshotState,
    rows: &[StakeSnapshotEventRow],
    reverse: bool,
) -> Result<(), DbError> {
    let tx_kind = classify_stake_snapshot_transaction(rows);
    let apply_inflation_result_soul_delta = rows
        .iter()
        .find_map(|row| row.tx_apply_inflation_result_soul_delta_raw.as_ref());
    let mut applied_apply_inflation_result_soul_delta = false;
    for row in rows {
        if !matches!(
            row.kind.as_str(),
            "TokenStake" | "TokenClaim" | "TokenMint" | "TokenBurn"
        ) {
            continue;
        }
        let Some(value_raw) = row.value_raw.as_ref() else {
            continue;
        };
        let is_soul = row
            .token_symbol
            .as_deref()
            .is_some_and(|symbol| symbol.eq_ignore_ascii_case("SOUL"));
        if !is_soul {
            continue;
        }
        if value_raw <= &BigInt::zero() {
            continue;
        }
        if tx_kind != StakeSnapshotTxKind::Normal
            && matches!(row.kind.as_str(), "TokenStake" | "TokenClaim")
        {
            continue;
        }

        match (row.kind.as_str(), reverse) {
            ("TokenStake", false) | ("TokenClaim", true) => {
                apply_stake_snapshot_stake_delta(state, row, value_raw)?;
            }
            ("TokenStake", true) | ("TokenClaim", false) => {
                apply_stake_snapshot_stake_delta(state, row, &-value_raw)?;
            }
            ("TokenMint", false) | ("TokenBurn", true) => {
                if let Some(delta) = apply_inflation_result_soul_delta {
                    if !applied_apply_inflation_result_soul_delta {
                        state.soul_supply_raw += delta;
                        applied_apply_inflation_result_soul_delta = true;
                    }
                } else {
                    state.soul_supply_raw += value_raw;
                }
            }
            ("TokenMint", true) | ("TokenBurn", false) => {
                if let Some(delta) = apply_inflation_result_soul_delta {
                    if !applied_apply_inflation_result_soul_delta {
                        state.soul_supply_raw -= delta;
                        applied_apply_inflation_result_soul_delta = true;
                    }
                } else {
                    state.soul_supply_raw -= value_raw;
                }
                if state.soul_supply_raw < BigInt::zero() {
                    return Err(DbError::StakeSnapshotReplay {
                        reason: format!(
                            "negative SOUL supply after {} event {}",
                            if reverse { "reverse" } else { "forward" },
                            row.event_id
                        ),
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_carbon_intx_i64_raw(field: &str, value: &str) -> Result<BigInt, DbError> {
    let value = value.trim();
    if value.len() != 18 {
        return Err(DbError::StakeSnapshotReplay {
            reason: format!("{field} is not an 8-byte Carbon intx"),
        });
    }
    let header =
        u8::from_str_radix(&value[0..2], 16).map_err(|error| DbError::StakeSnapshotReplay {
            reason: format!("invalid {field} intx header: {error}"),
        })?;
    if header != 0x08 && header != 0x88 {
        return Err(DbError::StakeSnapshotReplay {
            reason: format!("{field} is not an 8-byte Carbon intx"),
        });
    }

    let mut raw = 0_u64;
    for index in 0..8 {
        let start = 2 + (index * 2);
        let byte = u8::from_str_radix(&value[start..start + 2], 16).map_err(|error| {
            DbError::StakeSnapshotReplay {
                reason: format!("invalid {field} intx byte: {error}"),
            }
        })?;
        raw |= u64::from(byte) << (index * 8);
    }
    let parsed = raw as i64;
    if (header == 0x08 && parsed < 0) || (header == 0x88 && parsed >= 0) {
        return Err(DbError::StakeSnapshotReplay {
            reason: format!("{field} has an invalid Carbon intx sign extension"),
        });
    }
    Ok(BigInt::from(parsed))
}

fn classify_stake_snapshot_transaction(rows: &[StakeSnapshotEventRow]) -> StakeSnapshotTxKind {
    let has_stake_call = rows.iter().any(|row| row.tx_has_stake_call);
    let has_unstake_call = rows.iter().any(|row| row.tx_has_unstake_call);
    let has_claim_call = rows.iter().any(|row| row.tx_has_claim_call);

    for row in rows {
        if row.kind == "TokenMint"
            && row
                .token_symbol
                .as_deref()
                .is_some_and(|symbol| symbol.eq_ignore_ascii_case("SOUL"))
        {
            return StakeSnapshotTxKind::SmReward;
        }
    }
    // A KCAL mint from the stake contract is not enough to classify the
    // transaction as reward-only: Stake and Unstake auto-claim KCAL rewards too,
    // while their SOUL TokenStake/TokenClaim rows are still principal deltas.
    // Only a standalone stake.Claim call is reward accounting for snapshots.
    if has_claim_call && !has_stake_call && !has_unstake_call {
        return StakeSnapshotTxKind::StakeReward;
    }
    // Market and reward-only transactions can emit SOUL TokenStake/TokenClaim
    // rows that are not principal stake changes. Treating those rows as stake
    // deltas is the exact bug that made the legacy C# v2 path need aggregate
    // calibration.
    if rows.iter().any(|row| {
        row.market_quote_symbol
            .as_deref()
            .is_some_and(|symbol| symbol.eq_ignore_ascii_case("SOUL"))
    }) {
        return StakeSnapshotTxKind::MarketEvent;
    }
    StakeSnapshotTxKind::Normal
}

fn apply_stake_snapshot_stake_delta(
    state: &mut StakeSnapshotState,
    row: &StakeSnapshotEventRow,
    delta: &BigInt,
) -> Result<(), DbError> {
    let address = row
        .address
        .as_deref()
        .filter(|address| !address.trim().is_empty())
        .ok_or_else(|| DbError::StakeSnapshotReplay {
            reason: format!("empty address in staking event {}", row.event_id),
        })?;
    let old_value = state
        .stakes_by_address
        .get(address)
        .cloned()
        .unwrap_or_else(BigInt::zero);
    let new_value = &old_value + delta;
    if new_value < BigInt::zero() {
        return Err(DbError::StakeSnapshotReplay {
            reason: format!(
                "negative staked amount for address {address} at event {}",
                row.event_id
            ),
        });
    }

    let master_threshold = stake_snapshot_master_threshold();
    let was_staker = old_value > BigInt::zero();
    let is_staker = new_value > BigInt::zero();
    if was_staker != is_staker {
        state.stakers_count += if is_staker { 1 } else { -1 };
    }
    let was_master = old_value >= master_threshold;
    let is_master = new_value >= master_threshold;
    if was_master != is_master {
        state.masters_count += if is_master { 1 } else { -1 };
    }

    state.total_staked_raw += &new_value - &old_value;
    if state.total_staked_raw < BigInt::zero() {
        return Err(DbError::StakeSnapshotReplay {
            reason: "negative total staked amount".to_owned(),
        });
    }
    if new_value.is_zero() {
        state.stakes_by_address.remove(address);
    } else {
        state
            .stakes_by_address
            .insert(address.to_owned(), new_value);
    }
    Ok(())
}

async fn upsert_stake_snapshot_daily_points(
    conn: &mut PgConnection,
    chain_id: i32,
    points: &[StakeSnapshotDailyPoint],
) -> Result<u64, DbError> {
    let mut affected = 0;
    for point in points {
        affected += sqlx::query(
            r#"
            INSERT INTO staking_progress_dailies (
                chain_id,
                date_unix_seconds,
                staked_soul_raw,
                soul_supply_raw,
                stakers_count,
                masters_count,
                staking_ratio,
                captured_at_unix_seconds,
                source
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                CASE
                    WHEN NULLIF($4, '')::numeric > 0
                    THEN NULLIF($3, '')::numeric / NULLIF($4, '')::numeric
                    ELSE 0
                END,
                $7,
                $8
            )
            ON CONFLICT (chain_id, date_unix_seconds) DO UPDATE SET
                staked_soul_raw = EXCLUDED.staked_soul_raw,
                soul_supply_raw = EXCLUDED.soul_supply_raw,
                stakers_count = EXCLUDED.stakers_count,
                masters_count = EXCLUDED.masters_count,
                staking_ratio = EXCLUDED.staking_ratio,
                captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
                source = EXCLUDED.source
            "#,
        )
        .bind(chain_id)
        .bind(point.date_unix_seconds)
        .bind(&point.staked_soul_raw)
        .bind(&point.soul_supply_raw)
        .bind(point.stakers_count)
        .bind(point.masters_count)
        .bind(point.captured_at_unix_seconds)
        .bind(STAKE_SNAPSHOT_PROJECTOR_SOURCE)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    }
    Ok(affected)
}

async fn upsert_stake_snapshot_monthlies_from_daily(
    conn: &mut PgConnection,
    chain_id: i32,
    from_day: i64,
    to_exclusive_day: i64,
) -> Result<u64, DbError> {
    let rows = sqlx::query(
        r#"
        WITH months AS (
            SELECT
                EXTRACT(EPOCH FROM month_start)::bigint AS month_unix_seconds,
                EXTRACT(EPOCH FROM (
                    month_start + INTERVAL '1 month' - INTERVAL '1 day'
                ))::bigint AS month_end_day_unix_seconds
            FROM generate_series(
                date_trunc('month', to_timestamp($2::double precision)),
                date_trunc('month', to_timestamp(($3::bigint - 86400)::double precision)),
                INTERVAL '1 month'
            ) AS month_start
        ),
        closed_months AS (
            SELECT *
            FROM months
            WHERE month_end_day_unix_seconds < $3
        ),
        snapshot_rows AS (
            SELECT
                month.month_unix_seconds,
                daily.masters_count,
                (month.month_end_day_unix_seconds + 86399)::bigint AS captured_at_unix_seconds
            FROM closed_months month
            JOIN LATERAL (
                SELECT masters_count
                FROM staking_progress_dailies daily
                WHERE daily.chain_id = $1
                  AND daily.date_unix_seconds <= month.month_end_day_unix_seconds
                ORDER BY daily.date_unix_seconds DESC
                LIMIT 1
            ) daily ON TRUE
        )
        INSERT INTO soul_masters_monthlies (
            chain_id,
            month_unix_seconds,
            masters_count,
            captured_at_unix_seconds,
            source
        )
        SELECT
            $1,
            month_unix_seconds,
            masters_count,
            captured_at_unix_seconds,
            $4
        FROM snapshot_rows
        ON CONFLICT (chain_id, month_unix_seconds) DO UPDATE SET
            masters_count = EXCLUDED.masters_count,
            captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
            source = EXCLUDED.source
        RETURNING month_unix_seconds
        "#,
    )
    .bind(chain_id)
    .bind(from_day)
    .bind(to_exclusive_day)
    .bind(STAKE_SNAPSHOT_PROJECTOR_SOURCE)
    .fetch_all(&mut *conn)
    .await?;

    u64::try_from(rows.len()).map_err(|_| DbError::StakeSnapshotReplay {
        reason: "monthly upsert row count does not fit u64".to_owned(),
    })
}

fn parse_stake_snapshot_raw(field: &'static str, value: &str) -> Result<BigInt, DbError> {
    BigInt::from_str(value).map_err(|_| DbError::StakeSnapshotInvalidRaw {
        field,
        value: value.to_owned(),
    })
}

fn stake_snapshot_day_start(unix_seconds: i64) -> i64 {
    unix_seconds - unix_seconds.rem_euclid(STAKE_SNAPSHOT_SECONDS_PER_DAY)
}

fn stake_snapshot_day_end(day_start_unix_seconds: i64) -> i64 {
    day_start_unix_seconds + STAKE_SNAPSHOT_SECONDS_PER_DAY - 1
}

/// First instant (UTC midnight) of the month AFTER the month containing `unix_seconds`.
/// Used so the monthly rollup begins after the seed month, preserving the seed-month value (never
/// overwritten).
fn stake_snapshot_next_month_start(unix_seconds: i64) -> i64 {
    let Some(moment) = Utc.timestamp_opt(unix_seconds, 0).single() else {
        return unix_seconds;
    };
    let (year, month) = if moment.month() == 12 {
        (moment.year() + 1, 1)
    } else {
        (moment.year(), moment.month() + 1)
    };
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .map_or(unix_seconds, |start| start.timestamp())
}

fn stake_snapshot_master_threshold() -> BigInt {
    BigInt::from(STAKE_SNAPSHOT_MASTER_THRESHOLD_RAW)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stake_snapshot_replay_builds_closed_daily_points_from_anchor_state() -> Result<(), DbError> {
        // A normal SOUL stake followed by a normal SOUL claim must round-trip:
        // reverse replay returns to the trusted anchor, and forward replay emits
        // one closed-day point per day with the principal stake amount at day end.
        let day_one = 1_700_000_000 - 1_700_000_000_i64.rem_euclid(86_400);
        let day_two = day_one + 86_400;
        let day_three = day_two + 86_400;
        let rows = vec![
            test_stake_snapshot_token_row(1, 1, "TokenStake", day_one + 10, "PTESTA", "50")?,
            test_stake_snapshot_token_row(2, 2, "TokenClaim", day_two + 10, "PTESTA", "20")?,
        ];
        let mut current_state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([(
                "PTESTA".to_owned(),
                BigInt::from(30),
            )]),
            total_staked_raw: BigInt::from(30),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 1,
            masters_count: 0,
        };

        reverse_replay_stake_snapshot_events(&mut current_state, &rows)?;
        assert_eq!(current_state.total_staked_raw, BigInt::zero());
        assert_eq!(current_state.stakers_count, 0);

        let points = build_stake_snapshot_daily_points(current_state, &rows, day_one, day_three)?;
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].date_unix_seconds, day_one);
        assert_eq!(points[0].staked_soul_raw, "50");
        assert_eq!(points[1].date_unix_seconds, day_two);
        assert_eq!(points[1].staked_soul_raw, "30");

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_uses_open_day_events_only_for_reverse_convergence()
    -> Result<(), DbError> {
        // Current account state includes the open chain day. Those events must be
        // reversed for anchor convergence, but they must not produce a closed-day
        // snapshot until the day is actually closed.
        let day_one = 1_700_000_000 - 1_700_000_000_i64.rem_euclid(86_400);
        let open_day = day_one + 86_400;
        let rows = vec![
            test_stake_snapshot_token_row(1, 1, "TokenStake", day_one + 10, "PTESTA", "50")?,
            test_stake_snapshot_token_row(2, 2, "TokenStake", open_day + 10, "PTESTA", "10")?,
        ];
        let mut current_state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([(
                "PTESTA".to_owned(),
                BigInt::from(60),
            )]),
            total_staked_raw: BigInt::from(60),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 1,
            masters_count: 0,
        };

        reverse_replay_stake_snapshot_events(&mut current_state, &rows)?;
        assert_eq!(current_state.total_staked_raw, BigInt::zero());

        let points = build_stake_snapshot_daily_points(current_state, &rows, day_one, open_day)?;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].date_unix_seconds, day_one);
        assert_eq!(points[0].staked_soul_raw, "50");

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_ignores_stake_reward_principal_artifacts() -> Result<(), DbError> {
        // Standalone stake.Claim reward calls can carry stake-contract SOUL rows
        // in historical data. Those rows are reward-accounting artifacts, not
        // principal stake deltas, and applying them is what caused the legacy C#
        // catch-up path to need unsafe aggregate calibration.
        let mut state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([(
                "PTESTA".to_owned(),
                BigInt::from(90),
            )]),
            total_staked_raw: BigInt::from(90),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 1,
            masters_count: 0,
        };
        let mut reward_row =
            test_stake_snapshot_token_row(1, 1, "TokenClaim", 1_700_000_010, "PTESTA", "95")?;
        reward_row.tx_has_claim_call = true;
        let mut kcal_row = test_stake_snapshot_kcal_mint_row(2, 1, 1_700_000_010, "PTESTA", "340")?;
        kcal_row.tx_has_claim_call = true;
        let rows = vec![reward_row, kcal_row];

        apply_stake_snapshot_transaction(&mut state, &rows, false)?;
        assert_eq!(state.total_staked_raw, BigInt::from(90));
        assert_eq!(
            state.stakes_by_address.get("PTESTA"),
            Some(&BigInt::from(90))
        );
        assert_eq!(state.soul_supply_raw, BigInt::from(1_000));

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_applies_stake_calls_that_auto_claim_kcal() -> Result<(), DbError> {
        // stake.Stake and stake.Unstake can mint KCAL through automatic reward
        // claiming, but their SOUL TokenStake/TokenClaim rows are still principal
        // deltas. Classifying on the KCAL mint alone would drop real stake changes.
        let mut state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([(
                "PTESTA".to_owned(),
                BigInt::from(100),
            )]),
            total_staked_raw: BigInt::from(100),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 1,
            masters_count: 0,
        };
        let mut stake_row =
            test_stake_snapshot_token_row(1, 1, "TokenStake", 1_700_000_010, "PTESTA", "50")?;
        stake_row.tx_has_stake_call = true;
        let mut stake_kcal_row =
            test_stake_snapshot_kcal_mint_row(2, 1, 1_700_000_010, "PTESTA", "340")?;
        stake_kcal_row.tx_has_stake_call = true;
        let mut unstake_row =
            test_stake_snapshot_token_row(3, 2, "TokenClaim", 1_700_000_020, "PTESTA", "20")?;
        unstake_row.tx_has_unstake_call = true;
        let mut unstake_kcal_row =
            test_stake_snapshot_kcal_mint_row(4, 2, 1_700_000_020, "PTESTA", "120")?;
        unstake_kcal_row.tx_has_unstake_call = true;
        let rows = vec![stake_row, stake_kcal_row, unstake_row, unstake_kcal_row];

        apply_stake_snapshot_transaction(&mut state, &rows, false)?;
        assert_eq!(state.total_staked_raw, BigInt::from(130));
        assert_eq!(
            state.stakes_by_address.get("PTESTA"),
            Some(&BigInt::from(130))
        );
        assert_eq!(state.soul_supply_raw, BigInt::from(1_000));

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_uses_apply_inflation_result_for_soul_supply() -> Result<(), DbError> {
        // Token.ApplyInflation returns the SOUL delta that belongs in staking
        // stats. Historical RPC events can include a system data-pool side
        // effect in the aggregate TokenMint value; replaying that value directly
        // drifts from the trusted daily series.
        let mut mint_row =
            test_stake_snapshot_token_row(1, 1, "TokenMint", 1_700_000_010, "PTESTA", "12")?;
        mint_row.tx_apply_inflation_result_soul_delta_raw = Some(BigInt::from(10));
        let claim_row =
            test_stake_snapshot_token_row(2, 1, "TokenClaim", 1_700_000_010, "PTESTA", "12")?;
        let data_pool_claim_row = test_stake_snapshot_token_row(
            3,
            1,
            "TokenClaim",
            1_700_000_010,
            "S3d7TbZxtNPdXy11hfmBLJLYn67gZTG2ibL7fJBcVdihWU4",
            "2",
        )?;
        let rows = vec![mint_row, claim_row, data_pool_claim_row];

        let mut forward_state = StakeSnapshotState {
            stakes_by_address: HashMap::new(),
            total_staked_raw: BigInt::zero(),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 0,
            masters_count: 0,
        };
        apply_stake_snapshot_transaction(&mut forward_state, &rows, false)?;
        assert_eq!(forward_state.soul_supply_raw, BigInt::from(1_010));
        assert_eq!(forward_state.total_staked_raw, BigInt::zero());

        apply_stake_snapshot_transaction(&mut forward_state, &rows, true)?;
        assert_eq!(forward_state.soul_supply_raw, BigInt::from(1_000));
        assert_eq!(forward_state.total_staked_raw, BigInt::zero());

        Ok(())
    }

    #[test]
    fn parse_carbon_intx_i64_raw_decodes_apply_inflation_result() -> Result<(), DbError> {
        assert_eq!(
            parse_carbon_intx_i64_raw("tx.result", "088AF5DD19852C0000")?,
            BigInt::from(48_950_176_249_226_i64)
        );
        Ok(())
    }

    #[tokio::test]
    async fn current_stake_snapshots_smoke() -> Result<(), Box<dyn std::error::Error>> {
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let now_unix_seconds = Utc::now().timestamp();
        let date_unix_seconds = now_unix_seconds - now_unix_seconds.rem_euclid(86_400);

        upsert_current_stake_snapshots(&mut transaction, chain_id, now_unix_seconds).await?;

        let snapshot_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM staking_progress_dailies
            WHERE chain_id = $1
              AND date_unix_seconds = $2
              AND source = 'balance-sync.v1'
            "#,
        )
        .bind(chain_id)
        .bind(date_unix_seconds)
        .fetch_one(&mut *transaction)
        .await?;
        assert_eq!(snapshot_count, 1);

        transaction.rollback().await?;
        Ok(())
    }

    fn test_stake_snapshot_token_row(
        event_id: i32,
        tx_id: i32,
        kind: &str,
        timestamp_unix_seconds: i64,
        address: &str,
        value_raw: &str,
    ) -> Result<StakeSnapshotEventRow, DbError> {
        Ok(StakeSnapshotEventRow {
            event_id,
            tx_id,
            kind: kind.to_owned(),
            timestamp_unix_seconds,
            payload_identity: format!("{event_id}:{kind}:{address}:{value_raw}"),
            token_symbol: Some("SOUL".to_owned()),
            value_raw: Some(parse_stake_snapshot_raw("value_raw", value_raw)?),
            address: Some(address.to_owned()),
            market_quote_symbol: None,
            tx_has_stake_call: false,
            tx_has_unstake_call: false,
            tx_has_claim_call: false,
            tx_apply_inflation_result_soul_delta_raw: None,
        })
    }

    fn test_stake_snapshot_kcal_mint_row(
        event_id: i32,
        tx_id: i32,
        timestamp_unix_seconds: i64,
        address: &str,
        value_raw: &str,
    ) -> Result<StakeSnapshotEventRow, DbError> {
        Ok(StakeSnapshotEventRow {
            event_id,
            tx_id,
            kind: "TokenMint".to_owned(),
            timestamp_unix_seconds,
            payload_identity: format!("{event_id}:TokenMint:{address}:{value_raw}"),
            token_symbol: Some("KCAL".to_owned()),
            value_raw: Some(parse_stake_snapshot_raw("value_raw", value_raw)?),
            address: Some(address.to_owned()),
            market_quote_symbol: None,
            tx_has_stake_call: false,
            tx_has_unstake_call: false,
            tx_has_claim_call: false,
            tx_apply_inflation_result_soul_delta_raw: None,
        })
    }
}
