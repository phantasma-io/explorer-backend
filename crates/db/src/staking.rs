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
            SELECT COALESCE(current_supply_raw, 0) AS supply_raw
            FROM tokens
            WHERE chain_id = $1
              AND symbol = 'SOUL'
            ORDER BY id
            LIMIT 1
        ),
        staked AS (
            SELECT COALESCE(SUM(COALESCE(staked_amount_raw, 0)), 0) AS staked_raw
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
    payload_format: i16,
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
    /// Day of the persisted fold state this run resumed from; `None` on a
    /// full-from-boundary rebuild (no state, or state inconsistent with the curve).
    pub resumed_from_day_unix_seconds: Option<i64>,
    pub skipped_reason: Option<String>,
}

/// The latest day the forward projector has already written. Used to make the
/// projection idempotent: skip the rebuild scan while the curve is already built
/// through the cursor day (the historical events below it are immutable).
async fn load_max_projected_daily_day(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<i64>, DbError> {
    let day = sqlx::query_scalar::<_, Option<i64>>(
        r#"
        SELECT max(date_unix_seconds)
        FROM staking_progress_dailies
        WHERE chain_id = $1
          AND source = $2
        "#,
    )
    .bind(chain_id)
    .bind(STAKE_SNAPSHOT_PROJECTOR_SOURCE)
    .fetch_one(&mut *conn)
    .await?;
    Ok(day)
}

/// Reads the persisted fold state (see `save_stake_snapshot_resume`): the state as
/// of the last closed projected day. `None` when no state is stored.
async fn load_stake_snapshot_resume(
    conn: &mut PgConnection,
    chain_id: i32,
) -> Result<Option<(i64, StakeSnapshotState)>, DbError> {
    let Some(header) = sqlx::query(
        r#"
        SELECT
            last_projected_day_unix_seconds,
            total_staked_raw::text AS total_staked_raw,
            soul_supply_raw::text AS soul_supply_raw,
            stakers_count,
            masters_count
        FROM stake_snapshot_resume
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_optional(&mut *conn)
    .await?
    else {
        return Ok(None);
    };

    let rows = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT address, staked_amount_raw::text
        FROM stake_snapshot_resume_stakes
        WHERE chain_id = $1
        "#,
    )
    .bind(chain_id)
    .fetch_all(&mut *conn)
    .await?;

    let mut stakes_by_address = HashMap::with_capacity(rows.len());
    for (address, staked_amount_raw) in rows {
        stakes_by_address.insert(
            address,
            parse_stake_snapshot_raw("staked_amount_raw", &staked_amount_raw)?,
        );
    }

    let total_staked_raw: String = header.get("total_staked_raw");
    let soul_supply_raw: String = header.get("soul_supply_raw");
    Ok(Some((
        header.get("last_projected_day_unix_seconds"),
        StakeSnapshotState {
            stakes_by_address,
            total_staked_raw: parse_stake_snapshot_raw("total_staked_raw", &total_staked_raw)?,
            soul_supply_raw: parse_stake_snapshot_raw("soul_supply_raw", &soul_supply_raw)?,
            stakers_count: header.get("stakers_count"),
            masters_count: header.get("masters_count"),
        },
    )))
}

/// Replaces the persisted fold state with `state` as of `last_projected_day` (a
/// CLOSED day). Runs inside the same transaction as the curve upserts, so the
/// stored state and the stored curve can never disagree; only addresses with
/// stake > 0 are written, mirroring the in-memory map (and the boundary capture).
async fn save_stake_snapshot_resume(
    conn: &mut PgConnection,
    chain_id: i32,
    last_projected_day: i64,
    state: &StakeSnapshotState,
) -> Result<(), DbError> {
    sqlx::query("DELETE FROM stake_snapshot_resume_stakes WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM stake_snapshot_resume WHERE chain_id = $1")
        .bind(chain_id)
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        r#"
        INSERT INTO stake_snapshot_resume (
            chain_id, last_projected_day_unix_seconds, total_staked_raw,
            soul_supply_raw, stakers_count, masters_count
        )
        VALUES ($1, $2, $3::numeric, $4::numeric, $5, $6)
        "#,
    )
    .bind(chain_id)
    .bind(last_projected_day)
    .bind(state.total_staked_raw.to_string())
    .bind(state.soul_supply_raw.to_string())
    .bind(state.stakers_count)
    .bind(state.masters_count)
    .execute(&mut *conn)
    .await?;

    let mut addresses = Vec::new();
    let mut amounts = Vec::new();
    for (address, staked) in &state.stakes_by_address {
        if staked <= &BigInt::zero() {
            continue;
        }
        addresses.push(address.clone());
        amounts.push(staked.to_string());
    }
    sqlx::query(
        r#"
        INSERT INTO stake_snapshot_resume_stakes (chain_id, address, staked_amount_raw)
        SELECT $1, s.address, s.staked_amount_raw::numeric
        FROM unnest($2::text[], $3::text[]) AS s(address, staked_amount_raw)
        "#,
    )
    .bind(chain_id)
    .bind(&addresses)
    .bind(&amounts)
    .execute(&mut *conn)
    .await?;

    Ok(())
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
/// (`stake_boundary_*`) across the on-chain stake events up to the projected tip and
/// writes it — the forward build is the SOLE source of the post-boundary curve (no
/// second writer, no cross-validation). The per-address seed state is read from
/// `stake_boundary_*`, then `build_stake_snapshot_daily_points` advances it day by day
/// to the cursor block's day. Idempotent: the events below the cursor are immutable,
/// so once the curve is built through the cursor day there is nothing to redo until a
/// new block advances the day. The seed day and its month are never overwritten (the
/// monthly rollup starts at the month after the seed day).
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
        resumed_from_day_unix_seconds: None,
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
    // Anchor the curve to blockchain time: build through (and including) the cursor
    // block's own day, the clock the on-chain stake events live on.
    let target_exclusive_day =
        stake_snapshot_day_start(cursor_timestamp) + STAKE_SNAPSHOT_SECONDS_PER_DAY;
    let cursor_day = target_exclusive_day - STAKE_SNAPSHOT_SECONDS_PER_DAY;
    let from_day = boundary_day + STAKE_SNAPSHOT_SECONDS_PER_DAY;
    if from_day >= target_exclusive_day {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "boundary is already at the tip; nothing to build",
        ));
    }
    // Idempotence: if the curve is already built through the cursor day, the events
    // below it cannot have changed, so skip the full-history scan until a new block
    // advances the day.
    let last_built_day = load_max_projected_daily_day(conn, chain_id).await?;
    if let Some(last_built_day) = last_built_day
        && last_built_day >= cursor_day
    {
        return Ok(skip(
            boundary_day,
            boundary_masters,
            "curve already built through the tip; nothing to rebuild",
        ));
    }

    // Resume from the persisted fold state when it provably matches the stored
    // curve: a building run always leaves the state at (last built day − 1) — the
    // last CLOSED day — because the final built day is the open cursor day. Any
    // other relationship (no state, a manually trimmed curve, a state older than
    // this schema) falls back to the full-from-boundary rebuild, which remains
    // the source of truth; resuming only shortens the replay, never changes it.
    let resume = load_stake_snapshot_resume(conn, chain_id).await?;
    let (replay_from_day, replay_state, resumed_from_day) = match (resume, last_built_day) {
        (Some((resume_day, resume_state)), Some(last_built_day))
            if resume_day + STAKE_SNAPSHOT_SECONDS_PER_DAY == last_built_day
                && resume_day > boundary_day =>
        {
            (
                resume_day + STAKE_SNAPSHOT_SECONDS_PER_DAY,
                resume_state,
                Some(resume_day),
            )
        }
        _ => (from_day, boundary_state, None),
    };

    // Replay the stake events from the resume day (or the day after the seed) to the
    // cursor and write the forward curve. The build from the frozen seed + the
    // complete on-chain stake events IS the source of truth; the resume state is that
    // same fold, persisted.
    let events =
        load_stake_snapshot_events(conn, chain_id, replay_from_day, cursor_timestamp).await?;
    let (curve, closed_day_state) = build_stake_snapshot_daily_points(
        replay_state,
        &events,
        replay_from_day,
        target_exclusive_day,
    )?;
    let daily_upserted = upsert_stake_snapshot_daily_points(conn, chain_id, &curve).await?;
    let monthly_upserted = upsert_stake_snapshot_monthlies_from_daily(
        conn,
        chain_id,
        stake_snapshot_next_month_start(boundary_day),
        target_exclusive_day,
    )
    .await?;
    // Persist the fold state at the window's last closed day. A single-day window
    // (only the open cursor day was rebuilt) yields no newly closed day, and the
    // stored state — still at the previous closed day — stays exactly right.
    if let Some((closed_day, state)) = closed_day_state {
        save_stake_snapshot_resume(conn, chain_id, closed_day, &state).await?;
    }

    Ok(StakeForwardBuildReport {
        chain_id,
        boundary_day_unix_seconds: boundary_day,
        boundary_masters_count: boundary_masters,
        validated: true,
        daily_upserted,
        monthly_upserted,
        resumed_from_day_unix_seconds: resumed_from_day,
        skipped_reason: None,
    })
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
        SELECT current_supply_raw::text
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
        SELECT address, staked_amount_raw::text AS staked_amount_raw
        FROM addresses
        WHERE chain_id = $1
          AND address <> 'NULL'
          AND staked_amount_raw > 0
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
            event.payload_format AS payload_format,
            event.payload_json->'token_event'->>'token' AS token_symbol,
            event.payload_json->'token_event'->>'value_raw' AS value_raw,
            address.address AS address,
            COALESCE(
                event.payload_json->'market_event'->>'quote_symbol',
                event.payload_json->'market_event'->>'quote_token'
            ) AS market_quote_symbol,
            -- carbon_tx_data/script_raw are bytea; the markers ('Stake', 'Unstake',
            -- 'Claim' as ASCII hex) decode to bytea and are searched directly.
            (
                POSITION(DECODE('5374616B65', 'hex') IN COALESCE(tx.carbon_tx_data, ''::bytea)) > 0
                OR POSITION(DECODE('5374616B65', 'hex') IN COALESCE(tx.script_raw, ''::bytea)) > 0
            ) AS tx_has_stake_call,
            (
                POSITION(DECODE('556E7374616B65', 'hex') IN COALESCE(tx.carbon_tx_data, ''::bytea)) > 0
                OR POSITION(DECODE('556E7374616B65', 'hex') IN COALESCE(tx.script_raw, ''::bytea)) > 0
            ) AS tx_has_unstake_call,
            (
                POSITION(DECODE('436C61696D', 'hex') IN COALESCE(tx.carbon_tx_data, ''::bytea)) > 0
                OR POSITION(DECODE('436C61696D', 'hex') IN COALESCE(tx.script_raw, ''::bytea)) > 0
            ) AS tx_has_claim_call,
            COALESCE(
                tx.carbon_tx_data = DECODE('0100000016000000080000000200000000000000', 'hex'),
                FALSE
            ) AS tx_is_soul_apply_inflation,
            tx.result AS tx_result
        FROM events event
        JOIN event_kinds event_kind
          ON event_kind.id = event.event_kind_id
        JOIN transactions tx
          ON tx.id = event.transaction_id
        JOIN blocks block
          ON block.id = tx.block_id
         AND block.chain_id = event.chain_id
        LEFT JOIN addresses address
          ON address.id = event.address_id
        WHERE event.chain_id = $1
          AND tx.timestamp_unix_seconds >= $2
          AND tx.timestamp_unix_seconds <= $3
          -- 1=legacy.backfill.v1, 2=live.v1 (codes since migration 202608040003).
          -- Decoded legacy.raw rows (4) stay excluded: the historical curve keeps
          -- exactly its C#-era inputs.
          AND event.payload_format IN (1, 2)
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
                payload_format: row.get("payload_format"),
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

/// A built daily window plus the fold state at its last CLOSED day (`None` for a
/// single-day window) — the anchor the next run may resume from.
type StakeSnapshotBuildOutput = (
    Vec<StakeSnapshotDailyPoint>,
    Option<(i64, StakeSnapshotState)>,
);

/// Folds the stake events into one daily point per day in `[from_day,
/// to_exclusive_day)`. Also returns the fold state as of the last CLOSED day of
/// the window (the penultimate day; `None` when the window holds a single day):
/// the final day is the cursor day, still open to future blocks, so its state is
/// provisional and must never seed a resume — a resuming run re-reads the open
/// day's events in full and corrects its daily point.
fn build_stake_snapshot_daily_points(
    mut state: StakeSnapshotState,
    rows: &[StakeSnapshotEventRow],
    from_day: i64,
    to_exclusive_day: i64,
) -> Result<StakeSnapshotBuildOutput, DbError> {
    let mut snapshots = Vec::new();
    let mut closed_day_state = None;
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
        if day_cursor + 2 * STAKE_SNAPSHOT_SECONDS_PER_DAY == to_exclusive_day {
            closed_day_state = Some((day_cursor, state.clone()));
        }
        day_cursor += STAKE_SNAPSHOT_SECONDS_PER_DAY;
    }

    Ok((snapshots, closed_day_state))
}

/// `events.payload_format` storage code for the backfilled gen1/gen2 history
/// (see `events::payload_format_code`). It is the only format that is
/// de-duplicated; gen3 rows are taken as written.
const LEGACY_BACKFILL_PAYLOAD_FORMAT: i16 = 1;

fn deduplicate_stake_snapshot_tx_rows(
    rows: &[StakeSnapshotEventRow],
) -> Vec<StakeSnapshotEventRow> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(rows.len());
    for row in rows {
        // gen3 rows are never de-duplicated: the duplicated-event defect this
        // collapse exists for was fixed at the source, so an identical row
        // repeated inside one gen3 transaction is a REAL repeated operation.
        // Proven live on 2026-08-16: testnet tx 43B0CC45… unstakes 0.5 SOUL and
        // then stakes 0.1 SOUL twice (event_index 4 and 5, byte-identical
        // raw_data), and the node's own account balance counts both. Collapsing
        // them under-counted the address's stake, the next claim went below zero,
        // and the guard killed the whole Soul-Masters curve on devnet and testnet
        // every 30 s from 2026-08-13 until this fix.
        if row.payload_format != LEGACY_BACKFILL_PAYLOAD_FORMAT {
            deduped.push(row.clone());
            continue;
        }
        // The identity may fall back to payload_json::text, which since migration
        // 202608040003 no longer stores the event address; appending the
        // relational address keeps two same-shaped events from different
        // addresses distinct, exactly as the pre-strip payload text did (2,969
        // same-tx groups in the live data differ only by address).
        let key = format!(
            "{}|{}|{}",
            row.kind,
            row.payload_identity,
            row.address.as_deref().unwrap_or("")
        );
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
        -- Roll up every month up to and including the cursor's own (possibly partial)
        -- month. The current month takes the latest available daily as its value so the
        -- monthly series tracks the tip instead of lagging a whole month behind (the
        -- live `balance-sync.v1` writer used to fill the current month; the projector
        -- owns it now).
        in_range_months AS (
            SELECT *
            FROM months
            WHERE month_unix_seconds < $3
        ),
        snapshot_rows AS (
            SELECT
                month.month_unix_seconds,
                daily.masters_count,
                (daily.date_unix_seconds + 86399)::bigint AS captured_at_unix_seconds
            FROM in_range_months month
            JOIN LATERAL (
                SELECT masters_count, date_unix_seconds
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
    /// `events.payload_format` storage code for gen3 rows written by our own
    /// ingestion (`live.v1`); the default for rows these tests build.
    const LIVE_PAYLOAD_FORMAT: i16 = 2;

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

        let (points, closed_day_state) =
            build_stake_snapshot_daily_points(current_state, &rows, day_one, day_three)?;
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].date_unix_seconds, day_one);
        assert_eq!(points[0].staked_soul_raw, "50");
        assert_eq!(points[1].date_unix_seconds, day_two);
        assert_eq!(points[1].staked_soul_raw, "30");
        // The resume anchor is the penultimate (last closed) day of the window.
        let (closed_day, closed_state) =
            closed_day_state.ok_or_else(|| DbError::StakeSnapshotReplay {
                reason: "expected a closed-day state for a two-day window".to_owned(),
            })?;
        assert_eq!(closed_day, day_one);
        assert_eq!(closed_state.total_staked_raw, BigInt::from(50));

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

        let (points, closed_day_state) =
            build_stake_snapshot_daily_points(current_state, &rows, day_one, open_day)?;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].date_unix_seconds, day_one);
        assert_eq!(points[0].staked_soul_raw, "50");
        // A single-day window has no newly closed day to anchor a resume on.
        assert!(closed_day_state.is_none());

        Ok(())
    }

    #[test]
    fn stake_snapshot_resume_equals_full_rebuild_and_corrects_the_open_day() -> Result<(), DbError>
    {
        // The incremental-resume contract: folding from the persisted closed-day
        // state over the remaining events must equal the full-from-boundary fold —
        // including the day that was OPEN (and therefore provisional) when the
        // state was saved. Run 1 sees day three only up to its cursor; a late
        // day-three event lands afterwards. Run 2 resumes from the day-two state,
        // re-reads day three in full, corrects its point, and continues into day
        // four. The merged curve must match the one-shot full fold point for point.
        let day_one = 1_700_000_000 - 1_700_000_000_i64.rem_euclid(86_400);
        let day_two = day_one + 86_400;
        let day_three = day_two + 86_400;
        let day_four = day_three + 86_400;
        let day_five = day_four + 86_400;

        let start_state = || StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::new(),
            total_staked_raw: BigInt::zero(),
            soul_supply_raw: BigInt::from(1_000),
            stakers_count: 0,
            masters_count: 0,
        };
        let all_rows = vec![
            test_stake_snapshot_token_row(1, 1, "TokenStake", day_one + 10, "PTESTA", "50")?,
            test_stake_snapshot_token_row(2, 2, "TokenClaim", day_two + 10, "PTESTA", "20")?,
            test_stake_snapshot_token_row(3, 3, "TokenStake", day_three + 10, "PTESTA", "10")?,
            // The late day-three event: exists on chain only after run 1's cursor.
            test_stake_snapshot_token_row(4, 4, "TokenStake", day_three + 80_000, "PTESTA", "7")?,
            test_stake_snapshot_token_row(5, 5, "TokenStake", day_four + 10, "PTESTB", "3")?,
        ];

        // Run 1: cursor sits early in day three — the late event is not visible yet.
        let run_one_rows: Vec<StakeSnapshotEventRow> = all_rows
            .iter()
            .filter(|row| row.timestamp_unix_seconds <= day_three + 20)
            .cloned()
            .collect();
        let (run_one_points, run_one_closed) =
            build_stake_snapshot_daily_points(start_state(), &run_one_rows, day_one, day_four)?;
        assert_eq!(run_one_points.len(), 3);
        assert_eq!(
            run_one_points[2].staked_soul_raw, "40",
            "day three is provisional at run 1's cursor"
        );
        let (resume_day, resume_state) =
            run_one_closed.ok_or_else(|| DbError::StakeSnapshotReplay {
                reason: "run 1 must anchor its resume at day two".to_owned(),
            })?;
        assert_eq!(resume_day, day_two);

        // Run 2: resumes from the day-two state, re-reads day three IN FULL.
        let run_two_rows: Vec<StakeSnapshotEventRow> = all_rows
            .iter()
            .filter(|row| row.timestamp_unix_seconds >= day_three)
            .cloned()
            .collect();
        let (run_two_points, run_two_closed) =
            build_stake_snapshot_daily_points(resume_state, &run_two_rows, day_three, day_five)?;
        assert_eq!(run_two_points.len(), 2);

        // The one-shot full fold is the truth to match.
        let (full_points, full_closed) =
            build_stake_snapshot_daily_points(start_state(), &all_rows, day_one, day_five)?;
        assert_eq!(full_points.len(), 4);
        assert_eq!(
            full_points[2].staked_soul_raw, "47",
            "the closed day three includes the late event"
        );

        // Merged incremental curve == full curve, field by field.
        let merged: Vec<&StakeSnapshotDailyPoint> = run_one_points[..2]
            .iter()
            .chain(run_two_points.iter())
            .collect();
        for (merged_point, full_point) in merged.iter().zip(full_points.iter()) {
            assert_eq!(merged_point.date_unix_seconds, full_point.date_unix_seconds);
            assert_eq!(merged_point.staked_soul_raw, full_point.staked_soul_raw);
            assert_eq!(merged_point.soul_supply_raw, full_point.soul_supply_raw);
            assert_eq!(merged_point.stakers_count, full_point.stakers_count);
            assert_eq!(merged_point.masters_count, full_point.masters_count);
        }

        // And the next resume anchor is identical on both paths.
        let (run_two_day, run_two_state) =
            run_two_closed.ok_or_else(|| DbError::StakeSnapshotReplay {
                reason: "run 2 must anchor its resume at day three".to_owned(),
            })?;
        let (full_day, full_state) = full_closed.ok_or_else(|| DbError::StakeSnapshotReplay {
            reason: "the full fold must anchor its resume at day three".to_owned(),
        })?;
        assert_eq!(run_two_day, day_three);
        assert_eq!(full_day, day_three);
        assert_eq!(run_two_state.total_staked_raw, full_state.total_staked_raw);
        assert_eq!(run_two_state.soul_supply_raw, full_state.soul_supply_raw);
        assert_eq!(run_two_state.stakers_count, full_state.stakers_count);
        assert_eq!(run_two_state.masters_count, full_state.masters_count);
        assert_eq!(
            run_two_state.stakes_by_address,
            full_state.stakes_by_address
        );

        Ok(())
    }

    #[tokio::test]
    async fn stake_snapshot_event_loader_reads_bytea_tx_blobs()
    -> Result<(), Box<dyn std::error::Error>> {
        // transactions.carbon_tx_data/script_raw are bytea (migration 202608040001);
        // the loader's Stake/Unstake/Claim markers and the Token.ApplyInflation image
        // must be matched as bytea. This is the query the forward projector feeds on —
        // a text-typed predicate here fails the whole stake projection on the first
        // new day (the skip-gate hides it on an already-built curve).
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
        let actor = format!("PTESTSTKLOAD{suffix}");
        let timestamp = 1_800_600_000_i64;

        let block = upsert_block(
            &mut tx,
            &mut crate::ProjectionDimensionCache::new(),
            BlockUpsert {
                chain: chain.clone(),
                height: BlockHeight::new(9_900_600_000),
                hash: format!("TESTSTKLOADBLOCK{suffix}"),
                protocol: Some(19),
                chain_address: Some("NULL".to_owned()),
                validator_address: Some("NULL".to_owned()),
                producer_address: None,
                timestamp_unix_seconds: timestamp,
                reward: None,
            },
        )
        .await?;
        let seed_tx =
            |tx_index: i32, carbon_tx_data: &str, result: Option<&str>| TransactionUpsert {
                block_id: block.id,
                chain_id,
                tx_index,
                hash: format!("TESTSTKLOADTX{tx_index}{suffix}"),
                timestamp_unix_seconds: timestamp,
                state: "Halt".to_owned(),
                result: result.map(str::to_owned),
                debug_comment: None,
                payload: None,
                script_raw: None,
                fee_raw: None,
                gas_price_raw: None,
                gas_limit_raw: None,
                sender: Some(actor.clone()),
                gas_payer: Some(actor.clone()),
                gas_target: Some(actor.clone()),
                carbon_tx_type: None,
                carbon_tx_data: Some(carbon_tx_data.to_owned()),
                expiration_unix_seconds: 0,
                signatures: Vec::new(),
            };
        let seed_event = |transaction_id: i32, kind: &str| EventUpsert {
            transaction_id,
            chain_id,
            event_index: 0,
            event_kind: kind.to_owned(),
            event_name: None,
            address: Some(actor.clone()),
            target_address: None,
            contract: Some("SOUL".to_owned()),
            token_id: None,
            raw_data: None,
            payload_format: Some("live.v1".to_owned()),
            payload_json: Some(serde_json::json!({
                "token_event": { "token": "SOUL", "value_raw": "50" }
            })),
            timestamp_unix_seconds: timestamp,
            burned: None,
        };

        // 'Stake' in ASCII hex inside the carbon tx image.
        let stake_tx = upsert_transaction(&mut tx, seed_tx(0, "AA5374616B65BB", None)).await?;
        replace_events(
            &mut tx,
            stake_tx.id,
            &[seed_event(stake_tx.id, "TokenStake")],
        )
        .await?;
        // The Token.ApplyInflation image, whose SOUL delta lives in tx.result
        // (the literal from parse_carbon_intx_i64_raw's own test).
        let inflation_tx = upsert_transaction(
            &mut tx,
            seed_tx(
                1,
                "0100000016000000080000000200000000000000",
                Some("088AF5DD19852C0000"),
            ),
        )
        .await?;
        replace_events(
            &mut tx,
            inflation_tx.id,
            &[seed_event(inflation_tx.id, "TokenMint")],
        )
        .await?;

        let rows =
            load_stake_snapshot_events(&mut tx, chain_id, timestamp - 5, timestamp + 5).await?;
        let stake_row = rows
            .iter()
            .find(|row| row.tx_id == stake_tx.id)
            .ok_or("the TokenStake row must be loaded")?;
        assert!(stake_row.tx_has_stake_call, "'Stake' marker must match");
        assert!(!stake_row.tx_has_unstake_call);
        assert!(!stake_row.tx_has_claim_call);
        assert!(stake_row.tx_apply_inflation_result_soul_delta_raw.is_none());

        let inflation_row = rows
            .iter()
            .find(|row| row.tx_id == inflation_tx.id)
            .ok_or("the ApplyInflation row must be loaded")?;
        assert_eq!(
            inflation_row.tx_apply_inflation_result_soul_delta_raw,
            Some(BigInt::from(48_950_176_249_226_i64)),
            "the ApplyInflation image must be recognized in bytea form"
        );

        tx.rollback().await?;
        Ok(())
    }

    #[tokio::test]
    async fn stake_snapshot_resume_state_round_trips_and_replaces()
    -> Result<(), Box<dyn std::error::Error>> {
        // The persisted fold state must come back exactly as saved (the resumed
        // fold's arithmetic depends on it verbatim), and a newer save must fully
        // replace the previous run's rows — stale per-address stakes surviving a
        // replace would corrupt every later resumed fold.
        let Ok(database_url) = std::env::var("EXPLORER_TEST_DATABASE_URL") else {
            return Ok(());
        };
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await?;
        let mut transaction = pool.begin().await?;
        let chain_id = resolve_chain_id(&mut transaction, &ChainName::new("main")?).await?;
        let day = 1_700_000_000 - 1_700_000_000_i64.rem_euclid(86_400);

        let state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([
                ("PTESTRESA".to_owned(), BigInt::from(50)),
                // Above the master threshold, so masters_count is a real value.
                ("PTESTRESB".to_owned(), BigInt::from(7_000_000_000_000_i64)),
            ]),
            total_staked_raw: BigInt::from(7_000_000_000_050_i64),
            soul_supply_raw: BigInt::from(123_456_789),
            stakers_count: 2,
            masters_count: 1,
        };
        save_stake_snapshot_resume(&mut transaction, chain_id, day, &state).await?;
        let (loaded_day, loaded) = load_stake_snapshot_resume(&mut transaction, chain_id)
            .await?
            .ok_or("saved state must load back")?;
        assert_eq!(loaded_day, day);
        assert_eq!(loaded.stakes_by_address, state.stakes_by_address);
        assert_eq!(loaded.total_staked_raw, state.total_staked_raw);
        assert_eq!(loaded.soul_supply_raw, state.soul_supply_raw);
        assert_eq!(loaded.stakers_count, state.stakers_count);
        assert_eq!(loaded.masters_count, state.masters_count);

        let next = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::from([(
                "PTESTRESB".to_owned(),
                BigInt::from(7_000_000_000_000_i64),
            )]),
            total_staked_raw: BigInt::from(7_000_000_000_000_i64),
            soul_supply_raw: BigInt::from(123_456_790),
            stakers_count: 1,
            masters_count: 1,
        };
        save_stake_snapshot_resume(&mut transaction, chain_id, day + 86_400, &next).await?;
        let (replaced_day, replaced) = load_stake_snapshot_resume(&mut transaction, chain_id)
            .await?
            .ok_or("replaced state must load back")?;
        assert_eq!(replaced_day, day + 86_400);
        assert_eq!(
            replaced.stakes_by_address, next.stakes_by_address,
            "the previous run's per-address rows must be fully replaced"
        );

        transaction.rollback().await?;
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

    #[test]
    fn stake_snapshot_replay_keeps_repeated_gen3_rows_in_one_transaction() -> Result<(), DbError> {
        // A gen3 transaction may legitimately call stake twice for the same
        // amount; both events are then byte-identical and differ only by
        // event_index, so a content-keyed collapse would silently drop half the
        // stake. Only the backfilled gen1/gen2 history is de-duplicated.
        let mut first =
            test_stake_snapshot_token_row(1, 1, "TokenStake", 1_700_000_010, "PTESTA", "10000000")?;
        let mut second =
            test_stake_snapshot_token_row(2, 1, "TokenStake", 1_700_000_010, "PTESTA", "10000000")?;
        // What the node writes for two identical stakes: the same raw event image.
        first.payload_identity = "04534F554C0480969800046D61696E".to_owned();
        second.payload_identity = first.payload_identity.clone();
        let rows = deduplicate_stake_snapshot_tx_rows(&[first, second]);
        assert_eq!(rows.len(), 2);

        let mut state = StakeSnapshotState {
            stakes_by_address: std::collections::HashMap::new(),
            total_staked_raw: BigInt::zero(),
            soul_supply_raw: BigInt::from(1_000_000_000),
            stakers_count: 0,
            masters_count: 0,
        };
        apply_stake_snapshot_transaction(&mut state, &rows, false)?;
        assert_eq!(state.total_staked_raw, BigInt::from(20_000_000));

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_collapses_duplicate_legacy_rows() -> Result<(), DbError> {
        // The gen1/gen2 backfill can carry the same on-chain event twice with no
        // field left to tell the copies apart; those must still collapse.
        let mut first =
            test_stake_snapshot_token_row(1, 1, "TokenStake", 1_700_000_010, "PTESTA", "10000000")?;
        let mut second =
            test_stake_snapshot_token_row(2, 1, "TokenStake", 1_700_000_010, "PTESTA", "10000000")?;
        first.payload_format = LEGACY_BACKFILL_PAYLOAD_FORMAT;
        second.payload_format = LEGACY_BACKFILL_PAYLOAD_FORMAT;
        first.payload_identity = "legacy-image".to_owned();
        second.payload_identity = first.payload_identity.clone();

        let rows = deduplicate_stake_snapshot_tx_rows(&[first, second]);
        assert_eq!(rows.len(), 1);

        Ok(())
    }

    #[test]
    fn stake_snapshot_replay_survives_unstake_before_restake_in_one_transaction()
    -> Result<(), DbError> {
        // The live failure this fixes (testnet tx 43B0CC45…, devnet 9A195B20…):
        // an address stakes 0.1 SOUL twice per transaction, then one transaction
        // unstakes 0.5 (event_index 2, before that transaction's own stakes) and
        // stakes 0.1 twice again. With the repeated rows collapsed the running
        // balance held only 0.3 when the 0.5 claim landed, the replay went
        // negative and the whole curve stopped being written. The node reports
        // 0.4 SOUL staked for this sequence — 0.9 staked minus 0.5 claimed.
        let stake_image = "04534F554C0480969800046D61696E";
        let mut event_id = 0;
        let mut stake_row =
            |tx_id: i32, timestamp: i64| -> Result<StakeSnapshotEventRow, DbError> {
                event_id += 1;
                let mut row = test_stake_snapshot_token_row(
                    event_id,
                    tx_id,
                    "TokenStake",
                    timestamp,
                    "PTESTA",
                    "10000000",
                )?;
                row.payload_identity = stake_image.to_owned();
                row.tx_has_stake_call = true;
                Ok(row)
            };

        let mut rows = vec![
            stake_row(1, 1_700_000_010)?,
            stake_row(2, 1_700_000_020)?,
            stake_row(2, 1_700_000_020)?,
            stake_row(3, 1_700_000_030)?,
            stake_row(3, 1_700_000_030)?,
        ];
        // The unstake transaction: the claim precedes its own restakes, exactly
        // as the event_index order on chain.
        let mut claim = test_stake_snapshot_token_row(
            100,
            4,
            "TokenClaim",
            1_700_000_040,
            "PTESTA",
            "50000000",
        )?;
        claim.tx_has_unstake_call = true;
        claim.tx_has_stake_call = true;
        rows.push(claim);
        for _ in 0..2 {
            let mut row = stake_row(4, 1_700_000_040)?;
            row.tx_has_unstake_call = true;
            rows.push(row);
        }
        // One more ordinary stake transaction after it, as on chain.
        rows.push(stake_row(5, 1_700_000_050)?);
        rows.push(stake_row(5, 1_700_000_050)?);

        let day = stake_snapshot_day_start(1_700_000_010);
        let (curve, _) = build_stake_snapshot_daily_points(
            StakeSnapshotState {
                stakes_by_address: std::collections::HashMap::new(),
                total_staked_raw: BigInt::zero(),
                soul_supply_raw: BigInt::from(1_000_000_000),
                stakers_count: 0,
                masters_count: 0,
            },
            &rows,
            day,
            day + STAKE_SNAPSHOT_SECONDS_PER_DAY,
        )?;
        assert_eq!(curve.len(), 1);
        assert_eq!(curve[0].staked_soul_raw, "40000000");

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
            payload_format: LIVE_PAYLOAD_FORMAT,
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
            payload_format: LIVE_PAYLOAD_FORMAT,
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
