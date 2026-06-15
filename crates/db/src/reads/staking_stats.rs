//! Staking-stats read model (the `/stakingStats` endpoint).
//!
//! Returns the daily staking-progress points and the monthly Soul-Masters
//! points for a chain. `limit = Some(n)` returns the latest `n` points (newest
//! first, then re-sorted ascending); `limit = None` returns the full ascending
//! series. The API maps the rows and applies the supply adjustment.
use crate::*;
use sqlx::postgres::PgRow;

/// List daily staking-progress points for a chain. See module docs for `limit`.
pub async fn list_staking_dailies(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: i32,
    limit: Option<i64>,
) -> Result<Vec<PgRow>, DbError> {
    let rows = match limit {
        Some(limit) => {
            sqlx::query(
                r#"
                SELECT *
                FROM (
                    SELECT
                        date_unix_seconds,
                        staked_soul_raw,
                        soul_supply_raw,
                        stakers_count,
                        masters_count,
                        staking_ratio::double precision AS staking_ratio,
                        captured_at_unix_seconds,
                        source
                    FROM staking_progress_dailies
                    WHERE chain_id = $1
                    ORDER BY date_unix_seconds DESC
                    LIMIT $2
                ) limited
                ORDER BY date_unix_seconds ASC
                "#,
            )
            .bind(chain_id)
            .bind(limit)
            .fetch_all(executor)
            .await?
        }
        None => {
            sqlx::query(
                r#"
                SELECT
                    date_unix_seconds,
                    staked_soul_raw,
                    soul_supply_raw,
                    stakers_count,
                    masters_count,
                    staking_ratio::double precision AS staking_ratio,
                    captured_at_unix_seconds,
                    source
                FROM staking_progress_dailies
                WHERE chain_id = $1
                ORDER BY date_unix_seconds ASC
                "#,
            )
            .bind(chain_id)
            .fetch_all(executor)
            .await?
        }
    };
    Ok(rows)
}

/// List monthly Soul-Masters points for a chain. See module docs for `limit`.
pub async fn list_soul_masters_monthlies(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: i32,
    limit: Option<i64>,
) -> Result<Vec<PgRow>, DbError> {
    let rows = match limit {
        Some(limit) => {
            sqlx::query(
                r#"
                SELECT *
                FROM (
                    SELECT month_unix_seconds, masters_count, captured_at_unix_seconds, source
                    FROM soul_masters_monthlies
                    WHERE chain_id = $1
                    ORDER BY month_unix_seconds DESC
                    LIMIT $2
                ) limited
                ORDER BY month_unix_seconds ASC
                "#,
            )
            .bind(chain_id)
            .bind(limit)
            .fetch_all(executor)
            .await?
        }
        None => {
            sqlx::query(
                r#"
                SELECT month_unix_seconds, masters_count, captured_at_unix_seconds, source
                FROM soul_masters_monthlies
                WHERE chain_id = $1
                ORDER BY month_unix_seconds ASC
                "#,
            )
            .bind(chain_id)
            .fetch_all(executor)
            .await?
        }
    };
    Ok(rows)
}
