//! Address-stats read model (the `/addressStats` endpoint).
//!
//! Returns the daily new-address counts (and running cumulative total) for a
//! chain, gap-filled across every day in range. `daily_limit = 0` returns the
//! full series; a positive limit returns the most recent `daily_limit` days.
//! The API maps the rows.
use crate::*;
use sqlx::postgres::PgRow;

/// List the gap-filled daily new-address points for a chain.
pub async fn new_address_dailies(
    executor: impl sqlx::PgExecutor<'_>,
    chain_id: i32,
    daily_limit: i64,
) -> Result<Vec<PgRow>, DbError> {
    let rows = sqlx::query(
        r#"
        WITH daily AS (
            SELECT ((first_tx_unix_seconds / 86400) * 86400)::bigint AS day_unix_seconds,
                   COUNT(*)::bigint AS new_addresses_count
            FROM addresses
            WHERE chain_id = $1
              AND address != 'NULL'
              AND first_tx_unix_seconds IS NOT NULL
            GROUP BY ((first_tx_unix_seconds / 86400) * 86400)
        ),
        bounds AS (
            SELECT MIN(day_unix_seconds)::bigint AS first_day,
                   GREATEST(MAX(day_unix_seconds)::bigint, ((EXTRACT(EPOCH FROM now())::bigint / 86400) * 86400)) AS last_day
            FROM daily
        ),
        expanded AS (
            SELECT generate_series(first_day, last_day, 86400)::bigint AS day_unix_seconds
            FROM bounds
            WHERE first_day IS NOT NULL
        ),
        totals AS (
            SELECT
                expanded.day_unix_seconds,
                COALESCE(daily.new_addresses_count, 0)::bigint AS new_addresses_count,
                SUM(COALESCE(daily.new_addresses_count, 0)) OVER (ORDER BY expanded.day_unix_seconds)::bigint AS cumulative_addresses_count
            FROM expanded
            LEFT JOIN daily ON daily.day_unix_seconds = expanded.day_unix_seconds
        )
        SELECT *
        FROM totals
        WHERE $2::bigint = 0
           OR day_unix_seconds >= (
               SELECT day_unix_seconds
               FROM totals
               ORDER BY day_unix_seconds DESC
               OFFSET GREATEST($2::bigint - 1, 0)
               LIMIT 1
           )
        ORDER BY day_unix_seconds ASC
        "#,
    )
    .bind(chain_id)
    .bind(daily_limit)
    .fetch_all(executor)
    .await?;

    Ok(rows)
}
