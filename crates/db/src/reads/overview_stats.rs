//! Overview-stats read model (the `/overviewStats` endpoint).
//!
//! Computes the dashboard counters for a chain. `include_burned` is applied by
//! the API layer, which derives `nfts_total` from the burned/unburned split.
use crate::*;

/// Raw counts behind the overview stats.
#[derive(Debug, Clone)]
pub struct OverviewCounts {
    pub transactions_total: i64,
    pub tokens_total: i64,
    pub contracts_total: i64,
    pub addresses_total: i64,
    pub nfts_burned_total: i64,
    pub nfts_unburned_total: i64,
    pub nft_owners_total: i64,
    pub soul_masters_total: i64,
}

/// Count rows of a chain-scoped table (tokens/contracts/addresses), returning 0
/// when the chain is unknown. `table_name` is a fixed literal supplied by this
/// module, never user input, so interpolating it is injection-safe.
async fn count_chain_table(
    pool: &PgPool,
    table_name: &'static str,
    chain_id: Option<i32>,
) -> Result<i64, DbError> {
    let Some(chain_id) = chain_id else {
        return Ok(0);
    };
    let sql = format!("SELECT COUNT(*)::bigint FROM {table_name} WHERE chain_id = $1");
    let count = sqlx::query_scalar::<_, i64>(&sql)
        .bind(chain_id)
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// Compute the overview counts for a chain. `include_legacy_transactions` (0/1)
/// folds `main-generation-*` transactions into the `main` total.
pub async fn overview_counts(
    pool: &PgPool,
    chain: &str,
    chain_id: Option<i32>,
    include_legacy_transactions: i32,
) -> Result<OverviewCounts, DbError> {
    let transactions_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM transactions tx
        JOIN blocks block ON block.id = tx.block_id
        JOIN chains chain ON chain.id = block.chain_id
        WHERE (
            $1::text = ''
            OR chain.name = $1
            OR ($1::text = 'main' AND $2::integer = 1 AND chain.name LIKE 'main-generation-%')
        )
        "#,
    )
    .bind(chain)
    .bind(include_legacy_transactions)
    .fetch_one(pool)
    .await?;

    let tokens_total = count_chain_table(pool, "tokens", chain_id).await?;
    let contracts_total = count_chain_table(pool, "contracts", chain_id).await?;
    let addresses_total = count_chain_table(pool, "addresses", chain_id).await?;

    let nft_counters = sqlx::query(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE nft.burned = true)::bigint AS burned,
            COUNT(*) FILTER (WHERE nft.burned IS NULL OR nft.burned = false)::bigint AS unburned
        FROM nfts nft
        WHERE nft.nsfw = false
          AND nft.blacklisted = false
          AND ($1::integer IS NULL OR nft.chain_id = $1)
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;
    let nfts_burned_total = nft_counters.get::<i64, _>("burned");
    let nfts_unburned_total = nft_counters.get::<i64, _>("unburned");

    let nft_owners_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT ownership.address_id)::bigint
        FROM nft_ownerships ownership
        JOIN nfts nft ON nft.id = ownership.nft_id
        WHERE ownership.amount > 0
          AND nft.nsfw = false
          AND nft.blacklisted = false
          AND ($1::integer IS NULL OR nft.chain_id = $1)
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    let soul_masters_total = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(DISTINCT address.id)::bigint
        FROM addresses address
        JOIN organization_addresses org_address ON org_address.address_id = address.id
        JOIN organizations org ON org.id = org_address.organization_id
        WHERE org.name = 'masters'
          AND ($1::integer IS NULL OR address.chain_id = $1)
        "#,
    )
    .bind(chain_id)
    .fetch_one(pool)
    .await?;

    Ok(OverviewCounts {
        transactions_total,
        tokens_total,
        contracts_total,
        addresses_total,
        nfts_burned_total,
        nfts_unburned_total,
        nft_owners_total,
        soul_masters_total,
    })
}
