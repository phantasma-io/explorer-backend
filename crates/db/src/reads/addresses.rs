//! Addresses list read model (the `/addresses` endpoint).
//!
//! A materialized two-stage query: the `base`/`page` CTEs compute a sort key
//! (including a per-symbol balance for `order_by=balance`) and page the ids, then
//! the outer SELECT projects the address rows with an optional balances JSON.
//! The API maps the rows with `address_from_row`. See the design note in
//! [`super::tokens`] for why the wide list reads return rows.
use crate::*;
use sqlx::postgres::PgRow;

/// Resolve an address string to its unique id, matching the on-chain address OR the
/// address name (C# parity — `EP.Transactions.cs:276-283` / `EP.Events.cs:440-446`
/// resolve by `ADDRESS` else `ADDRESS_NAME`, because the frontend's `/address/<name>`
/// route sends the NAME, not the P2K string). An exact address match wins over a name
/// match. `addresses.address` is globally unique, so the resolved id can be bound as
/// an integer; matching on `address_id` lets the planner use the address indexes that
/// a string match across a join cannot. Returns `None` for an unknown address (the
/// caller can then skip the query and return no rows).
pub async fn address_id_by_address(
    executor: impl sqlx::PgExecutor<'_>,
    address: &str,
) -> Result<Option<i32>, DbError> {
    let id = sqlx::query_scalar::<_, i32>(
        "SELECT id FROM addresses WHERE address = $1 OR address_name = $1 \
         ORDER BY (address = $1) DESC LIMIT 1",
    )
    .bind(address)
    .fetch_optional(executor)
    .await?;
    Ok(id)
}

/// Sortable keys for the addresses list. Unlike the other resources, the ORDER
/// BY here is a multi-column expression (the `balance` key also sorts missing
/// balances last), so the enum yields the whole clause rather than one column.
#[derive(Debug, Clone, Copy)]
pub enum AddressOrderBy {
    Id,
    Address,
    AddressName,
    Balance,
}

impl AddressOrderBy {
    /// Parse the public `order_by` query param, defaulting to `id`.
    pub fn from_api_param(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or("id") {
            "id" => Some(Self::Id),
            "address" => Some(Self::Address),
            "address_name" => Some(Self::AddressName),
            "balance" => Some(Self::Balance),
            _ => None,
        }
    }
}

/// Filters for the addresses list. `symbol` selects the balance token used for
/// the `balance` sort and is always present (the API defaults it to SOUL).
#[derive(Debug, Clone, Copy)]
pub struct AddressFilter<'a> {
    pub chain_id: i32,
    pub address: Option<&'a str>,
    pub address_name: Option<&'a str>,
    pub address_partial: Option<&'a str>,
    pub symbol: &'a str,
    pub organization_name: Option<&'a str>,
    pub with_balance: bool,
}

/// List addresses for a chain matching the filter, ordered by the chosen key.
/// The caller passes `limit + 1` to detect a following page.
///
/// The query is shaped per ordering instead of one shape for all four
/// (the previous form materialized a balance for EVERY address of the chain —
/// a per-address LATERAL over `address_balances` — before paging, ~250 ms per
/// request): the non-balance orders touch no balance data at all; `balance`
/// with SOUL orders directly on the denormalized `addresses.total_soul_amount`
/// (`ix_addresses_chain_soul`); `balance` with any other symbol hash-joins the
/// one token's holder rows and appends the no-balance addresses in id order —
/// the same total order the old shape produced, row for row.
pub async fn list_addresses(
    executor: impl sqlx::PgExecutor<'_> + Copy,
    filter: &AddressFilter<'_>,
    order_by: AddressOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let dir = direction.as_sql();
    // The balance-sort token is resolved once, and only when the balances table
    // is actually consulted (any symbol but SOUL). Exact-case match with the
    // lowest id, like the LATERAL's arbitrary single row used to be — symbols
    // are unique per chain today, so this is one token or none. None (unknown
    // symbol) leaves every address balance-less: the id-order fallback below,
    // which is what the old shape answered too.
    let balance_token_id = match order_by {
        AddressOrderBy::Balance if filter.symbol != "SOUL" => {
            sqlx::query_scalar::<_, i32>(
                "SELECT id FROM tokens WHERE chain_id = $1 AND symbol = $2 ORDER BY id LIMIT 1",
            )
            .bind(filter.chain_id)
            .bind(filter.symbol)
            .fetch_optional(executor)
            .await?
        }
        _ => None,
    };

    let balance_by_holders = matches!(order_by, AddressOrderBy::Balance) && filter.symbol != "SOUL";
    let (holders_cte, holders_join, page_columns, page_order, outer_order) = if balance_by_holders {
        (
            r#"holders AS (
            SELECT balance.address_id, balance.amount_raw
            FROM address_balances balance
            WHERE balance.token_id = $9
        ),
        "#,
            "LEFT JOIN holders ON holders.address_id = address.id",
            r#"address.id,
                   (holders.address_id IS NULL) AS balance_missing,
                   COALESCE(holders.amount_raw, 0) AS balance_raw"#,
            format!(
                "(holders.address_id IS NULL) ASC, COALESCE(holders.amount_raw, 0) {dir}, address.id {dir}"
            ),
            format!("page.balance_missing ASC, page.balance_raw {dir}, address.id {dir}"),
        )
    } else {
        // SOUL's running total lives on the address row itself; the non-balance
        // orders never look at balances. Page and outer order share the same
        // address-column expressions.
        let order = match order_by {
            AddressOrderBy::Id => format!("address.id {dir}"),
            AddressOrderBy::Address => format!("address.address {dir}, address.id {dir}"),
            AddressOrderBy::AddressName => {
                format!("address.address_name {dir}, address.id {dir}")
            }
            AddressOrderBy::Balance => {
                format!("address.total_soul_amount {dir}, address.id {dir}")
            }
        };
        ("", "", "address.id", order.clone(), order)
    };

    let sql = format!(
        r#"
        WITH {holders_cte}page AS (
            SELECT {page_columns}
            FROM addresses address
            {holders_join}
            WHERE address.chain_id = $1
              AND ($2::text IS NULL OR address.address = $2 OR address.address_name = $2)
              AND ($3::text IS NULL OR address.address_name = $3)
              AND ($4::text IS NULL OR address.address ILIKE $4)
              AND (
                  $5::text IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM organization_addresses org_address
                      JOIN organizations org ON org.id = org_address.organization_id
                      WHERE org_address.address_id = address.id
                        AND org.name = $5
                  )
              )
            ORDER BY {page_order}
            LIMIT $6 OFFSET $7
        )
        SELECT
            address.id,
            address.address,
            address.address_name,
            trim_scale(address.staked_amount_raw * power(10::numeric, -8))::text AS staked_amount,
            address.staked_amount_raw::text AS staked_amount_raw,
            trim_scale(address.unclaimed_amount_raw * power(10::numeric, -10))::text AS unclaimed_amount,
            address.unclaimed_amount_raw::text AS unclaimed_amount_raw,
            address.stake_timestamp,
            CASE WHEN $8::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'amount', trim_scale(balance.amount_raw * power(10::numeric, -token.decimals))::text,
                    'amount_raw', balance.amount_raw::text,
                    'chain', jsonb_build_object('chain_name', balance_chain.name, 'chain_height', NULL),
                    'token', jsonb_build_object(
                        'name', token.name,
                        'symbol', token.symbol,
                        'fungible', token.fungible,
                        'transferable', token.transferable,
                        'finite', token.finite,
                        'divisible', token.divisible,
                        'fuel', token.fuel,
                        'stakable', token.stakable,
                        'fiat', token.fiat,
                        'swappable', token.swappable,
                        'burnable', token.burnable,
                        'mintable', token.mintable,
                        'decimals', token.decimals,
                        'current_supply', trim_scale(token.current_supply_raw * power(10::numeric, -token.decimals))::text,
                        'current_supply_raw', token.current_supply_raw::text,
                        'max_supply', trim_scale(token.max_supply_raw * power(10::numeric, -token.decimals))::text,
                        'max_supply_raw', token.max_supply_raw::text,
                        'burned_supply', trim_scale(token.burned_supply_raw * power(10::numeric, -token.decimals))::text,
                        'burned_supply_raw', token.burned_supply_raw::text,
                        'script_raw', NULL,
                        'price', NULL,
                        'token_logos', NULL
                    )
                ) ORDER BY balance.amount_raw DESC), '[]'::jsonb)
                FROM address_balances balance
                JOIN tokens token ON token.id = balance.token_id
                JOIN addresses balance_address ON balance_address.id = balance.address_id
                JOIN chains balance_chain ON balance_chain.id = balance_address.chain_id
                WHERE balance.address_id = address.id
            ) ELSE NULL END AS balances_json
        FROM page
        JOIN addresses address ON address.id = page.id
        ORDER BY {outer_order}
        "#,
    );
    let query = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.address)
        .bind(filter.address_name)
        .bind(filter.address_partial)
        .bind(filter.organization_name)
        .bind(limit)
        .bind(offset)
        .bind(filter.with_balance);
    // $9 exists only in the holders shape; binding it elsewhere would over-supply
    // the prepared statement.
    let query = if balance_by_holders {
        query.bind(balance_token_id)
    } else {
        query
    };
    let rows = query.fetch_all(executor).await?;

    Ok(rows)
}
