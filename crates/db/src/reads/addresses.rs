//! Addresses list read model (the `/addresses` endpoint).
//!
//! A materialized two-stage query: the `base`/`page` CTEs compute a sort key
//! (including a per-symbol balance for `order_by=balance`) and page the ids, then
//! the outer SELECT projects the address rows with an optional balances JSON.
//! The API maps the rows with `address_from_row`. See the design note in
//! [`super::tokens`] for why the wide list reads return rows.
use crate::*;
use sqlx::postgres::PgRow;

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

    /// The full ORDER BY clause for this key. Column names and the `balance_*`
    /// sort aliases are fixed literals; only `direction` varies, and it is a
    /// closed enum, so the clause is injection-safe.
    fn order_clause(self, direction: SortDirection) -> String {
        let dir = direction.as_sql();
        match self {
            Self::Id => format!("address.id {dir}, address.id {dir}"),
            Self::Address => format!("address.address {dir}, address.id {dir}"),
            Self::AddressName => format!("address.address_name {dir}, address.id {dir}"),
            Self::Balance => {
                format!("balance_missing ASC, balance_raw {dir}, address.id {dir}")
            }
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
    pub validator_kind: Option<&'a str>,
    pub with_balance: bool,
}

/// List addresses for a chain matching the filter, ordered by the chosen key.
/// The caller passes `limit + 1` to detect a following page.
pub async fn list_addresses(
    executor: impl sqlx::PgExecutor<'_>,
    filter: &AddressFilter<'_>,
    order_by: AddressOrderBy,
    direction: SortDirection,
    limit: i64,
    offset: i64,
) -> Result<Vec<PgRow>, DbError> {
    let sql = format!(
        r#"
        WITH base AS MATERIALIZED (
            SELECT
                address.id,
                CASE
                    WHEN $5::text = 'SOUL' THEN address.total_soul_amount
                    ELSE COALESCE(sort_balance.amount_raw, 0)
                END AS balance_raw,
                CASE
                    WHEN $5::text = 'SOUL' THEN 0
                    WHEN sort_balance.amount_raw IS NULL THEN 1
                    ELSE 0
                END AS balance_missing
            FROM addresses address
            LEFT JOIN LATERAL (
                SELECT balance.amount_raw
                FROM address_balances balance
                JOIN tokens token ON token.id = balance.token_id
                WHERE balance.address_id = address.id
                  AND token.symbol = $5
                LIMIT 1
            ) sort_balance ON true
            WHERE address.chain_id = $1
              AND ($2::text IS NULL OR address.address = $2 OR address.address_name = $2)
              AND ($3::text IS NULL OR address.address_name = $3)
              AND ($4::text IS NULL OR address.address ILIKE $4)
              AND (
                  $6::text IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM organization_addresses org_address
                      JOIN organizations org ON org.id = org_address.organization_id
                      WHERE org_address.address_id = address.id
                        AND org.name = $6
                  )
              )
              AND (
                  $7::text IS NULL
                  OR EXISTS (
                      SELECT 1
                      FROM address_validator_kinds validator_kind
                      WHERE validator_kind.id = address.address_validator_kind_id
                        AND validator_kind.name = $7
                  )
              )
        ),
        page AS MATERIALIZED (
            SELECT base.id, base.balance_raw, base.balance_missing
            FROM base
            JOIN addresses address ON address.id = base.id
            ORDER BY {order_clause}
            LIMIT $8 OFFSET $9
        )
        SELECT
            address.id,
            address.address,
            address.address_name,
            validator_kind.name AS validator_kind,
            address.staked_amount,
            address.staked_amount_raw,
            address.unclaimed_amount,
            address.unclaimed_amount_raw,
            address.stake_timestamp,
            address.storage_available,
            address.storage_used,
            address.avatar,
            CASE WHEN $10::boolean THEN (
                SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'amount', balance.amount,
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
                        'current_supply', token.current_supply,
                        'current_supply_raw', token.current_supply_raw,
                        'max_supply', token.max_supply,
                        'max_supply_raw', token.max_supply_raw,
                        'burned_supply', token.burned_supply,
                        'burned_supply_raw', token.burned_supply_raw,
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
        LEFT JOIN address_validator_kinds validator_kind ON validator_kind.id = address.address_validator_kind_id
        ORDER BY {order_clause}
        "#,
        order_clause = order_by.order_clause(direction),
    );
    let rows = sqlx::query(&sql)
        .bind(filter.chain_id)
        .bind(filter.address)
        .bind(filter.address_name)
        .bind(filter.address_partial)
        .bind(filter.symbol)
        .bind(filter.organization_name)
        .bind(filter.validator_kind)
        .bind(limit)
        .bind(offset)
        .bind(filter.with_balance)
        .fetch_all(executor)
        .await?;

    Ok(rows)
}
