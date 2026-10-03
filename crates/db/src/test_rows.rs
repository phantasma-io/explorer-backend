//! Functions that insert test data into the database. The tests of this crate that run
//! against a real database call them. The file is compiled only for tests.
use crate::*;

/// Inserts a token on `chain_id` with a contract of its own, owned by `owner_id`, and
/// returns the token's id. A fungible token is also marked divisible, as real fungible
/// tokens are.
pub(crate) async fn insert_token(
    conn: &mut PgConnection,
    chain_id: i32,
    symbol: &str,
    fungible: bool,
    owner_id: i32,
) -> Result<i32, DbError> {
    let contract_id = upsert_contract_id(conn, chain_id, symbol).await?;
    let token_id = sqlx::query_scalar::<_, i32>(
        r#"
        INSERT INTO tokens (
            symbol, fungible, transferable, finite, divisible, fuel, stakable, fiat,
            swappable, burnable, decimals,
            address_id, owner_id, price_usd, chain_id, contract_id,
            burned_supply_raw, current_supply_raw, max_supply_raw, mintable, name
        )
        VALUES (
            $1, $2, TRUE, FALSE, $2, FALSE, FALSE, FALSE, FALSE, TRUE, 8,
            $3, $3, 0, $4, $5,
            0, 0, 0, TRUE, $1
        )
        RETURNING id
        "#,
    )
    .bind(symbol)
    .bind(fungible)
    .bind(owner_id)
    .bind(chain_id)
    .bind(contract_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(token_id)
}
