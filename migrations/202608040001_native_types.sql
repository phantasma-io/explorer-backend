-- Store numbers as numbers and bytes as bytes; stop storing the presentation copies.
-- Every retyped column was proven pure-integer text and every blob proven pure uppercase hex
-- before this migration (2026-08-04 pre-checks, 0 exceptions). The dropped formatted columns
-- were proven exactly reproducible from raw * 10^-decimals over EVERY row, with one documented
-- exception the read path reproduces: gas_limit is NULL when gas_limit_raw is 2^64-1 (the
-- legacy "unlimited gas" sentinel, 89 rows). Each table below is rewritten ONCE — this is the
-- only migration of the batch allowed to rewrite transactions/blocks/signatures.

SET LOCAL max_parallel_workers_per_gather = 0;

ALTER TABLE transactions
    DROP COLUMN fee,
    DROP COLUMN gas_price,
    DROP COLUMN gas_limit,
    ALTER COLUMN fee_raw TYPE numeric USING NULLIF(fee_raw, '')::numeric,
    ALTER COLUMN gas_price_raw TYPE numeric USING NULLIF(gas_price_raw, '')::numeric,
    ALTER COLUMN gas_limit_raw TYPE numeric USING NULLIF(gas_limit_raw, '')::numeric,
    ALTER COLUMN script_raw TYPE bytea USING decode(script_raw, 'hex'),
    ALTER COLUMN carbon_tx_data TYPE bytea USING decode(carbon_tx_data, 'hex');

ALTER TABLE blocks
    ALTER COLUMN reward TYPE numeric USING NULLIF(reward, '')::numeric;

ALTER TABLE signatures
    ALTER COLUMN data TYPE bytea USING decode(data, 'hex');

ALTER TABLE tokens
    DROP COLUMN current_supply,
    DROP COLUMN max_supply,
    DROP COLUMN burned_supply,
    ALTER COLUMN current_supply_raw TYPE numeric USING NULLIF(current_supply_raw, '')::numeric,
    ALTER COLUMN max_supply_raw TYPE numeric USING NULLIF(max_supply_raw, '')::numeric,
    ALTER COLUMN burned_supply_raw TYPE numeric USING NULLIF(burned_supply_raw, '')::numeric;

ALTER TABLE addresses
    DROP COLUMN staked_amount,
    DROP COLUMN unclaimed_amount,
    ALTER COLUMN staked_amount_raw TYPE numeric USING NULLIF(staked_amount_raw, '')::numeric,
    ALTER COLUMN unclaimed_amount_raw TYPE numeric USING NULLIF(unclaimed_amount_raw, '')::numeric;

ALTER TABLE address_balances
    DROP COLUMN amount;

COMMENT ON COLUMN transactions.gas_limit_raw IS
    'Raw KCAL atoms; 18446744073709551615 (u64 max) is the legacy unlimited-gas sentinel — the API serves the formatted gas_limit as NULL for it.';
