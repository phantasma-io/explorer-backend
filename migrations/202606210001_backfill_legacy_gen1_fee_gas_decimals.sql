-- Fix the first-generation chain (`main-generation-1`) transaction fee/gas display
-- columns. That chain was imported via a legacy path that stored fee, gas_price,
-- and gas_limit RAW in the display columns (fee == fee_raw, etc.), never applying
-- the per-token decimal scaling that the gen2 (`main` <= the 6,422,526 boundary) and
-- gen3 ingestion do. So gen1 transactions display e.g. "38100000" / "2100" where the
-- correct values are "0.00381" / "0.0000002".
--
-- This backfills the three display columns from their *_raw source EXACTLY as the
-- live ingestion does for new blocks (crates/ingestion/src/lib.rs `build_transaction`
-- + `format_token_amount`, KCAL = 10 decimals): fee/gas_price/gas_limit are each
-- scaled by 10^10, and gas_limit additionally maps the u64::MAX "unlimited" sentinel
-- to NULL (rendered as "unlimited"). The *_raw columns are NOT touched.
--
-- `trim_scale(raw::numeric / 1e10)::text` was verified byte-for-byte equal to
-- `format_token_amount(raw, 10)` across all 6,423,104 gen2 rows (gas_limit + gas_price)
-- and the boundary fee values. Idempotent: each value is recomputed from the unchanged
-- *_raw column and the WHERE gate skips already-scaled rows.
UPDATE transactions t
SET
    fee = trim_scale(t.fee_raw::numeric / 10000000000)::text,
    gas_price = trim_scale(t.gas_price_raw::numeric / 10000000000)::text,
    gas_limit = CASE
        WHEN t.gas_limit_raw = '18446744073709551615' THEN NULL
        ELSE trim_scale(t.gas_limit_raw::numeric / 10000000000)::text
    END
FROM blocks b
WHERE b.id = t.block_id
  AND b.chain_id IN (SELECT id FROM chains WHERE name = 'main-generation-1')
  AND t.fee_raw IS NOT NULL
  AND t.gas_price_raw IS NOT NULL
  AND t.gas_limit_raw IS NOT NULL
  AND t.fee = t.fee_raw;
