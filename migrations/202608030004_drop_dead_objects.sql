-- Remove the objects the 2026-08-03 audit proved dead: tables with no writer in this codebase and
-- nothing but empty reads (empty through ALL of mainnet history), and columns that are either
-- 100% empty, unmaintained duplicates, or derivable. Deliberately KEPT despite having no writer,
-- because they hold real frozen pre-boundary data: addresses.{avatar,storage_available,
-- storage_used,address_validator_kind_id} + address_validator_kinds (the F1 preservation set),
-- tokens.script_raw (48 C#-era tokens carry real scripts), series.attr_* (served by the API),
-- organizations (live dimension: stakers/masters membership targets).

-- Children before parents; platforms itself stays (1 live row, served by /platforms).
DROP TABLE token_logos;
DROP TABLE token_logo_types;
DROP TABLE block_oracles;
DROP TABLE oracles;
DROP TABLE externals;
DROP TABLE platform_interops;
DROP TABLE platform_tokens;
DROP TABLE fiat_exchange_rates;
DROP TABLE global_variables;
DROP TABLE staking_snapshot_projection_state;
DROP TABLE rejected_transaction_candidates;
DROP TABLE ef_migrations_history;

-- 0 of 40,344 rows ever used it; the organization relation lives in organization_addresses.
ALTER TABLE addresses DROP COLUMN organization_id;
-- 100% NULL; the two probe sites that referenced it are removed with this migration's code.
ALTER TABLE addresses DROP COLUMN user_name;

-- The unmaintained half of a circular 1:1: Rust never writes it (66 of 114 tokens lack it) while
-- tokens.contract_id is maintained and identical where both exist (0 mismatches). Dropping the
-- column drops FK_Contracts_Tokens_TokenId and IX_Contracts_TokenId with it.
ALTER TABLE contracts DROP COLUMN token_id;

-- Stale C#-era price-feed cache on 10 pre-boundary rows; USD is the only currency the system
-- prices (fiat_exchange_rates was empty forever, the frontend renders no non-USD price).
ALTER TABLE tokens
    DROP COLUMN price_eur,
    DROP COLUMN price_gbp,
    DROP COLUMN price_jpy,
    DROP COLUMN price_cad,
    DROP COLUMN price_aud,
    DROP COLUMN price_cny,
    DROP COLUMN price_rub;

-- 100% derivable: previous_hash = hash of (chain_id, height-1), zero-hash sentinel at height 1
-- (verified on a 100k sample). The read path composes it from the ux_blocks_chain_height self-join.
ALTER TABLE blocks DROP COLUMN previous_hash;
