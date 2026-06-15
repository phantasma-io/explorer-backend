-- Add the Carbon (legacy chain) token id on tokens, with a per-chain unique index so
-- a Carbon id maps to a single token. The column is nullable and populated separately
-- (from RPC) where the legacy id is known.
ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS carbon_id bigint;

CREATE UNIQUE INDEX IF NOT EXISTS ix_tokens_chain_carbon_id
    ON tokens (chain_id, carbon_id)
    WHERE carbon_id IS NOT NULL;

