-- Speed up legacy burn-marker lookup by token tuple.
-- The C# burn plugin marks Events/Nfts globally by (ContractId, TOKEN_ID)
-- for non-KCAL TokenBurn rows. Rust must check that historical burn state
-- during forward projection, and the restored EF schema only has
-- (contract_id, token_id), which is too broad for common fungible amount
-- values.

CREATE INDEX IF NOT EXISTS ix_events_burn_lookup_chain_contract_token_kind
    ON events (chain_id, contract_id, token_id, event_kind_id)
    WHERE token_id IS NOT NULL;
