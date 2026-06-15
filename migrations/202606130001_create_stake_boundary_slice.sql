-- Immutable per-address SOUL stake state captured AT the zero-state boundary
-- (`main` block 6,422,526 / 2025-01-18). This is the missing "oracle": the chain's
-- per-address staked amount at the boundary is not stored anywhere (RPC getAccount
-- returns only current state; the C# Addresses table is overwritten with current
-- state; the legacy event stream does not fully capture staking). It is computed
-- ONCE, offline, by unwinding the known current state back across the clean
-- carbon-era events, then frozen here as baseline data.
--
-- The worker NEVER reverse-replays at runtime: the stake-snapshot builder reads this
-- frozen slice as its starting anchor and builds the daily/monthly Soul-Masters curve
-- strictly FORWARD from the boundary as it syncs. Rebuilding the zero-state DB only
-- requires restoring its backup, which already contains this slice.

-- Header: the boundary aggregate + SOUL supply needed to seed the forward build.
CREATE TABLE IF NOT EXISTS stake_boundary_state (
    chain_id integer NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    boundary_day_unix_seconds bigint NOT NULL,
    soul_supply_raw text NOT NULL,
    masters_count integer NOT NULL,
    stakers_count integer NOT NULL,
    staked_soul_raw text NOT NULL,
    captured_at_unix_seconds bigint NOT NULL,
    source text NOT NULL,
    CONSTRAINT ux_stake_boundary_state_chain UNIQUE (chain_id)
);

-- Per-address staked SOUL (raw) at the boundary. Only addresses with a positive
-- stake are stored.
CREATE TABLE IF NOT EXISTS stake_boundary_balances (
    chain_id integer NOT NULL REFERENCES chains(id) ON DELETE CASCADE,
    address text NOT NULL,
    staked_amount_raw text NOT NULL,
    CONSTRAINT ux_stake_boundary_balances_chain_address UNIQUE (chain_id, address)
);

CREATE INDEX IF NOT EXISTS ix_stake_boundary_balances_chain
    ON stake_boundary_balances (chain_id);
