-- Incremental-resume state for the Soul-Masters forward projector. The forward build
-- is a pure fold (per-address stakes + totals) over the stake events; without a saved
-- fold state every run that has a new day to build replays the whole event history
-- from the boundary seed — O(chain age) per run. These tables persist the fold state
-- as of the last CLOSED projected day, written atomically with the daily/monthly
-- upserts of each building run and replaced per run.
--
-- Empty tables (or any inconsistency with the stored curve) simply mean the next run
-- rebuilds from the boundary seed — the previous behavior, kept as the permanent
-- fallback. Deleting these rows is always safe.
--
-- The state deliberately anchors at the last CLOSED day, never the cursor day: the
-- cursor day is still open (more blocks can land on it), so a resuming run re-reads
-- that day's events in full and corrects its provisional daily point — the same way
-- the full rescan used to close it.

CREATE TABLE stake_snapshot_resume (
    chain_id integer PRIMARY KEY REFERENCES chains (id),
    last_projected_day_unix_seconds bigint NOT NULL,
    total_staked_raw numeric NOT NULL,
    soul_supply_raw numeric NOT NULL,
    stakers_count integer NOT NULL,
    masters_count integer NOT NULL
);

-- Per-address stakes of the fold state (only addresses with stake > 0, mirroring the
-- in-memory map). Addresses are stored by value like the stake_boundary_* family:
-- this is projector-internal scratch state, joined against nothing.
CREATE TABLE stake_snapshot_resume_stakes (
    chain_id integer NOT NULL REFERENCES chains (id),
    address text NOT NULL,
    staked_amount_raw numeric NOT NULL,
    PRIMARY KEY (chain_id, address)
);
