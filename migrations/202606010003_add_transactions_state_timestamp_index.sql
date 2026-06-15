-- Keep state-filtered transaction pagination on the same indexed order used by
-- the API. Fault/Break rows are sparse in restored mainnet history, so scanning
-- the global timestamp index can walk millions of Halt rows before finding a
-- page for the Transactions state filter.

CREATE INDEX IF NOT EXISTS ix_transactions_state_timestamp_id
    ON transactions (state_id, timestamp_unix_seconds DESC, id DESC);
