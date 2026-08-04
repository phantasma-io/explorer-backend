-- Declare the natural keys the data has always satisfied (verified duplicate-free across the
-- whole database, incl. 76M events, on 2026-08-03). Each unique index replaces the non-unique
-- index of the same shape, so re-projection bugs become constraint errors instead of silent
-- duplicate facts. transactions.hash is deliberately NOT unique: 63 pre-boundary hashes really
-- do appear in more than one block (gen1 adjacent-block double inclusions and a Feb-2023 gen2
-- cluster); the lookup endpoint disambiguates by block height + tx index.

CREATE UNIQUE INDEX ux_blocks_chain_height ON blocks (chain_id, height);
DROP INDEX "IX_Blocks_ChainId_HEIGHT";

CREATE UNIQUE INDEX ux_transactions_block_tx_index ON transactions (block_id, tx_index);
DROP INDEX "IX_Transactions_BlockId_INDEX";

CREATE UNIQUE INDEX ux_events_transaction_event_index ON events (transaction_id, event_index);
DROP INDEX "IX_Events_TransactionId";

-- The resolve path matches an address string without a chain (LIMIT 1); the data agrees it is
-- globally unique (0 addresses exist on both chains), but the invariant stays UNDECLARED: every
-- address writer upserts with ON CONFLICT (chain_id, address), and a second unique index on the
-- same row lets PostgreSQL's speculative insertion cross-lock two transactions inserting the
-- same address concurrently (sporadic 40P01 — the block projection and the metadata passes hit
-- the same hot addresses). A plain index keeps the resolve fast path.
CREATE INDEX ix_addresses_address ON addresses (address);

CREATE UNIQUE INDEX ux_token_daily_prices_token_date ON token_daily_prices (token_id, date_unix_seconds);
DROP INDEX "IX_TokenDailyPrices_TokenId";
