-- Remove every index the exhaustive 2026-08-03 read/write map proved unreachable, add the three
-- the read path actually wants, and write the model's traps onto the schema itself.

-- Substring search over hashes: /searches matches exact values only, no UI element sends
-- hash_partial, and the free-text `q` ORs ILIKEs across joined relations where no per-table index
-- can help. 2.8 GB of GIN plus per-insert maintenance for a reachable-by-nobody feature; the
-- hash_partial API parameter is removed with it (owner decision 2026-08-03).
DROP INDEX "IX_Search_Blocks_Hash_trgm";
DROP INDEX "IX_Search_Transactions_Hash_trgm";
DROP EXTENSION IF EXISTS pg_trgm;

-- No reader anywhere: burn accounting uses ix_events_burn_lookup_chain_contract_token_kind, and
-- nothing filters events by burned.
DROP INDEX "IX_Events_BURNED_EventKindId";

-- order_by=id with a kind filter is reachable only through the raw API (the UI orders by date);
-- 1.67 GB for that ordering is not a fair trade. The parameter stays and answers via a sort.
DROP INDEX "IX_Events_EventKindId_ID";

-- signature_kinds holds a single row (Ed25519); an index over a one-value column selects nothing.
DROP INDEX "IX_Signatures_SignatureKindId";

-- FK-support indexes for address deletes that never happen (and now refuse instead of cascading).
-- The producer index stays: the write path scans it on every block.
DROP INDEX "IX_Blocks_ValidatorAddressId";
DROP INDEX "IX_Blocks_ChainAddressId";

-- No reader: the NFT maintenance queues key on chain_api_response/token_uri and order by id, the
-- burned readers wrap the column in COALESCE, and a two-value chain_id index selects half the table.
DROP INDEX "IX_Nfts_DM_UNIX_SECONDS";
DROP INDEX "IX_Nfts_BURNED";
DROP INDEX "IX_Nfts_ChainId";

-- The resolve path (`address = $1 OR address_name = $1`) needs each arm to have its own leading
-- index: ux_addresses_address (202608030001) serves the first, this partial serves the second —
-- 92.7% of rows have no name, so the old composite forced a 13.5 ms full-index walk per resolve.
CREATE INDEX ix_addresses_address_name ON addresses (address_name)
    WHERE address_name IS NOT NULL;
DROP INDEX "IX_Addresses_ADDRESS_ADDRESS_NAME";

-- The unfiltered events list pages by (timestamp, id); give the seek its exact shape.
CREATE INDEX ix_events_timestamp_id ON events (timestamp_unix_seconds, id);
DROP INDEX "IX_Events_TIMESTAMP_UNIX_SECONDS";

-- The /addresses balance ordering (symbol=SOUL, the default) sorts by total_soul_amount; today it
-- recomputes and sorts the whole chain's addresses per page.
CREATE INDEX ix_addresses_chain_soul ON addresses (chain_id, total_soul_amount DESC, id);

-- The traps that cost debugging time this year, written where the next reader will look.
COMMENT ON COLUMN events.token_id IS
    'Dual semantics inherited from C#: the NFT instance id for NFT events, the RAW AMOUNT for fungible token events (equals payload token_event.value_raw). Burn accounting depends on it.';
COMMENT ON COLUMN events.burned IS
    'Tri-state in practice: true or NULL, never false. Set retroactively on all of a token''s historical events when it burns; one of only two mutable event columns (the other is nft_id).';
COMMENT ON COLUMN transactions.hash IS
    'NOT unique by protocol history: 63 pre-boundary hashes appear in more than one block (gen1 double inclusions, Feb-2023 gen2 cluster). Look up via occurrence count + block height/tx index.';
COMMENT ON COLUMN chains.current_height IS
    'Sync cursor, advanced once per committed block; the only mutable column of this dimension.';
COMMENT ON COLUMN addresses.storage_available IS
    'Frozen pre-boundary gen1/gen2 account state (8,617 non-zero rows); no writer since the zero base. Preserved deliberately — see the F1 audit finding.';
COMMENT ON COLUMN addresses.avatar IS
    'Frozen pre-boundary account state (2 custom avatars); no writer since the zero base.';
COMMENT ON COLUMN addresses.address_validator_kind_id IS
    'Frozen pre-boundary validator kinds (4 Primary rows = the gen2 validators); no writer since the zero base.';
