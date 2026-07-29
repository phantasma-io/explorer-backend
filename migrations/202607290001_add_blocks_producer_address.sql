-- Gas model v2 adds an optional consensus-covered producer identity to every
-- block (RPC field producerAddress): the fee-payout address of the block's
-- producer, deliberately distinct from validator_address (the raft transport
-- leader) even though they coincide today.
-- Nullable with no backfill on purpose: pre-v2 blocks genuinely have no
-- producer, the flip block itself has none, and raw blocks are not archived so
-- there is no source to backfill from. Existing rows (including the immutable
-- zero-state <= main 6,422,526) keep NULL and their API output is unchanged.
ALTER TABLE blocks
    ADD COLUMN IF NOT EXISTS producer_address_id integer
        REFERENCES addresses (id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS "IX_Blocks_ProducerAddressId"
    ON blocks (producer_address_id);
