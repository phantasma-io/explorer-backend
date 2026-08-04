-- address_transactions: the (address_id, transaction_id) pair IS the row identity
-- (declared UNIQUE since the table's C# era), so the surrogate id adds a column and
-- an index level without distinguishing anything. Promote the natural key to the
-- primary key and re-point the timeline index's tie-break at transaction_id, which
-- the read path now uses as its cursor id.

-- The timeline seek index gains transaction_id in place of id so the
-- (timestamp, tie-break) page order stays fully index-defined.
CREATE INDEX ix_address_transactions_address_timestamp_tx
    ON address_transactions (address_id, timestamp_unix_seconds, transaction_id);

DROP INDEX "IX_AddressTransactions_AddressId_Timestamp";

ALTER TABLE address_transactions
    DROP CONSTRAINT "PK_AddressTransactions";

-- Metadata-only: no table rewrite. The identity sequence goes with the column.
ALTER TABLE address_transactions
    DROP COLUMN id;

-- Reuses the existing unique index (which gets renamed to the constraint name).
ALTER TABLE address_transactions
    ADD CONSTRAINT "PK_AddressTransactions"
    PRIMARY KEY USING INDEX "IX_AddressTransactions_AddressId_TransactionId";
