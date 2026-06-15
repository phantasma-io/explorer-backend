-- Denormalize the transaction timestamp onto address_transactions so an
-- address's activity can be paged in time order from a covering index, without
-- sorting the joined transactions of a high-activity address.
ALTER TABLE address_transactions ADD COLUMN timestamp_unix_seconds bigint;

UPDATE address_transactions address_tx
SET timestamp_unix_seconds = tx.timestamp_unix_seconds
FROM transactions tx
WHERE tx.id = address_tx.transaction_id;

ALTER TABLE address_transactions ALTER COLUMN timestamp_unix_seconds SET NOT NULL;

CREATE INDEX "IX_AddressTransactions_AddressId_Timestamp"
    ON address_transactions (address_id, timestamp_unix_seconds, id);
