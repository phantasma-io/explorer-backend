-- Restore the address -> transaction links the gen1 import never wrote.
--
-- The address page's transaction list reads `address_transactions`, so a
-- transaction shows up for an address only if a link row exists. For
-- `main-generation-1` every one of the 882,697 restored link rows is a
-- transaction ROLE — sender, gas payer, gas target — and not one comes from an
-- address that appears in the transaction's events. Measured on the gen2 base:
-- 441,350 gen1 transactions hold 1,645,589 (transaction, event address) pairs of
-- which 1,174,129 have no link, hiding gen1 history from 30,500 addresses. An
-- address that only received, claimed or swapped in a gen1 transaction saw
-- nothing of it, while the same transaction opened fine by hash.
--
-- gen2 and gen3 are complete: the ingestion writes links for the transaction
-- roles AND for every `events.address_id`
-- (`replace_address_transactions_for_block`), so this is a hole in the restored
-- gen1 baseline, not a defect of the write path. The fix therefore derives the
-- missing rows from the events already stored — it adds rows to a derived join
-- table and changes no historical row, id or value.
--
-- Runs over the whole events table (a few minutes on the full base) and is
-- idempotent: re-running inserts nothing.
INSERT INTO address_transactions (address_id, transaction_id, timestamp_unix_seconds)
SELECT DISTINCT event.address_id, event.transaction_id, tx.timestamp_unix_seconds
FROM events event
JOIN transactions tx ON tx.id = event.transaction_id
WHERE event.address_id IS NOT NULL
ON CONFLICT (address_id, transaction_id) DO NOTHING;

-- `addresses.first_tx_unix_seconds` is the denormalized minimum over an address's
-- links, maintained by the same ingestion step. New links can only move it
-- earlier, so the update mirrors that path's "lower it, never raise it" rule:
-- an address whose first activity was a gen1 event now reports that date instead
-- of its first sent transaction.
UPDATE addresses address
SET first_tx_unix_seconds = link.first_tx_unix_seconds
FROM (
    SELECT address_id, MIN(timestamp_unix_seconds) AS first_tx_unix_seconds
    FROM address_transactions
    GROUP BY address_id
) link
WHERE address.id = link.address_id
  AND (
      address.first_tx_unix_seconds IS NULL
      OR address.first_tx_unix_seconds > link.first_tx_unix_seconds
  );
