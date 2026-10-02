-- One row per transaction, event kind and token. A row holds what the description of
-- a transaction needs: how many such events there are, how many addresses they belong
-- to, the address when there is only one, the sum of their values for a fungible
-- token, and the part of the transaction sender.
--
-- A reader gets the rows of a transaction through the unique index, so a description
-- costs the same for any number of events. Every event is counted, gas events too, so
-- the counts of a transaction's rows add up to its number of events.
--
-- The worker writes these rows for every new block with the same statement
-- (`replace_transaction_summaries` in crates/db/src/transaction_summaries.rs). This
-- migration computes them for the stored history. It reads only stored events and
-- changes no existing row.
CREATE TABLE transaction_summaries (
    transaction_id integer NOT NULL,
    event_kind_id integer NOT NULL,
    -- The symbol of the events' token (`token_event.token`); NULL for events that
    -- carry no token.
    token_symbol text,
    event_count integer NOT NULL,
    address_count integer NOT NULL,
    -- The only address of these events; NULL when there are several.
    address_id integer,
    -- How many of these events belong to the transaction sender.
    sender_event_count integer NOT NULL,
    -- The sum of the event values and the sender's part of it. Set only for a
    -- fungible token, because the value of an NFT event is the NFT id.
    amount_raw numeric,
    sender_amount_raw numeric,
    -- The position of the first of these events, read through
    -- `ux_events_transaction_event_index`. Event ids are not used for order.
    first_event_index integer NOT NULL
);

-- For a fungible token, `events.token_id` holds the value of the event as a plain
-- integer. Other events keep an NFT id or text there, so the CASE turns the value into
-- a number for fungible tokens only.
INSERT INTO transaction_summaries (
    transaction_id,
    event_kind_id,
    token_symbol,
    event_count,
    address_count,
    address_id,
    sender_event_count,
    amount_raw,
    sender_amount_raw,
    first_event_index
)
SELECT
    event.transaction_id,
    event.event_kind_id,
    event.payload_json->'token_event'->>'token',
    count(*),
    count(DISTINCT event.address_id),
    CASE WHEN count(DISTINCT event.address_id) = 1 THEN min(event.address_id) END,
    count(*) FILTER (WHERE event.address_id = tx.sender_id),
    sum(CASE WHEN token.fungible THEN event.token_id::numeric END),
    CASE WHEN bool_and(token.fungible) THEN COALESCE(sum(
        CASE WHEN token.fungible AND event.address_id = tx.sender_id
             THEN event.token_id::numeric END
    ), 0) END,
    min(event.event_index)
FROM events event
JOIN transactions tx ON tx.id = event.transaction_id
LEFT JOIN tokens token ON token.symbol = event.payload_json->'token_event'->>'token'
GROUP BY event.transaction_id, event.event_kind_id, event.payload_json->'token_event'->>'token';

-- The index and the key come after the load: one index build and one check cost
-- much less than maintaining them row by row.
CREATE UNIQUE INDEX ux_transaction_summaries_transaction_kind_token
    ON transaction_summaries (transaction_id, event_kind_id, token_symbol) NULLS NOT DISTINCT;

-- The key to `transactions` deletes the rows of a transaction together with it.
-- Event kinds and addresses get no keys. Every id here comes from a stored event, and
-- the keys of `events` already guarantee that its row exists. A second check of every
-- written row would only slow the worker down.
ALTER TABLE transaction_summaries
    ADD FOREIGN KEY (transaction_id) REFERENCES transactions (id) ON DELETE CASCADE;
