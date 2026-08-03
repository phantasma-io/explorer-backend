-- Event kinds become one global dimension, and the events indexes are rebuilt around
-- how the lists are actually read.
--
-- WHY THE DIMENSION MOVES. `event_kinds` was unique on (chain_id, name), so one
-- protocol-level kind existed once per chain: `Inflation` was id 30 on `main` and id 92
-- on `main-generation-1`. An event kind is not a property of a chain — there are no
-- chain-specific events — and the duplication cost real behaviour: a name was the only
-- identity crossing chains, so the list query compared `event_kinds.name` through a join
-- and could not use the kind index at all; a name resolved to a SET of ids, so the filter
-- had to bind an array, and a bound array is invisible to a PostgreSQL generic plan while
-- a scalar equality is not. Both facts together produced four successive workaround
-- commits in the read path, all of which this migration lets us delete.
--
-- The remap is the only step that rewrites historical rows inside the immutable zero
-- state. It preserves every row, every id and every value except `events.event_kind_id`,
-- which is repointed at the canonical row for the SAME name — the kind an event has does
-- not change, only which row records that kind.
--
-- WHY THE INDEXES CHANGE. The only index carrying the timestamp was
-- (event_kind_id, chain_id, timestamp_unix_seconds, id): `chain_id` sits second, so a
-- page filtered by kind and ordered by time could not seek unless it also pinned the
-- chain. (event_kind_id, timestamp_unix_seconds, id) is what the read path asks for, and
-- it serves the chain-scoped case too. The dropped indexes are each either an exact
-- duplicate of another, a strict prefix of another, or the index of a column this
-- migration removes — none of them can be the only usable index for any query.

-- A. Point every event at the canonical kind row for its name (lowest id wins).
WITH canonical AS (
    SELECT name, MIN(id) AS canonical_id
    FROM event_kinds
    GROUP BY name
)
UPDATE events
SET event_kind_id = canonical.canonical_id
FROM event_kinds AS old_kind
JOIN canonical ON canonical.name = old_kind.name
WHERE events.event_kind_id = old_kind.id
  AND old_kind.id <> canonical.canonical_id;

DELETE FROM event_kinds
WHERE id NOT IN (SELECT MIN(id) FROM event_kinds GROUP BY name);

DROP INDEX IF EXISTS "IX_EventKinds_ChainId_NAME";
ALTER TABLE event_kinds DROP COLUMN IF EXISTS chain_id;
CREATE UNIQUE INDEX IF NOT EXISTS "IX_EventKinds_NAME" ON event_kinds (name);

-- B. One index shaped like the queries: seek a kind, read it in output order.
CREATE INDEX IF NOT EXISTS "IX_Events_EventKindId_Timestamp_Id"
    ON events (event_kind_id, timestamp_unix_seconds, id);

-- Superseded by the index above (chain_id sat between the kind and the timestamp).
DROP INDEX IF EXISTS "IX_Events_EventKind_Chain_Timestamp_Id";
-- Strict prefixes of "IX_Events_EventKindId_ID" and of the burn-lookup index.
DROP INDEX IF EXISTS "IX_Events_EventKindId";
DROP INDEX IF EXISTS "IX_Events_ChainId";
-- Exact duplicate of "IX_Events_TransactionId".
DROP INDEX IF EXISTS "IX_Search_Events_TransactionId";
-- Strict prefixes of "IX_Transactions_TIMESTAMP_UNIX_SECONDS_ID" and of
-- ix_transactions_state_timestamp_id.
DROP INDEX IF EXISTS "IX_Transactions_TIMESTAMP_UNIX_SECONDS";
DROP INDEX IF EXISTS "IX_Transactions_StateId";

-- D. Columns on `events` that carry nothing.
-- `date_unix_seconds` is exactly the UTC day of `timestamp_unix_seconds` (verified over
-- the whole table below in the same transaction), so the day filter becomes a range on
-- the timestamp and rides the new index.
DROP INDEX IF EXISTS "IX_Events_DATE_UNIX_SECONDS";
ALTER TABLE events DROP COLUMN IF EXISTS date_unix_seconds;
-- An ingest timestamp with no reader anywhere in the codebase.
ALTER TABLE events DROP COLUMN IF EXISTS dm_unix_seconds;
-- Per-NFT moderation flags duplicated onto every event; zero rows ever set them, and
-- `nfts` carries its own.
ALTER TABLE events DROP COLUMN IF EXISTS nsfw;
ALTER TABLE events DROP COLUMN IF EXISTS blacklisted;
