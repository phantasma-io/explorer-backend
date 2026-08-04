-- events.payload_json stored two relational facts in every one of its 76,091,411 rows:
-- 'chain' (= chains.name via chain_id) and 'address' (= addresses.address via address_id).
-- Both were measured equal on every row before this migration (2026-08-04 pre-checks:
-- zero mismatches, zero NULL-address exceptions), so the stored copies are pure weight.
-- The API re-inserts both keys at serve time; the served string stays byte-identical
-- because it is re-serialized from a sorted map either way.
--
-- The strip is equality-guarded: a key pair is removed only where both match the
-- relational values (the measurement shows that selects every row; a hypothetical
-- divergent row would keep BOTH keys — safe, just unstripped).
SET LOCAL max_parallel_workers_per_gather = 0;

UPDATE events event
SET payload_json = (event.payload_json - 'chain') - 'address'
FROM chains chain, addresses address
WHERE chain.id = event.chain_id
  AND address.id = event.address_id
  AND event.payload_json->>'chain' = chain.name
  AND event.payload_json->>'address' = address.address;

-- The four formats as smallint codes (1=legacy.backfill.v1, 2=live.v1, 3=legacy.raw.v1,
-- 4=legacy.decoded.v1). The whole-table rewrite this ALTER forces also compacts the
-- row-version bloat the UPDATE above left behind.
ALTER TABLE events
    ALTER COLUMN payload_format TYPE smallint
    USING CASE payload_format
        WHEN 'legacy.backfill.v1' THEN 1
        WHEN 'live.v1' THEN 2
        WHEN 'legacy.raw.v1' THEN 3
        WHEN 'legacy.decoded.v1' THEN 4
    END;

COMMENT ON COLUMN events.payload_format IS
    '1=legacy.backfill.v1, 2=live.v1, 3=legacy.raw.v1 (undecoded; raw_data holds the bytes), 4=legacy.decoded.v1';
