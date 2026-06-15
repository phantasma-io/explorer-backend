-- Backfill the post-boundary staking chart points the restored baseline was
-- missing, from the frozen C# read model. DB-to-DB baseline repair, not RPC
-- resync or historical replay.
WITH main_chain AS (
    SELECT id AS chain_id
    FROM chains
    WHERE name = 'main'
    ORDER BY id
    LIMIT 1
),
daily_values (
    date_unix_seconds,
    staked_soul_raw,
    soul_supply_raw,
    stakers_count,
    masters_count,
    staking_ratio,
    captured_at_unix_seconds,
    source
) AS (
    VALUES
        (1778803200::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1778868899::bigint, 'balance-sync.v1'),
        (1778889600::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1778906783::bigint, 'balance-sync.v1'),
        (1778976000::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1778994881::bigint, 'balance-sync.v1'),
        (1779062400::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1779148799::bigint, 'balance-sync.catchup.v2'),
        (1779148800::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1779234946::bigint, 'balance-sync.v1'),
        (1779235200::bigint, '7786081934959001', '14580070499708539', 6298, 1041, 0.534022241875623828886102519::numeric, 1779319884::bigint, 'balance-sync.v1'),
        (1779321600::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779328435::bigint, 'balance-sync.v1'),
        (1779408000::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779494399::bigint, 'balance-sync.catchup.v2'),
        (1779494400::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779555946::bigint, 'balance-sync.v1'),
        (1779580800::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779667199::bigint, 'balance-sync.catchup.v2'),
        (1779667200::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779753388::bigint, 'balance-sync.v1'),
        (1779753600::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779839999::bigint, 'balance-sync.catchup.v2'),
        (1779840000::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779926399::bigint, 'balance-sync.catchup.v2'),
        (1779926400::bigint, '7786082034959001', '14580070499708539', 6298, 1041, 0.5340222487343012270508807327::numeric, 1779997957::bigint, 'balance-sync.v1'),
        (1780012800::bigint, '7781080197209001', '14580070499708539', 6297, 1040, 0.5336791888190491033669822701::numeric, 1780090301::bigint, 'balance-sync.v1'),
        (1780099200::bigint, '7781080197209001', '14580070499708539', 6297, 1040, 0.5336791888190491033669822701::numeric, 1780185195::bigint, 'balance-sync.v1'),
        (1780185600::bigint, '7776156811259001', '14580070499708539', 6296, 1039, 0.5333415096596720331185584919::numeric, 1780268868::bigint, 'balance-sync.v1'),
        (1780272000::bigint, '7776156811259001', '14629020675957765', 6296, 1039, 0.5315568952635918268813210201::numeric, 1780341495::bigint, 'balance-sync.v1')
)
INSERT INTO staking_progress_dailies (
    chain_id,
    date_unix_seconds,
    staked_soul_raw,
    soul_supply_raw,
    stakers_count,
    masters_count,
    staking_ratio,
    captured_at_unix_seconds,
    source
)
SELECT
    main_chain.chain_id,
    daily_values.date_unix_seconds,
    daily_values.staked_soul_raw,
    daily_values.soul_supply_raw,
    daily_values.stakers_count,
    daily_values.masters_count,
    daily_values.staking_ratio,
    daily_values.captured_at_unix_seconds,
    daily_values.source
FROM main_chain
CROSS JOIN daily_values
ON CONFLICT (chain_id, date_unix_seconds) DO UPDATE SET
    staked_soul_raw = EXCLUDED.staked_soul_raw,
    soul_supply_raw = EXCLUDED.soul_supply_raw,
    stakers_count = EXCLUDED.stakers_count,
    masters_count = EXCLUDED.masters_count,
    staking_ratio = EXCLUDED.staking_ratio,
    captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
    source = EXCLUDED.source;

WITH main_chain AS (
    SELECT id AS chain_id
    FROM chains
    WHERE name = 'main'
    ORDER BY id
    LIMIT 1
),
monthly_values (
    month_unix_seconds,
    masters_count,
    captured_at_unix_seconds,
    source
) AS (
    VALUES
        (1738368000::bigint, 833, 1740787199::bigint, 'balance-sync.catchup.v2'),
        (1740787200::bigint, 833, 1743465599::bigint, 'balance-sync.catchup.v2'),
        (1743465600::bigint, 833, 1746057599::bigint, 'balance-sync.catchup.v2'),
        (1746057600::bigint, 833, 1748735999::bigint, 'balance-sync.catchup.v2'),
        (1748736000::bigint, 833, 1751327999::bigint, 'balance-sync.catchup.v2'),
        (1751328000::bigint, 833, 1754006399::bigint, 'balance-sync.catchup.v2'),
        (1754006400::bigint, 833, 1756684799::bigint, 'balance-sync.catchup.v2'),
        (1756684800::bigint, 833, 1759276799::bigint, 'balance-sync.catchup.v2'),
        (1759276800::bigint, 833, 1761955199::bigint, 'balance-sync.catchup.v2'),
        (1761955200::bigint, 833, 1764547199::bigint, 'balance-sync.catchup.v2'),
        (1764547200::bigint, 833, 1767225599::bigint, 'balance-sync.catchup.v2'),
        (1767225600::bigint, 833, 1769903999::bigint, 'balance-sync.catchup.v2'),
        (1769904000::bigint, 833, 1772323199::bigint, 'balance-sync.catchup.v2'),
        (1772323200::bigint, 833, 1775001599::bigint, 'balance-sync.catchup.v2'),
        (1775001600::bigint, 833, 1777593599::bigint, 'balance-sync.catchup.v2'),
        (1777593600::bigint, 1039, 1780268868::bigint, 'balance-sync.v1'),
        (1780272000::bigint, 1039, 1780341495::bigint, 'balance-sync.v1')
)
INSERT INTO soul_masters_monthlies (
    chain_id,
    month_unix_seconds,
    masters_count,
    captured_at_unix_seconds,
    source
)
SELECT
    main_chain.chain_id,
    monthly_values.month_unix_seconds,
    monthly_values.masters_count,
    monthly_values.captured_at_unix_seconds,
    monthly_values.source
FROM main_chain
CROSS JOIN monthly_values
ON CONFLICT (chain_id, month_unix_seconds) DO UPDATE SET
    masters_count = EXCLUDED.masters_count,
    captured_at_unix_seconds = EXCLUDED.captured_at_unix_seconds,
    source = EXCLUDED.source;

SELECT setval(
    'staking_progress_dailies_id_seq',
    GREATEST((SELECT COALESCE(MAX(id), 1) FROM staking_progress_dailies), 1),
    true
);

SELECT setval(
    'soul_masters_monthlies_id_seq',
    GREATEST((SELECT COALESCE(MAX(id), 1) FROM soul_masters_monthlies), 1),
    true
);
