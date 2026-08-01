-- Token metadata (getToken/getTokens extended `metadata`) has never been stored:
-- the token sync read only the supply fields, so the chain's own parameters —
-- SOUL's inflation targets `_ia` and its staking settings `_sb*`/`_s*`, a token's
-- `name`, and whatever else a contract writes — were dropped on arrival.
-- The values are VM values, so a scalar is a string while an array or a struct
-- keeps its real shape; jsonb holds all three without a second column.
-- Nullable with no backfill on purpose: the values were never captured, and the
-- node answers current state only, so there is nothing historical to backfill
-- from. Forward sync fills every token the node still knows on its first pass;
-- tokens that no longer exist on chain keep NULL, which is the honest answer.
-- Existing rows (including the immutable zero-state <= main 6,422,526) are
-- untouched.
ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS metadata jsonb;
