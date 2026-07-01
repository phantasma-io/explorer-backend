-- The node now returns ABI-declared contract events (whose on-chain kind byte
-- collides with a native kind) under kind "Custom_V2", carrying the real event
-- name (e.g. "AuctionCreated", "PostLiked") separately from the kind. Store that
-- name so the API and UI can label such events by their real name.
-- Nullable on purpose: existing rows keep NULL and readers fall back to the kind
-- name, so historical data (including the immutable zero-state <= main 6,422,526)
-- stays byte-identical in output and needs no resync or backfill.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS event_name text;
