-- Store the raw chain RPC response for a series so series metadata can be rendered
-- straight from the node's reply.
ALTER TABLE series
    ADD COLUMN IF NOT EXISTS chain_api_response jsonb;

