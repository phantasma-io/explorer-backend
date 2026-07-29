-- The legacy getAccount fields validator / storage.{available,used} / avatar
-- are dead gen2 leftovers: the gen3 RPC hardcodes them for every address
-- ("Invalid", zeros, empty avatar), no gen3 chain concept backs them, and no
-- new endpoint will provide them. The balance sync now reads the lightweight
-- account endpoints, which do not carry these fields, so the columns only held
-- constants. Drop them together with the per-address validator-kind link and
-- its lookup table; the /api/v1/validatorKinds endpoint and the API fields go
-- with them.
ALTER TABLE addresses
    DROP COLUMN IF EXISTS storage_available,
    DROP COLUMN IF EXISTS storage_used,
    DROP COLUMN IF EXISTS avatar,
    DROP COLUMN IF EXISTS address_validator_kind_id;

DROP TABLE IF EXISTS address_validator_kinds;
