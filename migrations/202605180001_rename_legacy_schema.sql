-- Keep the restored C# Explorer schema and data model intact.
-- This migration only changes SQL object names from EF/PascalCase style to
-- lower snake_case so Rust queries can be written plainly. It must not copy
-- data, rebuild read models, or replace integer IDs with UUIDs.

DO $$
BEGIN
    IF to_regclass('public."Blocks"') IS NULL THEN
        RAISE EXCEPTION 'expected restored legacy EF schema: public."Blocks" is missing';
    END IF;

    IF to_regclass('public.blocks') IS NOT NULL THEN
        RAISE EXCEPTION 'target snake_case table public.blocks already exists';
    END IF;
END $$;

ALTER TABLE "AddressBalances" RENAME TO address_balances;
ALTER TABLE "AddressTransactions" RENAME TO address_transactions;
ALTER TABLE "AddressValidatorKinds" RENAME TO address_validator_kinds;
ALTER TABLE "Addresses" RENAME TO addresses;
ALTER TABLE "BlockOracles" RENAME TO block_oracles;
ALTER TABLE "Blocks" RENAME TO blocks;
ALTER TABLE "Chains" RENAME TO chains;
ALTER TABLE "ContractMethods" RENAME TO contract_methods;
ALTER TABLE "Contracts" RENAME TO contracts;
ALTER TABLE "EventKinds" RENAME TO event_kinds;
ALTER TABLE "Events" RENAME TO events;
ALTER TABLE "Externals" RENAME TO externals;
ALTER TABLE "FiatExchangeRates" RENAME TO fiat_exchange_rates;
ALTER TABLE "GlobalVariables" RENAME TO global_variables;
ALTER TABLE "Infusions" RENAME TO infusions;
ALTER TABLE "NftOwnerships" RENAME TO nft_ownerships;
ALTER TABLE "Nfts" RENAME TO nfts;
ALTER TABLE "Oracles" RENAME TO oracles;
ALTER TABLE "OrganizationAddresses" RENAME TO organization_addresses;
ALTER TABLE "Organizations" RENAME TO organizations;
ALTER TABLE "PlatformInterops" RENAME TO platform_interops;
ALTER TABLE "PlatformTokens" RENAME TO platform_tokens;
ALTER TABLE "Platforms" RENAME TO platforms;
ALTER TABLE "RejectedTransactionCandidates" RENAME TO rejected_transaction_candidates;
ALTER TABLE "SeriesModes" RENAME TO series_modes;
ALTER TABLE "Serieses" RENAME TO series;
ALTER TABLE "SignatureKinds" RENAME TO signature_kinds;
ALTER TABLE "Signatures" RENAME TO signatures;
ALTER TABLE "SoulMastersMonthlies" RENAME TO soul_masters_monthlies;
ALTER TABLE "StakingProgressDailies" RENAME TO staking_progress_dailies;
ALTER TABLE "TokenDailyPrices" RENAME TO token_daily_prices;
ALTER TABLE "TokenLogoTypes" RENAME TO token_logo_types;
ALTER TABLE "TokenLogos" RENAME TO token_logos;
ALTER TABLE "Tokens" RENAME TO tokens;
ALTER TABLE "TransactionStates" RENAME TO transaction_states;
ALTER TABLE "Transactions" RENAME TO transactions;
ALTER TABLE "__EFMigrationsHistory" RENAME TO ef_migrations_history;

ALTER TABLE address_balances RENAME COLUMN "ID" TO id;
ALTER TABLE address_balances RENAME COLUMN "TokenId" TO token_id;
ALTER TABLE address_balances RENAME COLUMN "AddressId" TO address_id;
ALTER TABLE address_balances RENAME COLUMN "AMOUNT" TO amount;
ALTER TABLE address_balances RENAME COLUMN "AMOUNT_RAW" TO amount_raw;

ALTER TABLE address_transactions RENAME COLUMN "ID" TO id;
ALTER TABLE address_transactions RENAME COLUMN "AddressId" TO address_id;
ALTER TABLE address_transactions RENAME COLUMN "TransactionId" TO transaction_id;

ALTER TABLE address_validator_kinds RENAME COLUMN "ID" TO id;
ALTER TABLE address_validator_kinds RENAME COLUMN "NAME" TO name;

ALTER TABLE addresses RENAME COLUMN "ID" TO id;
ALTER TABLE addresses RENAME COLUMN "ADDRESS" TO address;
ALTER TABLE addresses RENAME COLUMN "ADDRESS_NAME" TO address_name;
ALTER TABLE addresses RENAME COLUMN "USER_NAME" TO user_name;
ALTER TABLE addresses RENAME COLUMN "NAME_LAST_UPDATED_UNIX_SECONDS" TO name_last_updated_unix_seconds;
ALTER TABLE addresses RENAME COLUMN "STAKED_AMOUNT" TO staked_amount;
ALTER TABLE addresses RENAME COLUMN "UNCLAIMED_AMOUNT" TO unclaimed_amount;
ALTER TABLE addresses RENAME COLUMN "UNCLAIMED_AMOUNT_RAW" TO unclaimed_amount_raw;
ALTER TABLE addresses RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE addresses RENAME COLUMN "AddressValidatorKindId" TO address_validator_kind_id;
ALTER TABLE addresses RENAME COLUMN "OrganizationId" TO organization_id;
ALTER TABLE addresses RENAME COLUMN "STAKED_AMOUNT_RAW" TO staked_amount_raw;
ALTER TABLE addresses RENAME COLUMN "STAKE_TIMESTAMP" TO stake_timestamp;
ALTER TABLE addresses RENAME COLUMN "AVATAR" TO avatar;
ALTER TABLE addresses RENAME COLUMN "STORAGE_AVAILABLE" TO storage_available;
ALTER TABLE addresses RENAME COLUMN "STORAGE_USED" TO storage_used;
ALTER TABLE addresses RENAME COLUMN "TOTAL_SOUL_AMOUNT" TO total_soul_amount;
ALTER TABLE addresses RENAME COLUMN "BALANCE_DIRTY_BLOCK" TO balance_dirty_block;
ALTER TABLE addresses RENAME COLUMN "FIRST_TX_UNIX_SECONDS" TO first_tx_unix_seconds;

ALTER TABLE block_oracles RENAME COLUMN "ID" TO id;
ALTER TABLE block_oracles RENAME COLUMN "OracleId" TO oracle_id;
ALTER TABLE block_oracles RENAME COLUMN "BlockId" TO block_id;

ALTER TABLE blocks RENAME COLUMN "ID" TO id;
ALTER TABLE blocks RENAME COLUMN "HEIGHT" TO height;
ALTER TABLE blocks RENAME COLUMN "TIMESTAMP_UNIX_SECONDS" TO timestamp_unix_seconds;
ALTER TABLE blocks RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE blocks RENAME COLUMN "HASH" TO hash;
ALTER TABLE blocks RENAME COLUMN "PREVIOUS_HASH" TO previous_hash;
ALTER TABLE blocks RENAME COLUMN "PROTOCOL" TO protocol;
ALTER TABLE blocks RENAME COLUMN "ChainAddressId" TO chain_address_id;
ALTER TABLE blocks RENAME COLUMN "ValidatorAddressId" TO validator_address_id;
ALTER TABLE blocks RENAME COLUMN "REWARD" TO reward;

ALTER TABLE chains RENAME COLUMN "ID" TO id;
ALTER TABLE chains RENAME COLUMN "NAME" TO name;
ALTER TABLE chains RENAME COLUMN "CURRENT_HEIGHT" TO current_height;

ALTER TABLE contract_methods RENAME COLUMN "ID" TO id;
ALTER TABLE contract_methods RENAME COLUMN "ContractId" TO contract_id;
ALTER TABLE contract_methods RENAME COLUMN "METHODS" TO methods;
ALTER TABLE contract_methods RENAME COLUMN "TIMESTAMP_UNIX_SECONDS" TO timestamp_unix_seconds;

ALTER TABLE contracts RENAME COLUMN "ID" TO id;
ALTER TABLE contracts RENAME COLUMN "NAME" TO name;
ALTER TABLE contracts RENAME COLUMN "HASH" TO hash;
ALTER TABLE contracts RENAME COLUMN "SYMBOL" TO symbol;
ALTER TABLE contracts RENAME COLUMN "SCRIPT_RAW" TO script_raw;
ALTER TABLE contracts RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE contracts RENAME COLUMN "AddressId" TO address_id;
ALTER TABLE contracts RENAME COLUMN "ContractMethodId" TO contract_method_id;
ALTER TABLE contracts RENAME COLUMN "LAST_UPDATED_UNIX_SECONDS" TO last_updated_unix_seconds;
ALTER TABLE contracts RENAME COLUMN "TokenId" TO token_id;
ALTER TABLE contracts RENAME COLUMN "CreateEventId" TO create_event_id;

ALTER TABLE event_kinds RENAME COLUMN "ID" TO id;
ALTER TABLE event_kinds RENAME COLUMN "NAME" TO name;
ALTER TABLE event_kinds RENAME COLUMN "ChainId" TO chain_id;

ALTER TABLE events RENAME COLUMN "ID" TO id;
ALTER TABLE events RENAME COLUMN "DM_UNIX_SECONDS" TO dm_unix_seconds;
ALTER TABLE events RENAME COLUMN "TIMESTAMP_UNIX_SECONDS" TO timestamp_unix_seconds;
ALTER TABLE events RENAME COLUMN "DATE_UNIX_SECONDS" TO date_unix_seconds;
ALTER TABLE events RENAME COLUMN "INDEX" TO event_index;
ALTER TABLE events RENAME COLUMN "TOKEN_ID" TO token_id;
ALTER TABLE events RENAME COLUMN "BURNED" TO burned;
ALTER TABLE events RENAME COLUMN "NSFW" TO nsfw;
ALTER TABLE events RENAME COLUMN "BLACKLISTED" TO blacklisted;
ALTER TABLE events RENAME COLUMN "AddressId" TO address_id;
ALTER TABLE events RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE events RENAME COLUMN "ContractId" TO contract_id;
ALTER TABLE events RENAME COLUMN "TransactionId" TO transaction_id;
ALTER TABLE events RENAME COLUMN "EventKindId" TO event_kind_id;
ALTER TABLE events RENAME COLUMN "NftId" TO nft_id;
ALTER TABLE events RENAME COLUMN "TargetAddressId" TO target_address_id;
ALTER TABLE events RENAME COLUMN "PAYLOAD_FORMAT" TO payload_format;
ALTER TABLE events RENAME COLUMN "PAYLOAD_JSON" TO payload_json;
ALTER TABLE events RENAME COLUMN "RAW_DATA" TO raw_data;

ALTER TABLE externals RENAME COLUMN "ID" TO id;
ALTER TABLE externals RENAME COLUMN "PlatformId" TO platform_id;
ALTER TABLE externals RENAME COLUMN "TokenId" TO token_id;
ALTER TABLE externals RENAME COLUMN "HASH" TO hash;

ALTER TABLE fiat_exchange_rates RENAME COLUMN "ID" TO id;
ALTER TABLE fiat_exchange_rates RENAME COLUMN "SYMBOL" TO symbol;
ALTER TABLE fiat_exchange_rates RENAME COLUMN "USD_PRICE" TO usd_price;

ALTER TABLE global_variables RENAME COLUMN "ID" TO id;
ALTER TABLE global_variables RENAME COLUMN "NAME" TO name;
ALTER TABLE global_variables RENAME COLUMN "LONG_VALUE" TO long_value;
ALTER TABLE global_variables RENAME COLUMN "STRING_VALUE" TO string_value;

ALTER TABLE infusions RENAME COLUMN "ID" TO id;
ALTER TABLE infusions RENAME COLUMN "KEY" TO key;
ALTER TABLE infusions RENAME COLUMN "VALUE" TO value;
ALTER TABLE infusions RENAME COLUMN "TokenId" TO token_id;
ALTER TABLE infusions RENAME COLUMN "NftId" TO nft_id;

ALTER TABLE nft_ownerships RENAME COLUMN "ID" TO id;
ALTER TABLE nft_ownerships RENAME COLUMN "LAST_CHANGE_UNIX_SECONDS" TO last_change_unix_seconds;
ALTER TABLE nft_ownerships RENAME COLUMN "AMOUNT" TO amount;
ALTER TABLE nft_ownerships RENAME COLUMN "NftId" TO nft_id;
ALTER TABLE nft_ownerships RENAME COLUMN "AddressId" TO address_id;

ALTER TABLE nfts RENAME COLUMN "ID" TO id;
ALTER TABLE nfts RENAME COLUMN "DM_UNIX_SECONDS" TO dm_unix_seconds;
ALTER TABLE nfts RENAME COLUMN "TOKEN_ID" TO token_id;
ALTER TABLE nfts RENAME COLUMN "TOKEN_URI" TO token_uri;
ALTER TABLE nfts RENAME COLUMN "DESCRIPTION" TO description;
ALTER TABLE nfts RENAME COLUMN "NAME" TO name;
ALTER TABLE nfts RENAME COLUMN "ROM" TO rom;
ALTER TABLE nfts RENAME COLUMN "RAM" TO ram;
ALTER TABLE nfts RENAME COLUMN "IMAGE" TO image;
ALTER TABLE nfts RENAME COLUMN "VIDEO" TO video;
ALTER TABLE nfts RENAME COLUMN "INFO_URL" TO info_url;
ALTER TABLE nfts RENAME COLUMN "MINT_DATE_UNIX_SECONDS" TO mint_date_unix_seconds;
ALTER TABLE nfts RENAME COLUMN "MINT_NUMBER" TO mint_number;
ALTER TABLE nfts RENAME COLUMN "OFFCHAIN_API_RESPONSE" TO offchain_api_response;
ALTER TABLE nfts RENAME COLUMN "CHAIN_API_RESPONSE" TO chain_api_response;
ALTER TABLE nfts RENAME COLUMN "BURNED" TO burned;
ALTER TABLE nfts RENAME COLUMN "NSFW" TO nsfw;
ALTER TABLE nfts RENAME COLUMN "BLACKLISTED" TO blacklisted;
ALTER TABLE nfts RENAME COLUMN "METADATA_UPDATE" TO metadata_update;
ALTER TABLE nfts RENAME COLUMN "SeriesId" TO series_id;
ALTER TABLE nfts RENAME COLUMN "CreatorAddressId" TO creator_address_id;
ALTER TABLE nfts RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE nfts RENAME COLUMN "ContractId" TO contract_id;
ALTER TABLE nfts RENAME COLUMN "InfusedIntoId" TO infused_into_id;
ALTER TABLE nfts RENAME COLUMN "METADATA" TO metadata;

ALTER TABLE oracles RENAME COLUMN "ID" TO id;
ALTER TABLE oracles RENAME COLUMN "URL" TO url;
ALTER TABLE oracles RENAME COLUMN "CONTENT" TO content;

ALTER TABLE organization_addresses RENAME COLUMN "ID" TO id;
ALTER TABLE organization_addresses RENAME COLUMN "OrganizationId" TO organization_id;
ALTER TABLE organization_addresses RENAME COLUMN "AddressId" TO address_id;

ALTER TABLE organizations RENAME COLUMN "ID" TO id;
ALTER TABLE organizations RENAME COLUMN "ORGANIZATION_ID" TO organization_id;
ALTER TABLE organizations RENAME COLUMN "NAME" TO name;
ALTER TABLE organizations RENAME COLUMN "CreateEventId" TO create_event_id;
ALTER TABLE organizations RENAME COLUMN "ADDRESS" TO address;
ALTER TABLE organizations RENAME COLUMN "ADDRESS_NAME" TO address_name;

ALTER TABLE platform_interops RENAME COLUMN "ID" TO id;
ALTER TABLE platform_interops RENAME COLUMN "PlatformId" TO platform_id;
ALTER TABLE platform_interops RENAME COLUMN "LocalAddressId" TO local_address_id;
ALTER TABLE platform_interops RENAME COLUMN "EXTERNAL" TO external;

ALTER TABLE platform_tokens RENAME COLUMN "ID" TO id;
ALTER TABLE platform_tokens RENAME COLUMN "PlatformId" TO platform_id;
ALTER TABLE platform_tokens RENAME COLUMN "NAME" TO name;

ALTER TABLE platforms RENAME COLUMN "ID" TO id;
ALTER TABLE platforms RENAME COLUMN "NAME" TO name;
ALTER TABLE platforms RENAME COLUMN "CHAIN" TO chain;
ALTER TABLE platforms RENAME COLUMN "FUEL" TO fuel;
ALTER TABLE platforms RENAME COLUMN "HIDDEN" TO hidden;
ALTER TABLE platforms RENAME COLUMN "CreateEventId" TO create_event_id;

ALTER TABLE rejected_transaction_candidates RENAME COLUMN "ID" TO id;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "HASH" TO hash;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "NEXUS" TO nexus;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "CHAIN" TO chain;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "BLOCK_HEIGHT" TO block_height;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "BLOCK_HASH" TO block_hash;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "TIMESTAMP_UNIX_SECONDS" TO timestamp_unix_seconds;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "STATE" TO state;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "RESULT" TO result;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "DEBUG_COMMENT" TO debug_comment;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "PAYLOAD" TO payload;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "SCRIPT_RAW" TO script_raw;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "FEE_RAW" TO fee_raw;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "EXPIRATION" TO expiration;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "GAS_PRICE_RAW" TO gas_price_raw;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "GAS_LIMIT_RAW" TO gas_limit_raw;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "SENDER" TO sender;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "GAS_PAYER" TO gas_payer;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "GAS_TARGET" TO gas_target;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "CANONICAL_STATUS" TO canonical_status;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "RPC_RESPONSE_JSON" TO rpc_response_json;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "BLOCK_RESPONSE_JSON" TO block_response_json;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "CAPTURED_AT_UNIX_SECONDS" TO captured_at_unix_seconds;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "UPDATED_AT_UNIX_SECONDS" TO updated_at_unix_seconds;
ALTER TABLE rejected_transaction_candidates RENAME COLUMN "LAST_SEEN_AT_UNIX_SECONDS" TO last_seen_at_unix_seconds;

ALTER TABLE series_modes RENAME COLUMN "ID" TO id;
ALTER TABLE series_modes RENAME COLUMN "MODE_NAME" TO mode_name;

ALTER TABLE series RENAME COLUMN "ID" TO id;
ALTER TABLE series RENAME COLUMN "ContractId" TO contract_id;
ALTER TABLE series RENAME COLUMN "SERIES_ID" TO series_id;
ALTER TABLE series RENAME COLUMN "CURRENT_SUPPLY" TO current_supply;
ALTER TABLE series RENAME COLUMN "MAX_SUPPLY" TO max_supply;
ALTER TABLE series RENAME COLUMN "SeriesModeId" TO series_mode_id;
ALTER TABLE series RENAME COLUMN "NAME" TO name;
ALTER TABLE series RENAME COLUMN "DESCRIPTION" TO description;
ALTER TABLE series RENAME COLUMN "IMAGE" TO image;
ALTER TABLE series RENAME COLUMN "ROYALTIES" TO royalties;
ALTER TABLE series RENAME COLUMN "TYPE" TO type;
ALTER TABLE series RENAME COLUMN "ATTR_TYPE_1" TO attr_type_1;
ALTER TABLE series RENAME COLUMN "ATTR_VALUE_1" TO attr_value_1;
ALTER TABLE series RENAME COLUMN "ATTR_TYPE_2" TO attr_type_2;
ALTER TABLE series RENAME COLUMN "ATTR_VALUE_2" TO attr_value_2;
ALTER TABLE series RENAME COLUMN "ATTR_TYPE_3" TO attr_type_3;
ALTER TABLE series RENAME COLUMN "ATTR_VALUE_3" TO attr_value_3;
ALTER TABLE series RENAME COLUMN "HAS_LOCKED" TO has_locked;
ALTER TABLE series RENAME COLUMN "NSFW" TO nsfw;
ALTER TABLE series RENAME COLUMN "BLACKLISTED" TO blacklisted;
ALTER TABLE series RENAME COLUMN "DM_UNIX_SECONDS" TO dm_unix_seconds;
ALTER TABLE series RENAME COLUMN "CreatorAddressId" TO creator_address_id;
ALTER TABLE series RENAME COLUMN "METADATA" TO metadata;
ALTER TABLE series RENAME COLUMN "SERIES_CREATED_UNIX_SECONDS" TO series_created_unix_seconds;

ALTER TABLE signature_kinds RENAME COLUMN "ID" TO id;
ALTER TABLE signature_kinds RENAME COLUMN "NAME" TO name;

ALTER TABLE signatures RENAME COLUMN "ID" TO id;
ALTER TABLE signatures RENAME COLUMN "SignatureKindId" TO signature_kind_id;
ALTER TABLE signatures RENAME COLUMN "DATA" TO data;
ALTER TABLE signatures RENAME COLUMN "TransactionId" TO transaction_id;

ALTER TABLE soul_masters_monthlies RENAME COLUMN "ID" TO id;
ALTER TABLE soul_masters_monthlies RENAME COLUMN "MONTH_UNIX_SECONDS" TO month_unix_seconds;
ALTER TABLE soul_masters_monthlies RENAME COLUMN "MASTERS_COUNT" TO masters_count;
ALTER TABLE soul_masters_monthlies RENAME COLUMN "CAPTURED_AT_UNIX_SECONDS" TO captured_at_unix_seconds;
ALTER TABLE soul_masters_monthlies RENAME COLUMN "SOURCE" TO source;
ALTER TABLE soul_masters_monthlies RENAME COLUMN "ChainId" TO chain_id;

ALTER TABLE staking_progress_dailies RENAME COLUMN "ID" TO id;
ALTER TABLE staking_progress_dailies RENAME COLUMN "DATE_UNIX_SECONDS" TO date_unix_seconds;
ALTER TABLE staking_progress_dailies RENAME COLUMN "STAKED_SOUL_RAW" TO staked_soul_raw;
ALTER TABLE staking_progress_dailies RENAME COLUMN "SOUL_SUPPLY_RAW" TO soul_supply_raw;
ALTER TABLE staking_progress_dailies RENAME COLUMN "STAKERS_COUNT" TO stakers_count;
ALTER TABLE staking_progress_dailies RENAME COLUMN "MASTERS_COUNT" TO masters_count;
ALTER TABLE staking_progress_dailies RENAME COLUMN "STAKING_RATIO" TO staking_ratio;
ALTER TABLE staking_progress_dailies RENAME COLUMN "CAPTURED_AT_UNIX_SECONDS" TO captured_at_unix_seconds;
ALTER TABLE staking_progress_dailies RENAME COLUMN "SOURCE" TO source;
ALTER TABLE staking_progress_dailies RENAME COLUMN "ChainId" TO chain_id;

ALTER TABLE token_daily_prices RENAME COLUMN "ID" TO id;
ALTER TABLE token_daily_prices RENAME COLUMN "DATE_UNIX_SECONDS" TO date_unix_seconds;
ALTER TABLE token_daily_prices RENAME COLUMN "PRICE_USD" TO price_usd;
ALTER TABLE token_daily_prices RENAME COLUMN "TokenId" TO token_id;

ALTER TABLE token_logo_types RENAME COLUMN "ID" TO id;
ALTER TABLE token_logo_types RENAME COLUMN "NAME" TO name;

ALTER TABLE token_logos RENAME COLUMN "ID" TO id;
ALTER TABLE token_logos RENAME COLUMN "TokenId" TO token_id;
ALTER TABLE token_logos RENAME COLUMN "TokenLogoTypeId" TO token_logo_type_id;
ALTER TABLE token_logos RENAME COLUMN "URL" TO url;

ALTER TABLE tokens RENAME COLUMN "ID" TO id;
ALTER TABLE tokens RENAME COLUMN "SYMBOL" TO symbol;
ALTER TABLE tokens RENAME COLUMN "FUNGIBLE" TO fungible;
ALTER TABLE tokens RENAME COLUMN "TRANSFERABLE" TO transferable;
ALTER TABLE tokens RENAME COLUMN "FINITE" TO finite;
ALTER TABLE tokens RENAME COLUMN "DIVISIBLE" TO divisible;
ALTER TABLE tokens RENAME COLUMN "FUEL" TO fuel;
ALTER TABLE tokens RENAME COLUMN "STAKABLE" TO stakable;
ALTER TABLE tokens RENAME COLUMN "FIAT" TO fiat;
ALTER TABLE tokens RENAME COLUMN "SWAPPABLE" TO swappable;
ALTER TABLE tokens RENAME COLUMN "BURNABLE" TO burnable;
ALTER TABLE tokens RENAME COLUMN "DECIMALS" TO decimals;
ALTER TABLE tokens RENAME COLUMN "CURRENT_SUPPLY" TO current_supply;
ALTER TABLE tokens RENAME COLUMN "MAX_SUPPLY" TO max_supply;
ALTER TABLE tokens RENAME COLUMN "BURNED_SUPPLY" TO burned_supply;
ALTER TABLE tokens RENAME COLUMN "SCRIPT_RAW" TO script_raw;
ALTER TABLE tokens RENAME COLUMN "AddressId" TO address_id;
ALTER TABLE tokens RENAME COLUMN "OwnerId" TO owner_id;
ALTER TABLE tokens RENAME COLUMN "PRICE_USD" TO price_usd;
ALTER TABLE tokens RENAME COLUMN "PRICE_EUR" TO price_eur;
ALTER TABLE tokens RENAME COLUMN "PRICE_GBP" TO price_gbp;
ALTER TABLE tokens RENAME COLUMN "PRICE_JPY" TO price_jpy;
ALTER TABLE tokens RENAME COLUMN "PRICE_CAD" TO price_cad;
ALTER TABLE tokens RENAME COLUMN "PRICE_AUD" TO price_aud;
ALTER TABLE tokens RENAME COLUMN "PRICE_CNY" TO price_cny;
ALTER TABLE tokens RENAME COLUMN "PRICE_RUB" TO price_rub;
ALTER TABLE tokens RENAME COLUMN "ChainId" TO chain_id;
ALTER TABLE tokens RENAME COLUMN "ContractId" TO contract_id;
ALTER TABLE tokens RENAME COLUMN "CreateEventId" TO create_event_id;
ALTER TABLE tokens RENAME COLUMN "BURNED_SUPPLY_RAW" TO burned_supply_raw;
ALTER TABLE tokens RENAME COLUMN "CURRENT_SUPPLY_RAW" TO current_supply_raw;
ALTER TABLE tokens RENAME COLUMN "MAX_SUPPLY_RAW" TO max_supply_raw;
ALTER TABLE tokens RENAME COLUMN "MINTABLE" TO mintable;
ALTER TABLE tokens RENAME COLUMN "NAME" TO name;
ALTER TABLE tokens RENAME COLUMN "CARBON_TOKEN_SCHEMAS" TO carbon_token_schemas;

ALTER TABLE transaction_states RENAME COLUMN "ID" TO id;
ALTER TABLE transaction_states RENAME COLUMN "NAME" TO name;

ALTER TABLE transactions RENAME COLUMN "ID" TO id;
ALTER TABLE transactions RENAME COLUMN "HASH" TO hash;
ALTER TABLE transactions RENAME COLUMN "INDEX" TO tx_index;
ALTER TABLE transactions RENAME COLUMN "BlockId" TO block_id;
ALTER TABLE transactions RENAME COLUMN "TIMESTAMP_UNIX_SECONDS" TO timestamp_unix_seconds;
ALTER TABLE transactions RENAME COLUMN "PAYLOAD" TO payload;
ALTER TABLE transactions RENAME COLUMN "SCRIPT_RAW" TO script_raw;
ALTER TABLE transactions RENAME COLUMN "RESULT" TO result;
ALTER TABLE transactions RENAME COLUMN "FEE" TO fee;
ALTER TABLE transactions RENAME COLUMN "EXPIRATION" TO expiration;
ALTER TABLE transactions RENAME COLUMN "StateId" TO state_id;
ALTER TABLE transactions RENAME COLUMN "GAS_PRICE" TO gas_price;
ALTER TABLE transactions RENAME COLUMN "GAS_LIMIT" TO gas_limit;
ALTER TABLE transactions RENAME COLUMN "SenderId" TO sender_id;
ALTER TABLE transactions RENAME COLUMN "GasPayerId" TO gas_payer_id;
ALTER TABLE transactions RENAME COLUMN "GasTargetId" TO gas_target_id;
ALTER TABLE transactions RENAME COLUMN "FEE_RAW" TO fee_raw;
ALTER TABLE transactions RENAME COLUMN "GAS_LIMIT_RAW" TO gas_limit_raw;
ALTER TABLE transactions RENAME COLUMN "GAS_PRICE_RAW" TO gas_price_raw;
ALTER TABLE transactions RENAME COLUMN "CARBON_TX_DATA" TO carbon_tx_data;
ALTER TABLE transactions RENAME COLUMN "CARBON_TX_TYPE" TO carbon_tx_type;
ALTER TABLE transactions RENAME COLUMN "DEBUG_COMMENT" TO debug_comment;

ALTER TABLE ef_migrations_history RENAME COLUMN "MigrationId" TO migration_id;
ALTER TABLE ef_migrations_history RENAME COLUMN "ProductVersion" TO product_version;

ALTER SEQUENCE IF EXISTS "AddressBalances_ID_seq" RENAME TO address_balances_id_seq;
ALTER SEQUENCE IF EXISTS "AddressTransactions_ID_seq" RENAME TO address_transactions_id_seq;
ALTER SEQUENCE IF EXISTS "AddressValidatorKinds_ID_seq" RENAME TO address_validator_kinds_id_seq;
ALTER SEQUENCE IF EXISTS "Addresses_ID_seq" RENAME TO addresses_id_seq;
ALTER SEQUENCE IF EXISTS "BlockOracles_ID_seq" RENAME TO block_oracles_id_seq;
ALTER SEQUENCE IF EXISTS "Blocks_ID_seq" RENAME TO blocks_id_seq;
ALTER SEQUENCE IF EXISTS "Chains_ID_seq" RENAME TO chains_id_seq;
ALTER SEQUENCE IF EXISTS "ContractMethods_ID_seq" RENAME TO contract_methods_id_seq;
ALTER SEQUENCE IF EXISTS "Contracts_ID_seq" RENAME TO contracts_id_seq;
ALTER SEQUENCE IF EXISTS "EventKinds_ID_seq" RENAME TO event_kinds_id_seq;
ALTER SEQUENCE IF EXISTS "Events_ID_seq" RENAME TO events_id_seq;
ALTER SEQUENCE IF EXISTS "Externals_ID_seq" RENAME TO externals_id_seq;
ALTER SEQUENCE IF EXISTS "FiatExchangeRates_ID_seq" RENAME TO fiat_exchange_rates_id_seq;
ALTER SEQUENCE IF EXISTS "GlobalVariables_ID_seq" RENAME TO global_variables_id_seq;
ALTER SEQUENCE IF EXISTS "Infusions_ID_seq" RENAME TO infusions_id_seq;
ALTER SEQUENCE IF EXISTS "NftOwnerships_ID_seq" RENAME TO nft_ownerships_id_seq;
ALTER SEQUENCE IF EXISTS "Nfts_ID_seq" RENAME TO nfts_id_seq;
ALTER SEQUENCE IF EXISTS "Oracles_ID_seq" RENAME TO oracles_id_seq;
ALTER SEQUENCE IF EXISTS "OrganizationAddresses_ID_seq" RENAME TO organization_addresses_id_seq;
ALTER SEQUENCE IF EXISTS "Organizations_ID_seq" RENAME TO organizations_id_seq;
ALTER SEQUENCE IF EXISTS "PlatformInterops_ID_seq" RENAME TO platform_interops_id_seq;
ALTER SEQUENCE IF EXISTS "PlatformTokens_ID_seq" RENAME TO platform_tokens_id_seq;
ALTER SEQUENCE IF EXISTS "Platforms_ID_seq" RENAME TO platforms_id_seq;
ALTER SEQUENCE IF EXISTS "RejectedTransactionCandidates_ID_seq" RENAME TO rejected_transaction_candidates_id_seq;
ALTER SEQUENCE IF EXISTS "SeriesModes_ID_seq" RENAME TO series_modes_id_seq;
ALTER SEQUENCE IF EXISTS "Serieses_ID_seq" RENAME TO series_id_seq;
ALTER SEQUENCE IF EXISTS "SignatureKinds_ID_seq" RENAME TO signature_kinds_id_seq;
ALTER SEQUENCE IF EXISTS "Signatures_ID_seq" RENAME TO signatures_id_seq;
ALTER SEQUENCE IF EXISTS "SoulMastersMonthlies_ID_seq" RENAME TO soul_masters_monthlies_id_seq;
ALTER SEQUENCE IF EXISTS "StakingProgressDailies_ID_seq" RENAME TO staking_progress_dailies_id_seq;
ALTER SEQUENCE IF EXISTS "TokenDailyPrices_ID_seq" RENAME TO token_daily_prices_id_seq;
ALTER SEQUENCE IF EXISTS "TokenLogoTypes_ID_seq" RENAME TO token_logo_types_id_seq;
ALTER SEQUENCE IF EXISTS "TokenLogos_ID_seq" RENAME TO token_logos_id_seq;
ALTER SEQUENCE IF EXISTS "Tokens_ID_seq" RENAME TO tokens_id_seq;
ALTER SEQUENCE IF EXISTS "TransactionStates_ID_seq" RENAME TO transaction_states_id_seq;
ALTER SEQUENCE IF EXISTS "Transactions_ID_seq" RENAME TO transactions_id_seq;
