-- Stop dimension deletes from cascading into recorded chain facts. Until now a single DELETE on
-- an addresses/chains/contracts/event_kinds/transaction_states/signature_kinds row would silently
-- erase every fact referencing it (blocks -> transactions -> events -> ...). Facts must refuse
-- (NO ACTION) so a wrong delete is an error, not data loss. Deliberately KEPT as CASCADE:
-- the fact chain itself (blocks -> transactions -> events/signatures/address_transactions), used
-- by height rollbacks, and the rebuildable derived caches (address_balances, nft_ownerships,
-- organization_addresses, token_daily_prices, contract_methods), where a parent delete is cleanup,
-- not history loss. Re-adding a foreign key revalidates it; the data satisfied the identical
-- constraint a statement earlier, so this is a scan, never a failure.

ALTER TABLE addresses
    DROP CONSTRAINT "FK_Addresses_Chains_ChainId",
    ADD CONSTRAINT "FK_Addresses_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id);

ALTER TABLE blocks
    DROP CONSTRAINT "FK_Blocks_Addresses_ChainAddressId",
    ADD CONSTRAINT "FK_Blocks_Addresses_ChainAddressId"
        FOREIGN KEY (chain_address_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Blocks_Addresses_ValidatorAddressId",
    ADD CONSTRAINT "FK_Blocks_Addresses_ValidatorAddressId"
        FOREIGN KEY (validator_address_id) REFERENCES addresses(id),
    DROP CONSTRAINT blocks_producer_address_id_fkey,
    ADD CONSTRAINT blocks_producer_address_id_fkey
        FOREIGN KEY (producer_address_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Blocks_Chains_ChainId",
    ADD CONSTRAINT "FK_Blocks_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id);

ALTER TABLE transactions
    DROP CONSTRAINT "FK_Transactions_Addresses_SenderId",
    ADD CONSTRAINT "FK_Transactions_Addresses_SenderId"
        FOREIGN KEY (sender_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Transactions_Addresses_GasPayerId",
    ADD CONSTRAINT "FK_Transactions_Addresses_GasPayerId"
        FOREIGN KEY (gas_payer_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Transactions_Addresses_GasTargetId",
    ADD CONSTRAINT "FK_Transactions_Addresses_GasTargetId"
        FOREIGN KEY (gas_target_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Transactions_TransactionStates_StateId",
    ADD CONSTRAINT "FK_Transactions_TransactionStates_StateId"
        FOREIGN KEY (state_id) REFERENCES transaction_states(id);

ALTER TABLE events
    DROP CONSTRAINT "FK_Events_Addresses_AddressId",
    ADD CONSTRAINT "FK_Events_Addresses_AddressId"
        FOREIGN KEY (address_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Events_Addresses_TargetAddressId",
    ADD CONSTRAINT "FK_Events_Addresses_TargetAddressId"
        FOREIGN KEY (target_address_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Events_Chains_ChainId",
    ADD CONSTRAINT "FK_Events_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id),
    DROP CONSTRAINT "FK_Events_Contracts_ContractId",
    ADD CONSTRAINT "FK_Events_Contracts_ContractId"
        FOREIGN KEY (contract_id) REFERENCES contracts(id),
    DROP CONSTRAINT "FK_Events_EventKinds_EventKindId",
    ADD CONSTRAINT "FK_Events_EventKinds_EventKindId"
        FOREIGN KEY (event_kind_id) REFERENCES event_kinds(id);

ALTER TABLE signatures
    DROP CONSTRAINT "FK_Signatures_SignatureKinds_SignatureKindId",
    ADD CONSTRAINT "FK_Signatures_SignatureKinds_SignatureKindId"
        FOREIGN KEY (signature_kind_id) REFERENCES signature_kinds(id);

ALTER TABLE contracts
    DROP CONSTRAINT "FK_Contracts_Chains_ChainId",
    ADD CONSTRAINT "FK_Contracts_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id);

ALTER TABLE nfts
    DROP CONSTRAINT "FK_Nfts_Chains_ChainId",
    ADD CONSTRAINT "FK_Nfts_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id),
    DROP CONSTRAINT "FK_Nfts_Contracts_ContractId",
    ADD CONSTRAINT "FK_Nfts_Contracts_ContractId"
        FOREIGN KEY (contract_id) REFERENCES contracts(id);

ALTER TABLE series
    DROP CONSTRAINT "FK_Serieses_Contracts_ContractId",
    ADD CONSTRAINT "FK_Serieses_Contracts_ContractId"
        FOREIGN KEY (contract_id) REFERENCES contracts(id);

ALTER TABLE infusions
    DROP CONSTRAINT "FK_Infusions_Nfts_NftId",
    ADD CONSTRAINT "FK_Infusions_Nfts_NftId"
        FOREIGN KEY (nft_id) REFERENCES nfts(id);

ALTER TABLE tokens
    DROP CONSTRAINT "FK_Tokens_Addresses_AddressId",
    ADD CONSTRAINT "FK_Tokens_Addresses_AddressId"
        FOREIGN KEY (address_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Tokens_Addresses_OwnerId",
    ADD CONSTRAINT "FK_Tokens_Addresses_OwnerId"
        FOREIGN KEY (owner_id) REFERENCES addresses(id),
    DROP CONSTRAINT "FK_Tokens_Chains_ChainId",
    ADD CONSTRAINT "FK_Tokens_Chains_ChainId"
        FOREIGN KEY (chain_id) REFERENCES chains(id);
