//! Read-model mappers, value/param helpers, and JSON/script helpers for the
//! HTTP API. The crate root holds the DTOs, ApiError, router, and cursors these
//! depend on.
use crate::*;

pub(crate) fn trim_page_rows(
    mut rows: Vec<PgRow>,
    limit: i64,
    cursor_kind: &'static str,
) -> Result<(Vec<PgRow>, Option<String>), ApiError> {
    let limit = usize::try_from(limit)
        .map_err(|_| ApiError::BadRequest("limit is outside usize range".to_owned()))?;
    let has_next_page = rows.len() > limit;
    if has_next_page {
        rows.truncate(limit);
    }

    let next_cursor = if has_next_page {
        rows.last()
            .map(|row| row_cursor(row, cursor_kind))
            .transpose()?
    } else {
        None
    };

    Ok((rows, next_cursor))
}

pub(crate) fn row_cursor(row: &PgRow, cursor_kind: &'static str) -> Result<String, ApiError> {
    // The list queries select the active sort column's value as `cursor_sort_value`
    // (cast to bigint) and the seek tie-break id as `cursor_id`/`id`, so the cursor
    // always keys on the same column the query orders by.
    let sort_value = row.try_get::<i64, _>("cursor_sort_value")?;
    let id = row
        .try_get::<i32, _>("cursor_id")
        .or_else(|_| row.try_get::<i32, _>("id"))?;
    Ok(PageCursor { sort_value, id }.encode(cursor_kind))
}

pub(crate) fn block_from_row(row: &PgRow) -> BlockResponse {
    BlockResponse {
        height: row.get::<i64, _>("height").to_string(),
        hash: row.get("hash"),
        previous_hash: row.get("previous_hash"),
        protocol: row.get("protocol"),
        chain_address: row.get("chain_address"),
        validator_address: row.get("validator_address"),
        date: Some(row.get::<i64, _>("timestamp_unix_seconds").to_string()),
        reward: row.get("reward"),
        transaction_count: row.get("transaction_count"),
        transactions: None,
    }
}

pub(crate) fn token_from_row(
    row: &PgRow,
    with_price: bool,
    with_logo: bool,
) -> Result<TokenResponse, ApiError> {
    let max_supply_raw = row.get::<Option<String>, _>("max_supply_raw");
    Ok(TokenResponse {
        name: row.get("name"),
        symbol: row.get("symbol"),
        fungible: row.get("fungible"),
        transferable: row.get("transferable"),
        finite: is_positive_raw(max_supply_raw.as_deref()),
        divisible: row.get("divisible"),
        fuel: row.get("fuel"),
        stakable: row.get("stakable"),
        fiat: row.get("fiat"),
        swappable: row.get("swappable"),
        burnable: row.get("burnable"),
        mintable: row.get("mintable"),
        decimals: row.get("decimals"),
        current_supply: row.get("current_supply"),
        current_supply_raw: row.get("current_supply_raw"),
        max_supply: row.get("max_supply"),
        max_supply_raw,
        burned_supply: row.get("burned_supply"),
        burned_supply_raw: row.get("burned_supply_raw"),
        script_raw: row.get("script_raw"),
        price: with_price.then(|| PriceResponse {
            usd: nonzero_f64(row.get("price_usd")),
            eur: nonzero_f64(row.get("price_eur")),
            gbp: nonzero_f64(row.get("price_gbp")),
            jpy: nonzero_f64(row.get("price_jpy")),
            cad: nonzero_f64(row.get("price_cad")),
            aud: nonzero_f64(row.get("price_aud")),
            cny: nonzero_f64(row.get("price_cny")),
            rub: nonzero_f64(row.get("price_rub")),
        }),
        token_logos: if with_logo {
            json_vec(row.get("token_logos_json"))?.or(Some(Vec::new()))
        } else {
            None
        },
    })
}

pub(crate) fn token_from_value(value: Option<Value>) -> Result<Option<TokenResponse>, ApiError> {
    json_opt(value)
}

pub(crate) fn address_from_row(
    row: &PgRow,
    with_storage: bool,
    with_stakes: bool,
    with_balance: bool,
) -> Result<AddressResponse, ApiError> {
    let storage_available = row.get::<i64, _>("storage_available");
    let storage = (with_storage && storage_available > 0).then(|| AddressStorageResponse {
        available: storage_available,
        used: row.get("storage_used"),
        avatar: row.get("avatar"),
    });
    let stake = row.get::<Option<String>, _>("staked_amount");
    let unclaimed = row.get::<Option<String>, _>("unclaimed_amount");
    let has_stakes = stake.as_deref().is_some_and(|value| !value.is_empty())
        || unclaimed.as_deref().is_some_and(|value| !value.is_empty());
    let stakes = (with_stakes && has_stakes).then(|| AddressStakesResponse {
        amount: stake.clone(),
        amount_raw: row.get("staked_amount_raw"),
        time: row.get("stake_timestamp"),
        unclaimed: unclaimed.clone(),
        unclaimed_raw: row.get("unclaimed_amount_raw"),
    });
    Ok(AddressResponse {
        address: row.get("address"),
        address_name: row.get("address_name"),
        validator_kind: row.get("validator_kind"),
        stake,
        stake_raw: row.get("staked_amount_raw"),
        unclaimed,
        unclaimed_raw: row.get("unclaimed_amount_raw"),
        storage,
        stakes,
        balances: if with_balance {
            json_vec(row.get("balances_json"))?.or(Some(Vec::new()))
        } else {
            None
        },
    })
}

pub(crate) fn contract_from_row(row: &PgRow) -> Result<ContractResponse, ApiError> {
    let address = row
        .get::<Option<String>, _>("address")
        .map(|address| AddressResponse {
            address: Some(address),
            address_name: row.get("address_name"),
            validator_kind: None,
            stake: None,
            stake_raw: None,
            unclaimed: None,
            unclaimed_raw: None,
            storage: None,
            stakes: None,
            balances: None,
        });
    Ok(ContractResponse {
        name: row.get("name"),
        hash: row.get("hash"),
        symbol: row.get("symbol"),
        compiler: None,
        create_date: None,
        r#type: None,
        address,
        script_raw: row.get("script_raw"),
        token: token_from_value(row.get("token_json"))?,
        methods: row.get("methods_json"),
    })
}

pub(crate) fn contract_from_parts(
    name: Option<String>,
    hash: Option<String>,
    symbol: Option<String>,
) -> Option<ContractResponse> {
    hash.as_ref()?;
    Some(ContractResponse {
        name,
        hash,
        symbol,
        compiler: None,
        create_date: None,
        r#type: None,
        address: None,
        script_raw: None,
        token: None,
        methods: None,
    })
}

pub(crate) fn nft_from_row(row: &PgRow) -> Result<NftResponse, ApiError> {
    let contract = contract_from_parts(
        row.get("contract_name"),
        row.get("contract_hash"),
        row.get("contract_symbol"),
    );
    let series = row
        .get::<Option<i32>, _>("series_db_id")
        .map(|id| SeriesResponse {
            id,
            series_id: row.get("series_id"),
            creator: row.get("series_creator"),
            chain: row.get("series_chain"),
            contract: row.get("series_contract_hash"),
            symbol: row.get("series_symbol"),
            created_unix_seconds: row.get("series_created_unix_seconds"),
            current_supply: row.get("series_current_supply"),
            max_supply: row.get("series_max_supply"),
            mode_name: row.get("series_mode_name"),
            name: row.get("series_name"),
            description: row.get("series_description"),
            image: row.get("series_image"),
            royalties: row.get("series_royalties"),
            r#type: row.get("series_type"),
            attr_type_1: row.get("attr_type_1"),
            attr_value_1: row.get("attr_value_1"),
            attr_type_2: row.get("attr_type_2"),
            attr_value_2: row.get("attr_value_2"),
            attr_type_3: row.get("attr_type_3"),
            attr_value_3: row.get("attr_value_3"),
            metadata: row.get("series_metadata"),
        });
    let infused_into = row
        .get::<Option<String>, _>("infused_into_token_id")
        .map(|token_id| InfusedIntoResponse {
            token_id: Some(token_id),
            chain: row.get("infused_into_chain"),
            contract: contract_from_parts(
                row.get("infused_contract_name"),
                row.get("infused_contract_hash"),
                row.get("infused_contract_symbol"),
            ),
        });
    Ok(NftResponse {
        token_id: row.get("token_id"),
        chain: row.get("chain_name"),
        symbol: row.get("contract_symbol"),
        creator_address: row.get("creator_address"),
        creator_onchain_name: row.get("creator_onchain_name"),
        owners: json_vec(row.get("owners_json"))?.or(Some(Vec::new())),
        contract,
        nft_metadata: Some(NftMetadataResponse {
            description: row.get("description"),
            name: row.get("name"),
            image_url: row.get("image"),
            video_url: row.get("video"),
            info_url: row.get("info_url"),
            rom: row.get("rom"),
            ram: row.get("ram"),
            mint_date: Some(row.get::<i64, _>("mint_date_unix_seconds").to_string()),
            mint_number: Some(row.get::<i32, _>("mint_number").to_string()),
            metadata: row.get("metadata"),
        }),
        series,
        infusion: json_vec(row.get("infusion_json"))?.or(Some(Vec::new())),
        infused_into,
    })
}

pub(crate) fn series_from_row(row: &PgRow) -> SeriesResponse {
    SeriesResponse {
        id: row.get("id"),
        series_id: row.get("series_id"),
        creator: row.get("creator"),
        chain: row.get("chain_name"),
        contract: row.get("contract_hash"),
        symbol: row.get("symbol"),
        created_unix_seconds: row.get("series_created_unix_seconds"),
        current_supply: row.get("current_supply"),
        max_supply: row.get("max_supply"),
        mode_name: row.get("mode_name"),
        name: row.get("name"),
        description: row.get("description"),
        image: row.get("image"),
        royalties: row.get("royalties"),
        r#type: row.get("type"),
        attr_type_1: row.get("attr_type_1"),
        attr_value_1: row.get("attr_value_1"),
        attr_type_2: row.get("attr_type_2"),
        attr_value_2: row.get("attr_value_2"),
        attr_type_3: row.get("attr_type_3"),
        attr_value_3: row.get("attr_value_3"),
        metadata: row.get("metadata"),
    }
}

pub(crate) fn organization_from_row(
    row: &OrganizationRow,
    with_address: bool,
) -> OrganizationResponse {
    OrganizationResponse {
        id: row.organization_id.clone(),
        name: row.name.clone(),
        size: row.size,
        address: (with_address && row.address.is_some()).then(|| AddressResponse {
            address: row.address.clone(),
            address_name: row.address_name.clone(),
            validator_kind: None,
            stake: None,
            stake_raw: None,
            unclaimed: None,
            unclaimed_raw: None,
            storage: None,
            stakes: None,
            balances: None,
        }),
    }
}

pub(crate) fn platform_from_row(row: &PgRow) -> PlatformResponse {
    let create_event = row
        .get::<Option<i32>, _>("create_event_id")
        .map(|event_id| EventResponse {
            event_id,
            event_index: row
                .get::<Option<i32>, _>("create_event_index")
                .unwrap_or_default(),
            event_source: "legacy".to_owned(),
            chain: row
                .get::<Option<String>, _>("create_chain")
                .unwrap_or_else(|| "main".to_owned()),
            date: row
                .get::<Option<i64>, _>("create_timestamp_unix_seconds")
                .map(|value| value.to_string())
                .unwrap_or_default(),
            block_hash: row
                .get::<Option<String>, _>("create_block_hash")
                .unwrap_or_default(),
            transaction_hash: row
                .get::<Option<String>, _>("create_transaction_hash")
                .unwrap_or_default(),
            event_kind: row
                .get::<Option<String>, _>("create_event_kind")
                .unwrap_or_default(),
            event_name: row.get("create_event_kind"),
            address: row.get("create_address"),
            address_name: row.get("create_address_name"),
            contract: row
                .get::<Option<String>, _>("create_contract_hash")
                .map(|hash| ContractRefResponse {
                    hash,
                    name: row.get("create_contract_name"),
                    symbol: row.get("create_contract_symbol"),
                }),
            token_id: row.get("create_token_id"),
            payload_json: row
                .get::<Option<Value>, _>("create_payload_json")
                .and_then(|value| serde_json::to_string(&value).ok()),
            raw_data: row.get("create_raw_data"),
            nft_metadata: None,
            series: None,
            event_data: EventDataFields::default(),
        });

    PlatformResponse {
        name: row.get("name"),
        chain: row.get("chain"),
        fuel: row.get("fuel"),
        externals: row.get("externals_json"),
        platform_interops: row.get("platform_interops_json"),
        platform_tokens: row.get("platform_tokens_json"),
        create_event,
    }
}

pub(crate) fn transaction_from_row(row: &PgRow) -> TransactionResponse {
    TransactionResponse {
        transaction_id: row.get::<i32, _>("id").to_string(),
        hash: row.get("hash"),
        block_hash: row.get("block_hash"),
        block_height: row.get::<i64, _>("block_height").to_string(),
        chain: row.get("chain_name"),
        previous_hash: row.get("previous_hash"),
        next_hash: row.get("next_hash"),
        index: row.get("tx_index"),
        date: row.get::<i64, _>("timestamp_unix_seconds").to_string(),
        fee: row.get("fee"),
        fee_raw: row.get("fee_raw"),
        script_raw: row.get("script_raw"),
        result: row.get("result"),
        debug_comment: row.get("debug_comment"),
        payload: row.get("payload"),
        expiration: row
            .get::<Option<i64>, _>("expiration_unix_seconds")
            .map(|value| value.to_string()),
        gas_price: row.get("gas_price"),
        gas_price_raw: row.get("gas_price_raw"),
        gas_limit: row.get("gas_limit"),
        gas_limit_raw: row.get("gas_limit_raw"),
        state: row.get("state"),
        sender: address_ref(
            row.get::<Option<String>, _>("sender_address"),
            row.get::<Option<String>, _>("sender_address_name"),
        ),
        gas_payer: address_ref(
            row.get::<Option<String>, _>("gas_payer_address"),
            row.get::<Option<String>, _>("gas_payer_address_name"),
        ),
        gas_target: address_ref(
            row.get::<Option<String>, _>("gas_target_address"),
            row.get::<Option<String>, _>("gas_target_address_name"),
        ),
        carbon_tx_type: row.get::<Option<i16>, _>("carbon_tx_type").map(i32::from),
        carbon_tx_data: row.get("carbon_tx_data"),
        events: None,
    }
}

pub(crate) fn transaction_occurrence_from_row(row: &PgRow) -> TransactionOccurrenceResponse {
    TransactionOccurrenceResponse {
        transaction_id: row.get::<i32, _>("id").to_string(),
        hash: row.get("hash"),
        block_hash: row.get("block_hash"),
        block_height: row.get::<i64, _>("block_height").to_string(),
        chain: row.get("chain_name"),
        index: row.get("tx_index"),
        date: row.get::<i64, _>("timestamp_unix_seconds").to_string(),
    }
}

pub(crate) async fn events_from_rows(
    pool: &PgPool,
    rows: &[PgRow],
    with_event_data: bool,
    with_metadata: bool,
    with_series: bool,
) -> Result<Vec<EventResponse>, ApiError> {
    let token_symbols = collect_event_token_symbols(rows);
    let tokens = load_event_tokens_by_symbols(pool, token_symbols).await?;
    rows.iter()
        .map(|row| event_from_row(row, &tokens, with_event_data, with_metadata, with_series))
        .collect::<Result<Vec<_>, _>>()
}

pub(crate) fn collect_event_token_symbols(rows: &[PgRow]) -> HashSet<String> {
    let mut symbols = HashSet::new();
    for row in rows {
        if let Some(symbol) = row.get::<Option<String>, _>("contract_symbol") {
            symbols.insert(symbol.to_uppercase());
        }
        let payload = row.get::<Option<Value>, _>("payload_json");
        if let Some(Value::Object(payload)) = payload {
            collect_token_symbol_from_payload_key(&payload, "token_event", "token", &mut symbols);
            collect_token_symbol_from_payload_key(
                &payload,
                "token_create_event",
                "token",
                &mut symbols,
            );
            collect_token_symbol_from_payload_key(
                &payload,
                "token_series_event",
                "token",
                &mut symbols,
            );
            collect_token_symbol_from_payload_key(
                &payload,
                "infusion_event",
                "base_token",
                &mut symbols,
            );
            collect_token_symbol_from_payload_key(
                &payload,
                "infusion_event",
                "infused_token",
                &mut symbols,
            );
            collect_token_symbol_from_payload_key(
                &payload,
                "market_event",
                "base_token",
                &mut symbols,
            );
            collect_token_symbol_from_payload_key(
                &payload,
                "market_event",
                "quote_token",
                &mut symbols,
            );
        }
    }
    symbols
}

pub(crate) fn collect_token_symbol_from_payload_key(
    payload: &serde_json::Map<String, Value>,
    event_key: &str,
    token_key: &str,
    symbols: &mut HashSet<String>,
) {
    payload
        .get(event_key)
        .and_then(|value| value.get(token_key))
        .and_then(json_scalar_to_string)
        .map(|symbol| symbols.insert(symbol.to_uppercase()));
}

pub(crate) async fn load_event_tokens_by_symbols(
    pool: &PgPool,
    symbols: HashSet<String>,
) -> Result<HashMap<String, Value>, ApiError> {
    if symbols.is_empty() {
        return Ok(HashMap::new());
    }
    let symbols = symbols.into_iter().collect::<Vec<_>>();
    let rows = list_event_tokens_by_symbols(pool, &symbols).await?;

    let mut tokens = HashMap::new();
    for row in rows {
        let symbol = row.get::<String, _>("symbol");
        let max_supply_raw = row.get::<Option<String>, _>("max_supply_raw");
        tokens.insert(
            symbol.to_uppercase(),
            serde_json::json!({
                "symbol": symbol,
                "fungible": row.get::<bool, _>("fungible"),
                "transferable": row.get::<bool, _>("transferable"),
                "finite": is_positive_raw(max_supply_raw.as_deref()),
                "divisible": row.get::<bool, _>("divisible"),
                "fuel": row.get::<bool, _>("fuel"),
                "stakable": row.get::<bool, _>("stakable"),
                "fiat": row.get::<bool, _>("fiat"),
                "swappable": row.get::<bool, _>("swappable"),
                "burnable": row.get::<bool, _>("burnable"),
                "mintable": row.get::<bool, _>("mintable"),
                "decimals": row.get::<i32, _>("decimals"),
            }),
        );
    }
    Ok(tokens)
}

pub(crate) fn event_from_row(
    row: &PgRow,
    tokens: &HashMap<String, Value>,
    with_event_data: bool,
    with_metadata: bool,
    with_series: bool,
) -> Result<EventResponse, ApiError> {
    let contract_hash = row
        .get::<Option<String>, _>("contract_hash")
        .or_else(|| row.get::<Option<String>, _>("raw_contract"));
    let event_kind = row.get::<String, _>("event_kind");
    let payload_value = row.get::<Option<Value>, _>("payload_json");
    let contract_symbol = row.get::<Option<String>, _>("contract_symbol");
    let event_data = if with_event_data {
        build_event_data_fields(
            &event_kind,
            payload_value.as_ref(),
            contract_symbol.as_deref(),
            tokens,
        )
    } else {
        EventDataFields::default()
    };
    let payload_json = if with_event_data {
        payload_value
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                ApiError::Internal(format!("stored event payload is invalid: {error}"))
            })?
    } else {
        None
    };

    Ok(EventResponse {
        event_id: row.get("id"),
        event_index: row.get("event_index"),
        event_source: row.get("event_source"),
        chain: row.get("chain_name"),
        date: row.get::<i64, _>("timestamp_unix_seconds").to_string(),
        block_hash: row.get("block_hash"),
        transaction_hash: row.get("transaction_hash"),
        event_kind,
        event_name: row.get("event_name"),
        address: row.get("address"),
        address_name: row.get("address_name"),
        contract: contract_hash.map(|hash| ContractRefResponse {
            hash,
            name: row.get("contract_name"),
            symbol: row.get("contract_symbol"),
        }),
        token_id: row.get("token_id"),
        payload_json,
        raw_data: row.get("raw_data"),
        nft_metadata: if with_metadata {
            json_opt(row.get("nft_metadata_json"))?
        } else {
            None
        },
        series: if with_series {
            json_opt(row.get("series_json"))?
        } else {
            None
        },
        event_data,
    })
}

pub(crate) fn build_event_data_fields(
    event_kind: &str,
    payload: Option<&Value>,
    contract_symbol: Option<&str>,
    tokens: &HashMap<String, Value>,
) -> EventDataFields {
    let mut fields = EventDataFields::default();
    let Some(payload) = payload.and_then(Value::as_object) else {
        return fields;
    };

    if let Some(value) = payload.get("address_event") {
        fields.address_event = Some(enrich_address_event(value));
    }
    if let Some(value) = payload.get("chain_event") {
        fields.chain_event = Some(enrich_chain_event(value));
    }
    if let Some(value) = payload.get("gas_event") {
        fields.gas_event = Some(enrich_gas_event(value));
    }
    if let Some(value) = payload.get("governance_gas_config_event") {
        fields.governance_gas_config_event = Some(value.clone());
    }
    if let Some(value) = payload.get("governance_chain_config_event") {
        fields.governance_chain_config_event = Some(value.clone());
    }
    if let Some(value) = payload.get("special_resolution_event") {
        fields.special_resolution_event = Some(value.clone());
    }
    if let Some(value) = payload.get("hash_event") {
        fields.hash_event = Some(value.clone());
    }
    if let Some(value) = payload.get("infusion_event") {
        fields.infusion_event = Some(enrich_multi_token_event(
            value,
            tokens,
            &["base_token", "infused_token"],
        ));
    }
    if let Some(value) = payload.get("market_event") {
        fields.market_event = Some(enrich_multi_token_event(
            value,
            tokens,
            &["base_token", "quote_token"],
        ));
    }
    if let Some(value) = payload.get("organization_event") {
        fields.organization_event = Some(value.clone());
    }
    if let Some(value) = payload.get("sale_event") {
        fields.sale_event = Some(value.clone());
    }
    if let Some(value) = payload.get("string_event") {
        fields.string_event = Some(value.clone());
    }
    if let Some(value) = payload.get("token_create_event") {
        fields.token_create_event = Some(enrich_single_token_event(value, tokens, "token"));
    }
    if let Some(value) = payload.get("token_event") {
        fields.token_event = Some(enrich_token_event(value, contract_symbol, tokens));
    }
    if let Some(value) = payload.get("token_series_event") {
        fields.token_series_event = Some(enrich_token_series_event(value, tokens));
    }
    if let Some(value) = payload.get("transaction_settle_event") {
        fields.transaction_settle_event = Some(value.clone());
    }

    if event_data_key(event_kind).is_some()
        && !has_any_event_data(&fields)
        && payload.contains_key("raw_data")
    {
        fields.unknown_event = Some(serde_json::json!({
            "payload_json": Value::Object(payload.clone()),
        }));
    }

    fields
}

pub(crate) fn has_any_event_data(fields: &EventDataFields) -> bool {
    fields.address_event.is_some()
        || fields.chain_event.is_some()
        || fields.gas_event.is_some()
        || fields.governance_gas_config_event.is_some()
        || fields.governance_chain_config_event.is_some()
        || fields.special_resolution_event.is_some()
        || fields.hash_event.is_some()
        || fields.infusion_event.is_some()
        || fields.market_event.is_some()
        || fields.organization_event.is_some()
        || fields.sale_event.is_some()
        || fields.string_event.is_some()
        || fields.token_create_event.is_some()
        || fields.token_event.is_some()
        || fields.token_series_event.is_some()
        || fields.transaction_settle_event.is_some()
}

pub(crate) fn event_data_key(event_kind: &str) -> Option<&'static str> {
    match event_kind {
        "AddressRegister" | "ChainCreate" | "ContractDeploy" | "ContractUpgrade" | "Custom"
        | "LeaderboardCreate" | "OrganizationCreate" | "PlatformCreate" | "ValidatorSwitch" => {
            Some("string_event")
        }
        "ChainSwap" => Some("transaction_settle_event"),
        "Crowdsale" => Some("sale_event"),
        "CrownRewards" | "TokenBurn" | "TokenClaim" | "TokenMint" | "TokenReceive"
        | "TokenSend" | "TokenStake" => Some("token_event"),
        "FileCreate" | "FileDelete" => Some("hash_event"),
        "GasEscrow" | "GasPayment" => Some("gas_event"),
        "GovernanceSetChainConfig" => Some("governance_chain_config_event"),
        "GovernanceSetGasConfig" => Some("governance_gas_config_event"),
        "Infusion" => Some("infusion_event"),
        "OrderBid" | "OrderCancelled" | "OrderClosed" | "OrderCreated" | "OrderFilled" => {
            Some("market_event")
        }
        "OrganizationAdd" | "OrganizationRemove" => Some("organization_event"),
        "SpecialResolution" => Some("special_resolution_event"),
        "TokenCreate" => Some("token_create_event"),
        "TokenSeriesCreate" => Some("token_series_event"),
        "ValidatorElect" | "ValidatorPropose" => Some("address_event"),
        "ValueCreate" | "ValueUpdate" => Some("chain_event"),
        _ => None,
    }
}

pub(crate) fn enrich_token_event(
    value: &Value,
    contract_symbol: Option<&str>,
    tokens: &HashMap<String, Value>,
) -> Value {
    let mut object = value.as_object().cloned().unwrap_or_default();
    let symbol = object
        .get("token")
        .and_then(json_scalar_to_string)
        .or_else(|| contract_symbol.map(str::to_owned));
    if let Some(symbol) = symbol {
        let normalized = symbol.to_uppercase();
        if let Some(token) = tokens.get(&normalized) {
            object.insert("token".to_owned(), token.clone());
            if let Some(raw) = object
                .get("value_raw")
                .and_then(json_scalar_to_string)
                .or_else(|| object.get("value").and_then(json_scalar_to_string))
            {
                let decimals = token
                    .get("decimals")
                    .and_then(Value::as_i64)
                    .and_then(|value| usize::try_from(value).ok())
                    .unwrap_or_default();
                object.insert(
                    "value".to_owned(),
                    Value::String(format_token_amount(&raw, decimals)),
                );
                object.insert("value_raw".to_owned(), Value::String(raw));
            }
        } else {
            object.insert("token".to_owned(), Value::String(symbol));
        }
    }
    Value::Object(object)
}

pub(crate) fn enrich_gas_event(value: &Value) -> Value {
    let mut object = value.as_object().cloned().unwrap_or_default();
    if let Some(address) = object.get("address").and_then(json_scalar_to_string) {
        object.insert(
            "address".to_owned(),
            serde_json::json!({
                "address": address
            }),
        );
    }
    if let Some(amount_raw) = object.get("amount").and_then(json_scalar_to_string) {
        let amount = format_token_amount(&amount_raw, 10);
        object.insert("amount".to_owned(), Value::String(amount.clone()));
        let fee_raw = object
            .get("price")
            .and_then(json_scalar_to_string)
            .and_then(|price| {
                let price = price.parse::<i128>().ok()?;
                let amount = amount_raw.parse::<i128>().ok()?;
                Some((price * amount).to_string())
            })
            .unwrap_or(amount_raw);
        object.insert(
            "fee".to_owned(),
            Value::String(format_token_amount(&fee_raw, 10)),
        );
    }
    Value::Object(object)
}

pub(crate) fn enrich_chain_event(value: &Value) -> Value {
    let mut object = value.as_object().cloned().unwrap_or_default();
    if let Some(chain) = object.get("chain").and_then(json_scalar_to_string) {
        object.insert(
            "chain".to_owned(),
            serde_json::json!({
                "chain_name": chain
            }),
        );
    }
    Value::Object(object)
}

pub(crate) fn enrich_single_token_event(
    value: &Value,
    tokens: &HashMap<String, Value>,
    token_key: &str,
) -> Value {
    enrich_multi_token_event(value, tokens, &[token_key])
}

pub(crate) fn enrich_token_series_event(value: &Value, tokens: &HashMap<String, Value>) -> Value {
    let mut object = enrich_single_token_event(value, tokens, "token")
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(owner) = object.get("owner").and_then(json_scalar_to_string) {
        object.insert(
            "owner".to_owned(),
            serde_json::json!({
                "address": owner
            }),
        );
    }
    Value::Object(object)
}

pub(crate) fn enrich_address_event(value: &Value) -> Value {
    if let Some(address) = value.as_object().and_then(|object| object.get("address")) {
        if address.is_object() {
            return value.clone();
        }
        if let Some(address) = json_scalar_to_string(address) {
            return serde_json::json!({
                "address": {
                    "address": address
                }
            });
        }
    }
    value.clone()
}

pub(crate) fn enrich_multi_token_event(
    value: &Value,
    tokens: &HashMap<String, Value>,
    token_keys: &[&str],
) -> Value {
    let mut object = value.as_object().cloned().unwrap_or_default();
    for key in token_keys {
        if let Some(symbol) = object.get(*key).and_then(json_scalar_to_string)
            && let Some(token) = tokens.get(&symbol.to_uppercase())
        {
            object.insert((*key).to_owned(), token.clone());
        }
    }
    Value::Object(object)
}

pub(crate) fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(crate) fn format_token_amount(amount: &str, token_decimals: usize) -> String {
    let amount = amount.trim();
    if amount == "0" || token_decimals == 0 {
        return amount.to_owned();
    }
    let negative = amount.strip_prefix('-');
    let digits = negative.unwrap_or(amount);
    if !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return amount.to_owned();
    }
    let formatted = if digits.len() <= token_decimals {
        let mut padded = "0".repeat(token_decimals - digits.len());
        padded.push_str(digits);
        let decimal_part = padded.trim_end_matches('0');
        if decimal_part.is_empty() {
            "0".to_owned()
        } else {
            format!("0.{decimal_part}")
        }
    } else {
        let decimal_start = digits.len() - token_decimals;
        let decimal_part = digits[decimal_start..].trim_end_matches('0');
        if decimal_part.is_empty() {
            digits[..decimal_start].to_owned()
        } else {
            format!("{}.{decimal_part}", &digits[..decimal_start])
        }
    };
    if negative.is_some() && formatted != "0" {
        format!("-{formatted}")
    } else {
        formatted
    }
}

pub(crate) fn clamp_limit(limit: Option<i64>) -> i64 {
    limit.unwrap_or(50).clamp(1, 100)
}

pub(crate) fn history_price_limit(limit: Option<i64>) -> Result<Option<i64>, ApiError> {
    // C# treats filtered history-price requests as bounded only when limit is
    // positive; 0 and -1 mean "return all matching points".
    let limit = limit.unwrap_or(50);
    if limit < -1 {
        return Err(ApiError::BadRequest(
            "limit cannot be less than -1".to_owned(),
        ));
    }
    Ok((limit > 0).then_some(limit))
}

pub(crate) fn nonnegative_offset(offset: Option<i64>) -> Result<i64, ApiError> {
    let offset = offset.unwrap_or(0);
    if offset < 0 {
        return Err(ApiError::BadRequest("offset cannot be negative".to_owned()));
    }
    Ok(offset)
}

pub(crate) fn trim_offset_rows<T>(
    mut rows: Vec<T>,
    limit: i64,
    offset: i64,
) -> Result<(Vec<T>, Option<String>), ApiError> {
    let limit = usize::try_from(limit)
        .map_err(|_| ApiError::BadRequest("limit is outside usize range".to_owned()))?;
    let has_next_page = rows.len() > limit;
    if has_next_page {
        rows.truncate(limit);
    }
    let next_cursor = has_next_page.then(|| OffsetCursor(offset + rows.len() as i64).encode());
    Ok((rows, next_cursor))
}

pub(crate) fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

pub(crate) fn normalize_block_id(value: String) -> String {
    value.trim().to_uppercase()
}

pub(crate) fn query_chain(value: Option<String>, default_chain: &str) -> String {
    empty_to_none(value).unwrap_or_else(|| default_chain.to_owned())
}

/// Parse the `order_direction` query param into the shared read-model
/// [`SortDirection`], answering 400 for unrecognised values. The db read layer
/// owns the asc/desc -> SQL keyword mapping (`SortDirection::as_sql`) and the
/// seek-cursor operator (`SortDirection::cursor_operator`).
pub(crate) fn parse_sort_direction(value: Option<&str>) -> Result<SortDirection, ApiError> {
    SortDirection::from_api_param(value).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_direction '{}'",
            value.unwrap_or("asc").to_ascii_lowercase()
        ))
    })
}

pub(crate) fn parse_optional_i64(
    value: Option<&str>,
    field: &'static str,
) -> Result<Option<i64>, ApiError> {
    value
        .map(|value| {
            value.parse::<i64>().map_err(|error| {
                ApiError::BadRequest(format!("{field} is not a valid integer: {error}"))
            })
        })
        .transpose()
}

pub(crate) fn parse_optional_i32(
    value: Option<&str>,
    field: &'static str,
) -> Result<Option<i32>, ApiError> {
    value
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                ApiError::BadRequest(format!("{field} is not a valid integer: {error}"))
            })
        })
        .transpose()
}

pub(crate) fn nonzero_f64(value: f64) -> Option<f64> {
    (value != 0.0).then_some(value)
}

pub(crate) fn is_positive_raw(value: Option<&str>) -> bool {
    value
        .and_then(|value| value.parse::<i128>().ok())
        .is_some_and(|value| value > 0)
}

pub(crate) fn json_opt<T: DeserializeOwned>(value: Option<Value>) -> Result<Option<T>, ApiError> {
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| ApiError::Internal(format!("stored JSON shape is invalid: {error}")))
}

pub(crate) fn json_vec<T: DeserializeOwned>(
    value: Option<Value>,
) -> Result<Option<Vec<T>>, ApiError> {
    json_opt(value)
}

pub(crate) fn normalized_required_path(
    field: &'static str,
    value: String,
) -> Result<String, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::BadRequest(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_owned())
}

pub(crate) fn parse_i32_id(field: &'static str, value: &str) -> Result<i32, ApiError> {
    value.parse::<i32>().map_err(|error| {
        ApiError::Internal(format!("stored {field} is not a valid integer ID: {error}"))
    })
}

pub(crate) fn address_ref(
    address: Option<String>,
    address_name: Option<String>,
) -> Option<AddressRefResponse> {
    address.map(|address| AddressRefResponse {
        address,
        address_name,
    })
}

pub(crate) fn decode_hex(value: &str) -> Result<Vec<u8>, ApiError> {
    decode_hex_field(value, "script_raw")
}

pub(crate) fn decode_hex_field(value: &str, field: &str) -> Result<Vec<u8>, ApiError> {
    let normalized = value
        .trim()
        .strip_prefix("0x")
        .or_else(|| value.trim().strip_prefix("0X"))
        .unwrap_or_else(|| value.trim());
    if !normalized.len().is_multiple_of(2) {
        return Err(ApiError::BadRequest(format!(
            "{field} must contain an even number of hex characters"
        )));
    }

    let mut bytes = Vec::with_capacity(normalized.len() / 2);
    for chunk in normalized.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(chunk[0], field)?;
        let lo = hex_nibble(chunk[1], field)?;
        bytes.push((hi << 4) | lo);
    }
    Ok(bytes)
}

pub(crate) fn hex_nibble(value: u8, field: &str) -> Result<u8, ApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(ApiError::BadRequest(format!(
            "{field} must be base16 encoded"
        ))),
    }
}

pub(crate) fn decode_formatted_bytes(
    value: &str,
    format: Option<&str>,
    field: &str,
    allow_plain: bool,
) -> Result<Vec<u8>, ApiError> {
    match format
        .unwrap_or("Plain")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "0" | "plain" if allow_plain => Ok(value.as_bytes().to_vec()),
        "0" | "plain" => Err(ApiError::BadRequest(format!(
            "{field}Format Plain is not supported"
        ))),
        "1" | "base16" | "hex" => decode_hex_field(value, field),
        "2" | "base64" => BASE64_STANDARD.decode(value.trim()).map_err(|error| {
            ApiError::BadRequest(format!("{field} must be base64 encoded: {error}"))
        }),
        other => Err(ApiError::BadRequest(format!(
            "{field}Format {other} is not supported"
        ))),
    }
}

pub(crate) fn is_ed25519_signature_kind(value: &str) -> bool {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "ed25519" => true,
        "0" | "none" | "2" | "ecdsa" | "3" | "ring" => false,
        _ => false,
    }
}

pub(crate) fn disassemble_script_bytes(bytes: &[u8]) -> Result<Vec<String>, ApiError> {
    let mut reader = VmScriptReader::new(bytes);
    let mut instructions = Vec::new();

    while reader.has_more() {
        let offset = reader.position();
        let opcode = reader.read_u8()?;
        let args = match opcode {
            OPCODE_RET => {
                instructions.push(format_vm_instruction(
                    offset,
                    opcode,
                    VmInstructionArgs::None,
                ));
                return Ok(instructions);
            }
            OPCODE_CTX | OPCODE_MOVE | OPCODE_COPY | OPCODE_SWAP | OPCODE_SIZE | OPCODE_COUNT
            | OPCODE_SIGN | OPCODE_NOT | OPCODE_NEGATE | OPCODE_ABS | OPCODE_UNPACK => {
                VmInstructionArgs::Regs2(reader.read_u8()?, reader.read_u8()?)
            }
            OPCODE_LOAD => {
                let dst = reader.read_u8()?;
                let vm_type = reader.read_u8()?;
                let len = reader.read_var(0xffff)? as usize;
                let data = reader.read_bytes(len)?;
                VmInstructionArgs::Load { dst, vm_type, data }
            }
            OPCODE_CAST => VmInstructionArgs::Cast {
                src: reader.read_u8()?,
                dst: reader.read_u8()?,
                vm_type: reader.read_u8()?,
            },
            OPCODE_POP | OPCODE_PUSH | OPCODE_EXTCALL | OPCODE_THROW | OPCODE_CLEAR => {
                VmInstructionArgs::Reg1(reader.read_u8()?)
            }
            OPCODE_CALL => VmInstructionArgs::Call {
                count: reader.read_u8()?,
                offset: reader.read_u16()?,
            },
            OPCODE_JMP => VmInstructionArgs::Jump(reader.read_i16()?),
            OPCODE_JMPIF | OPCODE_JMPNOT => VmInstructionArgs::JumpIf {
                src: reader.read_u8()?,
                offset: reader.read_i16()?,
            },
            OPCODE_AND | OPCODE_OR | OPCODE_XOR | OPCODE_CAT | OPCODE_EQUAL | OPCODE_LT
            | OPCODE_GT | OPCODE_LTE | OPCODE_GTE | OPCODE_ADD | OPCODE_SUB | OPCODE_MUL
            | OPCODE_DIV | OPCODE_MOD | OPCODE_SHR | OPCODE_SHL | OPCODE_MIN | OPCODE_MAX
            | OPCODE_POW | OPCODE_PUT | OPCODE_GET => {
                VmInstructionArgs::Regs3(reader.read_u8()?, reader.read_u8()?, reader.read_u8()?)
            }
            OPCODE_LEFT | OPCODE_RIGHT => VmInstructionArgs::Regs2Len {
                src: reader.read_u8()?,
                dst: reader.read_u8()?,
                len: reader.read_var(0xffff)? as u16,
            },
            OPCODE_RANGE => VmInstructionArgs::Range {
                src: reader.read_u8()?,
                dst: reader.read_u8()?,
                index: reader.read_var(0xffff)? as u32,
                len: reader.read_var(0xffff)? as u32,
            },
            OPCODE_INC | OPCODE_DEC | OPCODE_SWITCH => VmInstructionArgs::Reg1(reader.read_u8()?),
            _ => VmInstructionArgs::None,
        };
        instructions.push(format_vm_instruction(offset, opcode, args));
    }

    Ok(instructions)
}

pub(crate) struct VmScriptReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> VmScriptReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn has_more(&self) -> bool {
        self.position < self.bytes.len()
    }

    fn position(&self) -> usize {
        self.position
    }

    fn read_u8(&mut self) -> Result<u8, ApiError> {
        if self.position >= self.bytes.len() {
            return Err(ApiError::BadRequest(
                "Constraint failed: Outside of range".to_owned(),
            ));
        }
        let value = self.bytes[self.position];
        self.position += 1;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16, ApiError> {
        let a = self.read_u8()? as u16;
        let b = self.read_u8()? as u16;
        Ok(a + (b << 8))
    }

    fn read_i16(&mut self) -> Result<i16, ApiError> {
        Ok(self.read_u16()? as i16)
    }

    fn read_u32(&mut self) -> Result<u32, ApiError> {
        let a = self.read_u8()? as u32;
        let b = self.read_u8()? as u32;
        let c = self.read_u8()? as u32;
        let d = self.read_u8()? as u32;
        Ok(a + (b << 8) + (c << 16) + (d << 24))
    }

    fn read_u64(&mut self) -> Result<u64, ApiError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(8) {
            value += (self.read_u8()? as u64) << shift;
        }
        Ok(value)
    }

    fn read_var(&mut self, max: u64) -> Result<u64, ApiError> {
        let marker = self.read_u8()?;
        let value = match marker {
            0xfd => self.read_u16()? as u64,
            0xfe => self.read_u32()? as u64,
            0xff => self.read_u64()?,
            _ => marker as u64,
        };
        if value > max {
            return Err(ApiError::BadRequest(
                "Constraint failed: Input exceed max".to_owned(),
            ));
        }
        Ok(value)
    }

    fn read_bytes(&mut self, length: usize) -> Result<Vec<u8>, ApiError> {
        // Faithful port of the C# Phantasma VM `Disassembler.ReadBytes` bound
        // (Phantasma.Business VM/Disassembler.cs): it uses `>=`, which also rejects
        // a read ending exactly at the last byte. Kept as-is so `/instructions`
        // disassembles byte-for-byte like the reference VM — do not relax to `>`.
        if self.position + length >= self.bytes.len() {
            return Err(ApiError::BadRequest(
                "Constraint failed: Outside of range".to_owned(),
            ));
        }
        let data = self.bytes[self.position..self.position + length].to_vec();
        self.position += length;
        Ok(data)
    }
}

pub(crate) enum VmInstructionArgs {
    None,
    Reg1(u8),
    Regs2(u8, u8),
    Regs3(u8, u8, u8),
    Regs2Len {
        src: u8,
        dst: u8,
        len: u16,
    },
    Range {
        src: u8,
        dst: u8,
        index: u32,
        len: u32,
    },
    Cast {
        src: u8,
        dst: u8,
        vm_type: u8,
    },
    Load {
        dst: u8,
        vm_type: u8,
        data: Vec<u8>,
    },
    Call {
        count: u8,
        offset: u16,
    },
    Jump(i16),
    JumpIf {
        src: u8,
        offset: i16,
    },
}

pub(crate) fn format_vm_instruction(offset: usize, opcode: u8, args: VmInstructionArgs) -> String {
    let mut output = format!("{offset:03}: {}", opcode_name(opcode));
    match args {
        VmInstructionArgs::None => {}
        VmInstructionArgs::Reg1(reg) => append_register(&mut output, reg),
        VmInstructionArgs::Regs2(src, dst) => {
            append_register(&mut output, src);
            output.push(',');
            append_register(&mut output, dst);
        }
        VmInstructionArgs::Regs3(src_a, src_b, dst) => {
            append_register(&mut output, src_a);
            output.push(',');
            append_register(&mut output, src_b);
            output.push(',');
            append_register(&mut output, dst);
        }
        VmInstructionArgs::Regs2Len { src, dst, len } => {
            append_register(&mut output, src);
            output.push(',');
            append_register(&mut output, dst);
            output.push_str(", ");
            output.push_str(&len.to_string());
        }
        VmInstructionArgs::Range {
            src,
            dst,
            index,
            len,
        } => {
            append_register(&mut output, src);
            output.push(',');
            append_register(&mut output, dst);
            output.push_str(", ");
            output.push_str(&index.to_string());
            output.push_str(", ");
            output.push_str(&len.to_string());
        }
        VmInstructionArgs::Cast { src, dst, vm_type } => {
            append_register(&mut output, src);
            output.push(',');
            append_register(&mut output, dst);
            output.push_str(", ");
            output.push_str(&(vm_type as i32).to_string());
        }
        VmInstructionArgs::Load { dst, vm_type, data } => {
            append_register(&mut output, dst);
            output.push_str(", ");
            output.push_str(&format_vm_load_data(vm_type, &data));
        }
        VmInstructionArgs::Call { count, offset } => {
            append_register(&mut output, count);
            output.push_str(", ");
            output.push_str(&offset.to_string());
        }
        VmInstructionArgs::Jump(offset) => {
            output.push(' ');
            output.push_str(&offset.to_string());
        }
        VmInstructionArgs::JumpIf { src, offset } => {
            append_register(&mut output, src);
            output.push_str(", ");
            output.push_str(&offset.to_string());
        }
    }
    output
}

pub(crate) fn append_register(output: &mut String, reg: u8) {
    output.push_str(" r");
    output.push_str(&reg.to_string());
}

pub(crate) fn format_vm_load_data(vm_type: u8, data: &[u8]) -> String {
    match vm_type {
        VM_TYPE_STRING => format!("\"{}\"", String::from_utf8_lossy(data)),
        VM_TYPE_NUMBER => BigInt::from_signed_bytes_le(data).to_string(),
        _ => phantasma_sdk::encode_hex_upper(data),
    }
}

pub(crate) fn opcode_name(opcode: u8) -> String {
    match opcode {
        OPCODE_NOP => "NOP",
        OPCODE_MOVE => "MOVE",
        OPCODE_COPY => "COPY",
        OPCODE_PUSH => "PUSH",
        OPCODE_POP => "POP",
        OPCODE_SWAP => "SWAP",
        OPCODE_CALL => "CALL",
        OPCODE_EXTCALL => "EXTCALL",
        OPCODE_JMP => "JMP",
        OPCODE_JMPIF => "JMPIF",
        OPCODE_JMPNOT => "JMPNOT",
        OPCODE_RET => "RET",
        OPCODE_THROW => "THROW",
        OPCODE_LOAD => "LOAD",
        OPCODE_CAST => "CAST",
        OPCODE_CAT => "CAT",
        OPCODE_RANGE => "RANGE",
        OPCODE_LEFT => "LEFT",
        OPCODE_RIGHT => "RIGHT",
        OPCODE_SIZE => "SIZE",
        OPCODE_COUNT => "COUNT",
        OPCODE_NOT => "NOT",
        OPCODE_AND => "AND",
        OPCODE_OR => "OR",
        OPCODE_XOR => "XOR",
        OPCODE_EQUAL => "EQUAL",
        OPCODE_LT => "LT",
        OPCODE_GT => "GT",
        OPCODE_LTE => "LTE",
        OPCODE_GTE => "GTE",
        OPCODE_INC => "INC",
        OPCODE_DEC => "DEC",
        OPCODE_SIGN => "SIGN",
        OPCODE_NEGATE => "NEGATE",
        OPCODE_ABS => "ABS",
        OPCODE_ADD => "ADD",
        OPCODE_SUB => "SUB",
        OPCODE_MUL => "MUL",
        OPCODE_DIV => "DIV",
        OPCODE_MOD => "MOD",
        OPCODE_SHL => "SHL",
        OPCODE_SHR => "SHR",
        OPCODE_MIN => "MIN",
        OPCODE_MAX => "MAX",
        OPCODE_POW => "POW",
        OPCODE_CTX => "CTX",
        OPCODE_SWITCH => "SWITCH",
        OPCODE_PUT => "PUT",
        OPCODE_GET => "GET",
        OPCODE_CLEAR => "CLEAR",
        OPCODE_UNPACK => "UNPACK",
        OPCODE_PACK => "PACK",
        OPCODE_DEBUG => "DEBUG",
        OPCODE_SUBSTR => "SUBSTR",
        OPCODE_REMOVE => "REMOVE",
        OPCODE_EVM => "EVM",
        _ => return opcode.to_string(),
    }
    .to_owned()
}

const VM_TYPE_NUMBER: u8 = 3;
const VM_TYPE_STRING: u8 = 4;

const OPCODE_NOP: u8 = 0;
const OPCODE_MOVE: u8 = 1;
const OPCODE_COPY: u8 = 2;
const OPCODE_PUSH: u8 = 3;
const OPCODE_POP: u8 = 4;
const OPCODE_SWAP: u8 = 5;
const OPCODE_CALL: u8 = 6;
const OPCODE_EXTCALL: u8 = 7;
const OPCODE_JMP: u8 = 8;
const OPCODE_JMPIF: u8 = 9;
const OPCODE_JMPNOT: u8 = 10;
const OPCODE_RET: u8 = 11;
const OPCODE_THROW: u8 = 12;
const OPCODE_LOAD: u8 = 13;
const OPCODE_CAST: u8 = 14;
const OPCODE_CAT: u8 = 15;
const OPCODE_RANGE: u8 = 16;
const OPCODE_LEFT: u8 = 17;
const OPCODE_RIGHT: u8 = 18;
const OPCODE_SIZE: u8 = 19;
const OPCODE_COUNT: u8 = 20;
const OPCODE_NOT: u8 = 21;
const OPCODE_AND: u8 = 22;
const OPCODE_OR: u8 = 23;
const OPCODE_XOR: u8 = 24;
const OPCODE_EQUAL: u8 = 25;
const OPCODE_LT: u8 = 26;
const OPCODE_GT: u8 = 27;
const OPCODE_LTE: u8 = 28;
const OPCODE_GTE: u8 = 29;
const OPCODE_INC: u8 = 30;
const OPCODE_DEC: u8 = 31;
const OPCODE_SIGN: u8 = 32;
const OPCODE_NEGATE: u8 = 33;
const OPCODE_ABS: u8 = 34;
const OPCODE_ADD: u8 = 35;
const OPCODE_SUB: u8 = 36;
const OPCODE_MUL: u8 = 37;
const OPCODE_DIV: u8 = 38;
const OPCODE_MOD: u8 = 39;
const OPCODE_SHL: u8 = 40;
const OPCODE_SHR: u8 = 41;
const OPCODE_MIN: u8 = 42;
const OPCODE_MAX: u8 = 43;
const OPCODE_POW: u8 = 44;
const OPCODE_CTX: u8 = 45;
const OPCODE_SWITCH: u8 = 46;
const OPCODE_PUT: u8 = 47;
const OPCODE_GET: u8 = 48;
const OPCODE_CLEAR: u8 = 49;
const OPCODE_UNPACK: u8 = 50;
const OPCODE_PACK: u8 = 51;
const OPCODE_DEBUG: u8 = 52;
const OPCODE_SUBSTR: u8 = 53;
const OPCODE_REMOVE: u8 = 54;
const OPCODE_EVM: u8 = 255;

pub(crate) fn staking_daily_from_row(
    row: &PgRow,
    apply_supply_adjustment: bool,
) -> StakingDailyStatResponse {
    let mut item = StakingDailyStatResponse {
        date_unix_seconds: row.get("date_unix_seconds"),
        staked_soul_raw: row.get("staked_soul_raw"),
        soul_supply_raw: row.get("soul_supply_raw"),
        stakers_count: row.get("stakers_count"),
        masters_count: row.get("masters_count"),
        staking_ratio: row.get("staking_ratio"),
        staking_percent: 0.0,
        captured_at_unix_seconds: row.get("captured_at_unix_seconds"),
        source: row.get("source"),
    };
    apply_staking_supply_adjustment(&mut item, apply_supply_adjustment);
    item
}

pub(crate) fn apply_staking_supply_adjustment(item: &mut StakingDailyStatResponse, apply: bool) {
    if !apply {
        item.staking_percent = item.staking_ratio * 100.0;
        return;
    }
    let adjustment = match item.date_unix_seconds {
        value if value < 1_675_728_000 => 5_208_119_200_000_000_i128,
        value if value < 1_675_987_200 => 364_999_300_000_000_i128,
        _ => 0,
    };
    if adjustment <= 0 {
        item.staking_percent = item.staking_ratio * 100.0;
        return;
    }
    let Some(supply_raw) = item
        .soul_supply_raw
        .as_deref()
        .and_then(|value| value.parse::<i128>().ok())
    else {
        item.staking_percent = item.staking_ratio * 100.0;
        return;
    };
    let Some(staked_raw) = item
        .staked_soul_raw
        .as_deref()
        .and_then(|value| value.parse::<i128>().ok())
    else {
        item.staking_percent = item.staking_ratio * 100.0;
        return;
    };
    let adjusted_supply = supply_raw + adjustment;
    if adjusted_supply <= 0 {
        item.staking_percent = item.staking_ratio * 100.0;
        return;
    }
    item.soul_supply_raw = Some(adjusted_supply.to_string());
    item.staking_ratio = staked_raw as f64 / adjusted_supply as f64;
    item.staking_percent = item.staking_ratio * 100.0;
}

pub(crate) fn rejected_transaction_from_row(
    row: &RejectedTransactionRow,
) -> RejectedTransactionResponse {
    RejectedTransactionResponse {
        hash: row.hash.clone(),
        nexus: row.nexus.clone(),
        chain: row.chain.clone(),
        block_height: row.block_height.map(|value| value.to_string()),
        block_hash: row.block_hash.clone(),
        date: row.timestamp_unix_seconds.map(|value| value.to_string()),
        state: row.state.clone(),
        result: row.result.clone(),
        debug_comment: row.debug_comment.clone(),
        payload: row.payload.clone(),
        script_raw: row.script_raw.clone(),
        fee_raw: row.fee_raw.clone(),
        expiration: row.expiration.map(|value| value.to_string()),
        gas_price_raw: row.gas_price_raw.clone(),
        gas_limit_raw: row.gas_limit_raw.clone(),
        sender: row.sender.clone(),
        gas_payer: row.gas_payer.clone(),
        gas_target: row.gas_target.clone(),
        canonical_status: row.canonical_status.clone(),
        captured_at: row.captured_at_unix_seconds.to_string(),
        updated_at: row.updated_at_unix_seconds.to_string(),
        rpc_response_json: row.rpc_response_json.clone(),
        block_response_json: row.block_response_json.clone(),
    }
}
