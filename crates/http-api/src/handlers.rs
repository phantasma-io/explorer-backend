//! HTTP request handlers and their transaction/event read orchestration.
use crate::*;

pub(crate) async fn resolve_chain_id(state: &ApiState) -> Result<i32, ApiError> {
    resolve_chain_id_by_name(&state.pool, state.chain.as_str()).await
}

pub(crate) async fn resolve_chain_id_by_name(pool: &PgPool, chain: &str) -> Result<i32, ApiError> {
    let ids = chain_ids_by_name(pool, chain).await?;
    match ids.as_slice() {
        [id] => Ok(*id),
        [] => Err(ApiError::NotFound(format!("chain {chain} was not found"))),
        _ => Err(ApiError::Internal(format!(
            "chain {chain} is ambiguous in the database"
        ))),
    }
}

pub(crate) async fn resolve_transaction_state_id(
    pool: &PgPool,
    state: Option<&str>,
) -> Result<Option<i32>, ApiError> {
    let Some(state) = state else {
        return Ok(None);
    };

    transaction_state_id_by_name(pool, state)
        .await?
        .map(Some)
        .ok_or_else(|| ApiError::BadRequest("Unsupported value for 'state' parameter.".to_owned()))
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Service and database are healthy.", body = HealthResponse),
        (status = 503, description = "Service is reachable but database health check failed.", body = HealthResponse)
    )
)]
pub(crate) async fn health(State(state): State<ApiState>) -> impl IntoResponse {
    match check_health(&state.pool).await {
        Ok(database) => {
            let response = HealthResponse {
                service: state.service_name,
                status: "ok".to_owned(),
                started_at: state.started_at,
                checked_at: database.checked_at,
                database_ok: database.ok,
                database_server_version: database.server_version,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(error) => {
            // The health probe is reachable unauthenticated; report only that the DB check failed
            // and log the underlying error for the operator, instead of echoing the raw DB error
            // string back to the client in `database_server_version`.
            tracing::error!(detail = %error, "database health check failed");
            let response = HealthResponse {
                service: state.service_name,
                status: "degraded".to_owned(),
                started_at: state.started_at,
                checked_at: Utc::now(),
                database_ok: false,
                database_server_version: None,
            };
            (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response()
        }
    }
}

#[utoipa::path(
    get,
    path = "/version",
    tag = "system",
    responses((status = 200, description = "Service version metadata.", body = VersionResponse))
)]
pub(crate) async fn version(State(state): State<ApiState>) -> Json<VersionResponse> {
    Json(VersionResponse {
        service: state.service_name,
        version: env!("CARGO_PKG_VERSION").to_owned(),
        git_sha: option_env!("GIT_SHA").map(ToOwned::to_owned),
    })
}

pub(crate) async fn swagger_redirect() -> Redirect {
    Redirect::temporary("/swagger-ui/")
}

pub(crate) async fn chains(
    State(state): State<ApiState>,
    Query(query): Query<ChainListQuery>,
) -> Result<Json<ChainListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = nonnegative_offset(query.offset)?;
    let chain = empty_to_none(query.chain).unwrap_or_else(|| state.chain.as_str().to_owned());
    let with_total = query.with_total == Some(1);

    let rows = list_chains(&state.pool, Some(chain.as_str()), limit, offset).await?;

    let total_results = if with_total {
        Some(count_chains(&state.pool, Some(chain.as_str())).await?)
    } else {
        None
    };

    Ok(Json(ChainListResponse {
        total_results,
        chains: rows
            .into_iter()
            .map(|row| ChainRefResponse {
                chain_name: Some(row.name),
                chain_height: Some(row.current_height.to_string()),
            })
            .collect(),
    }))
}

pub(crate) async fn oracles(
    State(state): State<ApiState>,
    Query(query): Query<OracleListQuery>,
) -> Result<Json<OracleListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = nonnegative_offset(query.offset)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = OracleOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let block_hash = empty_to_none(query.block_hash);
    let block_height =
        parse_optional_i64(empty_to_none(query.block_height).as_deref(), "block_height")?;
    if block_hash.is_none() && block_height.is_none() {
        return Err(ApiError::BadRequest(
            "Need either block_hash or block_height != null".to_owned(),
        ));
    }

    let filter = OracleFilter {
        chain_id,
        block_hash: block_hash.as_deref(),
        block_height,
    };
    let rows = list_oracles(&state.pool, &filter, order_by, direction, limit, offset).await?;
    let total_results = if query.with_total == Some(1) {
        Some(count_oracles(&state.pool, &filter).await?)
    } else {
        None
    };

    Ok(Json(OracleListResponse {
        total_results,
        oracles: rows
            .into_iter()
            .map(|row| OracleResponse {
                url: row.url,
                content: row.content,
            })
            .collect(),
    }))
}

pub(crate) async fn validator_kinds(
    State(state): State<ApiState>,
    Query(query): Query<ValidatorKindListQuery>,
) -> Result<Json<ValidatorKindListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = nonnegative_offset(query.offset)?;
    // Map the public query params to typed read-layer inputs here (the API owns
    // the accepted spellings and the 400 responses); the db read fn then sees
    // only values it cannot reject, so unsupported sorts are unrepresentable
    // past this point.
    let order_by_param = empty_to_none(query.order_by);
    let order_by =
        ValidatorKindOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let name_filter = empty_to_none(query.validator_kind);

    let rows = list_validator_kinds(
        &state.pool,
        name_filter.as_deref(),
        order_by,
        direction,
        limit,
        offset,
    )
    .await?;
    let total_results = if query.with_total == Some(1) {
        Some(count_validator_kinds(&state.pool, name_filter.as_deref()).await?)
    } else {
        None
    };

    Ok(Json(ValidatorKindListResponse {
        total_results,
        validator_kinds: rows
            .into_iter()
            .map(|row| ValidatorKindResponse { name: row.name })
            .collect(),
    }))
}

pub(crate) async fn history_prices(
    State(state): State<ApiState>,
    Query(query): Query<HistoryPriceListQuery>,
) -> Result<Json<HistoryPriceListResponse>, ApiError> {
    let limit = history_price_limit(query.limit)?;
    let offset = nonnegative_offset(query.offset)?;
    let effective_offset = if limit.is_some() { offset } else { 0 };
    let order_by_param = empty_to_none(query.order_by);
    let order_by =
        HistoryPriceOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let symbol = empty_to_none(query.symbol)
        .map(|value| value.to_uppercase())
        .unwrap_or_else(|| "SOUL".to_owned());
    let date_less = parse_optional_i64(empty_to_none(query.date_less).as_deref(), "date_less")?;
    let date_greater =
        parse_optional_i64(empty_to_none(query.date_greater).as_deref(), "date_greater")?;
    let with_token = query.with_token == Some(1);

    let filter = HistoryPriceFilter {
        symbol: symbol.as_str(),
        date_less,
        date_greater,
        with_token,
    };
    let rows = list_history_prices(
        &state.pool,
        &filter,
        order_by,
        direction,
        limit,
        effective_offset,
    )
    .await?;
    let total_results = if query.with_total == Some(1) {
        Some(count_history_prices(&state.pool, &filter).await?)
    } else {
        None
    };
    let history_prices = rows
        .into_iter()
        .map(|row| {
            Ok(HistoryPriceResponse {
                symbol: row.symbol,
                price: HistoryPricePointResponse {
                    usd: nonzero_f64(row.price_usd),
                    eur: None,
                    gbp: None,
                    jpy: None,
                    cad: None,
                    aud: None,
                    cny: None,
                    rub: None,
                },
                token: token_from_value(row.token_json)?,
                date: Some(row.date_unix_seconds.to_string()),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(Json(HistoryPriceListResponse {
        total_results,
        history_prices,
    }))
}

pub(crate) async fn circulating_supply(
    State(state): State<ApiState>,
) -> Result<Json<f64>, ApiError> {
    let supply = circulating_soul_supply(&state.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("SOUL token was not found".to_owned()))?;

    let parsed = supply.parse::<f64>().map_err(|error| {
        ApiError::Internal(format!(
            "cannot parse SOUL current_supply '{supply}': {error}"
        ))
    })?;
    Ok(Json(parsed))
}

pub(crate) async fn instructions(
    Json(request): Json<InstructionRequest>,
) -> Result<Json<InstructionListResponse>, ApiError> {
    let script_raw = empty_to_none(request.script_raw)
        .ok_or_else(|| ApiError::BadRequest("script_raw is required".to_owned()))?;
    let bytes = decode_hex(&script_raw)?;
    let instructions = disassemble_script_bytes(&bytes)?
        .into_iter()
        .map(|instruction| InstructionResponse { instruction })
        .collect::<Vec<_>>();
    Ok(Json(InstructionListResponse {
        total_results: instructions.len() as i64,
        instructions,
    }))
}

pub(crate) async fn verify_message(
    Query(query): Query<VerifyMessageQuery>,
) -> Result<Json<bool>, ApiError> {
    let message = empty_to_none(query.message)
        .ok_or_else(|| ApiError::BadRequest("message is required".to_owned()))?;
    let signature = empty_to_none(query.signature)
        .ok_or_else(|| ApiError::BadRequest("signature is required".to_owned()))?;
    let signer_address = empty_to_none(query.signer_address)
        .ok_or_else(|| ApiError::BadRequest("signerAddress is required".to_owned()))?;

    let signature_kind = query.signature_kind.as_deref().unwrap_or("Ed25519");
    if !is_ed25519_signature_kind(signature_kind) {
        return Err(ApiError::BadRequest(format!(
            "signatureKind {signature_kind} is not supported by the Rust API yet"
        )));
    }
    if let Some(curve) = query.ecdsa_curve.as_deref()
        && !curve.eq_ignore_ascii_case("Secp256k1")
        && curve != "0"
    {
        return Err(ApiError::BadRequest(format!(
            "ecdsaCurve {curve} is not supported by the Rust API yet"
        )));
    }

    let message_bytes =
        decode_formatted_bytes(&message, query.message_format.as_deref(), "message", true)?;
    let signature_bytes = decode_formatted_bytes(
        &signature,
        query.signature_format.as_deref(),
        "signature",
        false,
    )?;
    let signer = PhantasmaAddress::from_text(&signer_address)
        .map_err(|error| ApiError::BadRequest(format!("invalid signerAddress: {error}")))?;
    let signature = Ed25519Signature::try_from_slice(&signature_bytes)
        .map_err(|error| ApiError::BadRequest(format!("invalid signature: {error}")))?;

    Ok(Json(signature.verify(&message_bytes, [&signer])))
}

pub(crate) async fn blocks(
    State(state): State<ApiState>,
    Query(query): Query<BlockListQuery>,
) -> Result<Json<BlockListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = BlockOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let id = empty_to_none(query.id).map(normalize_block_id);
    let id_height = id.as_deref().and_then(|value| value.parse::<i64>().ok());
    let hash = empty_to_none(query.hash).map(|value| value.to_uppercase());
    let hash_partial = empty_to_none(query.hash_partial).map(|value| format!("%{value}%"));
    let height = parse_optional_i64(empty_to_none(query.height).as_deref(), "height")?;
    let q = empty_to_none(query.q);
    let q_height = parse_optional_i64(q.as_deref(), "q").ok().flatten();
    let q_hash = q
        .as_deref()
        .filter(|value| value.len() >= 64)
        .map(|value| value.to_uppercase());
    let date_less = parse_optional_i64(empty_to_none(query.date_less).as_deref(), "date_less")?;
    let date_greater =
        parse_optional_i64(empty_to_none(query.date_greater).as_deref(), "date_greater")?;
    let with_transactions = query.with_transactions == Some(1);
    let with_events = query.with_events == Some(1);
    let with_event_data = query.with_event_data == Some(1);
    let with_script = query.with_script == Some(1);

    let filter = BlockFilter {
        chain_id,
        id: id.as_deref(),
        id_height,
        hash: hash.as_deref(),
        hash_partial: hash_partial.as_deref(),
        height,
        q_height,
        q_hash: q_hash.as_deref(),
        date_less,
        date_greater,
    };
    let rows = list_blocks(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    let mut blocks = rows.iter().map(block_from_row).collect::<Vec<_>>();
    if with_transactions {
        let block_ids = rows
            .iter()
            .map(|row| row.get::<i32, _>("id"))
            .collect::<Vec<_>>();
        let mut grouped = load_transactions_by_block_ids(
            &state.pool,
            &block_ids,
            with_events,
            with_event_data,
            with_script,
        )
        .await?;
        for (row, block) in rows.iter().zip(blocks.iter_mut()) {
            let block_id = row.get::<i32, _>("id");
            block.transactions = Some(grouped.remove(&block_id).unwrap_or_default());
        }
    }
    Ok(Json(BlockListResponse {
        total_results: None,
        blocks,
        next_cursor,
    }))
}

pub(crate) async fn tokens(
    State(state): State<ApiState>,
    Query(query): Query<TokenListQuery>,
) -> Result<Json<TokenListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = TokenOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let symbol = empty_to_none(query.symbol).map(|value| value.to_uppercase());
    let q = empty_to_none(query.q).map(|value| format!("%{value}%"));
    let with_price = query.with_price == Some(1);
    let with_logo = query.with_logo == Some(1);

    let filter = TokenFilter {
        chain_id,
        symbol: symbol.as_deref(),
        q: q.as_deref(),
        with_logo,
    };
    let rows = list_tokens(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    let tokens = rows
        .iter()
        .map(|row| token_from_row(row, with_price, with_logo))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(TokenListResponse {
        total_results: None,
        tokens,
        next_cursor,
    }))
}

pub(crate) async fn addresses(
    State(state): State<ApiState>,
    Query(query): Query<AddressListQuery>,
) -> Result<Json<AddressListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = AddressOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let address = empty_to_none(query.address);
    let address_name = empty_to_none(query.address_name);
    let address_partial = empty_to_none(query.address_partial).map(|value| format!("%{value}%"));
    let symbol = empty_to_none(query.symbol)
        .map(|value| value.to_uppercase())
        .unwrap_or_else(|| "SOUL".to_owned());
    let organization_name = empty_to_none(query.organization_name);
    let validator_kind = empty_to_none(query.validator_kind);
    let with_storage = query.with_storage == Some(1);
    let with_stakes = query.with_stakes == Some(1);
    let with_balance = query.with_balance == Some(1);

    let filter = AddressFilter {
        chain_id,
        address: address.as_deref(),
        address_name: address_name.as_deref(),
        address_partial: address_partial.as_deref(),
        symbol: symbol.as_str(),
        organization_name: organization_name.as_deref(),
        validator_kind: validator_kind.as_deref(),
        with_balance,
    };
    let rows = list_addresses(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    let addresses = rows
        .iter()
        .map(|row| address_from_row(row, with_storage, with_stakes, with_balance))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(AddressListResponse {
        total_results: None,
        addresses,
        next_cursor,
    }))
}

pub(crate) async fn contracts(
    State(state): State<ApiState>,
    Query(query): Query<ContractListQuery>,
) -> Result<Json<ContractListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = ContractOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let symbol = empty_to_none(query.symbol).map(|value| value.to_uppercase());
    let hash = empty_to_none(query.hash);
    let q = empty_to_none(query.q).map(|value| format!("%{value}%"));
    let with_methods = query.with_methods == Some(1);
    let with_script = query.with_script == Some(1);
    let with_token = query.with_token == Some(1);

    let filter = ContractFilter {
        chain_id,
        symbol: symbol.as_deref(),
        hash: hash.as_deref(),
        q: q.as_deref(),
        with_script,
        with_methods,
        with_token,
    };
    let rows = list_contracts(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    let contracts = rows
        .iter()
        .map(contract_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ContractListResponse {
        total_results: None,
        contracts,
        next_cursor,
    }))
}

pub(crate) async fn contract_method_histories(
    State(state): State<ApiState>,
    Query(query): Query<ContractMethodHistoryListQuery>,
) -> Result<Json<ContractMethodHistoryListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = nonnegative_offset(query.offset)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = ContractMethodHistoryOrderBy::from_api_param(order_by_param.as_deref())
        .ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let symbol = empty_to_none(query.symbol).map(|value| value.to_uppercase());
    let hash = empty_to_none(query.hash);
    let date_less = parse_optional_i64(empty_to_none(query.date_less).as_deref(), "date_less")?;
    let date_greater =
        parse_optional_i64(empty_to_none(query.date_greater).as_deref(), "date_greater")?;

    let filter = ContractMethodHistoryFilter {
        chain_id,
        symbol: symbol.as_deref(),
        hash: hash.as_deref(),
        date_less,
        date_greater,
    };
    let rows =
        list_contract_method_histories(&state.pool, &filter, order_by, direction, limit, offset)
            .await?;

    let total_results = if query.with_total == Some(1) {
        Some(count_contract_method_histories(&state.pool, &filter).await?)
    } else {
        None
    };

    let contract_method_histories = rows
        .iter()
        .map(|row| {
            Ok(ContractMethodHistoryResponse {
                contract: contract_from_row(row)?,
                date: Some(row.get::<i64, _>("timestamp_unix_seconds").to_string()),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(Json(ContractMethodHistoryListResponse {
        total_results,
        contract_method_histories,
    }))
}

pub(crate) async fn platforms(
    State(state): State<ApiState>,
    Query(query): Query<PlatformListQuery>,
) -> Result<Json<PlatformListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = nonnegative_offset(query.offset)?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = PlatformOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let name = empty_to_none(query.name);
    let with_external = query.with_external == Some(1);
    let with_interops = query.with_interops == Some(1);
    let with_token = query.with_token == Some(1);
    let with_creation_event = query.with_creation_event == Some(1);

    let filter = PlatformFilter {
        name: name.as_deref(),
        with_external,
        with_interops,
        with_token,
        with_creation_event,
    };
    let rows = list_platforms(&state.pool, &filter, order_by, direction, limit, offset).await?;

    let total_results = if query.with_total == Some(1) {
        Some(count_platforms(&state.pool, name.as_deref()).await?)
    } else {
        None
    };

    Ok(Json(PlatformListResponse {
        total_results,
        platforms: rows.iter().map(platform_from_row).collect(),
    }))
}

pub(crate) async fn nfts(
    State(state): State<ApiState>,
    Query(query): Query<NftListQuery>,
) -> Result<Json<NftListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = NftOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let creator = empty_to_none(query.creator);
    let owner = empty_to_none(query.owner);
    let contract_hash = empty_to_none(query.contract_hash);
    let name = empty_to_none(query.name).map(|value| format!("%{value}%"));
    let q_tokens: Option<Vec<String>> =
        empty_to_none(query.q).map(|value| value.split_whitespace().map(str::to_owned).collect());
    let symbol = empty_to_none(query.symbol).map(|value| value.to_uppercase());
    let token_id = empty_to_none(query.token_id);
    let series_id = empty_to_none(query.series_id);
    let status = empty_to_none(query.status).unwrap_or_else(|| "all".to_owned());
    if !matches!(status.as_str(), "all" | "active" | "infused") {
        return Err(ApiError::BadRequest(format!(
            "unsupported status '{status}'"
        )));
    }

    let filter = NftFilter {
        chain_id,
        creator: creator.as_deref(),
        contract_hash: contract_hash.as_deref(),
        name: name.as_deref(),
        q_tokens: q_tokens.as_deref(),
        symbol: symbol.as_deref(),
        token_id: token_id.as_deref(),
        series_id: series_id.as_deref(),
        status: status.as_str(),
        owner: owner.as_deref(),
    };
    let rows = list_nfts(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    let nfts = rows
        .iter()
        .map(nft_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(NftListResponse {
        total_results: None,
        nfts,
        next_cursor,
    }))
}

pub(crate) async fn series(
    State(state): State<ApiState>,
    Query(query): Query<SeriesListQuery>,
) -> Result<Json<SeriesListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by = SeriesOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let id = parse_optional_i32(empty_to_none(query.id).as_deref(), "id")?;
    let series_id = empty_to_none(query.series_id);
    let creator = empty_to_none(query.creator);
    let name = empty_to_none(query.name).map(|value| format!("%{value}%"));
    let q = empty_to_none(query.q).map(|value| format!("%{value}%"));
    let contract = empty_to_none(query.contract);
    let symbol = empty_to_none(query.symbol).map(|value| value.to_uppercase());
    let token_id = empty_to_none(query.token_id);

    let filter = SeriesFilter {
        chain_id,
        id,
        series_id: series_id.as_deref(),
        creator: creator.as_deref(),
        name: name.as_deref(),
        q: q.as_deref(),
        contract: contract.as_deref(),
        symbol: symbol.as_deref(),
        token_id: token_id.as_deref(),
    };
    let rows = list_series(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    Ok(Json(SeriesListResponse {
        total_results: None,
        series: rows.iter().map(series_from_row).collect(),
        next_cursor,
    }))
}

pub(crate) async fn organizations(
    State(state): State<ApiState>,
    Query(query): Query<OrganizationListQuery>,
) -> Result<Json<OrganizationListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by =
        OrganizationOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let organization_id = empty_to_none(query.organization_id);
    let organization_id_partial =
        empty_to_none(query.organization_id_partial).map(|value| format!("%{value}%"));
    let organization_name = empty_to_none(query.organization_name);
    let organization_name_partial =
        empty_to_none(query.organization_name_partial).map(|value| format!("%{value}%"));
    let q = empty_to_none(query.q).map(|value| format!("%{value}%"));
    let with_address = query.with_address == Some(1);

    let filter = OrganizationFilter {
        organization_id: organization_id.as_deref(),
        organization_id_partial: organization_id_partial.as_deref(),
        organization_name: organization_name.as_deref(),
        organization_name_partial: organization_name_partial.as_deref(),
        q: q.as_deref(),
    };
    let rows =
        list_organizations(&state.pool, &filter, order_by, direction, limit + 1, offset).await?;

    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    Ok(Json(OrganizationListResponse {
        total_results: None,
        organizations: rows
            .iter()
            .map(|row| organization_from_row(row, with_address))
            .collect(),
        next_cursor,
    }))
}

pub(crate) async fn event_kinds(
    State(state): State<ApiState>,
    Query(query): Query<EventKindListQuery>,
) -> Result<Json<EventKindListResponse>, ApiError> {
    let limit = clamp_limit(query.limit);
    let offset = OffsetCursor::parse_optional(query.cursor)?;
    let chain = empty_to_none(query.chain);
    let chain_id = match chain.as_deref() {
        Some(chain) => Some(resolve_chain_id_by_name(&state.pool, chain).await?),
        None => None,
    };
    let order_by_param = empty_to_none(query.order_by);
    let order_by =
        EventKindOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let event_kind = empty_to_none(query.event_kind);
    let with_total = query.with_total == Some(1);

    let rows = list_event_kinds(
        &state.pool,
        chain_id,
        event_kind.as_deref(),
        order_by,
        direction,
        limit + 1,
        offset,
    )
    .await?;
    let total_results = if with_total {
        Some(count_event_kinds(&state.pool, chain_id, event_kind.as_deref()).await?)
    } else {
        None
    };
    let (rows, next_cursor) = trim_offset_rows(rows, limit, offset)?;
    Ok(Json(EventKindListResponse {
        total_results,
        event_kinds: rows
            .into_iter()
            .map(|row| EventKindResponse { name: row.name })
            .collect(),
        next_cursor,
    }))
}

pub(crate) async fn event_kinds_with_events(
    State(state): State<ApiState>,
    Query(query): Query<EventKindListQuery>,
) -> Result<Json<EventKindListResponse>, ApiError> {
    let chain = empty_to_none(query.chain);
    let chain_id = match chain.as_deref() {
        Some(chain) => Some(resolve_chain_id_by_name(&state.pool, chain).await?),
        None => None,
    };
    let rows = list_event_kinds_with_events(&state.pool, chain_id).await?;

    Ok(Json(EventKindListResponse {
        total_results: (query.with_total == Some(1)).then_some(rows.len() as i64),
        event_kinds: rows
            .into_iter()
            .map(|row| EventKindResponse { name: row.name })
            .collect(),
        next_cursor: None,
    }))
}

pub(crate) async fn overview_stats(
    State(state): State<ApiState>,
    Query(query): Query<OverviewStatsQuery>,
) -> Result<Json<OverviewStatsResponse>, ApiError> {
    let chain = query_chain(query.chain, state.chain.as_str());
    let include_burned = query.include_burned.unwrap_or(0);
    let include_legacy_transactions = query.include_legacy_transactions.unwrap_or(1);
    if !matches!(include_burned, 0 | 1) {
        return Err(ApiError::BadRequest(
            "include_burned must be 0 or 1".to_owned(),
        ));
    }
    if !matches!(include_legacy_transactions, 0 | 1) {
        return Err(ApiError::BadRequest(
            "include_legacy_transactions must be 0 or 1".to_owned(),
        ));
    }
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await.ok();
    let cache_key = (chain.clone(), include_legacy_transactions);
    let cached = {
        let guard = state
            .overview_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .get(&cache_key)
            .filter(|(stored_at, _)| stored_at.elapsed() < OVERVIEW_CACHE_TTL)
            .map(|(_, counts)| counts.clone())
    };
    let counts = match cached {
        Some(counts) => counts,
        None => {
            // Single-flight: hold the flight lock so only one request runs the
            // expensive full-table count on a cold/expired cache; concurrent callers
            // wait here, then re-read the now-fresh cache instead of all recomputing.
            let _flight = state.overview_flight.lock().await;
            let fresh = {
                let guard = state
                    .overview_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard
                    .get(&cache_key)
                    .filter(|(stored_at, _)| stored_at.elapsed() < OVERVIEW_CACHE_TTL)
                    .map(|(_, counts)| counts.clone())
            };
            match fresh {
                Some(counts) => counts,
                None => {
                    let counts = overview_counts(
                        &state.pool,
                        chain.as_str(),
                        chain_id,
                        include_legacy_transactions,
                    )
                    .await?;
                    state
                        .overview_cache
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(cache_key, (Instant::now(), counts.clone()));
                    counts
                }
            }
        }
    };
    let nfts_total = if include_burned == 1 {
        counts.nfts_burned_total + counts.nfts_unburned_total
    } else {
        counts.nfts_unburned_total
    };

    Ok(Json(OverviewStatsResponse {
        chain,
        include_burned,
        include_legacy_transactions,
        transactions_total: counts.transactions_total,
        tokens_total: counts.tokens_total,
        nfts_total,
        nfts_unburned_total: counts.nfts_unburned_total,
        nfts_burned_total: counts.nfts_burned_total,
        contracts_total: counts.contracts_total,
        addresses_total: counts.addresses_total,
        nft_owners_total: counts.nft_owners_total,
        soul_masters_total: counts.soul_masters_total,
    }))
}

pub(crate) async fn staking_stats(
    State(state): State<ApiState>,
    Query(query): Query<StakingStatsQuery>,
) -> Result<Json<StakingStatsResponse>, ApiError> {
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let daily_limit = query.daily_limit.unwrap_or(0).clamp(0, 20_000);
    let monthly_limit = query.monthly_limit.unwrap_or(0).clamp(0, 20_000);
    let daily_rows = list_staking_dailies(
        &state.pool,
        chain_id,
        (daily_limit > 0).then_some(daily_limit),
    )
    .await?;
    let mut daily = daily_rows
        .iter()
        .map(|row| staking_daily_from_row(row, chain.as_str() == "main"))
        .collect::<Vec<_>>();

    let monthly_rows = list_soul_masters_monthlies(
        &state.pool,
        chain_id,
        (monthly_limit > 0).then_some(monthly_limit),
    )
    .await?;
    let monthly = monthly_rows
        .iter()
        .map(|row| SoulMastersMonthlyStatResponse {
            month_unix_seconds: row.get("month_unix_seconds"),
            masters_count: row.get("masters_count"),
            captured_at_unix_seconds: row.get("captured_at_unix_seconds"),
            source: row.get("source"),
        })
        .collect::<Vec<_>>();
    let latest_daily = daily.last_mut();
    let latest_staking_ratio = latest_daily.as_ref().map(|item| item.staking_ratio);
    let latest_staking_percent = latest_daily.as_ref().map(|item| item.staking_percent);
    let latest_staked_soul_raw = latest_daily
        .as_ref()
        .and_then(|item| item.staked_soul_raw.clone());
    let latest_soul_supply_raw = latest_daily
        .as_ref()
        .and_then(|item| item.soul_supply_raw.clone());
    let latest_stakers_count = latest_daily.as_ref().map(|item| item.stakers_count);
    let latest_masters_count = latest_daily.as_ref().map(|item| item.masters_count);

    Ok(Json(StakingStatsResponse {
        chain,
        daily_limit,
        monthly_limit,
        daily_points_total: daily.len() as i64,
        monthly_points_total: monthly.len() as i64,
        first_daily_date_unix_seconds: daily.first().map(|item| item.date_unix_seconds),
        latest_daily_date_unix_seconds: daily.last().map(|item| item.date_unix_seconds),
        first_month_unix_seconds: monthly.first().map(|item| item.month_unix_seconds),
        latest_month_unix_seconds: monthly.last().map(|item| item.month_unix_seconds),
        latest_staking_ratio,
        latest_staking_percent,
        latest_staked_soul_raw,
        latest_soul_supply_raw,
        latest_stakers_count,
        latest_masters_count,
        daily,
        monthly,
    }))
}

pub(crate) async fn address_stats(
    State(state): State<ApiState>,
    Query(query): Query<AddressStatsQuery>,
) -> Result<Json<AddressStatsResponse>, ApiError> {
    let chain = query_chain(query.chain, state.chain.as_str());
    let chain_id = resolve_chain_id_by_name(&state.pool, &chain).await?;
    let daily_limit = query.daily_limit.unwrap_or(0).clamp(0, 20_000);
    let rows = new_address_dailies(&state.pool, chain_id, daily_limit).await?;
    let daily = rows
        .iter()
        .map(|row| NewAddressDailyStatResponse {
            date_unix_seconds: row.get("day_unix_seconds"),
            new_addresses_count: row.get("new_addresses_count"),
            cumulative_addresses_count: row.get("cumulative_addresses_count"),
        })
        .collect::<Vec<_>>();

    Ok(Json(AddressStatsResponse {
        chain,
        daily_limit,
        new_addresses_points_total: daily.len() as i64,
        first_new_addresses_date_unix_seconds: daily.first().map(|item| item.date_unix_seconds),
        latest_new_addresses_date_unix_seconds: daily.last().map(|item| item.date_unix_seconds),
        latest_new_addresses_count: daily.last().map(|item| item.new_addresses_count),
        latest_cumulative_addresses_count: daily.last().map(|item| item.cumulative_addresses_count),
        new_addresses_daily: daily,
    }))
}

pub(crate) async fn searches(
    State(state): State<ApiState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<SearchListResponse>, ApiError> {
    let value = normalized_required_path("value", query.value)?;
    if value.len() < 3 {
        return Err(ApiError::BadRequest(
            "value must contain at least 3 characters".to_owned(),
        ));
    }
    let value_lower = value.to_lowercase();
    let existence = search_existence(&state.pool, value.as_str(), value_lower.as_str()).await?;
    let result = vec![
        SearchResponse {
            endpoint_name: "addresses".to_owned(),
            endpoint_parameter: "address".to_owned(),
            found: existence.addresses,
        },
        SearchResponse {
            endpoint_name: "blocks".to_owned(),
            endpoint_parameter: "hash".to_owned(),
            found: existence.blocks,
        },
        SearchResponse {
            endpoint_name: "chains".to_owned(),
            endpoint_parameter: "chain".to_owned(),
            found: existence.chains,
        },
        SearchResponse {
            endpoint_name: "contracts".to_owned(),
            endpoint_parameter: "hash".to_owned(),
            found: existence.contracts,
        },
        SearchResponse {
            endpoint_name: "organizations".to_owned(),
            endpoint_parameter: "organization_name".to_owned(),
            found: existence.organizations,
        },
        SearchResponse {
            endpoint_name: "tokens".to_owned(),
            endpoint_parameter: "symbol".to_owned(),
            found: existence.tokens,
        },
        SearchResponse {
            endpoint_name: "transactions".to_owned(),
            endpoint_parameter: "hash".to_owned(),
            found: existence.transactions,
        },
    ];

    Ok(Json(SearchListResponse { result }))
}

pub(crate) async fn rejected_transactions(
    State(state): State<ApiState>,
    Query(query): Query<RejectedTransactionQuery>,
) -> Result<Json<RejectedTransactionListResponse>, ApiError> {
    let hash = normalized_required_path(
        "hash",
        query
            .hash
            .ok_or_else(|| ApiError::BadRequest("hash is required".to_owned()))?,
    )?
    .to_uppercase();
    let chain = query_chain(query.chain, state.chain.as_str());
    let _ = query.capture.unwrap_or_default();

    let canonical_exists =
        rejected_transaction_canonical_exists(&state.pool, hash.as_str(), chain.as_str()).await?;

    if canonical_exists {
        return Ok(Json(RejectedTransactionListResponse {
            rejected_transactions: Vec::new(),
        }));
    }

    let rows =
        list_rejected_transaction_candidates(&state.pool, hash.as_str(), chain.as_str()).await?;

    Ok(Json(RejectedTransactionListResponse {
        rejected_transactions: rows.iter().map(rejected_transaction_from_row).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/raw-blocks/{height}",
    tag = "blocks",
    params(("height" = u64, Path, description = "Block height.")),
    responses(
        (status = 404, description = "Raw block archive is not available.", body = ErrorResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn raw_block_by_height(
    State(_state): State<ApiState>,
    Path(height): Path<u64>,
) -> Result<Json<RawBlockResponse>, ApiError> {
    Err(ApiError::NotFound(format!(
        "raw block {height} is not available"
    )))
}

pub(crate) async fn transaction_legacy(
    State(state): State<ApiState>,
    Query(mut query): Query<TransactionListQuery>,
) -> Result<Json<TransactionListResponse>, ApiError> {
    query.limit = Some(1);
    query.cursor = None;
    Ok(Json(load_transactions(&state, query).await?))
}

#[utoipa::path(
    get,
    path = "/api/v1/blocks/{height}",
    tag = "blocks",
    params(("height" = String, Path, description = "Block height or hash.")),
    responses(
        (status = 200, description = "SQL-first block projection.", body = BlockResponse),
        (status = 404, description = "Block projection not found.", body = ErrorResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn block_by_height(
    State(state): State<ApiState>,
    Path(block_id): Path<String>,
) -> Result<Json<BlockResponse>, ApiError> {
    let block_id = normalize_block_id(block_id);
    let height = block_id.parse::<i64>().ok();
    let hash = if height.is_none() {
        Some(block_id.as_str())
    } else {
        None
    };
    let chain_id = resolve_chain_id(&state).await?;
    let row = block_detail(&state.pool, chain_id, height, hash)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("block {block_id} was not found")))?;

    Ok(Json(BlockResponse {
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
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/transactions",
    tag = "transactions",
    params(TransactionListQuery),
    responses(
        (status = 200, description = "Bounded transaction list. Hash filter can return multiple occurrences.", body = TransactionListResponse),
        (status = 400, description = "Invalid cursor or query parameter.", body = ErrorResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn transactions(
    State(state): State<ApiState>,
    Query(query): Query<TransactionListQuery>,
) -> Result<Json<TransactionListResponse>, ApiError> {
    Ok(Json(load_transactions(&state, query).await?))
}

#[utoipa::path(
    get,
    path = "/api/v1/transactions/{hash}",
    tag = "transactions",
    params(
        ("hash" = String, Path, description = "Exact transaction hash. Legacy history can contain duplicate occurrences."),
        TransactionDetailQuery
    ),
    responses(
        (status = 200, description = "Transaction detail when the hash resolves to one occurrence or the query disambiguates it.", body = TransactionDetailResponse),
        (status = 400, description = "Only one of block_height/index was supplied.", body = ErrorResponse),
        (status = 404, description = "Transaction not found.", body = ErrorResponse),
        (status = 409, description = "Hash resolves to multiple occurrences and needs explicit block/index identity.", body = AmbiguousTransactionHashResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn transaction_by_hash(
    State(state): State<ApiState>,
    Path(hash): Path<String>,
    Query(query): Query<TransactionDetailQuery>,
) -> Result<Json<TransactionDetailResponse>, ApiError> {
    let hash = normalized_required_path("hash", hash)?;

    match (query.block_height, query.index) {
        (Some(block_height), Some(index)) => {
            let transaction =
                load_transaction_by_hash_block_index(&state, &hash, block_height, index)
                    .await?
                    .ok_or_else(|| {
                        ApiError::NotFound(format!(
                            "transaction {hash} at block {block_height} index {index} was not found"
                        ))
                    })?;
            load_transaction_detail(&state, transaction).await.map(Json)
        }
        (Some(_), None) | (None, Some(_)) => Err(ApiError::BadRequest(
            "block_height and index must be supplied together".to_owned(),
        )),
        (None, None) => {
            let (occurrence_count, matches) =
                load_transaction_occurrences_by_hash(&state, &hash).await?;
            match occurrence_count {
                0 => Err(ApiError::NotFound(format!(
                    "transaction {hash} was not found"
                ))),
                1 => {
                    let transaction = load_single_transaction_by_hash(&state, &hash)
                        .await?
                        .ok_or_else(|| {
                            ApiError::NotFound(format!("transaction {hash} was not found"))
                        })?;
                    load_transaction_detail(&state, transaction).await.map(Json)
                }
                _ => Err(ApiError::AmbiguousTransactionHash {
                    hash,
                    occurrence_count,
                    matches,
                }),
            }
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/blocks/{height}/transactions/{index}",
    tag = "transactions",
    params(
        ("height" = i64, Path, description = "Block height."),
        ("index" = i32, Path, description = "Transaction index inside the block.")
    ),
    responses(
        (status = 200, description = "Occurrence-safe transaction detail by block height and transaction index.", body = TransactionDetailResponse),
        (status = 404, description = "Transaction occurrence not found.", body = ErrorResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn transaction_by_block_index(
    State(state): State<ApiState>,
    Path((height, index)): Path<(i64, i32)>,
) -> Result<Json<TransactionDetailResponse>, ApiError> {
    let transaction = load_transaction_by_block_index(&state, height, index)
        .await?
        .ok_or_else(|| {
            ApiError::NotFound(format!(
                "transaction at block {height} index {index} was not found"
            ))
        })?;

    load_transaction_detail(&state, transaction).await.map(Json)
}

#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    params(EventListQuery),
    responses(
        (status = 200, description = "Bounded event list.", body = EventListResponse),
        (status = 400, description = "Invalid cursor or query parameter.", body = ErrorResponse),
        (status = 500, description = "Database error.", body = ErrorResponse)
    )
)]
pub(crate) async fn events(
    State(state): State<ApiState>,
    Query(query): Query<EventListQuery>,
) -> Result<Json<EventListResponse>, ApiError> {
    Ok(Json(load_events(&state, query).await?))
}

pub(crate) async fn load_transactions(
    state: &ApiState,
    query: TransactionListQuery,
) -> Result<TransactionListResponse, ApiError> {
    let limit = clamp_limit(query.limit);
    let hash = empty_to_none(query.hash);
    let hash_partial = empty_to_none(query.hash_partial).map(|value| format!("%{value}%"));
    let block_hash = empty_to_none(query.block_hash);
    let address = empty_to_none(query.address);
    // The global and address-scoped transaction lists page over different id
    // spaces (tx.id vs address_tx.id), so they use distinct cursor kinds: a cursor
    // minted on one list cannot be replayed against the other.
    let cursor_kind = if address.is_some() {
        "tx-address"
    } else {
        "tx"
    };
    let cursor = PageCursor::parse_optional(query.cursor, cursor_kind)?;
    let state_filter = empty_to_none(query.state);
    let state_id = resolve_transaction_state_id(&state.pool, state_filter.as_deref()).await?;
    let q = empty_to_none(query.q);
    let chain = empty_to_none(query.chain);
    let chain_id = match chain.as_deref() {
        Some(chain) => Some(resolve_chain_id_by_name(&state.pool, chain).await?),
        None => None,
    };
    let date_greater =
        parse_optional_i64(empty_to_none(query.date_greater).as_deref(), "date_greater")?;
    let date_less = parse_optional_i64(empty_to_none(query.date_less).as_deref(), "date_less")?;
    let order_by_param = empty_to_none(query.order_by);
    let order_by =
        TransactionOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
            ApiError::BadRequest(format!(
                "unsupported order_by '{}'",
                order_by_param.as_deref().unwrap_or_default()
            ))
        })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let filter = TransactionFilter {
        hash: hash.as_deref(),
        hash_partial: hash_partial.as_deref(),
        block_height: query.block_height,
        block_hash: block_hash.as_deref(),
        chain_id,
        state_id,
        q: q.as_deref(),
        date_greater,
        date_less,
    };
    let page = TransactionPage {
        order_by,
        direction,
        cursor_sort_value: cursor.as_ref().map(|cursor| cursor.sort_value),
        cursor_id: cursor.as_ref().map(|cursor| cursor.id),
        limit,
    };
    let with_neighbors = query.with_neighbors == Some(1);
    let with_events = query.with_events == Some(1);
    let with_event_data = query.with_event_data == Some(1);
    let with_script = query.with_script == Some(1);

    let rows = if let Some(address) = address {
        match address_id_by_address(&state.pool, &address).await? {
            Some(address_id) => {
                if hash.is_none()
                    && hash_partial.is_none()
                    && query.block_height.is_none()
                    && block_hash.is_none()
                    && chain_id.is_none()
                    && state_filter.is_none()
                    && q.is_none()
                    && date_greater.is_none()
                    && date_less.is_none()
                {
                    list_transactions_for_address_timeline(&state.pool, address_id, &page).await?
                } else {
                    list_transactions_for_filtered_address(&state.pool, address_id, &filter, &page)
                        .await?
                }
            }
            // An unknown address has no transactions; skip the query.
            None => Vec::new(),
        }
    } else {
        list_transactions_global(&state.pool, &filter, &page).await?
    };

    let (rows, next_cursor) = trim_page_rows(rows, limit, cursor_kind)?;
    let mut transactions = rows.iter().map(transaction_from_row).collect::<Vec<_>>();
    if !with_script {
        for transaction in &mut transactions {
            transaction.script_raw = None;
        }
    }
    if with_neighbors && hash.is_some() && rows.len() == 1 {
        let row = &rows[0];
        let (previous_hash, next_hash) = transaction_neighbors(
            &state.pool,
            row.get("id"),
            row.get("timestamp_unix_seconds"),
            chain_id,
        )
        .await?;
        if let Some(transaction) = transactions.first_mut() {
            transaction.previous_hash = previous_hash;
            transaction.next_hash = next_hash;
        }
    }
    if with_events {
        attach_events_to_transactions(&state.pool, &mut transactions, with_event_data).await?;
    }

    Ok(TransactionListResponse {
        total_results: None,
        transactions,
        next_cursor,
    })
}

pub(crate) async fn load_transactions_by_block_ids(
    pool: &PgPool,
    block_ids: &[i32],
    with_events: bool,
    with_event_data: bool,
    with_script: bool,
) -> Result<HashMap<i32, Vec<TransactionResponse>>, ApiError> {
    if block_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = list_transactions_by_block_ids(pool, block_ids).await?;

    let mut transactions = rows.iter().map(transaction_from_row).collect::<Vec<_>>();
    if !with_script {
        for transaction in &mut transactions {
            transaction.script_raw = None;
        }
    }
    if with_events {
        attach_events_to_transactions(pool, &mut transactions, with_event_data).await?;
    }

    let mut grouped: HashMap<i32, Vec<TransactionResponse>> = HashMap::new();
    for (row, transaction) in rows.iter().zip(transactions) {
        grouped
            .entry(row.get("tx_block_id"))
            .or_default()
            .push(transaction);
    }
    Ok(grouped)
}

pub(crate) async fn load_events(
    state: &ApiState,
    query: EventListQuery,
) -> Result<EventListResponse, ApiError> {
    let limit = clamp_limit(query.limit);
    let cursor = PageCursor::parse_optional(query.cursor, "event")?;
    let transaction_hash = empty_to_none(query.transaction_hash);
    let event_kind = empty_to_none(query.event_kind);
    let event_source = empty_to_none(query.event_source);
    let address = empty_to_none(query.address);
    let contract = empty_to_none(query.contract);
    let q = empty_to_none(query.q);
    let token_id = empty_to_none(query.token_id);
    let block_hash = empty_to_none(query.block_hash);
    let date_less = parse_optional_i64(empty_to_none(query.date_less).as_deref(), "date_less")?;
    let date_greater =
        parse_optional_i64(empty_to_none(query.date_greater).as_deref(), "date_greater")?;
    let date_day = parse_optional_i64(empty_to_none(query.date_day).as_deref(), "date_day")?;
    let event_kind_partial =
        empty_to_none(query.event_kind_partial).map(|value| format!("%{value}%"));
    let nft_name_partial = empty_to_none(query.nft_name_partial).map(|value| format!("%{value}%"));
    let nft_description_partial =
        empty_to_none(query.nft_description_partial).map(|value| format!("%{value}%"));
    let address_partial = empty_to_none(query.address_partial).map(|value| format!("%{value}%"));
    let chain = empty_to_none(query.chain);
    let chain_filter_id = match chain.as_deref() {
        Some(name) => Some(resolve_chain_id_by_name(&state.pool, name).await?),
        None => None,
    };
    let order_by_param = empty_to_none(query.order_by);
    let order_by = EventOrderBy::from_api_param(order_by_param.as_deref()).ok_or_else(|| {
        ApiError::BadRequest(format!(
            "unsupported order_by '{}'",
            order_by_param.as_deref().unwrap_or_default()
        ))
    })?;
    let direction = parse_sort_direction(query.order_direction.as_deref())?;
    let filter = EventFilter {
        transaction_hash: transaction_hash.as_deref(),
        block_height: query.block_height,
        event_kind: event_kind.as_deref(),
        event_source: event_source.as_deref(),
        contract: contract.as_deref(),
        q: q.as_deref(),
        event_id: query.event_id,
        show_nsfw: query.with_nsfw == Some(1),
        show_blacklisted: query.with_blacklisted == Some(1),
        token_id: token_id.as_deref(),
        block_hash: block_hash.as_deref(),
        date_less,
        date_greater,
        date_day,
        event_kind_partial: event_kind_partial.as_deref(),
        nft_name_partial: nft_name_partial.as_deref(),
        nft_description_partial: nft_description_partial.as_deref(),
        address_partial: address_partial.as_deref(),
        chain_id: chain_filter_id,
    };
    let page = EventPage {
        order_by,
        direction,
        cursor_sort_value: cursor.as_ref().map(|cursor| cursor.sort_value),
        cursor_id: cursor.as_ref().map(|cursor| cursor.id),
        limit,
    };

    let rows = if let Some(address) = address.as_deref() {
        match address_id_by_address(&state.pool, address).await? {
            Some(address_id) => {
                list_events_by_address(
                    &state.pool,
                    state.chain.as_str(),
                    address_id,
                    &filter,
                    &page,
                )
                .await?
            }
            // An unknown address has no events; skip the query.
            None => Vec::new(),
        }
    } else {
        let chain_id = match chain_filter_id {
            Some(id) => id,
            None => resolve_chain_id(state).await?,
        };
        let chain_name = chain.as_deref().unwrap_or_else(|| state.chain.as_str());
        list_events_global(&state.pool, chain_id, chain_name, &filter, &page).await?
    };

    let (rows, next_cursor) = trim_page_rows(rows, limit, "event")?;
    let events = events_from_rows(&state.pool, &rows, query.with_event_data == Some(1)).await?;

    Ok(EventListResponse {
        total_results: None,
        events,
        next_cursor,
    })
}

pub(crate) async fn load_transaction_by_block_index(
    state: &ApiState,
    block_height: i64,
    index: i32,
) -> Result<Option<TransactionResponse>, ApiError> {
    let row =
        transaction_row_by_block_index(&state.pool, state.chain.as_str(), block_height, index)
            .await?;

    Ok(row.as_ref().map(transaction_from_row))
}

pub(crate) async fn load_transaction_by_hash_block_index(
    state: &ApiState,
    hash: &str,
    block_height: i64,
    index: i32,
) -> Result<Option<TransactionResponse>, ApiError> {
    let row = transaction_by_hash_block_index(
        &state.pool,
        state.chain.as_str(),
        hash,
        block_height,
        index,
    )
    .await?;

    Ok(row.as_ref().map(transaction_from_row))
}

pub(crate) async fn load_single_transaction_by_hash(
    state: &ApiState,
    hash: &str,
) -> Result<Option<TransactionResponse>, ApiError> {
    let row = single_transaction_by_hash(&state.pool, state.chain.as_str(), hash).await?;

    Ok(row.as_ref().map(transaction_from_row))
}

pub(crate) async fn load_transaction_occurrences_by_hash(
    state: &ApiState,
    hash: &str,
) -> Result<(i64, Vec<TransactionOccurrenceResponse>), ApiError> {
    let occurrence_count =
        transaction_occurrence_count(&state.pool, state.chain.as_str(), hash).await?;
    let rows = list_transaction_occurrences(&state.pool, state.chain.as_str(), hash).await?;

    Ok((
        occurrence_count,
        rows.iter().map(transaction_occurrence_from_row).collect(),
    ))
}

pub(crate) async fn load_transaction_detail(
    state: &ApiState,
    transaction: TransactionResponse,
) -> Result<TransactionDetailResponse, ApiError> {
    let transaction_id = parse_i32_id("transaction_id", &transaction.transaction_id)?;
    let signatures = load_signatures(&state.pool, transaction_id).await?;
    let events = load_transaction_events(&state.pool, transaction_id).await?;

    Ok(TransactionDetailResponse {
        transaction,
        signatures,
        events,
    })
}

pub(crate) async fn load_signatures(
    pool: &PgPool,
    transaction_id: i32,
) -> Result<Vec<SignatureResponse>, ApiError> {
    let rows = list_signatures(pool, transaction_id).await?;

    Ok(rows
        .iter()
        .map(|row| SignatureResponse {
            signature_index: row.get("signature_index"),
            kind: row.get("kind"),
            data: row.get("data"),
        })
        .collect())
}

pub(crate) async fn load_transaction_events(
    pool: &PgPool,
    transaction_id: i32,
) -> Result<Vec<EventResponse>, ApiError> {
    let mut grouped = load_events_by_transaction_ids(pool, &[transaction_id], true).await?;
    Ok(grouped.remove(&transaction_id).unwrap_or_default())
}

pub(crate) async fn attach_events_to_transactions(
    pool: &PgPool,
    transactions: &mut [TransactionResponse],
    with_event_data: bool,
) -> Result<(), ApiError> {
    let ids = transactions
        .iter()
        .map(|transaction| parse_i32_id("transaction_id", &transaction.transaction_id))
        .collect::<Result<Vec<_>, _>>()?;
    let mut grouped = load_events_by_transaction_ids(pool, &ids, with_event_data).await?;
    for transaction in transactions {
        let id = parse_i32_id("transaction_id", &transaction.transaction_id)?;
        transaction.events = Some(grouped.remove(&id).unwrap_or_default());
    }
    Ok(())
}

pub(crate) async fn load_events_by_transaction_ids(
    pool: &PgPool,
    transaction_ids: &[i32],
    with_event_data: bool,
) -> Result<HashMap<i32, Vec<EventResponse>>, ApiError> {
    if transaction_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = list_events_by_transaction_ids(pool, transaction_ids).await?;

    let token_symbols = collect_event_token_symbols(&rows);
    let tokens = load_event_tokens_by_symbols(pool, token_symbols).await?;
    let mut grouped: HashMap<i32, Vec<EventResponse>> = HashMap::new();
    for row in rows {
        let transaction_id = row.get("event_transaction_id");
        grouped
            .entry(transaction_id)
            .or_default()
            .push(event_from_row(&row, &tokens, with_event_data)?);
    }
    Ok(grouped)
}
