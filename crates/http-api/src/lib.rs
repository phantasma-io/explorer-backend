use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::middleware::from_fn_with_state;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use chrono::{DateTime, Utc};
use explorer_db::{
    AddressFilter, AddressOrderBy, BlockFilter, BlockOrderBy, ContractFilter,
    ContractMethodHistoryFilter, ContractMethodHistoryOrderBy, ContractOrderBy, DbError,
    EventFilter, EventKindOrderBy, EventOrderBy, EventPage, HistoryPriceFilter,
    HistoryPriceOrderBy, NftFilter, NftOrderBy, OracleFilter, OracleOrderBy, OrganizationFilter,
    OrganizationOrderBy, OrganizationRow, OverviewCounts, PlatformFilter, PlatformOrderBy,
    RejectedTransactionRow, SeriesFilter, SeriesOrderBy, SortDirection, TokenFilter, TokenOrderBy,
    TransactionFilter, TransactionOrderBy, TransactionPage, ValidatorKindOrderBy,
    address_id_by_address, block_detail, chain_ids_by_name, check_health, circulating_soul_supply,
    count_chains, count_contract_method_histories, count_event_kinds, count_history_prices,
    count_oracles, count_platforms, count_validator_kinds, list_addresses, list_blocks,
    list_chains, list_contract_method_histories, list_contracts, list_event_kinds,
    list_event_kinds_with_events, list_event_tokens_by_symbols, list_events_by_address,
    list_events_by_transaction_ids, list_events_global, list_history_prices, list_nfts,
    list_oracles, list_organizations, list_platforms, list_rejected_transaction_candidates,
    list_series, list_signatures, list_soul_masters_monthlies, list_staking_dailies, list_tokens,
    list_transaction_occurrences, list_transactions_by_block_ids,
    list_transactions_for_address_timeline, list_transactions_for_filtered_address,
    list_transactions_global, list_validator_kinds, new_address_dailies, overview_counts,
    rejected_transaction_canonical_exists, search_existence, single_transaction_by_hash,
    transaction_by_hash_block_index, transaction_neighbors, transaction_occurrence_count,
    transaction_row_by_block_index, transaction_state_id_by_name,
};
use explorer_domain::ChainName;
use num_bigint::BigInt;
use phantasma_sdk::{Address as PhantasmaAddress, Ed25519Signature};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

mod handlers;
mod rate_limit;
mod support;

pub(crate) use handlers::*;
pub use rate_limit::RateLimiter;
pub(crate) use support::*;

/// Cache key for the overview counters: (chain name, include-legacy flag).
type OverviewCacheKey = (String, i32);

/// Time-to-live for cached overview counters. The dashboard counts come from
/// full-table aggregates that take several seconds; serving them from a short
/// cache keeps the endpoint responsive at the cost of slight staleness.
const OVERVIEW_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct ApiState {
    service_name: String,
    started_at: DateTime<Utc>,
    pool: PgPool,
    chain: ChainName,
    overview_cache: Arc<Mutex<HashMap<OverviewCacheKey, (Instant, OverviewCounts)>>>,
    /// Serializes the expensive overview recompute so a cold/expired cache triggers a
    /// single full-table count, not one per concurrent caller (single-flight).
    overview_flight: Arc<tokio::sync::Mutex<()>>,
}

impl ApiState {
    pub fn new(service_name: impl Into<String>, pool: PgPool, chain: ChainName) -> Self {
        Self {
            service_name: service_name.into(),
            started_at: Utc::now(),
            pool,
            chain,
            overview_cache: Arc::new(Mutex::new(HashMap::new())),
            overview_flight: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Phantasma Explorer Rust API",
        description = "SQL-first Rust Explorer API with occurrence-safe transaction lookup semantics."
    ),
    paths(
        health,
        version,
        block_by_height,
        raw_block_by_height,
        transactions,
        transaction_by_hash,
        transaction_by_block_index,
        events
    ),
    components(schemas(
        AddressRefResponse,
        AmbiguousTransactionHashResponse,
        BlockResponse,
        ContractRefResponse,
        ErrorResponse,
        EventListResponse,
        EventResponse,
        HealthResponse,
        RawBlockResponse,
        SignatureResponse,
        TransactionDetailResponse,
        TransactionListResponse,
        TransactionOccurrenceResponse,
        TransactionResponse,
        VersionResponse
    )),
    tags(
        (name = "system", description = "Service health, version, and API contract metadata."),
        (name = "blocks", description = "Block and raw block read models."),
        (name = "transactions", description = "Transaction lookup, occurrence-safe detail, and list APIs."),
        (name = "events", description = "Event list APIs backed by the SQL-first projection.")
    )
)]
struct ApiDoc;

#[derive(Debug, Serialize, ToSchema)]
struct HealthResponse {
    service: String,
    status: String,
    started_at: DateTime<Utc>,
    checked_at: DateTime<Utc>,
    database_ok: bool,
    database_server_version: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct VersionResponse {
    service: String,
    version: String,
    git_sha: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ErrorResponse {
    code: String,
    error: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct AmbiguousTransactionHashResponse {
    code: String,
    error: String,
    hash: String,
    occurrence_count: i64,
    returned_occurrences: usize,
    resolution_hint: String,
    matches: Vec<TransactionOccurrenceResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
struct RawBlockResponse {
    id: String,
    nexus: String,
    chain: String,
    height: String,
    hash: Option<String>,
    rpc_node: String,
    payload_json: Value,
    payload_bytes: i32,
    fetched_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BlockResponse {
    height: String,
    hash: String,
    previous_hash: Option<String>,
    protocol: Option<i32>,
    chain_address: Option<String>,
    validator_address: Option<String>,
    date: Option<String>,
    reward: Option<String>,
    transaction_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    transactions: Option<Vec<TransactionResponse>>,
}

#[derive(Debug, Deserialize)]
struct BlockListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    id: Option<String>,
    hash: Option<String>,
    hash_partial: Option<String>,
    height: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    date_less: Option<String>,
    date_greater: Option<String>,
    with_transactions: Option<i32>,
    with_events: Option<i32>,
    with_event_data: Option<i32>,
    with_script: Option<i32>,
}

#[derive(Debug, Serialize)]
struct BlockListResponse {
    total_results: Option<i64>,
    blocks: Vec<BlockResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    symbol: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    with_price: Option<i32>,
    with_logo: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PriceResponse {
    usd: Option<f64>,
    eur: Option<f64>,
    gbp: Option<f64>,
    jpy: Option<f64>,
    cad: Option<f64>,
    aud: Option<f64>,
    cny: Option<f64>,
    rub: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenLogoResponse {
    r#type: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenResponse {
    name: Option<String>,
    symbol: Option<String>,
    fungible: bool,
    transferable: bool,
    finite: bool,
    divisible: bool,
    fuel: bool,
    stakable: bool,
    fiat: bool,
    swappable: bool,
    burnable: bool,
    mintable: bool,
    decimals: i32,
    current_supply: Option<String>,
    current_supply_raw: Option<String>,
    max_supply: Option<String>,
    max_supply_raw: Option<String>,
    burned_supply: Option<String>,
    burned_supply_raw: Option<String>,
    script_raw: Option<String>,
    price: Option<PriceResponse>,
    token_logos: Option<Vec<TokenLogoResponse>>,
}

#[derive(Debug, Serialize)]
struct TokenListResponse {
    total_results: Option<i64>,
    tokens: Vec<TokenResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AddressListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    chain: Option<String>,
    address: Option<String>,
    address_name: Option<String>,
    address_partial: Option<String>,
    symbol: Option<String>,
    organization_name: Option<String>,
    validator_kind: Option<String>,
    with_storage: Option<i32>,
    with_stakes: Option<i32>,
    with_balance: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddressStorageResponse {
    available: i64,
    used: i64,
    avatar: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddressStakesResponse {
    amount: Option<String>,
    amount_raw: Option<String>,
    time: i64,
    unclaimed: Option<String>,
    unclaimed_raw: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChainRefResponse {
    chain_name: Option<String>,
    chain_height: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddressBalanceResponse {
    amount: Option<String>,
    amount_raw: Option<String>,
    chain: Option<ChainRefResponse>,
    token: Option<TokenResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddressResponse {
    address: Option<String>,
    address_name: Option<String>,
    validator_kind: Option<String>,
    stake: Option<String>,
    stake_raw: Option<String>,
    unclaimed: Option<String>,
    unclaimed_raw: Option<String>,
    storage: Option<AddressStorageResponse>,
    stakes: Option<AddressStakesResponse>,
    balances: Option<Vec<AddressBalanceResponse>>,
}

#[derive(Debug, Serialize)]
struct AddressListResponse {
    total_results: Option<i64>,
    addresses: Vec<AddressResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ContractListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    symbol: Option<String>,
    hash: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    with_methods: Option<i32>,
    with_script: Option<i32>,
    with_token: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractResponse {
    name: Option<String>,
    hash: Option<String>,
    symbol: Option<String>,
    compiler: Option<String>,
    create_date: Option<String>,
    r#type: Option<String>,
    address: Option<AddressResponse>,
    script_raw: Option<String>,
    token: Option<TokenResponse>,
    methods: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ContractListResponse {
    total_results: Option<i64>,
    contracts: Vec<ContractResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NftListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    creator: Option<String>,
    owner: Option<String>,
    contract_hash: Option<String>,
    name: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    symbol: Option<String>,
    token_id: Option<String>,
    series_id: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NftMetadataResponse {
    description: Option<String>,
    name: Option<String>,
    #[serde(rename = "imageURL")]
    image_url: Option<String>,
    #[serde(rename = "videoURL")]
    video_url: Option<String>,
    #[serde(rename = "infoURL")]
    info_url: Option<String>,
    rom: Option<String>,
    ram: Option<String>,
    mint_date: Option<String>,
    mint_number: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NftOwnerResponse {
    address: Option<String>,
    onchain_name: Option<String>,
    amount: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct SeriesResponse {
    id: i32,
    series_id: Option<String>,
    creator: Option<String>,
    chain: Option<String>,
    contract: Option<String>,
    symbol: Option<String>,
    created_unix_seconds: Option<i64>,
    current_supply: i32,
    max_supply: i32,
    mode_name: Option<String>,
    name: Option<String>,
    description: Option<String>,
    image: Option<String>,
    royalties: Option<String>,
    r#type: i32,
    attr_type_1: Option<String>,
    attr_value_1: Option<String>,
    attr_type_2: Option<String>,
    attr_value_2: Option<String>,
    attr_type_3: Option<String>,
    attr_value_3: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct InfusionResponse {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct InfusedIntoResponse {
    token_id: Option<String>,
    chain: Option<String>,
    contract: Option<ContractResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NftResponse {
    token_id: Option<String>,
    chain: Option<String>,
    symbol: Option<String>,
    creator_address: Option<String>,
    creator_onchain_name: Option<String>,
    owners: Option<Vec<NftOwnerResponse>>,
    contract: Option<ContractResponse>,
    nft_metadata: Option<NftMetadataResponse>,
    series: Option<SeriesResponse>,
    infusion: Option<Vec<InfusionResponse>>,
    infused_into: Option<InfusedIntoResponse>,
}

#[derive(Debug, Serialize)]
struct NftListResponse {
    total_results: Option<i64>,
    nfts: Vec<NftResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SeriesListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    id: Option<String>,
    series_id: Option<String>,
    creator: Option<String>,
    name: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    contract: Option<String>,
    symbol: Option<String>,
    token_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct SeriesListResponse {
    total_results: Option<i64>,
    series: Vec<SeriesResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrganizationListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    organization_id: Option<String>,
    organization_id_partial: Option<String>,
    organization_name: Option<String>,
    organization_name_partial: Option<String>,
    q: Option<String>,
    with_address: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OrganizationResponse {
    id: Option<String>,
    name: Option<String>,
    size: i64,
    address: Option<AddressResponse>,
}

#[derive(Debug, Serialize)]
struct OrganizationListResponse {
    total_results: Option<i64>,
    organizations: Vec<OrganizationResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EventKindListQuery {
    limit: Option<i64>,
    cursor: Option<String>,
    order_by: Option<String>,
    order_direction: Option<String>,
    event_kind: Option<String>,
    chain: Option<String>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct EventKindResponse {
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct EventKindListResponse {
    total_results: Option<i64>,
    event_kinds: Vec<EventKindResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OverviewStatsQuery {
    chain: Option<String>,
    include_burned: Option<i32>,
    include_legacy_transactions: Option<i32>,
}

#[derive(Debug, Serialize)]
struct OverviewStatsResponse {
    chain: String,
    include_burned: i32,
    include_legacy_transactions: i32,
    transactions_total: i64,
    tokens_total: i64,
    nfts_total: i64,
    nfts_unburned_total: i64,
    nfts_burned_total: i64,
    contracts_total: i64,
    addresses_total: i64,
    nft_owners_total: i64,
    soul_masters_total: i64,
}

#[derive(Debug, Deserialize)]
struct StakingStatsQuery {
    chain: Option<String>,
    daily_limit: Option<i64>,
    monthly_limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct StakingDailyStatResponse {
    date_unix_seconds: i64,
    staked_soul_raw: Option<String>,
    soul_supply_raw: Option<String>,
    stakers_count: i32,
    masters_count: i32,
    staking_ratio: f64,
    staking_percent: f64,
    captured_at_unix_seconds: i64,
    source: Option<String>,
}

#[derive(Debug, Serialize)]
struct SoulMastersMonthlyStatResponse {
    month_unix_seconds: i64,
    masters_count: i32,
    captured_at_unix_seconds: i64,
    source: Option<String>,
}

#[derive(Debug, Serialize)]
struct StakingStatsResponse {
    chain: String,
    daily_limit: i64,
    monthly_limit: i64,
    daily_points_total: i64,
    monthly_points_total: i64,
    first_daily_date_unix_seconds: Option<i64>,
    latest_daily_date_unix_seconds: Option<i64>,
    first_month_unix_seconds: Option<i64>,
    latest_month_unix_seconds: Option<i64>,
    latest_staking_ratio: Option<f64>,
    latest_staking_percent: Option<f64>,
    latest_staked_soul_raw: Option<String>,
    latest_soul_supply_raw: Option<String>,
    latest_stakers_count: Option<i32>,
    latest_masters_count: Option<i32>,
    daily: Vec<StakingDailyStatResponse>,
    monthly: Vec<SoulMastersMonthlyStatResponse>,
}

#[derive(Debug, Deserialize)]
struct AddressStatsQuery {
    chain: Option<String>,
    daily_limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct NewAddressDailyStatResponse {
    date_unix_seconds: i64,
    new_addresses_count: i64,
    cumulative_addresses_count: i64,
}

#[derive(Debug, Serialize)]
struct AddressStatsResponse {
    chain: String,
    daily_limit: i64,
    new_addresses_points_total: i64,
    first_new_addresses_date_unix_seconds: Option<i64>,
    latest_new_addresses_date_unix_seconds: Option<i64>,
    latest_new_addresses_count: Option<i64>,
    latest_cumulative_addresses_count: Option<i64>,
    new_addresses_daily: Vec<NewAddressDailyStatResponse>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    value: String,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    endpoint_name: String,
    endpoint_parameter: String,
    found: bool,
}

#[derive(Debug, Serialize)]
struct SearchListResponse {
    result: Vec<SearchResponse>,
}

#[derive(Debug, Deserialize)]
struct RejectedTransactionQuery {
    hash: Option<String>,
    chain: Option<String>,
    capture: Option<i32>,
}

#[derive(Debug, Serialize)]
struct RejectedTransactionResponse {
    hash: String,
    nexus: String,
    chain: String,
    block_height: Option<String>,
    block_hash: Option<String>,
    date: Option<String>,
    state: Option<String>,
    result: Option<String>,
    debug_comment: Option<String>,
    payload: Option<String>,
    script_raw: Option<String>,
    fee_raw: Option<String>,
    expiration: Option<String>,
    gas_price_raw: Option<String>,
    gas_limit_raw: Option<String>,
    sender: Option<String>,
    gas_payer: Option<String>,
    gas_target: Option<String>,
    canonical_status: Option<String>,
    captured_at: String,
    updated_at: String,
    rpc_response_json: Option<String>,
    block_response_json: Option<String>,
}

#[derive(Debug, Serialize)]
struct RejectedTransactionListResponse {
    rejected_transactions: Vec<RejectedTransactionResponse>,
}

#[derive(Debug, Deserialize)]
struct ChainListQuery {
    offset: Option<i64>,
    limit: Option<i64>,
    chain: Option<String>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct ChainListResponse {
    total_results: Option<i64>,
    chains: Vec<ChainRefResponse>,
}

#[derive(Debug, Deserialize)]
struct OracleListQuery {
    order_by: Option<String>,
    order_direction: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
    block_hash: Option<String>,
    block_height: Option<String>,
    chain: Option<String>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct OracleResponse {
    url: Option<String>,
    content: Option<String>,
}

#[derive(Debug, Serialize)]
struct OracleListResponse {
    total_results: Option<i64>,
    oracles: Vec<OracleResponse>,
}

#[derive(Debug, Deserialize)]
struct ValidatorKindListQuery {
    order_by: Option<String>,
    order_direction: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
    validator_kind: Option<String>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct ValidatorKindResponse {
    name: Option<String>,
}

#[derive(Debug, Serialize)]
struct ValidatorKindListResponse {
    total_results: Option<i64>,
    validator_kinds: Vec<ValidatorKindResponse>,
}

#[derive(Debug, Deserialize)]
struct HistoryPriceListQuery {
    order_by: Option<String>,
    order_direction: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
    symbol: Option<String>,
    date_less: Option<String>,
    date_greater: Option<String>,
    with_token: Option<i32>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct HistoryPricePointResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    eur: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gbp: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    jpy: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cad: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aud: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cny: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rub: Option<f64>,
}

#[derive(Debug, Serialize)]
struct HistoryPriceResponse {
    symbol: Option<String>,
    price: HistoryPricePointResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<TokenResponse>,
    date: Option<String>,
}

#[derive(Debug, Serialize)]
struct HistoryPriceListResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    total_results: Option<i64>,
    history_prices: Vec<HistoryPriceResponse>,
}

#[derive(Debug, Deserialize)]
struct PlatformListQuery {
    order_by: Option<String>,
    order_direction: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
    name: Option<String>,
    with_external: Option<i32>,
    with_interops: Option<i32>,
    with_token: Option<i32>,
    with_creation_event: Option<i32>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct PlatformResponse {
    name: Option<String>,
    chain: Option<String>,
    fuel: Option<String>,
    externals: Option<Value>,
    platform_interops: Option<Value>,
    platform_tokens: Option<Value>,
    create_event: Option<EventResponse>,
}

#[derive(Debug, Serialize)]
struct PlatformListResponse {
    total_results: Option<i64>,
    platforms: Vec<PlatformResponse>,
}

#[derive(Debug, Deserialize)]
struct ContractMethodHistoryListQuery {
    order_by: Option<String>,
    order_direction: Option<String>,
    offset: Option<i64>,
    limit: Option<i64>,
    symbol: Option<String>,
    hash: Option<String>,
    chain: Option<String>,
    date_less: Option<String>,
    date_greater: Option<String>,
    with_total: Option<i32>,
}

#[derive(Debug, Serialize)]
struct ContractMethodHistoryResponse {
    contract: ContractResponse,
    date: Option<String>,
}

#[derive(Debug, Serialize)]
struct ContractMethodHistoryListResponse {
    total_results: Option<i64>,
    #[serde(rename = "contract_method_histories")]
    contract_method_histories: Vec<ContractMethodHistoryResponse>,
}

#[derive(Debug, Deserialize)]
struct InstructionRequest {
    script_raw: Option<String>,
}

#[derive(Debug, Serialize)]
struct InstructionResponse {
    instruction: String,
}

#[derive(Debug, Serialize)]
struct InstructionListResponse {
    total_results: i64,
    instructions: Vec<InstructionResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VerifyMessageQuery {
    message: Option<String>,
    message_format: Option<String>,
    signature: Option<String>,
    signature_format: Option<String>,
    signer_address: Option<String>,
    signature_kind: Option<String>,
    ecdsa_curve: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct TransactionListQuery {
    #[param(minimum = 1, maximum = 100)]
    limit: Option<i64>,
    order_by: Option<String>,
    order_direction: Option<String>,
    /// Exact transaction hash lookup. Hashes are not globally unique in legacy history.
    hash: Option<String>,
    hash_partial: Option<String>,
    block_height: Option<i64>,
    block_hash: Option<String>,
    /// Address involved through sender, gas, event address, or event target projections.
    address: Option<String>,
    state: Option<String>,
    q: Option<String>,
    chain: Option<String>,
    date_greater: Option<String>,
    date_less: Option<String>,
    with_neighbors: Option<i32>,
    with_events: Option<i32>,
    with_event_data: Option<i32>,
    with_script: Option<i32>,
    /// Seek cursor returned by the previous page.
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct TransactionDetailQuery {
    /// Optional block height disambiguator for hash lookup.
    block_height: Option<i64>,
    /// Optional transaction index disambiguator for hash lookup.
    index: Option<i32>,
}

#[derive(Debug, Serialize, ToSchema)]
struct TransactionListResponse {
    total_results: Option<i64>,
    transactions: Vec<TransactionResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct TransactionDetailResponse {
    transaction: TransactionResponse,
    signatures: Vec<SignatureResponse>,
    events: Vec<EventResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
struct TransactionOccurrenceResponse {
    transaction_id: String,
    hash: String,
    block_hash: String,
    block_height: String,
    chain: String,
    index: i32,
    date: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct TransactionResponse {
    transaction_id: String,
    hash: String,
    block_hash: String,
    block_height: String,
    chain: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    previous_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_hash: Option<String>,
    index: i32,
    date: String,
    fee: Option<String>,
    fee_raw: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    script_raw: Option<String>,
    result: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    debug_comment: Option<String>,
    payload: Option<String>,
    expiration: Option<String>,
    gas_price: Option<String>,
    gas_price_raw: Option<String>,
    gas_limit: Option<String>,
    gas_limit_raw: Option<String>,
    state: String,
    sender: Option<AddressRefResponse>,
    gas_payer: Option<AddressRefResponse>,
    gas_target: Option<AddressRefResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    carbon_tx_type: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    carbon_tx_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    events: Option<Vec<EventResponse>>,
}

#[derive(Debug, Serialize, ToSchema)]
struct SignatureResponse {
    signature_index: i32,
    kind: String,
    data: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct AddressRefResponse {
    address: String,
    address_name: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct EventListQuery {
    #[param(minimum = 1, maximum = 100)]
    limit: Option<i64>,
    order_by: Option<String>,
    order_direction: Option<String>,
    event_id: Option<i32>,
    transaction_hash: Option<String>,
    block_height: Option<i64>,
    event_kind: Option<String>,
    event_source: Option<String>,
    address: Option<String>,
    contract: Option<String>,
    q: Option<String>,
    with_event_data: Option<i32>,
    with_nsfw: Option<i32>,
    with_blacklisted: Option<i32>,
    token_id: Option<String>,
    block_hash: Option<String>,
    date_less: Option<String>,
    date_greater: Option<String>,
    date_day: Option<String>,
    event_kind_partial: Option<String>,
    nft_name_partial: Option<String>,
    nft_description_partial: Option<String>,
    address_partial: Option<String>,
    chain: Option<String>,
    /// Seek cursor returned by the previous page.
    cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct EventListResponse {
    total_results: Option<i64>,
    events: Vec<EventResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct EventResponse {
    event_id: i32,
    event_index: i32,
    event_source: String,
    chain: String,
    date: String,
    block_hash: String,
    transaction_hash: String,
    event_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    address_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contract: Option<ContractRefResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    raw_data: Option<String>,
    #[serde(flatten)]
    event_data: EventDataFields,
}

#[derive(Debug, Default, Serialize, ToSchema)]
struct EventDataFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    address_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chain_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gas_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    governance_gas_config_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    governance_chain_config_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    special_resolution_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hash_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    infusion_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    market_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    organization_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sale_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    string_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_create_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_series_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_settle_event: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unknown_event: Option<Value>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ContractRefResponse {
    hash: String,
    name: Option<String>,
    symbol: Option<String>,
}

#[derive(Debug)]
enum ApiError {
    BadRequest(String),
    NotFound(String),
    AmbiguousTransactionHash {
        hash: String,
        occurrence_count: i64,
        matches: Vec<TransactionOccurrenceResponse>,
    },
    /// Keys-only mode rejected a request with a missing/unknown API key (401).
    Unauthorized(String),
    /// A per-key/per-IP window or the global in-flight cap was exceeded (429);
    /// carries the advertised `Retry-After` in seconds.
    RateLimited {
        retry_after_secs: u64,
    },
    Internal(String),
}

impl ApiError {
    fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::NotFound(_) => "not_found",
            Self::AmbiguousTransactionHash { .. } => "ambiguous_transaction_hash",
            Self::Unauthorized(_) => "unauthorized",
            Self::RateLimited { .. } => "rate_limited",
            Self::Internal(_) => "internal_error",
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::AmbiguousTransactionHash { .. } => StatusCode::CONFLICT,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Self::AmbiguousTransactionHash {
            hash,
            occurrence_count,
            matches,
        } = self
        {
            let response = AmbiguousTransactionHashResponse {
                code: "ambiguous_transaction_hash".to_owned(),
                error: "transaction hash has multiple block occurrences".to_owned(),
                hash,
                occurrence_count,
                returned_occurrences: matches.len(),
                resolution_hint:
                    "Use /api/v1/blocks/{height}/transactions/{index} or pass block_height and index."
                        .to_owned(),
                matches,
            };
            return (StatusCode::CONFLICT, Json(response)).into_response();
        }

        // Rate-limit rejections carry a `Retry-After` header alongside the body.
        if let Self::RateLimited { retry_after_secs } = self {
            let response = ErrorResponse {
                code: "rate_limited".to_owned(),
                error: "rate limit exceeded".to_owned(),
            };
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [(
                    axum::http::header::RETRY_AFTER,
                    retry_after_secs.to_string(),
                )],
                Json(response),
            )
                .into_response();
        }

        let status = self.status_code();
        let code = self.code().to_owned();
        // `Internal` carries diagnostic detail (DB/SQL text, serde/parse internals) meant for the
        // operator only. Log it here at the single response choke point and return a generic body,
        // so the public, unauthenticated client never receives internal error strings. The
        // `BadRequest`/`NotFound` messages are written for the caller and are safe to return as-is.
        let error = match self {
            Self::BadRequest(error) | Self::NotFound(error) | Self::Unauthorized(error) => error,
            Self::Internal(detail) => {
                tracing::error!(detail = %detail, "internal API error");
                "internal server error".to_owned()
            }
            Self::AmbiguousTransactionHash { .. } | Self::RateLimited { .. } => unreachable!(),
        };
        let response = ErrorResponse { code, error };
        (status, Json(response)).into_response()
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(error: sqlx::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

impl From<DbError> for ApiError {
    fn from(error: DbError) -> Self {
        Self::Internal(error.to_string())
    }
}

/// Convert a caught handler panic into a logged 500 response. sqlx's `Row::get`
/// panics on a NULL or type mismatch in a column; without this the panic would
/// drop the client connection instead of returning a response. The detail is
/// logged via `ApiError::Internal` and never sent to the client.
fn handle_panic(panic: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned());
    ApiError::Internal(format!("panic in request handler: {detail}")).into_response()
}

pub fn router(state: ApiState, rate_limiter: RateLimiter) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(swagger_redirect))
        .route("/api/v1/blocks", get(blocks))
        .route("/api/v1/blocks/{height}", get(block_by_height))
        .route(
            "/api/v1/blocks/{height}/transactions/{index}",
            get(transaction_by_block_index),
        )
        .route("/api/v1/chains", get(chains))
        .route("/api/v1/tokens", get(tokens))
        .route("/api/v1/addresses", get(addresses))
        .route("/api/v1/contracts", get(contracts))
        .route(
            "/api/v1/contractMethodHistories",
            get(contract_method_histories),
        )
        .route("/api/v1/instructions", post(instructions))
        .route("/api/v1/nfts", get(nfts))
        .route("/api/v1/oracles", get(oracles))
        .route("/api/v1/platforms", get(platforms))
        .route("/api/v1/series", get(series))
        .route("/api/v1/organizations", get(organizations))
        .route("/api/v1/eventKinds", get(event_kinds))
        .route("/api/v1/eventKindsWithEvents", get(event_kinds_with_events))
        .route("/api/v1/validatorKinds", get(validator_kinds))
        .route("/api/v1/historyPrices", get(history_prices))
        .route("/api/v1/circulatingSupply", get(circulating_supply))
        .route("/api/v1/verifyMessage", get(verify_message))
        .route("/api/v1/overviewStats", get(overview_stats))
        .route("/api/v1/stakingStats", get(staking_stats))
        .route("/api/v1/addressStats", get(address_stats))
        .route("/api/v1/searches", get(searches))
        .route("/api/v1/rejected-transactions", get(rejected_transactions))
        .route("/api/v1/raw-blocks/{height}", get(raw_block_by_height))
        .route("/api/v1/transaction", get(transaction_legacy))
        .route("/api/v1/transactions", get(transactions))
        .route("/api/v1/transactions/{hash}", get(transaction_by_hash))
        .route("/api/v1/events", get(events))
        .route("/health", get(health))
        .route("/version", get(version))
        // Innermost layer: turn a handler panic into a logged 500 instead of a
        // dropped connection, then let the outer layers (request-id, trace)
        // observe that 500 like any other response.
        .layer(CatchPanicLayer::custom(handle_panic))
        .layer(PropagateRequestIdLayer::x_request_id())
        .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .layer(TraceLayer::new_for_http())
        // Outermost: reject over-limit / keys-only requests before any handler work.
        // A disabled limiter makes this middleware a cheap pass-through.
        .layer(from_fn_with_state(
            rate_limiter,
            rate_limit::rate_limit_middleware,
        ))
        .with_state(state)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OffsetCursor(i64);

impl OffsetCursor {
    fn parse_optional(value: Option<String>) -> Result<i64, ApiError> {
        value
            .map(|value| Self::parse(&value))
            .transpose()
            .map(|cursor| cursor.map_or(0, |cursor| cursor.0))
            .map_err(|error| ApiError::BadRequest(format!("invalid cursor: {error}")))
    }

    fn parse(value: &str) -> Result<Self, String> {
        let payload = value
            .strip_prefix("offset:")
            .ok_or_else(|| "expected 'offset:<n>'".to_owned())?;
        let offset = payload
            .parse::<i64>()
            .map_err(|error| format!("invalid offset: {error}"))?;
        if offset < 0 {
            return Err("offset cannot be negative".to_owned());
        }
        Ok(Self(offset))
    }

    fn encode(self) -> String {
        format!("offset:{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PageCursor {
    sort_value: i64,
    id: i32,
}

impl PageCursor {
    fn parse_optional(
        value: Option<String>,
        expected_kind: &'static str,
    ) -> Result<Option<Self>, ApiError> {
        value
            .map(|value| Self::parse(&value, expected_kind))
            .transpose()
            .map_err(|error| ApiError::BadRequest(format!("invalid cursor: {error}")))
    }

    fn parse(value: &str, expected_kind: &'static str) -> Result<Self, String> {
        let (kind, payload) = value
            .split_once(':')
            .ok_or_else(|| "expected '<kind>:<value>:<id>'".to_owned())?;
        if kind != expected_kind {
            return Err(format!("cursor kind must be '{expected_kind}'"));
        }

        let (sort_value, id) = payload
            .split_once(':')
            .ok_or_else(|| "expected sort-value and integer ID payload".to_owned())?;
        let sort_value = sort_value
            .parse::<i64>()
            .map_err(|error| format!("invalid sort value: {error}"))?;
        let id = id
            .parse::<i32>()
            .map_err(|error| format!("invalid ID: {error}"))?;

        Ok(Self { sort_value, id })
    }

    fn encode(&self, kind: &'static str) -> String {
        format!("{kind}:{}:{}", self.sort_value, self.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_message_decodes_and_validates_ed25519_signature()
    -> Result<(), Box<dyn std::error::Error>> {
        let keys = phantasma_sdk::PhantasmaKeys::from_wif(
            "KxMn2TgXukYaNXx7tEdjh7qB2YaMgeuKy47j4rvKigHhBuZWeP3r",
        )?;
        let message = "phantasma-rust-api";
        let signature = keys.sign(message.as_bytes());
        let signature_hex = phantasma_sdk::encode_hex_upper(signature.data());

        let message_bytes = decode_formatted_bytes(message, Some("Plain"), "message", true)
            .map_err(|error| format!("{error:?}"))?;
        let signature_bytes =
            decode_formatted_bytes(&signature_hex, Some("Base16"), "signature", false)
                .map_err(|error| format!("{error:?}"))?;
        let parsed_signature = Ed25519Signature::try_from_slice(&signature_bytes)?;

        assert!(parsed_signature.verify(&message_bytes, [&keys.address()]));
        Ok(())
    }

    #[test]
    fn cursor_round_trips_sort_value_and_integer_id() {
        // Cursor parsing is part of the public paging contract; this check
        // ensures clients can feed a returned cursor back into the same API.
        let cursor = PageCursor {
            sort_value: 1_764_000_000,
            id: 123,
        };

        assert_eq!(
            PageCursor::parse(&cursor.encode("tx"), "tx"),
            Ok(PageCursor {
                sort_value: cursor.sort_value,
                id: cursor.id,
            })
        );
    }

    #[test]
    fn handle_panic_maps_any_payload_to_internal_error() {
        // A caught handler panic (whether the payload is a &str or a String) must
        // become a 500 response, never propagate as a dropped connection.
        assert_eq!(
            handle_panic(Box::new("boom")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            handle_panic(Box::new("boom".to_owned())).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn api_error_maps_to_expected_status_codes() {
        // The handler error type must map to stable HTTP statuses. `Internal` carries
        // operator-only detail (logged in into_response, never returned) and surfaces
        // as a 500.
        assert_eq!(
            ApiError::BadRequest("bad".to_owned()).status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ApiError::NotFound("missing".to_owned()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::Internal("db exploded".to_owned()).status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn cursor_rejects_unknown_kind() {
        // Unknown cursor kinds should fail as bad client input instead of
        // silently being accepted by a different endpoint family.
        assert!(PageCursor::parse("block:10:123", "tx").is_err());
    }
}
