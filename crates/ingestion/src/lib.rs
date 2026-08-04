use explorer_config::{ChainConfig, WorkerConfig, WorkerSyncMode};
use explorer_db::{
    AddressAccountUpsert, AddressBalanceUpsert, BlockRecord, BlockUpsert,
    ContractRpcMetadataCandidate, ContractRpcMetadataUpsert, ContractStringEventSideEffectReport,
    ContractUpgradeMethodCandidate, ContractUpgradeMethodUpsert, DirtyAddress, EventSource,
    EventUpsert, NftRpcMetadataCandidate, NftRpcMetadataUpsert, RawBlockRecord,
    SeriesRpcMetadataCandidate, SeriesRpcMetadataUpsert, TokenMetadataUpsert, TokenSupplyUpsert,
    TransactionRecord, TransactionSignatureUpsert, TransactionUpsert,
};
use explorer_domain::{BlockHeight, ChainName, MAIN_ZERO_STATE_BOUNDARY_HEIGHT};
use explorer_rpc::{
    PhantasmaSdkClient, RpcError, SdkAccountInfoResult, SdkBalanceResult, SdkBlockResult,
    SdkContractResult, SdkEventData, SdkEventExResult, SdkEventResult, SdkSpecialResolutionCall,
    SdkSpecialResolutionData, SdkTokenCreateData, SdkTokenDataResult, SdkTokenPropertyResult,
    SdkTokenResult, SdkTokenSeriesCreateData, SdkTokenSeriesResult, SdkTransactionResult,
    SdkVmValue, decode_block_result,
};
use phantasma_sdk::{
    Address, BinaryReader, CarbonSerializable, ChainConfig as CarbonChainConfig, GasConfig,
    MAX_ARRAY_SIZE, decode_hex, deserialize, encode_hex_upper,
};
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::PgPool;
use sqlx::postgres::PgConnection;
use std::collections::BTreeMap;
use thiserror::Error;
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tracing::{error, info, warn};

const LEGACY_UNLIMITED_GAS_RAW: &str = "18446744073709551615";
const SPECIAL_RESOLUTION_REFETCH_ATTEMPTS: usize = 25;
/// Block size above which an incomplete extended payload is no longer chased by
/// refetching the whole block. Ordinary blocks are kilobytes; the ones that carry a
/// repair resolution are 100+ MB, and the node cannot serve those repeatedly.
const EXTENDED_PAYLOAD_REFETCH_MAX_BLOCK_BYTES: usize = 16 * 1024 * 1024;
const SPECIAL_RESOLUTION_REFETCH_DELAY_MS: u64 = 50;
const BALANCE_SYNC_LAG_THRESHOLD: u64 = 50;
const BALANCE_SYNC_CHUNK_SIZE: usize = 100;
/// Page size for the cursor-paginated account endpoints; the node bounds it to
/// 1..100 and rejects anything outside (0 is no longer "unlimited").
const BALANCE_PAGE_SIZE: u32 = 100;
/// Bound on the per-address balance-page fan-out, so a 100..700-address dirty
/// batch cannot open hundreds of concurrent calls against the node's global
/// concurrency cap.
const BALANCE_FETCH_CONCURRENCY: usize = 8;
/// Hard cap on pages walked per cursor-paginated account endpoint. At
/// `BALANCE_PAGE_SIZE` rows a page this is orders of magnitude above any real
/// portfolio; it exists so a node whose cursor never terminates cannot pin the
/// balance task in an endless request loop.
const BALANCE_MAX_PAGES: usize = 1000;
const STAKE_PROJECTION_INTERVAL_SECONDS: u64 = 30;
const TOKEN_SUPPLY_SYNC_INTERVAL_SECONDS: u64 = 60;
/// Token metadata is read from the extended token answer, which also carries every
/// series of every token: measured on devnet, `getTokens(false)` is 35 KB while
/// `getTokens(true)` is 1.34 MB. Metadata only changes through a governance call, so it
/// gets its own slow tick instead of riding on the per-minute supply sync.
const TOKEN_METADATA_SYNC_INTERVAL_SECONDS: u64 = 600;
const CONTRACT_RPC_METADATA_SYNC_INTERVAL_SECONDS: u64 = 300;
const CONTRACT_RPC_METADATA_STALE_SECONDS: i64 = 30 * 60;
const CONTRACT_RPC_METADATA_SYNC_BATCH_SIZE: i64 = 1_000;
const CONTRACT_UPGRADE_METHOD_SYNC_BATCH_SIZE: i64 = 1_000;
const NFT_RPC_METADATA_SYNC_INTERVAL_SECONDS: u64 = 60;
const NFT_RPC_METADATA_SYNC_BATCH_SIZE: i64 = 100;
const SERIES_RPC_METADATA_SYNC_INTERVAL_SECONDS: u64 = 60;
const SERIES_RPC_METADATA_SYNC_BATCH_SIZE: i64 = 100;
const FAILED_TX_DEBUG_SYNC_INTERVAL_SECONDS: u64 = 30;
const FAILED_TX_DEBUG_SEED_WINDOW_SECONDS: i64 = 7 * 24 * 60 * 60;
const FAILED_TX_DEBUG_BATCH_SIZE: i64 = 25;
const TOKEN_PRICE_SYNC_INTERVAL_SECONDS: u64 = 60;
/// Cap on how many days of daily-price history one tick backfills, so a months-long
/// cold-start gap can't monopolize a worker tick or burn the CoinGecko rate limit in
/// one shot. Remaining days resume on the next tick.
const TOKEN_PRICE_DAILY_BACKFILL_MAX_DAYS_PER_RUN: u64 = 40;
/// Pace between CoinGecko daily-history requests to respect the free-tier rate limit
/// (the C# plugin stops on a 429; we pace pre-emptively to avoid hitting it).
const TOKEN_PRICE_DAILY_REQUEST_DELAY_MS: u64 = 1500;
const TTRS_OFFCHAIN_SYNC_INTERVAL_SECONDS: u64 = 60;
/// NFT ids fetched from 22series per run (the C# plugin pages by 100). The backlog of
/// NFTs missing off-chain metadata drains across many near-tip ticks.
const TTRS_OFFCHAIN_BATCH_SIZE: i64 = 100;

#[derive(Clone)]
pub struct BlockIngestionDriver {
    rpc: PhantasmaSdkClient,
    pool: PgPool,
    chain: ChainConfig,
    settings: WorkerConfig,
    /// Latches once the configured node is verified to match the stored chain, so the
    /// startup guard's RPC check runs only once per process, not on every sync pass.
    node_guard_checked: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Automatic RPC load-shed ("relief") state. Shared by every clone of the
    /// driver — the block loop and all maintenance tasks — so a node in distress
    /// is spared by the whole worker, not just by the task that noticed.
    relief: std::sync::Arc<driver::RpcReliefState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartupProbe {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_endpoints: Vec<String>,
    pub sync_mode: String,
    pub rpc_tip_height: u64,
    pub cursor_height: u64,
    pub next_planned_height: Option<u64>,
    pub fetch_batch_size: u64,
    pub fetch_concurrency: usize,
    pub inter_block_delay_ms: u64,
    pub batch_delay_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SyncBatchReport {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_endpoints: Vec<String>,
    pub sync_mode: String,
    pub rpc_tip_height: u64,
    pub cursor_height_before: u64,
    pub from_height: Option<u64>,
    pub to_height: Option<u64>,
    pub projected_blocks: u64,
    pub cursor_height_after: u64,
    /// In-flight block-fetch concurrency used for this pass (0 when idle).
    pub fetch_concurrency: usize,
    /// True when this pass ran under automatic RPC load shedding: one block per
    /// window, one request in flight, regardless of the configured batch size.
    pub load_shed: bool,
    /// Lowest height this pass could not fetch, if any. Everything below it was
    /// committed; the worker backs off on an escalating schedule keyed on this
    /// height before asking the node for it again.
    pub stalled_height: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BalanceSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_tip_height: u64,
    pub cursor_height: u64,
    pub lag: u64,
    pub dirty_before: i64,
    pub selected_addresses: usize,
    pub updated_accounts: usize,
    pub reset_dirty_flags: u64,
    pub skipped_catchup: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BalanceDirtyMarkReport {
    pub configured_nexus: String,
    pub chain: String,
    pub cursor_height: u64,
    pub marked_addresses: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractStringEventSideEffectSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub upserted_contracts: u64,
    pub linked_contract_creates: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractRpcMetadataSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub selected_contracts: usize,
    pub fetched_contracts: usize,
    pub updated_contracts: usize,
    pub inserted_methods: usize,
    pub failed_contracts: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractUpgradeMethodSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub selected_upgrades: usize,
    pub fetched_contracts: usize,
    pub inserted_methods: usize,
    pub linked_contracts: usize,
    pub failed_contracts: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenSupplySyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub fetched_tokens: usize,
    pub updated_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenMetadataSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub fetched_tokens: usize,
    pub updated_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TokenPriceSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    /// Token rows whose live `price_*` columns were refreshed from `/simple/price`.
    pub live_prices_updated: u64,
    /// Days of daily USD history fetched this run (bounded per run).
    pub daily_days_processed: u64,
    /// Rows newly inserted into `token_daily_prices` this run.
    pub daily_rows_inserted: u64,
    /// True when the daily history is now current through today (no gap left).
    pub daily_caught_up: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TtrsOffchainSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    /// NFT ids selected as still missing off-chain metadata this run.
    pub selected: usize,
    /// Records the 22series API returned for them.
    pub fetched: usize,
    /// `nfts` rows whose off-chain metadata was written.
    pub updated: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NftRpcMetadataSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_tip_height: u64,
    pub cursor_height: u64,
    pub lag: u64,
    pub selected_nfts: usize,
    pub fetched_nfts: usize,
    pub updated_nfts: u64,
    pub skipped_catchup: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SeriesRpcMetadataSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_tip_height: u64,
    pub cursor_height: u64,
    pub lag: u64,
    pub selected_series: usize,
    pub fetched_series: usize,
    pub updated_series: u64,
    pub skipped_catchup: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FailedTransactionDebugSyncReport {
    pub configured_nexus: String,
    pub chain: String,
    pub rpc_tip_height: u64,
    pub cursor_height: u64,
    pub lag: u64,
    pub selected_transactions: usize,
    pub updated_transactions: usize,
    pub skipped_catchup: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchWindow {
    pub from_height: BlockHeight,
    pub to_height: BlockHeight,
    pub concurrency: usize,
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("RPC call failed")]
    Rpc(#[from] RpcError),
    #[error("price feed call failed")]
    PriceFeed(#[from] prices::PriceFeedError),
    #[error("ttrs feed call failed")]
    Ttrs(#[from] ttrs::TtrsFeedError),
    #[error("database write failed")]
    Db(#[from] explorer_db::DbError),
    #[error("database connection or transaction failed")]
    Sqlx(#[from] sqlx::Error),
    #[error("worker fetch batch size must be greater than zero")]
    EmptyFetchBatch,
    #[error("worker fetch task failed")]
    FetchTask(#[from] tokio::task::JoinError),
    #[error("RPC payload for block {height} is too large to store its byte length")]
    PayloadTooLarge { height: u64 },
    #[error("raw block {height} is not stored yet")]
    RawBlockMissing { height: u64 },
    #[error("raw block {height} cannot be projected: missing {field}")]
    MissingBlockField { height: u64, field: &'static str },
    #[error("raw block {height} field {field} is out of range")]
    BlockFieldOutOfRange { height: u64, field: &'static str },
    #[error("transaction at block {height} index {index} cannot be projected: missing {field}")]
    MissingTransactionField {
        height: u64,
        index: usize,
        field: &'static str,
    },
    #[error("transaction at block {height} index {index} field {field} is out of range")]
    TransactionFieldOutOfRange {
        height: u64,
        index: usize,
        field: &'static str,
    },
    #[error(
        "event at block {height}, transaction index {transaction_index}, source {event_source}, event index {event_index} field {field} is out of range"
    )]
    EventFieldOutOfRange {
        height: u64,
        transaction_index: usize,
        event_source: &'static str,
        event_index: usize,
        field: &'static str,
    },
    #[error(
        "event at block {height}, transaction index {transaction_index}, event index {event_index} raw data cannot be decoded as {event_kind}"
    )]
    EventPayloadDecode {
        height: u64,
        transaction_index: usize,
        event_index: usize,
        event_kind: String,
    },
    #[error(
        "refusing to sync chain {chain:?}: cursor {cursor_height} is below the configured start height {boundary_height}"
    )]
    ProtectedZeroStateCursorBelowBoundary {
        chain: String,
        cursor_height: u64,
        boundary_height: u64,
    },
    #[error(
        "refusing to project block {height} for chain {chain:?}: below the configured start height {boundary_height}"
    )]
    ProtectedZeroStateBlock {
        chain: String,
        height: u64,
        boundary_height: u64,
    },
    #[error(
        "node block at height {height} (hash {node_hash}) does not match the stored block hash {db_hash} for chain {chain:?}/nexus {configured_nexus:?}; the configured RPC likely points at a different network — refusing to sync"
    )]
    NodeChainMismatch {
        height: u64,
        db_hash: String,
        node_hash: String,
        chain: String,
        configured_nexus: String,
    },
}

// The BlockIngestionDriver orchestrator's (large) inherent impl lives in its own
// module to keep this crate root focused on types and free helper functions.
mod driver;
mod prices;
mod ttrs;

fn block_result_to_projection(
    chain: &ChainName,
    height: BlockHeight,
    block: &SdkBlockResult,
) -> Result<BlockUpsert, IngestionError> {
    let hash = non_empty_string(&block.hash).ok_or(IngestionError::MissingBlockField {
        height: height.value(),
        field: "hash",
    })?;
    Ok(BlockUpsert {
        chain: chain.clone(),
        height,
        hash,
        protocol: Some(i32::try_from(block.protocol).map_err(|_| {
            IngestionError::BlockFieldOutOfRange {
                height: height.value(),
                field: "protocol",
            }
        })?),
        chain_address: non_empty_string(&block.chain_address),
        validator_address: non_empty_string(&block.validator_address),
        producer_address: block.producer_address.as_deref().and_then(non_empty_string),
        timestamp_unix_seconds: i64::try_from(block.timestamp).map_err(|_| {
            IngestionError::BlockFieldOutOfRange {
                height: height.value(),
                field: "timestamp",
            }
        })?,
        reward: non_empty_string(&block.reward),
    })
}

fn transaction_result_to_projection(
    block: &BlockRecord,
    tx_index: usize,
    transaction: &SdkTransactionResult,
) -> Result<TransactionUpsert, IngestionError> {
    let block_height =
        u64::try_from(block.height).map_err(|_| IngestionError::BlockFieldOutOfRange {
            height: 0,
            field: "height",
        })?;
    let hash =
        non_empty_string(&transaction.hash).ok_or(IngestionError::MissingTransactionField {
            height: block_height,
            index: tx_index,
            field: "hash",
        })?;
    let state =
        non_empty_string(&transaction.state).ok_or(IngestionError::MissingTransactionField {
            height: block_height,
            index: tx_index,
            field: "state",
        })?;
    let timestamp_unix_seconds = i64::try_from(transaction.timestamp).map_err(|_| {
        IngestionError::TransactionFieldOutOfRange {
            height: block_height,
            index: tx_index,
            field: "timestamp",
        }
    })?;
    let expiration_unix_seconds = i64::try_from(transaction.expiration).map_err(|_| {
        IngestionError::TransactionFieldOutOfRange {
            height: block_height,
            index: tx_index,
            field: "expiration",
        }
    })?;
    let carbon_tx_type = i32::try_from(transaction.carbon_tx_type).map_err(|_| {
        IngestionError::TransactionFieldOutOfRange {
            height: block_height,
            index: tx_index,
            field: "carbon_tx_type",
        }
    })?;
    if carbon_tx_type > 255 {
        return Err(IngestionError::TransactionFieldOutOfRange {
            height: block_height,
            index: tx_index,
            field: "carbon_tx_type",
        });
    }
    let fee_raw = non_empty_string(&transaction.fee);
    let gas_price_raw = non_empty_string(&transaction.gas_price);
    let gas_limit_raw = non_empty_string(&transaction.gas_limit);

    Ok(TransactionUpsert {
        block_id: block.id,
        chain_id: block.chain_id,
        tx_index: i32::try_from(tx_index).map_err(|_| {
            IngestionError::TransactionFieldOutOfRange {
                height: block_height,
                index: tx_index,
                field: "tx_index",
            }
        })?,
        hash,
        timestamp_unix_seconds,
        state,
        result: Some(transaction.result.clone()),
        debug_comment: transaction.debug_comment.clone(),
        payload: Some(transaction.payload.clone()),
        script_raw: Some(transaction.script.clone()),
        fee_raw,
        gas_price_raw,
        gas_limit_raw,
        sender: non_empty_string(&transaction.sender),
        gas_payer: non_empty_string(&transaction.gas_payer),
        gas_target: non_empty_string(&transaction.gas_target),
        carbon_tx_type: Some(carbon_tx_type),
        carbon_tx_data: non_empty_string(&transaction.carbon_tx_data),
        expiration_unix_seconds,
        signatures: transaction
            .signatures
            .iter()
            .enumerate()
            .map(|(signature_index, signature)| {
                Ok(TransactionSignatureUpsert {
                    signature_index: i32::try_from(signature_index).map_err(|_| {
                        IngestionError::TransactionFieldOutOfRange {
                            height: block_height,
                            index: tx_index,
                            field: "signature_index",
                        }
                    })?,
                    kind: signature.kind.clone(),
                    data: signature.data.clone(),
                })
            })
            .collect::<Result<Vec<_>, IngestionError>>()?,
    })
}

/// One dirty address's freshly fetched account state: the lightweight overview
/// (name + stake from `getAccountInfo(s)`) plus the assembled balance rows —
/// fungible pages from `getAccountFungibleTokens` and per-token NFT ownership
/// counts from `getTokenBalance` over the `getAccountOwnedTokens` index.
#[derive(Debug, Clone)]
pub(crate) struct FetchedBalanceAccount {
    pub(crate) info: SdkAccountInfoResult,
    pub(crate) balances: Vec<SdkBalanceResult>,
}

fn account_info_to_upsert(
    address_id: i32,
    account: &FetchedBalanceAccount,
    now_unix_seconds: i64,
) -> AddressAccountUpsert {
    // The wire key is `stake` here (an object), not the legacy AccountResult's
    // `stakes`; the SDK models the two endpoints with separate DTOs, so a
    // mis-map cannot slip through the deserializer.
    let staked_amount_raw = normalized_amount_raw(&account.info.stake.amount);
    let unclaimed_amount_raw = normalized_amount_raw(&account.info.stake.unclaimed);
    let soul_balance_raw = account
        .balances
        .iter()
        .find(|balance| balance.symbol == "SOUL")
        .map(|balance| normalized_amount_raw(&balance.amount))
        .unwrap_or_else(|| "0".to_owned());
    let balances = account
        .balances
        .iter()
        .filter_map(|balance| {
            let symbol = non_empty_string(&balance.symbol)?;
            let amount_raw = normalized_amount_raw(&balance.amount);
            Some(AddressBalanceUpsert { symbol, amount_raw })
        })
        .collect();
    let address_name =
        non_empty_string(&account.info.name).filter(|name| !name.eq_ignore_ascii_case("anonymous"));

    AddressAccountUpsert {
        address_id,
        address_name,
        name_last_updated_unix_seconds: now_unix_seconds,
        stake_timestamp: i64::try_from(account.info.stake.time).unwrap_or(i64::MAX),
        staked_amount_raw,
        unclaimed_amount_raw,
        soul_balance_raw,
        balances,
    }
}

fn token_result_to_supply_upsert(token: &SdkTokenResult) -> TokenSupplyUpsert {
    let current_supply_raw = normalized_amount_raw(&token.current_supply);
    let max_supply_raw = normalized_amount_raw(&token.max_supply);
    let burned_supply_raw = normalized_amount_raw(&token.burned_supply);

    TokenSupplyUpsert {
        symbol: token.symbol.clone(),
        carbon_id: non_empty_string(&token.carbon_id).and_then(|value| value.parse().ok()),
        current_supply_raw,
        max_supply_raw,
        burned_supply_raw,
    }
}

/// The token's on-chain metadata, or `None` when the answer carried no metadata field
/// at all — which is what a non-extended token answer looks like, and is not the same
/// as a token that genuinely has none.
fn token_result_to_metadata_upsert(token: &SdkTokenResult) -> Option<TokenMetadataUpsert> {
    let symbol = non_empty_string(&token.symbol)?;
    let properties = token.metadata.as_ref()?;
    Some(TokenMetadataUpsert {
        symbol,
        metadata: Value::Object(token_properties_to_metadata(properties)),
    })
}

fn contract_result_to_rpc_metadata_upsert(
    contract_id: i32,
    contract: &SdkContractResult,
    insert_current_method: bool,
    now_unix_seconds: i64,
) -> ContractRpcMetadataUpsert {
    ContractRpcMetadataUpsert {
        contract_id,
        address: non_empty_string(&contract.address),
        script_raw: non_empty_string(&contract.script),
        methods: contract
            .methods
            .as_ref()
            .and_then(|methods| serde_json::to_value(methods).ok()),
        insert_current_method,
        last_updated_unix_seconds: now_unix_seconds,
    }
}

fn contract_result_to_upgrade_method_upsert(
    contract_id: i32,
    contract: &SdkContractResult,
    timestamp_unix_seconds: i64,
) -> Option<ContractUpgradeMethodUpsert> {
    let methods = contract
        .methods
        .as_ref()
        .and_then(|methods| serde_json::to_value(methods).ok())?;

    Some(ContractUpgradeMethodUpsert {
        contract_id,
        methods,
        timestamp_unix_seconds,
    })
}

fn nft_result_to_metadata_upsert(
    symbol: &str,
    nft: &SdkTokenDataResult,
) -> Option<NftRpcMetadataUpsert> {
    let token_id = non_empty_string(&nft.id)?;
    let series_id = non_empty_string(&nft.series);
    let creator_address = non_empty_string(&nft.creator_address);
    let mint_number = non_empty_string(&nft.mint)
        .and_then(|mint| mint.parse::<u64>().ok())
        .map(|mint| mint.min(i32::MAX as u64) as i32);
    let mint_date = token_property_value(&nft.properties, "mint_date")
        .or_else(|| token_property_value(&nft.properties, "created"));
    let mint_date_unix_seconds = mint_date.as_deref().and_then(parse_i64_clamped);
    let rom = non_empty_string(&nft.rom);
    let ram = non_empty_string(&nft.ram);
    let name = token_property_value(&nft.properties, "name");
    let description = token_property_value(&nft.properties, "description");
    let image = normalize_rpc_image_url(token_property_value(&nft.properties, "imageURL"));
    let info_url = token_property_value(&nft.properties, "infoURL");

    let mut metadata = token_properties_to_metadata(&nft.properties);
    insert_metadata_string(&mut metadata, "token_id", Some(token_id.clone()));
    insert_metadata_string(&mut metadata, "creatorAddress", creator_address.clone());
    insert_metadata_string(&mut metadata, "series", series_id.clone());
    insert_metadata_string(&mut metadata, "rom", rom.clone());
    insert_metadata_string(&mut metadata, "ram", ram.clone());
    insert_metadata_string(&mut metadata, "mint", non_empty_string(&nft.mint));
    insert_metadata_string(&mut metadata, "mint_date", mint_date);
    insert_metadata_string(&mut metadata, "name", name.clone());
    insert_metadata_string(&mut metadata, "description", description.clone());
    insert_metadata_string(&mut metadata, "imageURL", image.clone());
    insert_metadata_string(&mut metadata, "infoURL", info_url.clone());
    insert_metadata_string(&mut metadata, "status", non_empty_string(&nft.status));
    insert_metadata_string(
        &mut metadata,
        "carbonTokenId",
        non_empty_string(&nft.carbon_token_id),
    );
    insert_metadata_string(
        &mut metadata,
        "carbonSeriesId",
        non_empty_string(&nft.carbon_series_id),
    );
    insert_metadata_string(
        &mut metadata,
        "carbonNftAddress",
        non_empty_string(&nft.carbon_nft_address),
    );

    let chain_api_response = serde_json::to_value(nft).unwrap_or(Value::Null);

    Some(NftRpcMetadataUpsert {
        symbol: symbol.to_owned(),
        token_id,
        series_id,
        creator_address,
        mint_number,
        mint_date_unix_seconds,
        rom,
        ram,
        name,
        description,
        image,
        info_url,
        metadata: Value::Object(metadata),
        chain_api_response,
    })
}

fn series_result_to_metadata_upsert(
    symbol: &str,
    series: &SdkTokenSeriesResult,
) -> Option<SeriesRpcMetadataUpsert> {
    let series_id = non_empty_string(&series.series_id)?;
    let creator_address = non_empty_string(&series.owner_address);
    let current_supply = parse_i32_clamped(&series.current_supply);
    let max_supply = parse_i32_clamped(&series.max_supply);
    let name = token_property_value(&series.metadata, "name");
    let description = token_property_value(&series.metadata, "description");
    let image = normalize_rpc_image_url(
        token_property_value(&series.metadata, "imageURL")
            .or_else(|| token_property_value(&series.metadata, "image")),
    );
    let royalties = token_property_value(&series.metadata, "royalties")
        .and_then(|value| parse_i32_clamped(&value));
    let series_type =
        token_property_value(&series.metadata, "type").and_then(|value| parse_i32_clamped(&value));
    let has_locked = token_property_value(&series.metadata, "hasLocked")
        .or_else(|| token_property_value(&series.metadata, "has_locked"))
        .and_then(|value| parse_boolish(&value));
    let mode = normalize_series_mode(
        series
            .mode
            .as_deref()
            .and_then(non_empty_string)
            .or_else(|| token_property_value(&series.metadata, "mode")),
    );

    let mut metadata = token_properties_to_metadata(&series.metadata);
    insert_metadata_string(&mut metadata, "seriesId", Some(series_id.clone()));
    insert_metadata_string(
        &mut metadata,
        "carbonTokenId",
        non_empty_string(&series.carbon_token_id),
    );
    insert_metadata_string(
        &mut metadata,
        "carbonSeriesId",
        non_empty_string(&series.carbon_series_id),
    );
    insert_metadata_string(&mut metadata, "ownerAddress", creator_address.clone());
    insert_metadata_string(&mut metadata, "maxMint", non_empty_string(&series.max_mint));
    insert_metadata_string(
        &mut metadata,
        "mintCount",
        non_empty_string(&series.mint_count),
    );
    insert_metadata_string(
        &mut metadata,
        "currentSupply",
        non_empty_string(&series.current_supply),
    );
    insert_metadata_string(
        &mut metadata,
        "maxSupply",
        non_empty_string(&series.max_supply),
    );
    insert_metadata_string(&mut metadata, "mode", mode.clone());
    insert_metadata_string(&mut metadata, "name", name.clone());
    insert_metadata_string(&mut metadata, "description", description.clone());
    insert_metadata_string(&mut metadata, "imageURL", image.clone());

    let chain_api_response = serde_json::to_value(series).unwrap_or(Value::Null);

    Some(SeriesRpcMetadataUpsert {
        symbol: symbol.to_owned(),
        series_id,
        current_supply,
        max_supply,
        mode,
        creator_address,
        name,
        description,
        image,
        royalties,
        series_type,
        has_locked,
        metadata: Value::Object(metadata),
        chain_api_response,
    })
}

/// A negative-cache sentinel for an NFT whose metadata permanently fails to
/// resolve on the node (e.g. `getNFT` returns "ID not found"). Mirrors
/// [`series_error_to_metadata_upsert`]: it writes an error object into
/// `chain_api_response`, which flips the NULL-`chain_api_response` candidate gate
/// (`fetch_nft_rpc_metadata_candidates`) so the worker stops re-fetching the same
/// unresolvable token every cycle. Only used for permanent (non-transient) errors
/// so a node outage cannot poison a token that would otherwise resolve. All
/// metadata columns stay `None`, so `apply_nft_rpc_metadata`'s `COALESCE`s leave
/// any existing values untouched and only the sentinel is written.
fn nft_error_to_metadata_upsert(
    symbol: &str,
    token_id: &str,
    error: &RpcError,
) -> NftRpcMetadataUpsert {
    NftRpcMetadataUpsert {
        symbol: symbol.to_owned(),
        token_id: token_id.to_owned(),
        series_id: None,
        creator_address: None,
        mint_number: None,
        mint_date_unix_seconds: None,
        rom: None,
        ram: None,
        name: None,
        description: None,
        image: None,
        info_url: None,
        metadata: Value::Object(Map::new()),
        chain_api_response: serde_json::json!({
            "error": error.to_string(),
            "method": "getNFT",
            "symbol": symbol,
            "tokenId": token_id
        }),
    }
}

fn series_error_to_metadata_upsert(
    candidate: &SeriesRpcMetadataCandidate,
    error: &RpcError,
) -> SeriesRpcMetadataUpsert {
    SeriesRpcMetadataUpsert {
        symbol: candidate.symbol.clone(),
        series_id: candidate.series_id.clone(),
        current_supply: None,
        max_supply: None,
        mode: None,
        creator_address: None,
        name: None,
        description: None,
        image: None,
        royalties: None,
        series_type: None,
        has_locked: None,
        metadata: Value::Object(Map::new()),
        chain_api_response: serde_json::json!({
            "error": error.to_string(),
            "method": "getTokenSeriesById",
            "symbol": candidate.symbol.as_str(),
            "seriesId": candidate.series_id.as_str()
        }),
    }
}

/// Stores each property under its own key, keeping the value's real VM shape.
///
/// A property value is a scalar, an array, or a struct, recursively. Flattening the
/// non-scalars back into a string would put a JSON document inside a JSON string —
/// exactly the shape the node stopped answering — and would make the value
/// unrenderable and unfilterable. The metadata columns are already `jsonb`, so the
/// real shape needs no schema change. An empty scalar is dropped, as before; an empty
/// array or struct is a real answer and is kept.
fn token_properties_to_metadata(properties: &[SdkTokenPropertyResult]) -> Map<String, Value> {
    let mut metadata = Map::new();
    for property in properties {
        if property.value.as_text().is_some_and(str::is_empty) {
            continue;
        }
        insert_metadata_value(
            &mut metadata,
            &property.key,
            vm_value_to_json(&property.value),
        );
    }
    metadata
}

/// Maps a VM value onto JSON: a scalar is a string (chain numbers are big integers and
/// the node renders them as decimal strings), an array is an array, a struct is an object
/// whose field names are the chain's own — the node does not rename dictionary keys.
fn vm_value_to_json(value: &SdkVmValue) -> Value {
    match value {
        SdkVmValue::Text(text) => Value::String(text.clone()),
        SdkVmValue::Items(items) => Value::Array(items.iter().map(vm_value_to_json).collect()),
        SdkVmValue::Fields(fields) => Value::Object(
            fields
                .iter()
                .map(|(name, field)| (name.clone(), vm_value_to_json(field)))
                .collect(),
        ),
    }
}

/// Returns a property's value only when it is a scalar.
///
/// Every caller feeds a text column (`nfts.name`, `series.image`, ...) or a numeric
/// parse, and an array or struct has no faithful string form: serializing one into those
/// columns would be a lie. The complete value stays available in the stored metadata.
fn token_property_value(properties: &[SdkTokenPropertyResult], key: &str) -> Option<String> {
    properties
        .iter()
        .find(|property| property.key.eq_ignore_ascii_case(key))
        .and_then(|property| property.value.as_text())
        .and_then(non_empty_string)
}

fn parse_i32_clamped(value: &str) -> Option<i32> {
    non_empty_string(value)
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.min(i32::MAX as u64) as i32)
}

fn parse_i64_clamped(value: &str) -> Option<i64> {
    non_empty_string(value)
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.min(i64::MAX as u64) as i64)
}

fn parse_boolish(value: &str) -> Option<bool> {
    let value = non_empty_string(value)?;
    if value.eq_ignore_ascii_case("true") || value == "1" {
        Some(true)
    } else if value.eq_ignore_ascii_case("false") || value == "0" {
        Some(false)
    } else {
        None
    }
}

fn normalize_series_mode(value: Option<String>) -> Option<String> {
    let value = value.and_then(|value| non_empty_string(&value))?;
    if value == "0" {
        Some("Unique".to_owned())
    } else if value == "1" {
        Some("Duplicated".to_owned())
    } else {
        Some(value)
    }
}

fn insert_metadata_string(metadata: &mut Map<String, Value>, key: &str, value: Option<String>) {
    let Some(value) = value.and_then(|value| non_empty_string(&value)) else {
        return;
    };
    insert_metadata_value(metadata, key, Value::String(value));
}

/// Inserts one metadata entry, replacing any key that differs only in case.
///
/// Chain metadata is written by contracts, so the same logical key can arrive as `Name`
/// and `name` in one answer; keeping both would show the reader two versions of the same
/// field. The last one answered wins, under its own casing.
fn insert_metadata_value(metadata: &mut Map<String, Value>, key: &str, value: Value) {
    let Some(key) = non_empty_string(key) else {
        return;
    };
    if let Some(existing_key) = metadata
        .keys()
        .find(|existing_key| existing_key.eq_ignore_ascii_case(&key))
        .cloned()
    {
        metadata.remove(&existing_key);
    }
    metadata.insert(key, value);
}

fn normalize_rpc_image_url(url: Option<String>) -> Option<String> {
    let trimmed = url.and_then(|url| non_empty_string(&url))?;
    if trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
        || trimmed.contains("://")
    {
        return Some(trimmed);
    }
    if trimmed.starts_with("//") {
        return Some(format!("https:{trimmed}"));
    }
    Some(format!("https://{trimmed}"))
}

fn normalized_amount_raw(value: &str) -> String {
    non_empty_string(value).unwrap_or_else(|| "0".to_owned())
}

fn balance_dirty_batch_size(dirty_count: i64) -> i64 {
    if dirty_count >= 30_000 {
        700
    } else if dirty_count >= 10_000 {
        500
    } else if dirty_count >= 3_000 {
        350
    } else if dirty_count >= 1_000 {
        250
    } else if dirty_count >= 300 {
        150
    } else {
        100
    }
}

fn transaction_events_to_projections(
    block: &BlockRecord,
    transaction_record: &TransactionRecord,
    tx_index: usize,
    transaction: &SdkTransactionResult,
) -> Result<Vec<EventUpsert>, IngestionError> {
    let block_height =
        u64::try_from(block.height).map_err(|_| IngestionError::BlockFieldOutOfRange {
            height: 0,
            field: "height",
        })?;
    warn_unmodeled_extended_events(block_height, tx_index, transaction);
    let mut extended_context = TxExtendedEventContext::from_transaction(transaction);
    let mut events = Vec::with_capacity(transaction.events.len() + 1);
    let mut has_legacy_special_resolution = false;
    let mut has_legacy_token_series_create = false;

    for (event_index, event) in transaction.events.iter().enumerate() {
        let event_kind = legacy_event_kind_name(event);
        if event_kind.eq_ignore_ascii_case("SpecialResolution") {
            has_legacy_special_resolution = true;
        }
        if event_kind.eq_ignore_ascii_case("TokenSeriesCreate") {
            has_legacy_token_series_create = true;
        }
        if is_numeric_legacy_event_kind(&event_kind) {
            // C# attempts to resolve numeric event kinds through its EventKind
            // enum and EventKinds lookup. Unsupported numeric names fail before
            // EventMethods.Upsert, so no row is written for historical Saturn
            // admin events such as raw kind `72`.
            warn!(
                block_height,
                tx_index,
                event_index,
                event_kind,
                "skipping numeric legacy event kind to match C# ingestion"
            );
            continue;
        }
        events.push(event_to_projection(
            block_height,
            transaction_record,
            tx_index,
            event_index,
            event,
            &mut extended_context,
        )?);
    }

    let mut next_synthetic_event_index = transaction.events.len();

    if !has_legacy_special_resolution
        && let Some(special_resolution) = extended_context.special_resolution
    {
        let synthetic_index = next_synthetic_event_index;
        next_synthetic_event_index += 1;
        let synthetic_event = SdkEventResult {
            address: transaction.gas_payer.clone(),
            contract: "governance".to_owned(),
            kind: "SpecialResolution".to_owned(),
            name: "SpecialResolution".to_owned(),
            data: special_resolution_raw_data(special_resolution),
        };
        events.push(event_to_projection(
            block_height,
            transaction_record,
            tx_index,
            synthetic_index,
            &synthetic_event,
            &mut extended_context,
        )?);
    }

    if !has_legacy_token_series_create
        && let Some(token_series_create) = extended_context.token_series_create
    {
        let synthetic_index = next_synthetic_event_index;
        let synthetic_event = SdkEventResult {
            address: non_empty_string(&token_series_create.owner)
                .unwrap_or_else(|| transaction.gas_payer.clone()),
            contract: non_empty_string(&token_series_create.symbol)
                .unwrap_or_else(|| "token".to_owned()),
            kind: "TokenSeriesCreate".to_owned(),
            name: "TokenSeriesCreate".to_owned(),
            data: String::new(),
        };
        events.push(event_to_projection(
            block_height,
            transaction_record,
            tx_index,
            synthetic_index,
            &synthetic_event,
            &mut extended_context,
        )?);
    }

    // An extended-only TokenCreate (no legacy TokenCreate event to attach to, and —
    // unlike SpecialResolution/TokenSeriesCreate above — not synthesized) is dropped
    // here. This matches C#, which only emits TokenCreate rows for legacy events, but
    // it is lossy: no `tokens` row is created, so later mints of that symbol leave the
    // holder addresses permanently dirty. Log it so the condition is visible rather
    // than silently swallowed.
    if extended_context.token_create.is_some() && !extended_context.token_create_consumed {
        warn!(
            block_height,
            tx_index,
            tx_hash = %transaction_record.hash,
            "dropping extended-only TokenCreate with no legacy event (C# parity; no tokens row created)"
        );
    }

    Ok(events)
}

#[derive(Debug, Clone, Copy)]
struct IncompleteExtendedPayload {
    tx_index: usize,
    event_kind: &'static str,
}

fn incomplete_extended_payload(block: &SdkBlockResult) -> Option<IncompleteExtendedPayload> {
    block
        .txs
        .iter()
        .enumerate()
        .find_map(|(tx_index, transaction)| {
            if transaction_has_incomplete_special_resolution(transaction) {
                Some(IncompleteExtendedPayload {
                    tx_index,
                    event_kind: "SpecialResolution",
                })
            } else if transaction_has_incomplete_token_create(transaction) {
                Some(IncompleteExtendedPayload {
                    tx_index,
                    event_kind: "TokenCreate",
                })
            } else if transaction_has_incomplete_token_series_create(transaction) {
                Some(IncompleteExtendedPayload {
                    tx_index,
                    event_kind: "TokenSeriesCreate",
                })
            } else {
                None
            }
        })
}

fn transaction_has_incomplete_extended_payload(transaction: &SdkTransactionResult) -> bool {
    transaction_has_incomplete_special_resolution(transaction)
        || transaction_has_incomplete_token_create(transaction)
        || transaction_has_incomplete_token_series_create(transaction)
}

fn transaction_has_incomplete_special_resolution(transaction: &SdkTransactionResult) -> bool {
    let has_governance_legacy_special_resolution = transaction.events.iter().any(|event| {
        event.kind.eq_ignore_ascii_case("SpecialResolution")
            && event.contract.eq_ignore_ascii_case("governance")
    });
    if !has_governance_legacy_special_resolution {
        return false;
    }

    let Some(extended) = transaction.extended_events.iter().find(|event| {
        event.kind.eq_ignore_ascii_case("SpecialResolution")
            && event.contract.eq_ignore_ascii_case("governance")
    }) else {
        return false;
    };

    matches!(extended.data, SdkEventData::SpecialResolution(_))
        && special_resolution_payload(&extended.data).is_none()
}

fn transaction_has_incomplete_token_create(transaction: &SdkTransactionResult) -> bool {
    let has_legacy_token_create = transaction
        .events
        .iter()
        .any(|event| event.kind.eq_ignore_ascii_case("TokenCreate"));
    if !has_legacy_token_create {
        return false;
    }

    let Some(extended) = transaction
        .extended_events
        .iter()
        .find(|event| event.kind.eq_ignore_ascii_case("TokenCreate"))
    else {
        return false;
    };

    matches!(extended.data, SdkEventData::TokenCreate(_))
        && token_create_payload(&extended.data).is_none()
}

fn transaction_has_incomplete_token_series_create(transaction: &SdkTransactionResult) -> bool {
    let Some(extended) = transaction
        .extended_events
        .iter()
        .find(|event| event.kind.eq_ignore_ascii_case("TokenSeriesCreate"))
    else {
        return false;
    };

    matches!(extended.data, SdkEventData::TokenSeriesCreate(_))
        && token_series_create_payload(&extended.data).is_none()
}

/// The extended-event kinds this build carries a typed shape for. A payload of one of
/// these that still arrives untyped means the node answers a shape newer than our SDK.
const MODELED_EXTENDED_EVENT_KINDS: [&str; 6] = [
    "TokenCreate",
    "TokenSeriesCreate",
    "OrderCreated",
    "OrderCancelled",
    "OrderFilled",
    "SpecialResolution",
];

/// Reports extended payloads this build could not type.
///
/// Decoding is total — an unmodeled payload is kept verbatim and ingestion continues —
/// so nothing here is fatal and nothing is dropped. It still has to be visible: an
/// untyped payload under a modeled kind, or a call whose module and method this build
/// does not know, both mean the node moved ahead of our SDK. Kinds outside the modeled
/// set are normal (`TokenMint`, which the node stopped emitting in December 2025, is the
/// common one) and stay quiet, and unrecognised calls are counted rather than logged one
/// by one: a single repair resolution can carry thousands.
fn warn_unmodeled_extended_events(
    block_height: u64,
    tx_index: usize,
    transaction: &SdkTransactionResult,
) {
    for event in &transaction.extended_events {
        if event.data.as_unknown().is_some()
            && MODELED_EXTENDED_EVENT_KINDS
                .iter()
                .any(|kind| event.kind.eq_ignore_ascii_case(kind))
        {
            warn!(
                block_height,
                tx_index,
                event_kind = %event.kind,
                "extended event payload does not match the shape this build models; stored verbatim"
            );
            continue;
        }

        let Some(resolution) = event.data.as_special_resolution() else {
            continue;
        };
        let mut unrecognized = Vec::new();
        collect_unrecognized_calls(&resolution.calls, &mut unrecognized);
        if let Some((module, method)) = unrecognized.first() {
            warn!(
                block_height,
                tx_index,
                resolution_id = resolution.resolution_id,
                unrecognized_calls = unrecognized.len(),
                first_module = %module,
                first_method = %method,
                "special resolution carries calls this build does not model; arguments stored verbatim"
            );
        }
    }
}

/// Collects the module/method pairs of every call whose arguments stayed untyped,
/// walking nested resolutions as well.
fn collect_unrecognized_calls<'a>(
    calls: &'a [SdkSpecialResolutionCall],
    unrecognized: &mut Vec<(&'a str, &'a str)>,
) {
    for call in calls {
        if call
            .arguments
            .as_ref()
            .is_some_and(|arguments| arguments.as_unrecognized().is_some())
        {
            unrecognized.push((call.module.as_str(), call.method.as_str()));
        }
        if let Some(nested) = &call.calls {
            collect_unrecognized_calls(nested, unrecognized);
        }
    }
}

/// The usable payload of a `SpecialResolution` extended event, or `None` when the node
/// answered a shell.
///
/// The node's endpoint cache used to re-serialize `data` as `{"valueKind":"Object"}`
/// (chain note `json-rpc-cache-extended-event-data-bug-2026-05-20`). Serde ignores the
/// unknown field and every modeled field defaults, so a shell types cleanly into an
/// all-default struct — an id of zero is the signal, since the chain never issues
/// resolution 0. Re-requesting the transaction returns the real payload, which is why
/// this condition is worth repairing.
///
/// `calls` is deliberately NOT part of the test: an empty call list cannot be told apart
/// from an absent one after typing, and treating it as a shell would refetch a block
/// twenty-five times and then refuse it outright — a far worse failure than storing a
/// resolution with no calls.
fn special_resolution_payload(data: &SdkEventData) -> Option<&SdkSpecialResolutionData> {
    data.as_special_resolution()
        .filter(|resolution| resolution.resolution_id != 0)
}

/// The usable payload of a `TokenCreate` extended event, or `None` for a shell. A real
/// token always has a symbol; every other field has a legitimate zero value (decimals 0,
/// max supply "0" for an infinite token, no metadata).
fn token_create_payload(data: &SdkEventData) -> Option<&SdkTokenCreateData> {
    data.as_token_create()
        .filter(|token_create| !token_create.symbol.is_empty())
}

/// The usable payload of a `TokenSeriesCreate` extended event, or `None` for a shell. A
/// real series always carries both its symbol and its owner.
fn token_series_create_payload(data: &SdkEventData) -> Option<&SdkTokenSeriesCreateData> {
    data.as_token_series_create()
        .filter(|series| !series.symbol.is_empty() && !series.owner.is_empty())
}

fn legacy_event_kind_name(event: &SdkEventResult) -> String {
    non_empty_string(&event.kind)
        .or_else(|| non_empty_string(&event.name))
        .unwrap_or_else(|| "Unknown".to_owned())
}

fn is_numeric_legacy_event_kind(event_kind: &str) -> bool {
    !event_kind.is_empty() && event_kind.bytes().all(|byte| byte.is_ascii_digit())
}

/// `TokenMint` has no modeled shape: the node stopped emitting that extended event on
/// 2025-12-08 and the SDK deliberately does not port `TokenMintData`, so the payload
/// arrives verbatim in [`SdkEventData::Unknown`]. Blocks between the zero-state boundary
/// and that date still carry it, so a forward resync must keep reading it as raw JSON.
fn token_mint_payload(data: &SdkEventData) -> Option<&Value> {
    data.as_unknown().filter(|data| {
        data.get("symbol").and_then(Value::as_str).is_some()
            && data.get("tokenId").is_some()
            && data.get("mintNumber").is_some()
            && data.get("carbonTokenId").is_some()
            && data.get("carbonSeriesId").is_some()
            && data.get("carbonInstanceId").is_some()
    })
}

#[derive(Debug, Default)]
struct TxExtendedEventContext<'a> {
    special_resolution: Option<&'a SdkSpecialResolutionData>,
    token_create: Option<&'a SdkTokenCreateData>,
    token_create_consumed: bool,
    token_series_create: Option<&'a SdkTokenSeriesCreateData>,
    token_mint: Option<&'a Value>,
}

impl<'a> TxExtendedEventContext<'a> {
    fn from_transaction(transaction: &'a SdkTransactionResult) -> Self {
        let events = &transaction.extended_events;
        let special_resolution = events
            .iter()
            .filter(|event| event.kind.eq_ignore_ascii_case("SpecialResolution"))
            .find_map(|event| special_resolution_payload(&event.data));
        let token_create = token_create_payload_from_extended_events(events);
        let token_series_create = events
            .iter()
            .find(|event| event.kind.eq_ignore_ascii_case("TokenSeriesCreate"))
            .and_then(|event| token_series_create_payload(&event.data));
        let token_mint = events
            .iter()
            .find(|event| event.kind.eq_ignore_ascii_case("TokenMint"))
            .and_then(|event| token_mint_payload(&event.data));

        Self {
            special_resolution,
            token_create,
            token_create_consumed: false,
            token_series_create,
            token_mint,
        }
    }

    fn take_token_create_for_event(&mut self) -> Option<&'a SdkTokenCreateData> {
        let token_create = self.token_create?;
        if self.token_create_consumed {
            return None;
        }

        // The C# backend calls ExtendedEventParser.GetTokenCreateData(), which
        // returns the first TokenCreate extended event only. Once that payload
        // is applied, later TokenCreate rows in the same transaction are
        // left as raw compatibility envelopes even when their raw symbol differs.
        self.token_create_consumed = true;
        Some(token_create)
    }
}

fn event_to_projection(
    block_height: u64,
    transaction_record: &TransactionRecord,
    tx_index: usize,
    event_index: usize,
    event: &SdkEventResult,
    extended_context: &mut TxExtendedEventContext<'_>,
) -> Result<EventUpsert, IngestionError> {
    let event_kind = legacy_event_kind_name(event);
    let address = normalize_legacy_event_address(&event.address);
    let payload_contract =
        non_empty_string(&event.contract).unwrap_or_else(|| "unknown".to_owned());
    let raw_data = if event_kind == "TokenSeriesCreate" && event.data.is_empty() {
        Some(String::new())
    } else {
        non_empty_string(&event.data)
    };
    let mut contract = payload_contract.clone();
    let mut token_id = None;
    // ABI event name for self-describing contract events (set in the Custom_V2
    // branch below); stays None for native kinds, where the kind is the label.
    let mut event_name = None;
    // The stored payload carries no 'chain'/'address' keys: they duplicate the
    // relational chain_id/address_id on every row (equality was measured over
    // all 76M rows before migration 202608040003 stripped the stored copies).
    // The API re-inserts both at serve time, byte-identically, because the
    // served string is re-serialized from a sorted map either way.
    let mut payload_json = serde_json::json!({
        "event_kind": &event_kind,
        "contract": &payload_contract,
    });

    if is_legacy_token_event_kind(&event_kind) {
        if let Some(raw_data) = raw_data.as_deref() {
            let token_event = decode_legacy_token_event(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            )?;
            contract = token_event.token.clone();
            token_id = Some(token_event.value_raw.clone());
            payload_json["token_id"] = serde_json::json!(&token_event.value_raw);
            payload_json["token_event"] = serde_json::json!({
                "token": &token_event.token,
                "value": &token_event.value_raw,
                "value_raw": &token_event.value_raw,
                "chain_name": &token_event.chain_name,
            });
            if event_kind == "TokenMint"
                && let Some(token_mint) = extended_context.token_mint
                && token_mint_payload_matches(
                    token_mint,
                    &token_event.token,
                    &token_event.value_raw,
                )
            {
                payload_json["token_mint_extended"] = build_token_mint_extended_payload(token_mint);
            }
        }
    } else if event_kind == "Infusion" {
        if let Some(raw_data) = raw_data.as_deref() {
            let infusion_event = decode_legacy_infusion_event(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            )?;
            contract = infusion_event.base_token.clone();
            token_id = Some(infusion_event.token_id.clone());
            payload_json["token_id"] = serde_json::json!(&infusion_event.token_id);
            payload_json["infusion_event"] = serde_json::json!({
                "token_id": &infusion_event.token_id,
                "base_token": &infusion_event.base_token,
                "infused_token": &infusion_event.infused_token,
                "infused_value": &infusion_event.infused_value,
            });
        }
    } else if is_legacy_market_event_kind(&event_kind) {
        if let Some(raw_data) = raw_data.as_deref() {
            let market_event = decode_legacy_market_event(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            )?;
            contract = market_event.base_token.clone();
            token_id = Some(market_event.market_id.clone());
            payload_json["token_id"] = serde_json::json!(&market_event.market_id);
            payload_json["market_event"] = serde_json::json!({
                "base_token": &market_event.base_token,
                "quote_token": &market_event.quote_token,
                "market_event_kind": &market_event.market_event_kind,
                "market_id": &market_event.market_id,
                "price": &market_event.price,
                "end_price": &market_event.end_price,
            });
        }
    } else if matches!(event_kind.as_str(), "GasEscrow" | "GasPayment") {
        if let Some(raw_data) = raw_data.as_deref() {
            let gas_event = decode_legacy_gas_event(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            )?;
            let mut gas_payload = serde_json::json!({
                "price": &gas_event.price,
                "address": &gas_event.address,
            });
            if gas_event.amount != LEGACY_UNLIMITED_GAS_RAW {
                gas_payload["amount"] = serde_json::json!(&gas_event.amount);
            }
            payload_json["gas_event"] = gas_payload;
        }
    } else if event_kind == "GovernanceSetGasConfig" {
        if let Some(raw_data) = raw_data.as_deref() {
            let gas_config = decode_carbon_event_or_default::<GasConfig>(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            );
            payload_json["governance_gas_config_event"] =
                build_governance_gas_config_payload(&gas_config);
        }
    } else if event_kind == "GovernanceSetChainConfig" {
        if let Some(raw_data) = raw_data.as_deref() {
            let chain_config = decode_carbon_event_or_default::<CarbonChainConfig>(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            );
            payload_json["governance_chain_config_event"] =
                build_governance_chain_config_payload(&chain_config);
        }
    } else if event_kind == "SpecialResolution" {
        if let Some(special_resolution) = extended_context.special_resolution {
            payload_json["special_resolution_event"] =
                build_special_resolution_payload(special_resolution);
        }
    } else if event_kind == "TokenCreate" {
        if let Some(token_create) = extended_context.take_token_create_for_event() {
            let token_create_payload = build_token_create_payload(token_create);
            payload_json["token_create"] = token_create_payload.clone();
            payload_json["token_create_event"] = token_create_payload;
        }
    } else if event_kind == "TokenSeriesCreate" {
        if let Some(token_series_create) = extended_context.token_series_create {
            if let Some(series_id) = token_series_identity(token_series_create) {
                token_id = Some(series_id.clone());
                payload_json["token_id"] = serde_json::json!(series_id);
            }
            payload_json["token_series_event"] =
                build_token_series_create_payload(token_series_create);
        }
    } else if is_legacy_string_event_kind(&event_kind) {
        if let Some(raw_data) = raw_data.as_deref() {
            let string_event = decode_legacy_string_event(
                block_height,
                tx_index,
                event_index,
                &event_kind,
                raw_data,
            )?;
            payload_json["string_event"] = serde_json::json!({
                "string_value": string_event,
            });
        }
    } else if matches!(
        event_kind.as_str(),
        "Custom" | "Custom_V2" | "LeaderboardCreate" | "ValidatorSwitch"
    ) {
        // Opaque kinds: the payload keeps the InitPayload shape, the bytes stay
        // in raw_data, and no native decoder is applied (C# parity).
        //
        // Only "Custom_V2" carries a name worth storing. The node collapses
        // ABI-declared contract events (whose on-chain kind byte collides with a
        // native kind, e.g. marketplace.AuctionCreated on byte 68) to that kind
        // and puts the real ABI event name in `name`, so the API and UI can label
        // the event properly. For the other three kinds the node echoes the kind
        // itself: storing that would duplicate what
        // `COALESCE(event_name, event_kind.name)` already yields on read, and
        // would hand the node a way to silently relabel a native kind in the API.
        if event_kind.as_str() == "Custom_V2" {
            event_name = non_empty_string(&event.name);
        }
    } else {
        payload_json = serde_json::json!({
            "contract": &event.contract,
            "kind": &event.kind,
            "name": &event.name,
            "data": &event.data
        });
    }

    Ok(EventUpsert {
        transaction_id: transaction_record.id,
        chain_id: transaction_record.chain_id,
        event_index: event_index_to_i32(block_height, tx_index, EventSource::Legacy, event_index)?
            + 1,
        event_kind,
        event_name,
        address: Some(address),
        target_address: None,
        contract: Some(contract),
        token_id,
        raw_data,
        payload_format: Some("live.v1".to_owned()),
        payload_json: Some(payload_json),
        timestamp_unix_seconds: transaction_record.timestamp_unix_seconds,
        burned: None,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyTokenEventData {
    token: String,
    value_raw: String,
    chain_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyGasEventData {
    address: String,
    price: String,
    amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyInfusionEventData {
    base_token: String,
    token_id: String,
    infused_token: String,
    infused_value: String,
    chain_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyMarketEventData {
    base_token: String,
    quote_token: String,
    market_id: String,
    price: String,
    end_price: String,
    market_event_kind: &'static str,
}

fn normalize_legacy_event_address(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("[Null address]") {
        "NULL".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn is_legacy_token_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        "TokenMint"
            | "TokenClaim"
            | "TokenBurn"
            | "TokenSend"
            | "TokenReceive"
            | "TokenStake"
            | "CrownRewards"
            | "Inflation"
    )
}

fn is_legacy_market_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        "OrderCancelled" | "OrderClosed" | "OrderCreated" | "OrderFilled" | "OrderBid"
    )
}

fn is_legacy_string_event_kind(event_kind: &str) -> bool {
    matches!(
        event_kind,
        "ChainCreate"
            | "ContractUpgrade"
            | "AddressRegister"
            | "ContractDeploy"
            | "PlatformCreate"
            | "OrganizationCreate"
            | "Log"
            | "AddressUnregister"
    )
}

fn decode_legacy_token_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<LegacyTokenEventData, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let token = legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let value_raw =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let chain_name =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;

    Ok(LegacyTokenEventData {
        token,
        value_raw,
        chain_name,
    })
}

fn decode_legacy_infusion_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<LegacyInfusionEventData, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let base_token =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let token_id =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let infused_token =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let infused_value =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let chain_name =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;

    Ok(LegacyInfusionEventData {
        base_token,
        token_id,
        infused_token,
        infused_value,
        chain_name,
    })
}

fn decode_legacy_market_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<LegacyMarketEventData, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let base_token =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let quote_token =
        legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let market_id =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let price =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let end_price =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let market_event_kind =
        match legacy_read_var_uint(&mut reader, block_height, tx_index, event_index, event_kind)? {
            0 => "Fixed",
            1 => "Classic",
            2 => "Reserve",
            3 => "Dutch",
            _ => {
                return Err(legacy_event_decode_error(
                    block_height,
                    tx_index,
                    event_index,
                    event_kind,
                ));
            }
        };
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;

    Ok(LegacyMarketEventData {
        base_token,
        quote_token,
        market_id,
        price,
        end_price,
        market_event_kind,
    })
}

fn decode_legacy_string_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<String, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let value = legacy_read_string(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;
    Ok(value)
}

fn decode_legacy_gas_event(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<LegacyGasEventData, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    let mut reader = BinaryReader::new(&bytes);
    let address =
        legacy_read_address(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let price =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    let amount =
        legacy_read_big_integer(&mut reader, block_height, tx_index, event_index, event_kind)?;
    legacy_assert_eof(reader, block_height, tx_index, event_index, event_kind)?;

    Ok(LegacyGasEventData {
        address,
        price,
        amount,
    })
}

fn decode_carbon_event<T: CarbonSerializable>(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<T, IngestionError> {
    let bytes =
        decode_legacy_event_bytes(block_height, tx_index, event_index, event_kind, raw_data)?;
    deserialize::<T>(&bytes)
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn decode_carbon_event_or_default<T: CarbonSerializable + Default>(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> T {
    match decode_carbon_event::<T>(block_height, tx_index, event_index, event_kind, raw_data) {
        Ok(value) => value,
        Err(error) => {
            // The C# importer stores malformed governance config events by
            // calling `GetParsedData<T>()`, which returns `default(T)` after a
            // failed Carbon parse. Preserve that observable DB/API payload
            // instead of rejecting the whole block.
            //
            // error!, not warn!: with the Custom_V2 read-time remap the RPC no
            // longer presents contract events under governance kinds, so a decode
            // failure here means a genuine system event this build cannot parse
            // (e.g. a config format newer than the pinned SDK) — and the stored
            // payload silently degrades to an all-zero default. The blob length
            // distinguishes a truncated blob from an unknown newer layout.
            error!(
                block_height,
                tx_index,
                event_index,
                event_kind,
                raw_data_bytes = raw_data.len() / 2,
                error = %error,
                "using default governance config payload for undecodable Carbon event"
            );
            T::default()
        }
    }
}

fn token_create_payload_from_extended_events(
    events: &[SdkEventExResult],
) -> Option<&SdkTokenCreateData> {
    events
        .iter()
        .find(|event| event.kind.eq_ignore_ascii_case("TokenCreate"))
        .and_then(|event| token_create_payload(&event.data))
}

#[cfg(test)]
fn legacy_token_create_raw_data(symbol: &str, chain_name: &str) -> String {
    let mut writer = phantasma_sdk::BinaryWriter::new();
    writer.write_string(symbol);
    writer.write_var_bytes([0]);
    writer.write_string(chain_name);
    encode_hex_upper(writer.into_bytes())
}

fn build_governance_gas_config_payload(config: &GasConfig) -> Value {
    let mut payload = serde_json::json!({
        "version": config.version.to_string(),
        "max_name_length": config.max_name_length.to_string(),
        "max_token_symbol_length": config.max_token_symbol_length.to_string(),
        "fee_shift": config.fee_shift.to_string(),
        "max_structure_size": config.max_structure_size.to_string(),
        "fee_multiplier": config.fee_multiplier.to_string(),
        "gas_token_id": config.gas_token_id.to_string(),
        "data_token_id": config.data_token_id.to_string(),
        "minimum_gas_offer": config.minimum_gas_offer.to_string(),
        "data_escrow_per_row": config.data_escrow_per_row.to_string(),
        "gas_fee_transfer": config.gas_fee_transfer.to_string(),
        "gas_fee_query": config.gas_fee_query.to_string(),
        "gas_fee_create_token_base": config.gas_fee_create_token_base.to_string(),
        "gas_fee_create_token_symbol": config.gas_fee_create_token_symbol.to_string(),
        "gas_fee_create_token_series": config.gas_fee_create_token_series.to_string(),
        "gas_fee_per_byte": config.gas_fee_per_byte.to_string(),
        "gas_fee_register_name": config.gas_fee_register_name.to_string(),
        "gas_burn_ratio_mul": config.gas_burn_ratio_mul.to_string(),
        "gas_burn_ratio_shift": config.gas_burn_ratio_shift.to_string(),
    });
    // The gas-model-v2 tail exists on the wire only for version >= 1; emit its
    // keys under the same gate the node serializer uses so version-0 payloads
    // stay byte-identical to every historical row already in the database.
    if config.has_gas_model_v2()
        && let Some(object) = payload.as_object_mut()
    {
        let v2_fields = [
            ("minimum_gas_bill", config.minimum_gas_bill.to_string()),
            (
                "gas_producer_ratio_mul",
                config.gas_producer_ratio_mul.to_string(),
            ),
            (
                "gas_producer_ratio_shift",
                config.gas_producer_ratio_shift.to_string(),
            ),
            ("gas_dapp_ratio_mul", config.gas_dapp_ratio_mul.to_string()),
            (
                "gas_dapp_ratio_shift",
                config.gas_dapp_ratio_shift.to_string(),
            ),
            (
                "policy_fee_create_token_base",
                config.policy_fee_create_token_base.to_string(),
            ),
            (
                "policy_fee_create_token_symbol",
                config.policy_fee_create_token_symbol.to_string(),
            ),
            (
                "policy_fee_create_token_series",
                config.policy_fee_create_token_series.to_string(),
            ),
            (
                "policy_fee_register_name",
                config.policy_fee_register_name.to_string(),
            ),
            (
                "legacy_data_escrow_per_row",
                config.legacy_data_escrow_per_row.to_string(),
            ),
        ];
        for (key, value) in v2_fields {
            object.insert(key.to_owned(), Value::String(value));
        }
    }
    payload
}

fn build_governance_chain_config_payload(config: &CarbonChainConfig) -> Value {
    serde_json::json!({
        "version": config.version.to_string(),
        "reserved_1": config.reserved1.to_string(),
        "reserved_2": config.reserved2.to_string(),
        "reserved_3": config.reserved3.to_string(),
        "allowed_tx_types": config.allowed_tx_types.to_string(),
        "expiry_window": config.expiry_window.to_string(),
        "block_rate_target": config.block_rate_target.to_string(),
    })
}

fn build_special_resolution_payload(data: &SdkSpecialResolutionData) -> Value {
    let mut payload = Map::new();
    payload.insert(
        "resolution_id".to_owned(),
        Value::String(data.resolution_id.to_string()),
    );
    if let Some(description) = &data.description {
        payload.insert("description".to_owned(), Value::String(description.clone()));
    }
    payload.insert(
        "calls".to_owned(),
        build_special_resolution_calls(&data.calls),
    );
    Value::Object(payload)
}

fn build_special_resolution_calls(calls: &[SdkSpecialResolutionCall]) -> Value {
    Value::Array(
        calls
            .iter()
            .map(|call| {
                let mut payload = Map::new();
                if !call.module.is_empty() {
                    payload.insert("module".to_owned(), Value::String(call.module.clone()));
                }
                // Written unconditionally: 0 is a real id on this chain (module
                // `governance`, method `TransferFungible`), so it cannot double as
                // "absent", and the SDK's own rule — a missing id reads as 0 — is what
                // every consumer of this payload already sees. The names above stay the
                // display key; an older node that omits the ids leaves zeros here.
                payload.insert("module_id".to_owned(), Value::from(call.module_id));
                if !call.method.is_empty() {
                    payload.insert("method".to_owned(), Value::String(call.method.clone()));
                }
                payload.insert("method_id".to_owned(), Value::from(call.method_id));
                if let Some(arguments) = &call.arguments {
                    // Arguments keep the node's own camelCase field names: they are the
                    // shape every other SDK and the frontend already read, and a third
                    // naming convention here would buy nothing. Serializing plain data
                    // with string keys cannot fail; degrading to null rather than losing
                    // the whole call is still the safer branch.
                    payload.insert(
                        "arguments".to_owned(),
                        serde_json::to_value(arguments).unwrap_or(Value::Null),
                    );
                }
                payload.insert(
                    "calls".to_owned(),
                    build_special_resolution_calls(call.calls.as_deref().unwrap_or_default()),
                );
                Value::Object(payload)
            })
            .collect(),
    )
}

fn build_token_create_payload(data: &SdkTokenCreateData) -> Value {
    let mut payload = Map::new();
    if !data.symbol.is_empty() {
        payload.insert("symbol".to_owned(), Value::String(data.symbol.clone()));
    }
    if !data.max_supply.is_empty() {
        payload.insert(
            "max_supply".to_owned(),
            Value::String(data.max_supply.clone()),
        );
    }
    payload.insert(
        "decimals".to_owned(),
        Value::String(data.decimals.to_string()),
    );
    payload.insert(
        "is_non_fungible".to_owned(),
        Value::Bool(data.is_non_fungible),
    );
    // Carbon ids are 1-based identities, so a zero means the payload did not carry one
    // and the key stays out — unlike decimals and the fungibility flag above, whose zero
    // and false are real answers.
    if data.carbon_token_id != 0 {
        payload.insert(
            "carbon_token_id".to_owned(),
            Value::String(data.carbon_token_id.to_string()),
        );
    }
    payload.insert("metadata".to_owned(), string_map_to_json(&data.metadata));
    Value::Object(payload)
}

fn build_token_series_create_payload(data: &SdkTokenSeriesCreateData) -> Value {
    let mut payload = Map::new();
    if !data.symbol.is_empty() {
        payload.insert("token".to_owned(), Value::String(data.symbol.clone()));
    }
    if let Some(series_id) = token_series_identity(data) {
        payload.insert("series_id".to_owned(), Value::String(series_id));
    }
    payload.insert(
        "max_mint".to_owned(),
        Value::String(data.max_mint.to_string()),
    );
    payload.insert(
        "max_supply".to_owned(),
        Value::String(data.max_supply.to_string()),
    );
    if !data.owner.is_empty() {
        payload.insert("owner".to_owned(), Value::String(data.owner.clone()));
    }
    // See build_token_create_payload: a zero Carbon id means the payload carried none.
    if data.carbon_token_id != 0 {
        payload.insert(
            "carbon_token_id".to_owned(),
            Value::String(data.carbon_token_id.to_string()),
        );
    }
    if data.carbon_series_id != 0 {
        payload.insert(
            "carbon_series_id".to_owned(),
            Value::String(data.carbon_series_id.to_string()),
        );
    }
    payload.insert("metadata".to_owned(), string_map_to_json(&data.metadata));
    Value::Object(payload)
}

fn token_mint_payload_matches(data: &Value, symbol: &str, token_id: &str) -> bool {
    let Some(mint_symbol) = data.get("symbol").and_then(Value::as_str) else {
        return false;
    };
    if !mint_symbol.eq_ignore_ascii_case(symbol) {
        return false;
    }

    data.get("tokenId")
        .and_then(json_scalar_to_string)
        .is_some_and(|mint_token_id| mint_token_id.eq_ignore_ascii_case(token_id))
}

fn build_token_mint_extended_payload(data: &Value) -> Value {
    let mut payload = Map::new();
    if let Some(token_id) = data.get("tokenId").and_then(json_scalar_to_string) {
        payload.insert("token_id".to_owned(), Value::String(token_id));
    }
    if let Some(series_id) = data.get("seriesId").and_then(json_scalar_to_string) {
        payload.insert("series_id".to_owned(), Value::String(series_id));
    }
    if let Some(mint_number) = data.get("mintNumber").and_then(json_scalar_to_string) {
        payload.insert("mint_number".to_owned(), Value::String(mint_number));
    }
    if let Some(carbon_token_id) = data.get("carbonTokenId").and_then(json_scalar_to_string) {
        payload.insert("carbon_token_id".to_owned(), Value::String(carbon_token_id));
    }
    if let Some(carbon_series_id) = data.get("carbonSeriesId").and_then(json_scalar_to_string) {
        payload.insert(
            "carbon_series_id".to_owned(),
            Value::String(carbon_series_id),
        );
    }
    if let Some(carbon_instance_id) = data.get("carbonInstanceId").and_then(json_scalar_to_string) {
        payload.insert(
            "carbon_instance_id".to_owned(),
            Value::String(carbon_instance_id),
        );
    }
    if let Some(owner) = data.get("owner").and_then(Value::as_str) {
        payload.insert("owner".to_owned(), Value::String(owner.to_owned()));
    }
    Value::Object(payload)
}

/// The id a series is known by: its Phantasma series id, or the Carbon id when the
/// payload predates it. `None` only for a payload that carries neither.
fn token_series_identity(data: &SdkTokenSeriesCreateData) -> Option<String> {
    non_empty_string(&data.series_id)
        .or_else(|| (data.carbon_series_id != 0).then(|| data.carbon_series_id.to_string()))
}

/// The node renders extended-event metadata to a string-to-string map — that field did
/// NOT become a VM value — so it maps straight onto JSON.
fn string_map_to_json(metadata: &BTreeMap<String, String>) -> Value {
    Value::Object(
        metadata
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect(),
    )
}

/// The compatibility `data` blob of a synthesized legacy SpecialResolution event: the
/// resolution id in the same little-endian encoding the chain writes.
fn special_resolution_raw_data(data: &SdkSpecialResolutionData) -> String {
    encode_hex_upper(data.resolution_id.to_le_bytes())
}

fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn decode_legacy_event_bytes(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
    raw_data: &str,
) -> Result<Vec<u8>, IngestionError> {
    decode_hex(raw_data).map_err(|_| IngestionError::EventPayloadDecode {
        height: block_height,
        transaction_index: tx_index,
        event_index,
        event_kind: event_kind.to_owned(),
    })
}

fn legacy_read_string(
    reader: &mut BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<String, IngestionError> {
    reader
        .read_string()
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn legacy_read_big_integer(
    reader: &mut BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<String, IngestionError> {
    reader
        .read_big_integer()
        .map(|value| value.to_string())
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn legacy_read_var_uint(
    reader: &mut BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<u64, IngestionError> {
    reader
        .read_var_uint()
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn legacy_read_address(
    reader: &mut BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<String, IngestionError> {
    let raw = reader
        .read_var_bytes(MAX_ARRAY_SIZE)
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))?;
    Address::try_from_slice(&raw)
        .map(|address| address.to_text())
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn legacy_assert_eof(
    reader: BinaryReader<'_>,
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> Result<(), IngestionError> {
    reader
        .assert_eof()
        .map_err(|_| legacy_event_decode_error(block_height, tx_index, event_index, event_kind))
}

fn legacy_event_decode_error(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    event_kind: &str,
) -> IngestionError {
    IngestionError::EventPayloadDecode {
        height: block_height,
        transaction_index: tx_index,
        event_index,
        event_kind: event_kind.to_owned(),
    }
}

fn event_index_to_i32(
    block_height: u64,
    tx_index: usize,
    event_source: EventSource,
    event_index: usize,
) -> Result<i32, IngestionError> {
    i32::try_from(event_index).map_err(|_| IngestionError::EventFieldOutOfRange {
        height: block_height,
        transaction_index: tx_index,
        event_source: event_source.as_str(),
        event_index,
        field: "event_index",
    })
}

fn extract_block_hash(block: &SdkBlockResult) -> Option<String> {
    // The SDK owns block response deserialization; Explorer only decides
    // whether an empty hash is usable for its raw-block lookup column.
    non_empty_string(&block.hash)
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

/// Plan the next contiguous fetch window.
///
/// `load_shed` is the automatic relief switch (see `driver::RpcReliefState`):
/// while it is set, the window collapses to a single block with a single request
/// in flight no matter how large the configured batch is. That is the whole point
/// of shedding — a node that just failed to serve a block must be asked for one
/// block at a time (with the escalating per-attempt timeout), never for a fresh
/// fan-out of concurrent multi-megabyte fetches.
pub fn plan_fetch_window(
    current_height: BlockHeight,
    rpc_tip: BlockHeight,
    settings: &WorkerConfig,
    load_shed: bool,
) -> Result<Option<FetchWindow>, IngestionError> {
    if settings.fetch_batch_size == 0 {
        return Err(IngestionError::EmptyFetchBatch);
    }

    let target_tip = settings
        .height_limit
        .map(|limit| limit.min(rpc_tip.value()))
        .unwrap_or_else(|| rpc_tip.value());

    let next_height = current_height.value().saturating_add(1);
    if next_height > target_tip {
        return Ok(None);
    }

    let batch_size = if load_shed {
        1
    } else {
        settings.effective_fetch_batch_size()
    };
    let to_height = next_height
        .saturating_add(batch_size.saturating_sub(1))
        .min(target_tip);

    let block_count = to_height.saturating_sub(next_height).saturating_add(1);
    let requested_concurrency = if load_shed {
        1
    } else {
        settings.effective_fetch_concurrency()
    };
    let concurrency = requested_concurrency.min(block_count as usize);

    Ok(Some(FetchWindow {
        from_height: BlockHeight::new(next_height),
        to_height: BlockHeight::new(to_height),
        concurrency,
    }))
}

fn duration_millis_u64(duration: std::time::Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Builds an extended event the way the RPC layer does — by deserializing the wire
    /// object, so the SDK's own `kind` dispatch decides the payload type. Assembling the
    /// typed variant by hand would test the fixture instead of the dispatch, and would
    /// hide the shell payloads these tests exist to catch.
    fn extended_event(address: &str, contract: &str, kind: &str, data: Value) -> SdkEventExResult {
        // Decoding an extended event is total by design, so a fixture cannot fail to
        // deserialize; the default keeps the helper free of the unwrap the repo lints
        // against.
        serde_json::from_value(serde_json::json!({
            "address": address,
            "contract": contract,
            "kind": kind,
            "data": data,
        }))
        .unwrap_or_default()
    }

    fn worker_config() -> WorkerConfig {
        WorkerConfig {
            poll_interval: Duration::from_secs(5),
            queue_capacity: 500,
            fetch_batch_size: 50,
            fetch_concurrency: 4,
            project_concurrency: 1,
            sync_mode: explorer_config::WorkerSyncMode::Sequential,
            inter_block_delay: Duration::from_millis(0),
            batch_delay: Duration::from_millis(0),
            height_limit: None,
        }
    }

    #[test]
    fn fetch_window_is_bounded_by_batch_and_tip() {
        // Worker fetch fan-out must stay bounded so catch-up sync does not turn
        // one lagging range into unbounded RPC pressure.
        let window = plan_fetch_window(
            BlockHeight::new(10),
            BlockHeight::new(25),
            &worker_config(),
            false,
        );

        assert!(matches!(
            window,
            Ok(Some(FetchWindow {
                from_height,
                to_height,
                concurrency: 4,
            })) if from_height.value() == 11 && to_height.value() == 25
        ));
    }

    #[test]
    fn fetch_window_respects_height_limit() {
        // Height limits are used for repeatable parity runs and should clamp
        // the target before concurrency is calculated.
        let mut settings = worker_config();
        settings.height_limit = Some(12);

        let window =
            plan_fetch_window(BlockHeight::new(10), BlockHeight::new(25), &settings, false);

        assert!(matches!(
            window,
            Ok(Some(FetchWindow {
                from_height,
                to_height,
                concurrency: 2,
            })) if from_height.value() == 11 && to_height.value() == 12
        ));
    }

    #[test]
    fn fetch_window_is_empty_when_cursor_reached_tip() {
        let window = plan_fetch_window(
            BlockHeight::new(25),
            BlockHeight::new(25),
            &worker_config(),
            false,
        );

        assert!(matches!(window, Ok(None)));
    }

    #[test]
    fn relief_mode_forces_single_block_windows() {
        // Relief mode is the Rust equivalent of the old C# worker's load-shed
        // path: isolate difficult blocks and keep RPC/DB pressure to one block.
        let mut settings = worker_config();
        settings.sync_mode = explorer_config::WorkerSyncMode::Relief;
        settings.fetch_batch_size = 50;
        settings.fetch_concurrency = 6;
        settings.project_concurrency = 6;

        let window =
            plan_fetch_window(BlockHeight::new(10), BlockHeight::new(25), &settings, false);

        assert!(matches!(
            window,
            Ok(Some(FetchWindow {
                from_height,
                to_height,
                concurrency: 1,
            })) if from_height.value() == 11 && to_height.value() == 11
        ));
        assert_eq!(settings.effective_project_concurrency(), 1);
    }

    #[test]
    fn automatic_load_shedding_overrides_the_configured_fan_out() {
        // The wedge this prevents: a node that just failed on a 100+ MB block gets
        // the SAME window again — `fetch_concurrency` concurrent giant fetches —
        // because the configured batch size never adapts. While shedding, the
        // window must collapse to one block and one in-flight request no matter
        // what the config says.
        let mut settings = worker_config();
        settings.sync_mode = explorer_config::WorkerSyncMode::Normal;
        settings.fetch_batch_size = 1000;
        settings.fetch_concurrency = 6;

        let shedding = plan_fetch_window(
            BlockHeight::new(8_736_256),
            BlockHeight::new(8_827_221),
            &settings,
            true,
        );
        assert!(matches!(
            shedding,
            Ok(Some(FetchWindow {
                from_height,
                to_height,
                concurrency: 1,
            })) if from_height.value() == 8_736_257 && to_height.value() == 8_736_257
        ));

        // Same inputs without shedding keep the configured throughput, so recovery
        // returns to full speed rather than crawling one block per pass forever.
        let normal = plan_fetch_window(
            BlockHeight::new(8_736_256),
            BlockHeight::new(8_827_221),
            &settings,
            false,
        );
        assert!(matches!(
            normal,
            Ok(Some(FetchWindow {
                from_height,
                to_height,
                concurrency: 6,
            })) if from_height.value() == 8_736_257 && to_height.value() == 8_737_256
        ));
    }

    #[test]
    fn extracts_hash_from_raw_block_payload() {
        // Hash extraction now starts from the SDK block contract, not from
        // Explorer-local JSON field probing.
        let block = decode_block_result(serde_json::json!({ "hash": "ABC", "height": 42 }));

        assert!(matches!(
            block.map(|block| extract_block_hash(&block)),
            Ok(Some(hash)) if hash == "ABC"
        ));
    }

    #[test]
    fn parses_minimal_block_projection_fields() -> Result<(), Box<dyn std::error::Error>> {
        // The block projection and decoded SDK block stay together so the
        // worker can project transactions from the same typed SDK payload.
        let block = decode_block_result(serde_json::json!({
            "hash": "ABC",
            "previousHash": "PREV",
            "protocol": 18,
            "chainAddress": "PCHAIN",
            "validatorAddress": "PVALIDATOR",
            "timestamp": 123456,
            "reward": "0",
            "txs": [{ "hash": "TX1" }, { "hash": "TX2" }]
        }))?;

        let projection =
            block_result_to_projection(&ChainName::new("main")?, BlockHeight::new(42), &block);

        assert!(matches!(
            projection,
            Ok(BlockUpsert {
                hash,
                protocol: Some(18),
                // Pre-v2 blocks carry no producerAddress on the wire; the
                // projection must leave it None, never default it.
                producer_address: None,
                ..
            }) if hash == "ABC"
        ));
        Ok(())
    }

    #[test]
    fn projects_the_gas_model_v2_producer_address() -> Result<(), Box<dyn std::error::Error>> {
        // Post-flip blocks expose the consensus-covered fee-payout identity as
        // producerAddress. It must survive the SDK decode + projection intact.
        let block = decode_block_result(serde_json::json!({
            "hash": "ABC",
            "previousHash": "PREV",
            "protocol": 18,
            "chainAddress": "PCHAIN",
            "validatorAddress": "PVALIDATOR",
            "producerAddress": "PPRODUCER",
            "timestamp": 123456,
            "reward": "0",
            "txs": []
        }))?;

        let projection =
            block_result_to_projection(&ChainName::new("main")?, BlockHeight::new(42), &block)?;

        assert_eq!(projection.producer_address.as_deref(), Some("PPRODUCER"));
        Ok(())
    }

    #[test]
    fn account_balance_projection_reads_the_account_info_stake_object()
    -> Result<(), Box<dyn std::error::Error>> {
        // getAccountInfo carries the staking object under the wire key `stake`
        // (the legacy getAccount used `stakes` and reserved `stake` for a
        // deprecated flat scalar); a mis-map would silently zero every stake
        // and propagate into the Soul-Masters derivation, so the test feeds the
        // real wire shape through serde rather than building the DTO directly.
        let info: SdkAccountInfoResult = serde_json::from_value(serde_json::json!({
            "address": "PADDR",
            "name": "anonymous",
            "stake": {
                "amount": "5000000000000",
                "time": 123,
                "unclaimed": "467"
            }
        }))?;
        // Balance rows arrive separately: fungible pages plus a per-token NFT
        // ownership count (decimals 0, amount = owned items).
        let balances: Vec<SdkBalanceResult> = serde_json::from_value(serde_json::json!([
            { "chain": "main", "symbol": "SOUL", "amount": "42", "decimals": 8 },
            { "chain": "main", "symbol": "KCAL", "amount": "467", "decimals": 10 },
            { "chain": "main", "symbol": "CROWN", "amount": "20", "decimals": 0 }
        ]))?;
        let account = FetchedBalanceAccount { info, balances };

        let projection = account_info_to_upsert(7, &account, 999);

        assert_eq!(projection.address_id, 7);
        assert_eq!(projection.address_name, None);
        assert_eq!(projection.stake_timestamp, 123);
        assert_eq!(projection.staked_amount_raw, "5000000000000");
        assert_eq!(projection.unclaimed_amount_raw, "467");
        assert_eq!(projection.soul_balance_raw, "42");
        assert_eq!(projection.balances.len(), 3);
        assert_eq!(projection.balances[1].amount_raw, "467");
        assert_eq!(projection.balances[2].amount_raw, "20");
        Ok(())
    }

    #[test]
    fn account_balance_projection_keeps_a_real_address_name()
    -> Result<(), Box<dyn std::error::Error>> {
        let info: SdkAccountInfoResult = serde_json::from_value(serde_json::json!({
            "address": "PADDR",
            "name": "moneymaker01",
            "stake": { "amount": "0", "time": 0, "unclaimed": "0" }
        }))?;
        let account = FetchedBalanceAccount {
            info,
            balances: Vec::new(),
        };

        let projection = account_info_to_upsert(7, &account, 999);

        assert_eq!(projection.address_name.as_deref(), Some("moneymaker01"));
        assert_eq!(projection.soul_balance_raw, "0");
        assert!(projection.balances.is_empty());
        Ok(())
    }

    #[test]
    fn token_supply_projection_passes_rpc_raw_values_through() {
        let token = SdkTokenResult {
            symbol: "KCAL".to_owned(),
            carbon_id: "1".to_owned(),
            decimals: 10,
            current_supply: "2093700588047349606".to_owned(),
            max_supply: "0".to_owned(),
            burned_supply: "9242814271535702".to_owned(),
            ..Default::default()
        };

        let supply = token_result_to_supply_upsert(&token);

        assert_eq!(supply.symbol, "KCAL");
        assert_eq!(supply.carbon_id, Some(1));
        assert_eq!(supply.current_supply_raw, "2093700588047349606");
        assert_eq!(supply.max_supply_raw, "0");
        assert_eq!(supply.burned_supply_raw, "9242814271535702");
    }

    #[test]
    fn contract_rpc_metadata_upsert_uses_rpc_abi_methods() {
        let contract = SdkContractResult {
            name: "market".to_owned(),
            address: "PCONTRACT".to_owned(),
            owner: None,
            script: "AABBCC".to_owned(),
            methods: Some(vec![phantasma_sdk::AbiMethodResult {
                name: "getContractVersion".to_owned(),
                return_type: "Number".to_owned(),
                parameters: Vec::new(),
            }]),
            events: None,
        };

        let upsert = contract_result_to_rpc_metadata_upsert(42, &contract, true, 1234);

        assert_eq!(upsert.contract_id, 42);
        assert!(upsert.insert_current_method);
        assert_eq!(upsert.address.as_deref(), Some("PCONTRACT"));
        assert_eq!(upsert.script_raw.as_deref(), Some("AABBCC"));
        assert_eq!(upsert.last_updated_unix_seconds, 1234);
        assert_eq!(
            upsert
                .methods
                .as_ref()
                .and_then(|methods| methods.get(0))
                .and_then(|method| method.get("returnType"))
                .and_then(Value::as_str),
            Some("Number")
        );

        let upgrade = contract_result_to_upgrade_method_upsert(42, &contract, 5678);
        assert!(upgrade.is_some());
        let Some(upgrade) = upgrade else {
            return;
        };
        assert_eq!(upgrade.contract_id, 42);
        assert_eq!(upgrade.timestamp_unix_seconds, 5678);
        assert_eq!(
            upgrade
                .methods
                .get(0)
                .and_then(|method| method.get("name"))
                .and_then(Value::as_str),
            Some("getContractVersion")
        );
    }

    #[test]
    fn dirty_balance_batch_size_scales_with_backlog() {
        assert_eq!(balance_dirty_batch_size(0), 100);
        assert_eq!(balance_dirty_batch_size(300), 150);
        assert_eq!(balance_dirty_batch_size(1_000), 250);
        assert_eq!(balance_dirty_batch_size(10_000), 500);
        assert_eq!(balance_dirty_batch_size(30_000), 700);
    }

    /// An NFT answer with the given properties, everything else at a fixed shape, so a
    /// property test states only the property under test.
    fn nft_with_properties(properties: Vec<SdkTokenPropertyResult>) -> SdkTokenDataResult {
        SdkTokenDataResult {
            id: "123".to_owned(),
            series: "456".to_owned(),
            chain_name: "main".to_owned(),
            owner_address: "Powner".to_owned(),
            creator_address: "Pcreator".to_owned(),
            status: "Transferable".to_owned(),
            properties,
            ..Default::default()
        }
    }

    #[test]
    fn stores_a_non_scalar_property_in_its_real_shape() {
        // A property value is a VM value: scalar, array or struct, recursively. The
        // stored metadata must keep that shape — flattening it into a string would put a
        // JSON document inside a JSON string, which is what the node stopped answering.
        let nft = nft_with_properties(vec![
            SdkTokenPropertyResult {
                key: "name".to_owned(),
                value: "RPC NFT".into(),
            },
            SdkTokenPropertyResult {
                key: "_ia".to_owned(),
                value: SdkVmValue::Items(vec![SdkVmValue::Fields(BTreeMap::from([
                    ("mul".to_owned(), SdkVmValue::text("25")),
                    ("div".to_owned(), SdkVmValue::text("10000")),
                    (
                        "who".to_owned(),
                        SdkVmValue::Items(vec![SdkVmValue::text("64D5")]),
                    ),
                ]))]),
            },
        ]);

        let upsert = nft_result_to_metadata_upsert("TEST", &nft);
        assert!(upsert.is_some());
        let Some(upsert) = upsert else {
            return;
        };

        assert_eq!(
            upsert.metadata.get("_ia"),
            Some(&serde_json::json!([{ "mul": "25", "div": "10000", "who": ["64D5"] }])),
            "an array of structs must survive as an array of structs"
        );
        assert_eq!(
            upsert.metadata.get("name"),
            Some(&serde_json::json!("RPC NFT")),
            "a scalar property stays a plain string"
        );
    }

    #[test]
    fn keeps_a_non_scalar_property_out_of_the_text_columns() {
        // `nfts.name` is a text column. A struct or array value has no faithful string
        // form, so the column must stay empty rather than carry a serialized document;
        // the complete value is still reachable through the stored metadata.
        let nft = nft_with_properties(vec![SdkTokenPropertyResult {
            key: "name".to_owned(),
            value: SdkVmValue::Items(vec![SdkVmValue::text("first"), SdkVmValue::text("second")]),
        }]);

        let upsert = nft_result_to_metadata_upsert("TEST", &nft);
        assert!(upsert.is_some());
        let Some(upsert) = upsert else {
            return;
        };

        assert_eq!(upsert.name, None);
        assert_eq!(
            upsert.metadata.get("name"),
            Some(&serde_json::json!(["first", "second"]))
        );
    }

    #[test]
    fn nft_rpc_metadata_upsert_uses_rpc_properties_without_rom_decode() {
        let nft = SdkTokenDataResult {
            id: "123".to_owned(),
            series: "456".to_owned(),
            carbon_token_id: "7".to_owned(),
            carbon_series_id: "8".to_owned(),
            carbon_nft_address: "0xabc".to_owned(),
            mint: "9".to_owned(),
            chain_name: "main".to_owned(),
            owner_address: "Powner".to_owned(),
            creator_address: "Pcreator".to_owned(),
            ram: "CCDD".to_owned(),
            rom: "AABB".to_owned(),
            status: "Transferable".to_owned(),
            infusion: Vec::new(),
            properties: vec![
                SdkTokenPropertyResult {
                    key: "name".to_owned(),
                    value: "RPC NFT".into(),
                },
                SdkTokenPropertyResult {
                    key: "imageURL".to_owned(),
                    value: "//cdn.example/nft.png".into(),
                },
                SdkTokenPropertyResult {
                    key: "Created".to_owned(),
                    value: "1800123456".into(),
                },
            ],
        };

        let upsert = nft_result_to_metadata_upsert("TEST", &nft);
        assert!(upsert.is_some());
        let Some(upsert) = upsert else {
            return;
        };

        assert_eq!(upsert.symbol, "TEST");
        assert_eq!(upsert.token_id, "123");
        assert_eq!(upsert.series_id.as_deref(), Some("456"));
        assert_eq!(upsert.creator_address.as_deref(), Some("Pcreator"));
        assert_eq!(upsert.mint_number, Some(9));
        assert_eq!(upsert.mint_date_unix_seconds, Some(1_800_123_456));
        assert_eq!(upsert.rom.as_deref(), Some("AABB"));
        assert_eq!(upsert.ram.as_deref(), Some("CCDD"));
        assert_eq!(upsert.name.as_deref(), Some("RPC NFT"));
        assert_eq!(upsert.image.as_deref(), Some("https://cdn.example/nft.png"));
        assert_eq!(
            upsert.metadata.get("rom").and_then(Value::as_str),
            Some("AABB")
        );
        assert_eq!(
            upsert.metadata.get("imageURL").and_then(Value::as_str),
            Some("https://cdn.example/nft.png")
        );
        assert_eq!(
            upsert.metadata.get("mint_date").and_then(Value::as_str),
            Some("1800123456")
        );
        assert_eq!(
            upsert
                .chain_api_response
                .get("creatorAddress")
                .and_then(Value::as_str),
            Some("Pcreator")
        );
    }

    #[test]
    fn series_rpc_metadata_upsert_uses_direct_series_rpc_properties() {
        let series = SdkTokenSeriesResult {
            series_id: "456".to_owned(),
            carbon_token_id: "7".to_owned(),
            carbon_series_id: "8".to_owned(),
            owner_address: "Pcreator".to_owned(),
            max_mint: "25".to_owned(),
            mint_count: "9".to_owned(),
            current_supply: "9".to_owned(),
            max_supply: "25".to_owned(),
            burned_supply: Some("1".to_owned()),
            mode: None,
            script: None,
            methods: None,
            metadata: vec![
                SdkTokenPropertyResult {
                    key: "name".to_owned(),
                    value: "RPC Series".into(),
                },
                SdkTokenPropertyResult {
                    key: "imageURL".to_owned(),
                    value: "//cdn.example/series.png".into(),
                },
                SdkTokenPropertyResult {
                    key: "mode".to_owned(),
                    value: "1".into(),
                },
            ],
        };

        let upsert = series_result_to_metadata_upsert("TEST", &series);
        assert!(upsert.is_some());
        let Some(upsert) = upsert else {
            return;
        };

        assert_eq!(upsert.symbol, "TEST");
        assert_eq!(upsert.series_id, "456");
        assert_eq!(upsert.current_supply, Some(9));
        assert_eq!(upsert.max_supply, Some(25));
        assert_eq!(upsert.creator_address.as_deref(), Some("Pcreator"));
        assert_eq!(upsert.mode.as_deref(), Some("Duplicated"));
        assert_eq!(upsert.name.as_deref(), Some("RPC Series"));
        assert_eq!(
            upsert.image.as_deref(),
            Some("https://cdn.example/series.png")
        );
        assert_eq!(
            upsert.metadata.get("carbonTokenId").and_then(Value::as_str),
            Some("7")
        );
        assert_eq!(
            upsert
                .chain_api_response
                .get("ownerAddress")
                .and_then(Value::as_str),
            Some("Pcreator")
        );
    }

    #[test]
    fn transaction_projection_formats_kcal_fee_and_gas() -> Result<(), Box<dyn std::error::Error>> {
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 42,
            hash: "BLOCK".to_owned(),
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1743530760,
            reward: None,
        };
        let mut transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1743530760,
            state: "Halt".to_owned(),
            fee: "467".to_owned(),
            gas_price: "1".to_owned(),
            gas_limit: "2100000000".to_owned(),
            expiration: 1743534360,
            ..Default::default()
        };

        let projection = transaction_result_to_projection(&block, 0, &transaction)?;

        assert_eq!(projection.fee_raw.as_deref(), Some("467"));
        assert_eq!(projection.gas_price_raw.as_deref(), Some("1"));
        assert_eq!(projection.gas_limit_raw.as_deref(), Some("2100000000"));

        // The unlimited-gas sentinel is stored raw as-is; the READ path serves the
        // formatted gas_limit as NULL for it (the projection no longer formats).
        transaction.gas_limit = LEGACY_UNLIMITED_GAS_RAW.to_owned();
        let projection = transaction_result_to_projection(&block, 0, &transaction)?;

        assert_eq!(
            projection.gas_limit_raw.as_deref(),
            Some(LEGACY_UNLIMITED_GAS_RAW)
        );
        Ok(())
    }

    #[test]
    fn decodes_legacy_token_and_gas_event_payloads() -> Result<(), Box<dyn std::error::Error>> {
        let token =
            decode_legacy_token_event(6422527, 0, 1, "TokenBurn", "044B43414C02E900046D61696E")?;
        assert_eq!(
            token,
            LegacyTokenEventData {
                token: "KCAL".to_owned(),
                value_raw: "233".to_owned(),
                chain_name: "main".to_owned(),
            }
        );

        let gas = decode_legacy_gas_event(
            6422527,
            0,
            0,
            "GasEscrow",
            "2202000D6E4079E36703EBD37C00722F5891D28B0E2811DC114B129215123ADCCE36050201000500752B7D00",
        )?;
        assert_eq!(
            gas,
            LegacyGasEventData {
                address: "S3d7TbZxtNPdXy11hfmBLJLYn67gZTG2ibL7fJBcVdihWU4".to_owned(),
                price: "1".to_owned(),
                amount: "2100000000".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn projects_token_mint_extended_payload() -> Result<(), Box<dyn std::error::Error>> {
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 42,
            hash: "BLOCK".to_owned(),
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1767146140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1767146140,
            state: "Halt".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "stake".to_owned(),
                kind: "TokenMint".to_owned(),
                name: "TokenMint".to_owned(),
                data: "044B43414C02E900046D61696E".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "token",
                "TokenMint",
                serde_json::json!({
                    "symbol": "KCAL",
                    "tokenId": "233",
                    "seriesId": "7",
                    "mintNumber": 3,
                    "carbonTokenId": 1,
                    "carbonSeriesId": 7,
                    "carbonInstanceId": 3,
                    "owner": "PADDR"
                }),
            )],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1767146140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction)?;
        assert_eq!(
            events[0]
                .payload_json
                .as_ref()
                .and_then(|payload| payload.get("token_mint_extended"))
                .and_then(|payload| payload.get("series_id")),
            Some(&serde_json::json!("7"))
        );
        assert_eq!(
            events[0]
                .payload_json
                .as_ref()
                .and_then(|payload| payload.get("token_mint_extended"))
                .and_then(|payload| payload.get("mint_number")),
            Some(&serde_json::json!("3"))
        );
        Ok(())
    }

    #[test]
    fn decodes_late_legacy_event_payloads() -> Result<(), Box<dyn std::error::Error>> {
        let string_event = decode_legacy_string_event(
            8782346,
            0,
            2,
            "ContractDeploy",
            "0D766D7570676D6F69356331676E",
        )?;
        assert_eq!(string_event, "vmupgmoi5c1gn");

        let infusion = decode_legacy_infusion_event(
            8782417,
            0,
            3,
            "Infusion",
            "054E5541514621BF8E64E7BF56C69680320D64C9F3D5BEF7C33CC427DD031B60D9E50706C4147300054655504F4B020200046D61696E",
        )?;
        assert_eq!(
            infusion,
            LegacyInfusionEventData {
                base_token: "NUAQF".to_owned(),
                token_id:
                    "52052667433246593789336438663450911545914076201283171132542787506277422763711"
                        .to_owned(),
                infused_token: "FUPOK".to_owned(),
                infused_value: "2".to_owned(),
                chain_name: "main".to_owned(),
            }
        );

        let market = decode_legacy_market_event(
            8784909,
            0,
            3,
            "OrderCreated",
            "0A45564E54534F4B4E54410A45564654534F4B4E544121D80D61D1FE14FF261C989BAB545395942D6C77F3F78F76E4C3EF6B270E5AD6A300020700010000",
        )?;
        assert_eq!(
            market,
            LegacyMarketEventData {
                base_token: "EVNTSOKNTA".to_owned(),
                quote_token: "EVFTSOKNTA".to_owned(),
                market_id:
                    "74105721129697041952043878624175796809292042220308677069383500515824111783384"
                        .to_owned(),
                price: "7".to_owned(),
                end_price: "0".to_owned(),
                market_event_kind: "Fixed",
            }
        );

        Ok(())
    }

    #[test]
    fn legacy_token_create_raw_data_matches_csharp_shape() {
        assert_eq!(
            legacy_token_create_raw_data("EVFTSOKNTA", "main"),
            "0A45564654534F4B4E54410100046D61696E"
        );
    }

    #[test]
    fn projects_late_legacy_event_payloads() {
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 8784909,
            hash: "BLOCK".to_owned(),
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1767146140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1767146140,
            state: "Halt".to_owned(),
            events: vec![
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "entry".to_owned(),
                    kind: "ContractDeploy".to_owned(),
                    name: "ContractDeploy".to_owned(),
                    data: "0D766D7570676D6F69356331676E".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "ATPKORGY".to_owned(),
                    kind: "Custom".to_owned(),
                    name: "Custom".to_owned(),
                    data: "04144154504B4F5247593A6F6E4174746163683A7631".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "saturnliquidity".to_owned(),
                    // Self-describing contract event: the node reports kind
                    // "Custom_V2" and carries the real ABI event name. Keep the
                    // event row, preserve the name, and leave the payload opaque
                    // (no "data" key in payload_json; raw bytes go to raw_data).
                    kind: "Custom_V2".to_owned(),
                    name: "AuctionCreated".to_owned(),
                    data: "0103040A63616D706169676E4964030201000406706F6F6C496403020300040D706F6F6C4C6971756964697479030600E876481700".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "token".to_owned(),
                    kind: "Infusion".to_owned(),
                    name: "Infusion".to_owned(),
                    data: "054E5541514621BF8E64E7BF56C69680320D64C9F3D5BEF7C33CC427DD031B60D9E50706C4147300054655504F4B020200046D61696E".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "market".to_owned(),
                    kind: "OrderCreated".to_owned(),
                    name: "OrderCreated".to_owned(),
                    data: "0A45564E54534F4B4E54410A45564654534F4B4E544121D80D61D1FE14FF261C989BAB545395942D6C77F3F78F76E4C3EF6B270E5AD6A300020700010000".to_owned(),
                },
            ],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1767146140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction);

        assert!(events.is_ok(), "{events:?}");
        if let Ok(events) = events {
            assert_eq!(events.len(), 5);
            assert_eq!(events[0].contract.as_deref(), Some("entry"));
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("string_event"))
                    .and_then(|v| v.get("string_value")),
                Some(&serde_json::json!("vmupgmoi5c1gn"))
            );
            assert_eq!(events[1].event_kind, "Custom");
            assert!(
                events[1]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("data"))
                    .is_none()
            );
            // A legacy Custom event only echoes its own kind in `name`. Storing
            // that would duplicate what the read-side COALESCE already produces,
            // so the column stays NULL for every kind except Custom_V2.
            assert!(events[1].event_name.is_none());
            assert_eq!(events[2].event_kind, "Custom_V2");
            assert!(
                events[2]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("data"))
                    .is_none()
            );
            // The self-describing contract event keeps its ABI name, distinct
            // from the "Custom_V2" kind, so the API/UI can label it correctly.
            assert_eq!(events[2].event_name.as_deref(), Some("AuctionCreated"));
            // Native decoded kinds carry no separate name (kind is the label).
            assert!(events[0].event_name.is_none());
            assert!(events[4].event_name.is_none());
            assert_eq!(events[3].contract.as_deref(), Some("NUAQF"));
            assert_eq!(
                events[3].token_id.as_deref(),
                Some(
                    "52052667433246593789336438663450911545914076201283171132542787506277422763711"
                )
            );
            assert_eq!(
                events[3]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("infusion_event"))
                    .and_then(|v| v.get("infused_token")),
                Some(&serde_json::json!("FUPOK"))
            );
            assert_eq!(events[4].contract.as_deref(), Some("EVNTSOKNTA"));
            assert_eq!(
                events[4]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("market_event"))
                    .and_then(|v| v.get("market_event_kind")),
                Some(&serde_json::json!("Fixed"))
            );
        }
    }

    #[test]
    fn projects_governance_payloads_from_legacy_rows_and_extended_data() {
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 42,
            hash: "BLOCK".to_owned(),
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1743530760,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1743530760,
            state: "Halt".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "gas".to_owned(),
                kind: "GasEscrow".to_owned(),
                name: "GasEscrow".to_owned(),
                data: "2202000D6E4079E36703EBD37C00722F5891D28B0E2811DC114B129215123ADCCE36050201000500752B7D00".to_owned(),
            }, SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "GovernanceSetGasConfig".to_owned(),
                name: "GovernanceSetGasConfig".to_owned(),
                data: "00FFFF00000010001027000000000000010000000000000002000000000000000A0000000000000002000000000000000A000000000000000A0000000000000000E40B540200000000E40B540200000000F902950000000090D003000000000000A0724E18090000010000000000000001".to_owned(),
            }, SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "SpecialResolution".to_owned(),
                name: "SpecialResolution".to_owned(),
                data: "0100000000000000".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "governance",
                "SpecialResolution",
                serde_json::json!({
                    "resolutionId": 1,
                    "description": "Special",
                    "calls": [{
                        "moduleId": 0,
                        "module": "governance",
                        "methodId": 3,
                        "method": "SetGasConfig",
                        "arguments": { "gas_fee_query": "10" }
                    }]
                }),
            )],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1743530760,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction);

        assert!(events.is_ok(), "{events:?}");
        if let Ok(events) = events {
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].event_kind, "GasEscrow");
            assert_eq!(events[0].event_index, 1);
            assert_eq!(events[0].payload_format.as_deref(), Some("live.v1"));
            // The stored payload must NOT carry 'chain'/'address': both are
            // relational columns, stripped at rest since migration 202608040003
            // and re-inserted by the API at serve time.
            assert_eq!(
                events[0].payload_json.as_ref().and_then(|v| v.get("chain")),
                None
            );
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("address")),
                None
            );
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("gas_event"))
                    .and_then(|v| v.get("amount")),
                Some(&serde_json::json!("2100000000"))
            );
            assert_eq!(events[1].event_kind, "GovernanceSetGasConfig");
            assert_eq!(
                events[1]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("governance_gas_config_event"))
                    .and_then(|v| v.get("gas_fee_create_token_series")),
                Some(&serde_json::json!("2500000000"))
            );
            // A version-0 blob must produce exactly the 19 v1 keys and none of
            // the gas-model-v2 tail, so payloads of rows ingested before the v2
            // flip stay byte-identical on re-projection.
            let v0_payload = events[1]
                .payload_json
                .as_ref()
                .and_then(|v| v.get("governance_gas_config_event"))
                .and_then(Value::as_object);
            assert_eq!(v0_payload.map(serde_json::Map::len), Some(19));
            assert_eq!(v0_payload.and_then(|p| p.get("minimum_gas_bill")), None);
            assert_eq!(events[2].event_kind, "SpecialResolution");
            assert_eq!(events[2].event_index, 3);
            assert_eq!(
                events[2]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("special_resolution_event"))
                    .and_then(|v| v.get("resolution_id")),
                Some(&serde_json::json!("1"))
            );
            assert_eq!(
                events[2]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("special_resolution_event"))
                    .and_then(|v| v.get("calls"))
                    .and_then(Value::as_array)
                    .and_then(|calls| calls.first())
                    .and_then(|call| call.get("module_id")),
                Some(&serde_json::json!(0))
            );
        }
    }

    #[test]
    fn decodes_gas_model_v2_config_event_with_extension_fields() {
        // Real GovernanceSetGasConfig blob from localnet block 8,937,519 (the
        // gas-model-v2 flip SR): 179 bytes = the 113-byte v0 image plus the
        // 66-byte v2 tail. Expected values match the node's own decoding of the
        // resolution (note data_escrow_per_row 200000 is the value this SR set;
        // the chain's current config may differ after later SRs).
        let raw_data = "01FFFF00000010001027000000000000010000000000000002000000000000000A00000000000000400D0300000000000A000000000000000A0000000000000000E40B540200000000E40B540200000000F902950000000090D003000000000000A0724E18090000010000000000000001809698000000000001000000000000000201000000000000000300E876481700000000E876481700000000BA1DD2050000000010A5D4E80000000200000000000000";
        let config = decode_carbon_event_or_default::<GasConfig>(
            8_937_519,
            0,
            0,
            "GovernanceSetGasConfig",
            raw_data,
        );
        // A silent parse failure would fall back to an all-zero default; the
        // version check catches that before the field-level assertions.
        assert!(config.has_gas_model_v2());

        let payload = build_governance_gas_config_payload(&config);
        let object = payload.as_object();
        // 19 v1 keys + the 10-field v2 tail.
        assert_eq!(object.map(serde_json::Map::len), Some(29));
        let get = |key: &str| {
            object
                .and_then(|payload| payload.get(key))
                .and_then(Value::as_str)
        };
        assert_eq!(get("version"), Some("1"));
        assert_eq!(get("fee_multiplier"), Some("10000"));
        assert_eq!(get("data_escrow_per_row"), Some("200000"));
        assert_eq!(get("gas_burn_ratio_mul"), Some("1"));
        assert_eq!(get("gas_burn_ratio_shift"), Some("1"));
        assert_eq!(get("minimum_gas_bill"), Some("10000000"));
        assert_eq!(get("gas_producer_ratio_mul"), Some("1"));
        assert_eq!(get("gas_producer_ratio_shift"), Some("2"));
        assert_eq!(get("gas_dapp_ratio_mul"), Some("1"));
        assert_eq!(get("gas_dapp_ratio_shift"), Some("3"));
        assert_eq!(get("policy_fee_create_token_base"), Some("100000000000"));
        assert_eq!(get("policy_fee_create_token_symbol"), Some("100000000000"));
        assert_eq!(get("policy_fee_create_token_series"), Some("25000000000"));
        assert_eq!(get("policy_fee_register_name"), Some("1000000000000"));
        assert_eq!(get("legacy_data_escrow_per_row"), Some("2"));
    }

    #[test]
    fn defaults_malformed_governance_config_events_like_csharp() {
        // Saturn contracts on mainnet emitted dynamic VM structs under the
        // governance config event kinds. C# keeps those rows and serializes a
        // default config payload, so Rust must not fail the whole block.
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 8_785_038,
            hash: "BLOCK".to_owned(),
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1_743_530_760,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1_743_530_760,
            state: "Halt".to_owned(),
            events: vec![
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "saturnpools".to_owned(),
                    kind: "GovernanceSetGasConfig".to_owned(),
                    name: "GovernanceSetGasConfig".to_owned(),
                    data: "01040406706F6F6C49640302010004076E657752657341030600CC829C190004076E65775265734203060098053933000406726561736F6E040C6164644C6971756964697479".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "saturnholders".to_owned(),
                    kind: "GovernanceSetChainConfig".to_owned(),
                    name: "GovernanceSetChainConfig".to_owned(),
                    data: "0104040B746F6B656E53796D626F6C04074456414A58564204097363616C6564466565030420A1070004077265616C466565030420A10700040A746F74616C5374616B65030600E40B540200".to_owned(),
                },
            ],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1_743_530_760,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction);

        assert!(events.is_ok(), "{events:?}");
        if let Ok(events) = events {
            assert_eq!(events.len(), 2);
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("governance_gas_config_event"))
                    .and_then(|v| v.get("gas_fee_query")),
                Some(&serde_json::json!("0"))
            );
            assert_eq!(
                events[1]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("governance_chain_config_event"))
                    .and_then(|v| v.get("expiry_window")),
                Some(&serde_json::json!("0"))
            );
        }
    }

    #[test]
    fn skips_numeric_legacy_event_kinds_like_csharp() -> Result<(), Box<dyn std::error::Error>> {
        // C# drops unsupported numeric event kinds before insertion, but keeps
        // the original indexes for later events in the same transaction.
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 8_785_036,
            hash: "BLOCK".to_owned(),
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1_743_530_760,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1_743_530_760,
            state: "Halt".to_owned(),
            events: vec![
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "gas".to_owned(),
                    kind: "GasEscrow".to_owned(),
                    name: "GasEscrow".to_owned(),
                    data: "2202000D6E4079E36703EBD37C00722F5891D28B0E2811DC114B129215123ADCCE3605020100070080F420E6B500".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "saturnadmin".to_owned(),
                    kind: "72".to_owned(),
                    name: "72".to_owned(),
                    data: "0104040B7265696E7665737450637403023700040B70726F766964657250637403020A00040861646D696E50637403021E000409686F6C64657250637403020500".to_owned(),
                },
                SdkEventResult {
                    address: "PADDR".to_owned(),
                    contract: "gas".to_owned(),
                    kind: "GasPayment".to_owned(),
                    name: "GasPayment".to_owned(),
                    data: "2202000D6E4079E36703EBD37C00722F5891D28B0E2811DC114B129215123ADCCE360502010005F03B9F0200".to_owned(),
                },
            ],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1_743_530_760,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction)?;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_kind, "GasEscrow");
        assert_eq!(events[0].event_index, 1);
        assert_eq!(events[1].event_kind, "GasPayment");
        assert_eq!(events[1].event_index, 3);

        Ok(())
    }

    #[test]
    fn accepts_raw_non_governance_special_resolution_without_extended_payload() {
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "saturnrental".to_owned(),
                kind: "SpecialResolution".to_owned(),
                name: "SpecialResolution".to_owned(),
                data: "0103040872656E74616C496403020100".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "saturnrental",
                "SpecialResolution",
                serde_json::json!({ "valueKind": "Object" }),
            )],
            ..Default::default()
        };

        assert!(!transaction_has_incomplete_special_resolution(&transaction));
        assert!(
            TxExtendedEventContext::from_transaction(&transaction)
                .special_resolution
                .is_none()
        );
    }

    #[test]
    fn flags_governance_special_resolution_placeholder_payload() {
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "SpecialResolution".to_owned(),
                name: "SpecialResolution".to_owned(),
                data: "0100000000000000".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "governance",
                "SpecialResolution",
                serde_json::json!({ "valueKind": "Object" }),
            )],
            ..Default::default()
        };

        assert!(transaction_has_incomplete_special_resolution(&transaction));
        assert!(
            TxExtendedEventContext::from_transaction(&transaction)
                .special_resolution
                .is_none()
        );
    }

    #[test]
    fn does_not_chase_an_extended_payload_this_build_cannot_type() {
        // A payload that does not match the modeled shape — here `decimals` arriving as
        // text, the way a node newer than our SDK would answer — is kept verbatim by the
        // SDK and must NOT be treated as an incomplete payload: refetching cannot change
        // it, and the block would be refused after twenty-five tries, stalling the sync
        // on every block carrying that event.
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenCreate".to_owned(),
                name: "TokenCreate".to_owned(),
                data: "04464C414700046D61696E".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "token",
                "TokenCreate",
                serde_json::json!({
                    "symbol": "FLAG",
                    "maxSupply": "100000000",
                    "decimals": "eight",
                    "isNonFungible": false,
                    "carbonTokenId": 42,
                    "metadata": {}
                }),
            )],
            ..Default::default()
        };

        assert!(
            transaction.extended_events[0].data.as_unknown().is_some(),
            "the fixture must be a payload the SDK could not type"
        );
        assert!(!transaction_has_incomplete_token_create(&transaction));
        assert!(!transaction_has_incomplete_extended_payload(&transaction));
    }

    #[test]
    fn flags_placeholder_token_series_extended_events() {
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "SpecialResolution".to_owned(),
                name: "SpecialResolution".to_owned(),
                data: "2100000000000000".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "token",
                "TokenSeriesCreate",
                serde_json::json!({ "valueKind": "Object" }),
            )],
            ..Default::default()
        };

        assert!(transaction_has_incomplete_token_series_create(&transaction));
    }

    #[test]
    fn projects_token_create_payload_from_extended_data() {
        // TokenCreate projections must preserve extended metadata used later by
        // the DB layer to upsert the token row linked to the create event.
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 42,
            hash: "BLOCK".to_owned(),
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1767146140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1767146140,
            state: "Halt".to_owned(),
            events: vec![SdkEventResult {
                address: "PADDR".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenCreate".to_owned(),
                name: "TokenCreate".to_owned(),
                data: "065348414D414E0100046D61696E".to_owned(),
            }],
            extended_events: vec![extended_event(
                "PADDR",
                "token",
                "TokenCreate",
                serde_json::json!({
                    "symbol": "SHAMAN",
                    "maxSupply": "100",
                    "decimals": 0,
                    "isNonFungible": false,
                    "carbonTokenId": 49,
                    "metadata": {
                        "name": "Shaman Bronze",
                        "url": "https://en.wikipedia.org/wiki/Shamanism"
                    }
                }),
            )],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1767146140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction);

        assert!(events.is_ok(), "{events:?}");
        if let Ok(events) = events {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_kind, "TokenCreate");
            assert_eq!(events[0].contract.as_deref(), Some("token"));
            assert_eq!(events[0].token_id, None);
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("token_create_event"))
                    .and_then(|v| v.get("symbol")),
                Some(&serde_json::json!("SHAMAN"))
            );
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("token_create"))
                    .and_then(|v| v.get("carbon_token_id")),
                Some(&serde_json::json!("49"))
            );
        }
    }

    #[test]
    fn token_create_extended_payload_is_consumed_once_per_transaction()
    -> Result<(), Box<dyn std::error::Error>> {
        // C# stores only the first TokenCreate extended payload in a transaction.
        // Special-resolution repair rows after that must stay raw-only even when
        // the node exposes enough metadata to enrich them.
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 8_784_699,
            hash: "BLOCK".to_owned(),
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1_767_146_140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            timestamp: 1_767_146_140,
            state: "Halt".to_owned(),
            events: vec![
                SdkEventResult {
                    address: "PTAZ".to_owned(),
                    contract: "token".to_owned(),
                    kind: "TokenCreate".to_owned(),
                    name: "TokenCreate".to_owned(),
                    data: legacy_token_create_raw_data("TAZ", "main"),
                },
                SdkEventResult {
                    address: "PBAD".to_owned(),
                    contract: "token".to_owned(),
                    kind: "TokenCreate".to_owned(),
                    name: "TokenCreate".to_owned(),
                    data: legacy_token_create_raw_data("BADZEROQ", "main"),
                },
            ],
            extended_events: vec![
                extended_event(
                    "PTAZ",
                    "token",
                    "TokenCreate",
                    serde_json::json!({
                        "symbol": "TAZ",
                        "maxSupply": "0",
                        "decimals": 9,
                        "isNonFungible": false,
                        "carbonTokenId": 51,
                        "metadata": { "name": "Transplanetary Artificial Zenith" }
                    }),
                ),
                extended_event(
                    "PBAD",
                    "token",
                    "TokenCreate",
                    serde_json::json!({
                        "symbol": "BADZEROQ",
                        "maxSupply": "0",
                        "decimals": 8,
                        "isNonFungible": false,
                        "carbonTokenId": 312,
                        "metadata": { "name": "BADZEROQ token semantics V2 probe" }
                    }),
                ),
            ],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1_767_146_140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction)?;

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0]
                .payload_json
                .as_ref()
                .and_then(|payload| payload.get("token_create_event"))
                .and_then(|payload| payload.get("symbol")),
            Some(&serde_json::json!("TAZ"))
        );
        assert!(
            events[1]
                .payload_json
                .as_ref()
                .is_none_or(|payload| !payload
                    .as_object()
                    .is_some_and(|object| object.contains_key("token_create_event"))),
            "later TokenCreate rows must remain raw-only"
        );
        Ok(())
    }

    #[test]
    fn token_create_payload_matches_csharp_event_shape() {
        // Token table flags are derived by the DB side-effect. The event JSON
        // itself must stay compatible with C# API/parity payloads.
        let event = extended_event(
            "PADDR",
            "token",
            "TokenCreate",
            serde_json::json!({
                "symbol": "FLAG",
                "name": "Top Level Name",
                "maxSupply": "100000000",
                "decimals": 8,
                "isNonFungible": false,
                "metadata": {
                    "token_name": "Flag Token",
                    "token_flags": "Fungible|Transferable|Finite|Divisible|Burnable"
                }
            }),
        );
        let token_create = token_create_payload(&event.data);
        assert!(
            token_create.is_some(),
            "the fixture must decode into the modeled TokenCreate shape"
        );
        let Some(token_create) = token_create else {
            return;
        };
        let payload = build_token_create_payload(token_create);

        assert_eq!(
            payload,
            serde_json::json!({
                "symbol": "FLAG",
                "max_supply": "100000000",
                "decimals": "8",
                "is_non_fungible": false,
                "metadata": {
                    "token_name": "Flag Token",
                    "token_flags": "Fungible|Transferable|Finite|Divisible|Burnable"
                }
            })
        );
    }

    #[test]
    fn synthesizes_token_series_create_from_extended_data() {
        // TokenSeriesCreate has no legacy RPC event, so projection synthesizes
        // the event from extended data while preserving the legacy event order.
        let block = BlockRecord {
            id: 1,
            chain_id: 1,
            chain: "main".to_owned(),
            height: 42,
            hash: "BLOCK".to_owned(),
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            producer_address_id: None,
            producer_address: None,
            timestamp_unix_seconds: 1767146140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            gas_payer: "POWNER".to_owned(),
            timestamp: 1767146140,
            state: "Halt".to_owned(),
            events: Vec::new(),
            extended_events: vec![extended_event(
                "POWNER",
                "token",
                "TokenSeriesCreate",
                serde_json::json!({
                    "symbol": "POPIMEW",
                    "seriesId": "78420994489752471120082872831289854578636467435124725846496638966668030965675",
                    "maxMint": 0,
                    "maxSupply": 0,
                    "owner": "POWNER",
                    "carbonTokenId": 58,
                    "carbonSeriesId": 1,
                    "metadata": {
                        "seriesId": "78420994489752471120082872831289854578636467435124725846496638966668030965675",
                        "mode": "0",
                        "rom": ""
                    }
                }),
            )],
            ..Default::default()
        };
        let transaction_record = TransactionRecord {
            id: 1,
            block_id: block.id,
            chain_id: block.chain_id,
            tx_index: 0,
            hash: transaction.hash.clone(),
            timestamp_unix_seconds: 1767146140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee_raw: None,
            gas_price_raw: None,
            gas_limit_raw: None,
            sender_id: 1,
            gas_payer_id: 1,
            gas_target_id: 1,
            carbon_tx_type: None,
            carbon_tx_data: None,
            expiration_unix_seconds: 0,
        };

        let events =
            transaction_events_to_projections(&block, &transaction_record, 0, &transaction);

        assert!(events.is_ok(), "{events:?}");
        if let Ok(events) = events {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].event_kind, "TokenSeriesCreate");
            assert_eq!(events[0].contract.as_deref(), Some("POPIMEW"));
            assert_eq!(events[0].address.as_deref(), Some("POWNER"));
            assert_eq!(events[0].raw_data.as_deref(), Some(""));
            assert_eq!(
                events[0]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("token_series_event"))
                    .and_then(|v| v.get("carbon_series_id")),
                Some(&serde_json::json!("1"))
            );
        }
    }
}
