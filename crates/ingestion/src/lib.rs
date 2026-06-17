use explorer_config::{ChainConfig, WorkerConfig, WorkerSyncMode};
use explorer_db::{
    AddressAccountUpsert, AddressBalanceUpsert, BlockRecord, BlockUpsert,
    ContractRpcMetadataCandidate, ContractRpcMetadataUpsert, ContractStringEventSideEffectReport,
    ContractUpgradeMethodCandidate, ContractUpgradeMethodUpsert, DirtyAddress, EventSource,
    EventUpsert, NftRpcMetadataCandidate, NftRpcMetadataUpsert, RawBlockRecord,
    SeriesRpcMetadataCandidate, SeriesRpcMetadataUpsert, TokenSupplyUpsert, TransactionRecord,
    TransactionSignatureUpsert, TransactionUpsert,
};
use explorer_domain::{BlockHeight, ChainName, MAIN_ZERO_STATE_BOUNDARY_HEIGHT};
use explorer_rpc::{
    PhantasmaSdkClient, RpcError, SdkAccountResult, SdkBlockResult, SdkContractResult,
    SdkEventResult, SdkTokenDataResult, SdkTokenPropertyResult, SdkTokenResult,
    SdkTokenSeriesResult, SdkTransactionResult, decode_block_result,
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
const LEGACY_GAS_TOKEN_SYMBOL: &str = "KCAL";
const SPECIAL_RESOLUTION_REFETCH_ATTEMPTS: usize = 25;
const SPECIAL_RESOLUTION_REFETCH_DELAY_MS: u64 = 50;
const BALANCE_SYNC_LAG_THRESHOLD: u64 = 50;
const BALANCE_SYNC_CHUNK_SIZE: usize = 100;
const STAKE_PROJECTION_INTERVAL_SECONDS: u64 = 30;
const TOKEN_SUPPLY_SYNC_INTERVAL_SECONDS: u64 = 60;
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
        "refusing to sync chain {chain:?} from configured nexus {configured_nexus:?}; cursor is already at {cursor_height}"
    )]
    ProtectedZeroStateNexusMismatch {
        configured_nexus: String,
        chain: String,
        cursor_height: u64,
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
        previous_hash: non_empty_string(&block.previous_hash),
        protocol: Some(i32::try_from(block.protocol).map_err(|_| {
            IngestionError::BlockFieldOutOfRange {
                height: height.value(),
                field: "protocol",
            }
        })?),
        chain_address: non_empty_string(&block.chain_address),
        validator_address: non_empty_string(&block.validator_address),
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
    kcal_decimals: i32,
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
    let kcal_decimals =
        usize::try_from(kcal_decimals).map_err(|_| IngestionError::TransactionFieldOutOfRange {
            height: block_height,
            index: tx_index,
            field: "kcal_decimals",
        })?;
    let fee_raw = non_empty_string(&transaction.fee);
    let gas_price_raw = non_empty_string(&transaction.gas_price);
    let gas_limit_raw = non_empty_string(&transaction.gas_limit);
    let fee = fee_raw
        .as_deref()
        .map(|amount| format_token_amount(amount, kcal_decimals));
    let gas_price = gas_price_raw
        .as_deref()
        .map(|amount| format_token_amount(amount, kcal_decimals));
    let gas_limit = gas_limit_raw.as_deref().and_then(|amount| {
        (amount != LEGACY_UNLIMITED_GAS_RAW).then(|| format_token_amount(amount, kcal_decimals))
    });

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
        fee,
        fee_raw,
        gas_price,
        gas_price_raw,
        gas_limit,
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

fn format_token_amount(amount: &str, token_decimals: usize) -> String {
    if amount == "0" || token_decimals == 0 {
        return amount.to_owned();
    }

    if amount.len() <= token_decimals {
        let mut padded = "0".repeat(token_decimals - amount.len());
        padded.push_str(amount);
        return format!("0.{}", padded.trim_end_matches('0'));
    }

    let decimal_start = amount.len() - token_decimals;
    let decimal_part = &amount[decimal_start..];
    let decimal_part = decimal_part
        .chars()
        .any(|character| character != '0')
        .then(|| decimal_part.trim_end_matches('0'));

    match decimal_part {
        Some(decimal_part) => format!("{}.{}", &amount[..decimal_start], decimal_part),
        None => amount[..decimal_start].to_owned(),
    }
}

fn account_result_to_upsert(
    address_id: i32,
    account: &SdkAccountResult,
    soul_decimals: i32,
    kcal_decimals: i32,
    now_unix_seconds: i64,
) -> AddressAccountUpsert {
    let staked_amount_raw = normalized_amount_raw(&account.stakes.amount);
    let unclaimed_amount_raw = normalized_amount_raw(&account.stakes.unclaimed);
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
            Some(AddressBalanceUpsert {
                symbol,
                amount: format_token_amount(&amount_raw, balance.decimals as usize),
                amount_raw,
            })
        })
        .collect();
    let address_name =
        non_empty_string(&account.name).filter(|name| !name.eq_ignore_ascii_case("anonymous"));
    let validator_kind =
        non_empty_string(&account.validator).unwrap_or_else(|| "Invalid".to_owned());

    AddressAccountUpsert {
        address_id,
        address_name,
        name_last_updated_unix_seconds: now_unix_seconds,
        stake_timestamp: i64::try_from(account.stakes.time).unwrap_or(i64::MAX),
        staked_amount: format_token_amount(&staked_amount_raw, decimals_to_usize(soul_decimals)),
        staked_amount_raw,
        unclaimed_amount: format_token_amount(
            &unclaimed_amount_raw,
            decimals_to_usize(kcal_decimals),
        ),
        unclaimed_amount_raw,
        soul_balance_raw,
        storage_available: i64::try_from(account.storage.available).unwrap_or(i64::MAX),
        storage_used: i64::try_from(account.storage.used).unwrap_or(i64::MAX),
        avatar: non_empty_string(&account.storage.avatar),
        validator_kind,
        balances,
    }
}

fn token_result_to_supply_upsert(token: &SdkTokenResult) -> TokenSupplyUpsert {
    let decimals = token.decimals as usize;
    let current_supply_raw = normalized_amount_raw(&token.current_supply);
    let max_supply_raw = normalized_amount_raw(&token.max_supply);
    let burned_supply_raw = normalized_amount_raw(&token.burned_supply);

    TokenSupplyUpsert {
        symbol: token.symbol.clone(),
        carbon_id: non_empty_string(&token.carbon_id).and_then(|value| value.parse().ok()),
        current_supply: format_token_amount(&current_supply_raw, decimals),
        current_supply_raw,
        max_supply: format_token_amount(&max_supply_raw, decimals),
        max_supply_raw,
        burned_supply: format_token_amount(&burned_supply_raw, decimals),
        burned_supply_raw,
    }
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

fn token_properties_to_metadata(properties: &[SdkTokenPropertyResult]) -> Map<String, Value> {
    let mut metadata = Map::new();
    for property in properties {
        insert_metadata_string(
            &mut metadata,
            &property.key,
            non_empty_string(&property.value),
        );
    }
    metadata
}

fn token_property_value(properties: &[SdkTokenPropertyResult], key: &str) -> Option<String> {
    properties
        .iter()
        .find(|property| property.key.eq_ignore_ascii_case(key))
        .and_then(|property| non_empty_string(&property.value))
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
    metadata.insert(key, Value::String(value));
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

fn decimals_to_usize(decimals: i32) -> usize {
    usize::try_from(decimals).unwrap_or_default()
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
            &block.chain,
            transaction_record,
            tx_index,
            event_index,
            event,
            &mut extended_context,
        )?);
    }

    let mut next_synthetic_event_index = transaction.events.len();

    if !has_legacy_special_resolution
        && let Some(special_resolution) = extended_context.special_resolution.as_ref()
    {
        let synthetic_index = next_synthetic_event_index;
        next_synthetic_event_index += 1;
        let synthetic_event = SdkEventResult {
            address: transaction.gas_payer.clone(),
            contract: "governance".to_owned(),
            kind: "SpecialResolution".to_owned(),
            name: "SpecialResolution".to_owned(),
            data: special_resolution_raw_data(
                block_height,
                tx_index,
                synthetic_index,
                special_resolution,
            )?,
        };
        events.push(event_to_projection(
            block_height,
            &block.chain,
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
            address: token_series_create
                .get("owner")
                .and_then(Value::as_str)
                .unwrap_or(&transaction.gas_payer)
                .to_owned(),
            contract: token_series_create
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or("token")
                .to_owned(),
            kind: "TokenSeriesCreate".to_owned(),
            name: "TokenSeriesCreate".to_owned(),
            data: String::new(),
        };
        events.push(event_to_projection(
            block_height,
            &block.chain,
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

    !special_resolution_payload_is_complete(&extended.data)
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

    !token_create_payload_is_complete(&extended.data)
}

fn transaction_has_incomplete_token_series_create(transaction: &SdkTransactionResult) -> bool {
    let Some(extended) = transaction
        .extended_events
        .iter()
        .find(|event| event.kind.eq_ignore_ascii_case("TokenSeriesCreate"))
    else {
        return false;
    };

    !token_series_create_payload_is_complete(&extended.data)
}

fn special_resolution_payload_is_complete(data: &Value) -> bool {
    data.get("resolutionId").is_some() && data.get("calls").and_then(Value::as_array).is_some()
}

fn legacy_event_kind_name(event: &SdkEventResult) -> String {
    non_empty_string(&event.kind)
        .or_else(|| non_empty_string(&event.name))
        .unwrap_or_else(|| "Unknown".to_owned())
}

fn is_numeric_legacy_event_kind(event_kind: &str) -> bool {
    !event_kind.is_empty() && event_kind.bytes().all(|byte| byte.is_ascii_digit())
}

fn token_create_payload_is_complete(data: &Value) -> bool {
    data.get("symbol").and_then(Value::as_str).is_some()
        && data.get("maxSupply").is_some()
        && data.get("decimals").is_some()
        && data.get("isNonFungible").and_then(Value::as_bool).is_some()
        && data.get("carbonTokenId").is_some()
        && data.get("metadata").and_then(Value::as_object).is_some()
}

fn token_series_create_payload_is_complete(data: &Value) -> bool {
    data.get("symbol").and_then(Value::as_str).is_some()
        && data.get("seriesId").is_some()
        && data.get("maxMint").is_some()
        && data.get("maxSupply").is_some()
        && data.get("owner").and_then(Value::as_str).is_some()
        && data.get("carbonTokenId").is_some()
        && data.get("carbonSeriesId").is_some()
        && data.get("metadata").and_then(Value::as_object).is_some()
}

fn token_mint_payload_is_complete(data: &Value) -> bool {
    data.get("symbol").and_then(Value::as_str).is_some()
        && data.get("tokenId").is_some()
        && data.get("mintNumber").is_some()
        && data.get("carbonTokenId").is_some()
        && data.get("carbonSeriesId").is_some()
        && data.get("carbonInstanceId").is_some()
}

#[derive(Debug, Default)]
struct TxExtendedEventContext<'a> {
    special_resolution: Option<Value>,
    token_create: Option<Value>,
    token_create_consumed: bool,
    token_series_create: Option<&'a Value>,
    token_mint: Option<&'a Value>,
}

impl<'a> TxExtendedEventContext<'a> {
    fn from_transaction(transaction: &'a SdkTransactionResult) -> Self {
        let events = &transaction.extended_events;
        let special_resolution = events
            .iter()
            .find(|event| {
                event.kind.eq_ignore_ascii_case("SpecialResolution")
                    && special_resolution_payload_is_complete(&event.data)
            })
            .map(|event| event.data.clone());
        let token_create = token_create_payload_from_extended_events(events);
        let token_series_create = events
            .iter()
            .find(|event| event.kind.eq_ignore_ascii_case("TokenSeriesCreate"))
            .map(|event| &event.data)
            .filter(|data| token_series_create_payload_is_complete(data));
        let token_mint = events
            .iter()
            .find(|event| event.kind.eq_ignore_ascii_case("TokenMint"))
            .map(|event| &event.data)
            .filter(|data| token_mint_payload_is_complete(data));

        Self {
            special_resolution,
            token_create,
            token_create_consumed: false,
            token_series_create,
            token_mint,
        }
    }

    fn take_token_create_for_event(&mut self) -> Option<&Value> {
        if self.token_create_consumed || self.token_create.is_none() {
            return None;
        }

        // The C# backend calls ExtendedEventParser.GetTokenCreateData(), which
        // returns the first TokenCreate extended event only. Once that payload
        // is applied, later TokenCreate rows in the same transaction are
        // left as raw compatibility envelopes even when their raw symbol differs.
        self.token_create_consumed = true;
        self.token_create.as_ref()
    }
}

fn event_to_projection(
    block_height: u64,
    chain_name: &str,
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
    let mut payload_json = serde_json::json!({
        "event_kind": &event_kind,
        "chain": chain_name,
        "contract": &payload_contract,
        "address": &address,
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
        if let Some(special_resolution) = extended_context.special_resolution.as_ref() {
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
            if let Some(series_id) = token_series_create
                .get("seriesId")
                .and_then(json_scalar_to_string)
                .or_else(|| {
                    token_series_create
                        .get("carbonSeriesId")
                        .and_then(json_scalar_to_string)
                })
            {
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
    } else {
        payload_json = serde_json::json!({
            "address": &event.address,
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
        address: Some(address),
        target_address: None,
        contract: Some(contract),
        token_id,
        raw_data,
        payload_format: Some("live.v1".to_owned()),
        payload_json: Some(payload_json),
        timestamp_unix_seconds: transaction_record.timestamp_unix_seconds,
        date_unix_seconds: unix_day_start(transaction_record.timestamp_unix_seconds),
        burned: None,
        nsfw: false,
        blacklisted: false,
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
            // instead of rejecting historical Saturn contract events.
            warn!(
                block_height,
                tx_index,
                event_index,
                event_kind,
                error = %error,
                "using default governance config payload for malformed Carbon event"
            );
            T::default()
        }
    }
}

fn token_create_payload_from_extended_events(
    events: &[explorer_rpc::SdkEventExResult],
) -> Option<Value> {
    events
        .iter()
        .find(|event| event.kind.eq_ignore_ascii_case("TokenCreate"))
        .map(|event| event.data.clone())
        .filter(token_create_payload_is_complete)
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
    serde_json::json!({
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
    })
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

fn build_special_resolution_payload(data: &Value) -> Value {
    let mut payload = Map::new();
    if let Some(resolution_id) = data.get("resolutionId").and_then(json_scalar_to_string) {
        payload.insert("resolution_id".to_owned(), Value::String(resolution_id));
    }
    if let Some(description) = data.get("description").and_then(Value::as_str) {
        payload.insert(
            "description".to_owned(),
            Value::String(description.to_owned()),
        );
    }
    payload.insert(
        "calls".to_owned(),
        build_special_resolution_calls(data.get("calls")),
    );
    Value::Object(payload)
}

fn build_special_resolution_calls(calls: Option<&Value>) -> Value {
    let Some(calls) = calls.and_then(Value::as_array) else {
        return Value::Array(Vec::new());
    };

    Value::Array(
        calls
            .iter()
            .map(|call| {
                let mut payload = Map::new();
                if let Some(module) = call.get("module").and_then(Value::as_str) {
                    payload.insert("module".to_owned(), Value::String(module.to_owned()));
                }
                if let Some(module_id) = call.get("moduleId") {
                    payload.insert("module_id".to_owned(), module_id.clone());
                }
                if let Some(method) = call.get("method").and_then(Value::as_str) {
                    payload.insert("method".to_owned(), Value::String(method.to_owned()));
                }
                if let Some(method_id) = call.get("methodId") {
                    payload.insert("method_id".to_owned(), method_id.clone());
                }
                if let Some(arguments) = call.get("arguments") {
                    payload.insert("arguments".to_owned(), arguments.clone());
                }
                payload.insert(
                    "calls".to_owned(),
                    build_special_resolution_calls(call.get("calls")),
                );
                Value::Object(payload)
            })
            .collect(),
    )
}

fn build_token_create_payload(data: &Value) -> Value {
    let mut payload = Map::new();
    let metadata = data.get("metadata").cloned().unwrap_or(Value::Null);

    if let Some(symbol) = data.get("symbol").and_then(Value::as_str) {
        payload.insert("symbol".to_owned(), Value::String(symbol.to_owned()));
    }
    if let Some(max_supply) = data.get("maxSupply").and_then(json_scalar_to_string) {
        payload.insert("max_supply".to_owned(), Value::String(max_supply));
    }
    if let Some(decimals) = data.get("decimals").and_then(json_scalar_to_string) {
        payload.insert("decimals".to_owned(), Value::String(decimals));
    }
    if let Some(is_non_fungible) = data.get("isNonFungible").and_then(Value::as_bool) {
        payload.insert("is_non_fungible".to_owned(), Value::Bool(is_non_fungible));
    }
    if let Some(carbon_token_id) = data.get("carbonTokenId").and_then(json_scalar_to_string) {
        payload.insert("carbon_token_id".to_owned(), Value::String(carbon_token_id));
    }
    payload.insert("metadata".to_owned(), metadata);
    Value::Object(payload)
}

fn build_token_series_create_payload(data: &Value) -> Value {
    let mut payload = Map::new();
    if let Some(symbol) = data.get("symbol").and_then(Value::as_str) {
        payload.insert("token".to_owned(), Value::String(symbol.to_owned()));
    }
    let series_id = data
        .get("seriesId")
        .and_then(json_scalar_to_string)
        .or_else(|| data.get("carbonSeriesId").and_then(json_scalar_to_string));
    if let Some(series_id) = series_id {
        payload.insert("series_id".to_owned(), Value::String(series_id));
    }
    if let Some(max_mint) = data.get("maxMint").and_then(json_scalar_to_string) {
        payload.insert("max_mint".to_owned(), Value::String(max_mint));
    }
    if let Some(max_supply) = data.get("maxSupply").and_then(json_scalar_to_string) {
        payload.insert("max_supply".to_owned(), Value::String(max_supply));
    }
    if let Some(owner) = data.get("owner").and_then(Value::as_str) {
        payload.insert("owner".to_owned(), Value::String(owner.to_owned()));
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
    payload.insert(
        "metadata".to_owned(),
        data.get("metadata").cloned().unwrap_or(Value::Null),
    );
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

fn special_resolution_raw_data(
    block_height: u64,
    tx_index: usize,
    event_index: usize,
    data: &Value,
) -> Result<String, IngestionError> {
    let resolution_id = data
        .get("resolutionId")
        .and_then(json_scalar_to_u64)
        .ok_or_else(|| {
            legacy_event_decode_error(block_height, tx_index, event_index, "SpecialResolution")
        })?;
    Ok(encode_hex_upper(resolution_id.to_le_bytes()))
}

fn json_scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

fn json_scalar_to_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Number(value) => value.as_u64(),
        Value::String(value) => value.parse().ok(),
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

fn unix_day_start(timestamp_unix_seconds: i64) -> i64 {
    timestamp_unix_seconds - timestamp_unix_seconds.rem_euclid(86_400)
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

pub fn plan_fetch_window(
    current_height: BlockHeight,
    rpc_tip: BlockHeight,
    settings: &WorkerConfig,
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

    let to_height = next_height
        .saturating_add(settings.effective_fetch_batch_size().saturating_sub(1))
        .min(target_tip);

    let block_count = to_height.saturating_sub(next_height).saturating_add(1);
    let concurrency = settings
        .effective_fetch_concurrency()
        .min(block_count as usize);

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
    use explorer_rpc::SdkEventExResult;
    use std::time::Duration;

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
        let window =
            plan_fetch_window(BlockHeight::new(10), BlockHeight::new(25), &worker_config());

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

        let window = plan_fetch_window(BlockHeight::new(10), BlockHeight::new(25), &settings);

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
        let window =
            plan_fetch_window(BlockHeight::new(25), BlockHeight::new(25), &worker_config());

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

        let window = plan_fetch_window(BlockHeight::new(10), BlockHeight::new(25), &settings);

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
                previous_hash: Some(previous_hash),
                protocol: Some(18),
                ..
            }) if hash == "ABC" && previous_hash == "PREV"
        ));
        Ok(())
    }

    #[test]
    fn formats_token_amount_like_csharp_utils() {
        assert_eq!(format_token_amount("0", 10), "0");
        assert_eq!(format_token_amount("467", 10), "0.0000000467");
        assert_eq!(format_token_amount("1", 10), "0.0000000001");
        assert_eq!(format_token_amount("2100000000", 10), "0.21");
        assert_eq!(format_token_amount("10000000000", 10), "1");
    }

    #[test]
    fn account_balance_projection_matches_csharp_shape() -> Result<(), Box<dyn std::error::Error>> {
        let account: SdkAccountResult = serde_json::from_value(serde_json::json!({
            "address": "PADDR",
            "name": "anonymous",
            "validator": "Primary",
            "stakes": {
                "amount": "5000000000000",
                "time": 123,
                "unclaimed": "467"
            },
            "storage": {
                "available": 100,
                "used": 7,
                "avatar": "avatar.png"
            },
            "balances": [
                { "chain": "main", "symbol": "SOUL", "amount": "42", "decimals": 8 },
                { "chain": "main", "symbol": "KCAL", "amount": "467", "decimals": 10 }
            ]
        }))?;

        let projection = account_result_to_upsert(7, &account, 8, 10, 999);

        assert_eq!(projection.address_id, 7);
        assert_eq!(projection.address_name, None);
        assert_eq!(projection.staked_amount, "50000");
        assert_eq!(projection.staked_amount_raw, "5000000000000");
        assert_eq!(projection.unclaimed_amount, "0.0000000467");
        assert_eq!(projection.soul_balance_raw, "42");
        assert_eq!(projection.storage_available, 100);
        assert_eq!(projection.storage_used, 7);
        assert_eq!(projection.validator_kind, "Primary");
        assert_eq!(projection.balances.len(), 2);
        assert_eq!(projection.balances[1].amount, "0.0000000467");
        Ok(())
    }

    #[test]
    fn token_supply_projection_formats_rpc_raw_values() {
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
        assert_eq!(supply.current_supply, "209370058.8047349606");
        assert_eq!(supply.current_supply_raw, "2093700588047349606");
        assert_eq!(supply.max_supply, "0");
        assert_eq!(supply.burned_supply, "924281.4271535702");
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
                    value: "RPC NFT".to_owned(),
                },
                SdkTokenPropertyResult {
                    key: "imageURL".to_owned(),
                    value: "//cdn.example/nft.png".to_owned(),
                },
                SdkTokenPropertyResult {
                    key: "Created".to_owned(),
                    value: "1800123456".to_owned(),
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
                    value: "RPC Series".to_owned(),
                },
                SdkTokenPropertyResult {
                    key: "imageURL".to_owned(),
                    value: "//cdn.example/series.png".to_owned(),
                },
                SdkTokenPropertyResult {
                    key: "mode".to_owned(),
                    value: "1".to_owned(),
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
            previous_hash: None,
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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

        let projection = transaction_result_to_projection(&block, 0, &transaction, 10)?;

        assert_eq!(projection.fee.as_deref(), Some("0.0000000467"));
        assert_eq!(projection.fee_raw.as_deref(), Some("467"));
        assert_eq!(projection.gas_price.as_deref(), Some("0.0000000001"));
        assert_eq!(projection.gas_price_raw.as_deref(), Some("1"));
        assert_eq!(projection.gas_limit.as_deref(), Some("0.21"));
        assert_eq!(projection.gas_limit_raw.as_deref(), Some("2100000000"));

        transaction.gas_limit = LEGACY_UNLIMITED_GAS_RAW.to_owned();
        let projection = transaction_result_to_projection(&block, 0, &transaction, 10)?;

        assert_eq!(projection.gas_limit, None);
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
            previous_hash: None,
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenMint".to_owned(),
                data: serde_json::json!({
                    "symbol": "KCAL",
                    "tokenId": "233",
                    "seriesId": "7",
                    "mintNumber": 3,
                    "carbonTokenId": 1,
                    "carbonSeriesId": 7,
                    "carbonInstanceId": 3,
                    "owner": "PADDR"
                }),
            }],
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            previous_hash: None,
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
                    // C# treats Custom_V2 like Custom: keep the event row but
                    // leave the payload at InitPayload shape without raw data.
                    kind: "Custom_V2".to_owned(),
                    name: "Custom_V2".to_owned(),
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            assert_eq!(events[2].event_kind, "Custom_V2");
            assert!(
                events[2]
                    .payload_json
                    .as_ref()
                    .and_then(|v| v.get("data"))
                    .is_none()
            );
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
            previous_hash: None,
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "SpecialResolution".to_owned(),
                data: serde_json::json!({
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
            }],
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            assert_eq!(events[2].date_unix_seconds, 1743465600);
        }
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
            previous_hash: None,
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            previous_hash: None,
            protocol: Some(18),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "saturnrental".to_owned(),
                kind: "SpecialResolution".to_owned(),
                data: serde_json::json!({ "valueKind": "Object" }),
            }],
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "governance".to_owned(),
                kind: "SpecialResolution".to_owned(),
                data: serde_json::json!({ "valueKind": "Object" }),
            }],
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenSeriesCreate".to_owned(),
                data: serde_json::json!({ "valueKind": "Object" }),
            }],
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
            previous_hash: None,
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
            extended_events: vec![SdkEventExResult {
                address: "PADDR".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenCreate".to_owned(),
                data: serde_json::json!({
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
            }],
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
            previous_hash: None,
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
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
                SdkEventExResult {
                    address: "PTAZ".to_owned(),
                    contract: "token".to_owned(),
                    kind: "TokenCreate".to_owned(),
                    data: serde_json::json!({
                        "symbol": "TAZ",
                        "maxSupply": "0",
                        "decimals": 9,
                        "isNonFungible": false,
                        "carbonTokenId": 51,
                        "metadata": { "name": "Transplanetary Artificial Zenith" }
                    }),
                },
                SdkEventExResult {
                    address: "PBAD".to_owned(),
                    contract: "token".to_owned(),
                    kind: "TokenCreate".to_owned(),
                    data: serde_json::json!({
                        "symbol": "BADZEROQ",
                        "maxSupply": "0",
                        "decimals": 8,
                        "isNonFungible": false,
                        "carbonTokenId": 312,
                        "metadata": { "name": "BADZEROQ token semantics V2 probe" }
                    }),
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
            timestamp_unix_seconds: 1_767_146_140,
            state: transaction.state.clone(),
            result: None,
            debug_comment: None,
            payload: None,
            script_raw: None,
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
        let payload = build_token_create_payload(&serde_json::json!({
            "symbol": "FLAG",
            "name": "Top Level Name",
            "maxSupply": "100000000",
            "decimals": 8,
            "isNonFungible": false,
            "metadata": {
                "token_name": "Flag Token",
                "token_flags": "Fungible|Transferable|Finite|Divisible|Burnable"
            }
        }));

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
            previous_hash: None,
            protocol: Some(19),
            chain_address_id: 1,
            chain_address: None,
            validator_address_id: 1,
            validator_address: None,
            timestamp_unix_seconds: 1767146140,
            reward: None,
        };
        let transaction = SdkTransactionResult {
            hash: "TX".to_owned(),
            gas_payer: "POWNER".to_owned(),
            timestamp: 1767146140,
            state: "Halt".to_owned(),
            events: Vec::new(),
            extended_events: vec![SdkEventExResult {
                address: "POWNER".to_owned(),
                contract: "token".to_owned(),
                kind: "TokenSeriesCreate".to_owned(),
                data: serde_json::json!({
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
            }],
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
            fee: None,
            fee_raw: None,
            gas_price: None,
            gas_price_raw: None,
            gas_limit: None,
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
