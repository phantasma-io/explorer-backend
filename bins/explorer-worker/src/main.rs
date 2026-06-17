use clap::Parser;
use explorer_config::AppConfig;
use explorer_db::{BlockRecord, RawBlockRecord};
use explorer_domain::BlockHeight;
use explorer_ingestion::{
    BalanceDirtyMarkReport, BalanceSyncReport, BlockIngestionDriver, ContractRpcMetadataSyncReport,
    ContractStringEventSideEffectSyncReport, ContractUpgradeMethodSyncReport,
    FailedTransactionDebugSyncReport, NftRpcMetadataSyncReport, SeriesRpcMetadataSyncReport,
    StartupProbe, SyncBatchReport, TokenPriceSyncReport, TokenSupplySyncReport,
    TtrsOffchainSyncReport,
};
use explorer_rpc::PhantasmaSdkClient;
use std::path::PathBuf;
use tracing::info;

#[derive(Debug, Parser)]
#[command(version, about = "Explorer block ingestion worker")]
struct Args {
    /// TOML config file. Env vars still override values from the file.
    #[arg(long, env = "EXPLORER_CONFIG_FILE")]
    config: Option<PathBuf>,
    /// Run one startup probe and exit instead of entering the worker loop.
    #[arg(long, env = "EXPLORER_WORKER_ONCE")]
    once: bool,
    /// Run one cursor-driven sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_SYNC_ONCE")]
    sync_once: bool,
    /// Run one dirty address balance sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_BALANCE_SYNC_ONCE")]
    balance_sync_once: bool,
    /// One-time: capture the per-address staking snapshot seed and exit.
    #[arg(long, env = "EXPLORER_WORKER_STAKE_BOUNDARY_SLICE_CAPTURE_ONCE")]
    stake_boundary_slice_capture_once: bool,
    /// Mark all known addresses dirty for balance sync and exit.
    #[arg(long, env = "EXPLORER_WORKER_MARK_ALL_BALANCES_DIRTY_ONCE")]
    mark_all_balances_dirty_once: bool,
    /// Reconcile contract rows/create links from ContractDeploy/ContractUpgrade string events.
    #[arg(long, env = "EXPLORER_WORKER_CONTRACT_STRING_EVENTS_SYNC_ONCE")]
    contract_string_events_sync_once: bool,
    /// Run one contract RPC metadata/method sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_CONTRACT_RPC_METADATA_SYNC_ONCE")]
    contract_rpc_metadata_sync_once: bool,
    /// Run one ContractUpgrade method-history sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_CONTRACT_UPGRADE_METHOD_SYNC_ONCE")]
    contract_upgrade_method_sync_once: bool,
    /// Run one token supply sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_TOKEN_SUPPLY_SYNC_ONCE")]
    token_supply_sync_once: bool,
    /// Run one token price sync (live CoinGecko prices + daily history) and exit.
    #[arg(long, env = "EXPLORER_WORKER_TOKEN_PRICE_SYNC_ONCE")]
    token_price_sync_once: bool,
    /// Run one TTRS off-chain NFT metadata sync batch (22series) and exit.
    #[arg(long, env = "EXPLORER_WORKER_TTRS_OFFCHAIN_SYNC_ONCE")]
    ttrs_offchain_sync_once: bool,
    /// Run one NFT RPC metadata sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_NFT_RPC_METADATA_SYNC_ONCE")]
    nft_rpc_metadata_sync_once: bool,
    /// Repair stored NFT placeholders through RPC without the near-tip catchup gate.
    #[arg(long, env = "EXPLORER_WORKER_NFT_RPC_METADATA_REPAIR_ONCE")]
    nft_rpc_metadata_repair_once: bool,
    /// Run one NFT RPC metadata repair batch for TokenMint events in one block.
    #[arg(long, env = "EXPLORER_WORKER_NFT_RPC_METADATA_SYNC_FOR_MINT_BLOCK")]
    nft_rpc_metadata_sync_for_mint_block: Option<u64>,
    /// Run one series RPC metadata sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_SERIES_RPC_METADATA_SYNC_ONCE")]
    series_rpc_metadata_sync_once: bool,
    /// Repair series rows whose materialized fields drifted from stored RPC metadata.
    #[arg(long, env = "EXPLORER_WORKER_SERIES_RPC_METADATA_REPAIR_ONCE")]
    series_rpc_metadata_repair_once: bool,
    /// Run one failed-transaction debug-comment sync batch and exit.
    #[arg(long, env = "EXPLORER_WORKER_FAILED_TX_DEBUG_SYNC_ONCE")]
    failed_tx_debug_sync_once: bool,
    /// Fetch one raw block payload through RPC, then exit.
    #[arg(long, env = "EXPLORER_WORKER_FETCH_BLOCK")]
    fetch_block: Option<u64>,
    /// Fetch and project one block into the SQL tables.
    #[arg(long, env = "EXPLORER_WORKER_PROJECT_BLOCK")]
    project_block: Option<u64>,
    /// Fetch and project one block, then exit.
    #[arg(long, env = "EXPLORER_WORKER_FETCH_PROJECT_BLOCK")]
    fetch_project_block: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = AppConfig::from_file_or_env("explorer-worker", args.config.as_deref())?;
    explorer_runtime::init_tracing_with_logging(
        config.logging.file.as_deref(),
        config.logging.console,
    );
    log_worker_config(&config);
    let pool = explorer_db::connect(&config.database).await?;
    let rpc = PhantasmaSdkClient::new(&config.rpc)?;
    let driver = BlockIngestionDriver::new(rpc, pool, config.chain.clone(), config.worker.clone());

    if let Some(height) = args.fetch_project_block {
        let record = driver
            .fetch_persist_and_project_block(BlockHeight::new(height))
            .await?;
        log_projected_block(&record);
        return Ok(());
    } else if args.balance_sync_once {
        let report = driver.sync_dirty_balances_once().await?;
        log_balance_sync_batch(&report);
        return Ok(());
    } else if args.stake_boundary_slice_capture_once {
        let report = driver.capture_stake_boundary_slice_once().await?;
        tracing::info!(
            "captured stake snapshot seed day={} masters={} stakers={} staked={} supply={} addresses={}",
            report.boundary_day_unix_seconds,
            report.masters_count,
            report.stakers_count,
            report.staked_soul_raw,
            report.soul_supply_raw,
            report.addresses_written
        );
        return Ok(());
    } else if args.mark_all_balances_dirty_once {
        let report = driver.mark_all_balances_dirty_once().await?;
        log_balance_dirty_mark_batch(&report);
        return Ok(());
    } else if args.contract_string_events_sync_once {
        let report = driver
            .sync_contract_string_event_side_effects_once()
            .await?;
        log_contract_string_event_side_effect_sync_batch(&report);
        return Ok(());
    } else if args.contract_rpc_metadata_sync_once {
        let report = driver.sync_contract_rpc_metadata_once().await?;
        log_contract_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if args.contract_upgrade_method_sync_once {
        let report = driver.sync_contract_upgrade_methods_once().await?;
        log_contract_upgrade_method_sync_batch(&report);
        return Ok(());
    } else if args.token_supply_sync_once {
        let report = driver.sync_token_supplies_once().await?;
        log_token_supply_sync_batch(&report);
        return Ok(());
    } else if args.token_price_sync_once {
        let report = driver.sync_token_prices_once().await?;
        log_token_price_sync_batch(&report);
        return Ok(());
    } else if args.ttrs_offchain_sync_once {
        let report = driver.sync_ttrs_offchain_nfts_once().await?;
        log_ttrs_offchain_sync_batch(&report);
        return Ok(());
    } else if args.nft_rpc_metadata_sync_once {
        let report = driver.sync_nft_rpc_metadata_once().await?;
        log_nft_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if args.nft_rpc_metadata_repair_once {
        let report = driver.repair_nft_rpc_metadata_once().await?;
        log_nft_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if let Some(height) = args.nft_rpc_metadata_sync_for_mint_block {
        let report = driver
            .sync_nft_rpc_metadata_for_mint_block(BlockHeight::new(height))
            .await?;
        log_nft_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if args.series_rpc_metadata_sync_once {
        let report = driver.sync_series_rpc_metadata_once().await?;
        log_series_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if args.series_rpc_metadata_repair_once {
        let report = driver.repair_series_rpc_metadata_once().await?;
        log_series_rpc_metadata_sync_batch(&report);
        return Ok(());
    } else if args.failed_tx_debug_sync_once {
        let report = driver.sync_failed_transaction_debug_comments_once().await?;
        log_failed_tx_debug_sync_batch(&report);
        return Ok(());
    } else if args.sync_once {
        let report = driver.sync_once().await?;
        log_sync_batch(&report);
        return Ok(());
    } else if let Some(height) = args.project_block {
        let record = driver.project_raw_block(BlockHeight::new(height)).await?;
        log_projected_block(&record);
        return Ok(());
    } else if let Some(height) = args.fetch_block {
        let record = driver
            .fetch_and_persist_raw_block(BlockHeight::new(height))
            .await?;
        log_raw_block(&record);
        return Ok(());
    } else if args.once {
        let probe = driver.startup_probe().await?;
        log_startup_probe(&probe);
        return Ok(());
    }

    driver.run_until_shutdown().await?;
    Ok(())
}

fn log_worker_config(config: &AppConfig) {
    let endpoints = config
        .rpc
        .rpc_endpoints
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let height_limit = config
        .worker
        .height_limit
        .map(|height| height.to_string())
        .unwrap_or_else(|| "none".to_owned());

    info!(
        "worker config chain={}/{} rpc={} mode={} fetch_batch={} fetch_concurrency={} queue_capacity={} poll={}s height_limit={}",
        config.chain.nexus,
        config.chain.chain,
        endpoints,
        config.worker.sync_mode,
        config.worker.fetch_batch_size,
        config.worker.fetch_concurrency,
        config.worker.queue_capacity,
        config.worker.poll_interval.as_secs(),
        height_limit
    );
}

fn log_startup_probe(probe: &StartupProbe) {
    let next = probe
        .next_planned_height
        .map(|height| height.to_string())
        .unwrap_or_else(|| "none".to_owned());
    info!(
        "startup probe cursor={} next={} tip={}",
        probe.cursor_height, next, probe.rpc_tip_height
    );
}

fn log_sync_batch(report: &SyncBatchReport) {
    let range = match (report.from_height, report.to_height) {
        (Some(from), Some(to)) => format!("{from}..{to}"),
        _ => "none".to_owned(),
    };
    info!(
        "synced range={} blocks={} cursor={}..{} tip={}",
        range,
        report.projected_blocks,
        report.cursor_height_before,
        report.cursor_height_after,
        report.rpc_tip_height
    );
}

fn log_balance_sync_batch(report: &BalanceSyncReport) {
    info!(
        "synced balances accounts={} selected={} reset_dirty={} dirty_before={} cursor={} tip={} lag={} skipped_catchup={}",
        report.updated_accounts,
        report.selected_addresses,
        report.reset_dirty_flags,
        report.dirty_before,
        report.cursor_height,
        report.rpc_tip_height,
        report.lag,
        report.skipped_catchup,
    );
}

fn log_balance_dirty_mark_batch(report: &BalanceDirtyMarkReport) {
    info!(
        "marked all balances dirty addresses={} cursor={}",
        report.marked_addresses, report.cursor_height
    );
}

fn log_contract_string_event_side_effect_sync_batch(
    report: &ContractStringEventSideEffectSyncReport,
) {
    info!(
        "synced contract string event side effects upserted_contracts={} linked_contract_creates={}",
        report.upserted_contracts, report.linked_contract_creates
    );
}

fn log_contract_rpc_metadata_sync_batch(report: &ContractRpcMetadataSyncReport) {
    info!(
        "synced contract RPC metadata selected={} fetched={} updated={} inserted_methods={} failed={}",
        report.selected_contracts,
        report.fetched_contracts,
        report.updated_contracts,
        report.inserted_methods,
        report.failed_contracts
    );
}

fn log_contract_upgrade_method_sync_batch(report: &ContractUpgradeMethodSyncReport) {
    info!(
        "synced contract upgrade methods selected={} fetched={} inserted_methods={} linked_contracts={} failed={}",
        report.selected_upgrades,
        report.fetched_contracts,
        report.inserted_methods,
        report.linked_contracts,
        report.failed_contracts
    );
}

fn log_token_supply_sync_batch(report: &TokenSupplySyncReport) {
    info!(
        "synced token supplies fetched={} updated={}",
        report.fetched_tokens, report.updated_tokens
    );
}

fn log_token_price_sync_batch(report: &TokenPriceSyncReport) {
    info!(
        "synced token prices live_updated={} daily_days={} daily_inserted={} daily_caught_up={}",
        report.live_prices_updated,
        report.daily_days_processed,
        report.daily_rows_inserted,
        report.daily_caught_up
    );
}

fn log_ttrs_offchain_sync_batch(report: &TtrsOffchainSyncReport) {
    info!(
        "synced TTRS off-chain NFTs selected={} fetched={} updated={}",
        report.selected, report.fetched, report.updated
    );
}

fn log_nft_rpc_metadata_sync_batch(report: &NftRpcMetadataSyncReport) {
    info!(
        "synced NFT RPC metadata selected={} fetched={} updated={} cursor={} tip={} lag={} skipped_catchup={}",
        report.selected_nfts,
        report.fetched_nfts,
        report.updated_nfts,
        report.cursor_height,
        report.rpc_tip_height,
        report.lag,
        report.skipped_catchup
    );
}

fn log_series_rpc_metadata_sync_batch(report: &SeriesRpcMetadataSyncReport) {
    info!(
        "synced series RPC metadata selected={} fetched={} updated={} cursor={} tip={} lag={} skipped_catchup={}",
        report.selected_series,
        report.fetched_series,
        report.updated_series,
        report.cursor_height,
        report.rpc_tip_height,
        report.lag,
        report.skipped_catchup
    );
}

fn log_failed_tx_debug_sync_batch(report: &FailedTransactionDebugSyncReport) {
    info!(
        "synced failed tx debug comments selected={} updated={} cursor={} tip={} lag={} skipped_catchup={}",
        report.selected_transactions,
        report.updated_transactions,
        report.cursor_height,
        report.rpc_tip_height,
        report.lag,
        report.skipped_catchup
    );
}

fn log_projected_block(record: &BlockRecord) {
    info!(
        "projected block height={} id={} hash={}",
        record.height, record.id, record.hash
    );
}

fn log_raw_block(record: &RawBlockRecord) {
    let hash = record.hash.as_deref().unwrap_or("<none>");
    info!(
        "fetched raw block height={} hash={} bytes={} rpc={}",
        record.height, hash, record.payload_bytes, record.rpc_node
    );
}
