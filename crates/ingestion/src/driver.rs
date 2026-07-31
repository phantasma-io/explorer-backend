//! Block ingestion orchestrator. Drives the worker's sync passes: ordered block
//! projection, balance sync, contract/NFT/series RPC metadata hydration, token
//! supply sync, stake-snapshot projection, and failed-tx debug recovery. The
//! `BlockIngestionDriver` struct is defined in the crate root; this module holds
//! its inherent impl.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::AbortHandle;

/// Committed blocks required, without a stall in between, before the worker
/// leaves load-shed mode. Recovery is deliberately conservative so one lucky
/// fetch does not immediately restore the full fan-out (C# parity:
/// `RpcReliefRecoveryCommitThreshold`).
const RELIEF_RECOVERY_COMMIT_BLOCKS: u32 = 4;
/// Wait after the first stalled fetch; it doubles on every consecutive stall at
/// the SAME height (C# parity: `BlockFetchFailureBackoffBaseMs`).
const RELIEF_STALL_BACKOFF_BASE: Duration = Duration::from_secs(30);
/// Ceiling for that wait (C# parity: `BlockFetchFailureBackoffMaxMs`).
const RELIEF_STALL_BACKOFF_MAX: Duration = Duration::from_secs(300);
/// Doublings applied before the ceiling (C# parity: exponent clamped to 5).
const RELIEF_STALL_BACKOFF_MAX_DOUBLINGS: u32 = 5;

/// Wait before retrying a height that keeps stalling: `base * 2^(stalls-1)`,
/// capped. Repeatedly re-requesting a block the node cannot serve is exactly how
/// a worker drives a struggling node into OOM, so each failed attempt buys the
/// node more room.
fn relief_stall_backoff(stall_count: u32) -> Duration {
    if stall_count == 0 {
        return Duration::ZERO;
    }
    let doublings = stall_count
        .saturating_sub(1)
        .min(RELIEF_STALL_BACKOFF_MAX_DOUBLINGS);
    RELIEF_STALL_BACKOFF_BASE
        .saturating_mul(1u32 << doublings)
        .min(RELIEF_STALL_BACKOFF_MAX)
}

/// Automatic RPC load-shed state — the Rust port of the C# plugin's
/// `RpcReliefModeState`.
///
/// Why this exists: a handful of historical blocks serialize to 100+ MB of JSON.
/// Fetching several of them concurrently (the normal `fetch_concurrency`
/// fan-out), failing, and immediately re-requesting the same set is what pushes
/// the node into OOM — and, because the window is retried unchanged, the worker
/// never recovers on its own. Once a fetch stalls, the worker drops to ONE block
/// per pass with ONE request in flight, waits on an escalating schedule, and only
/// restores the fan-out after a streak of committed blocks.
#[derive(Debug, Default)]
pub struct RpcReliefState {
    active: AtomicBool,
    /// Blocks committed since the last stall while shedding; resets on any stall.
    committed_streak: AtomicU32,
    /// Height of the current stall and how many consecutive passes stalled on it.
    /// A stall on a NEW height starts the escalation over, so an unrelated later
    /// failure does not inherit a long backoff.
    stalled_height: AtomicU64,
    stall_count: AtomicU32,
}

impl RpcReliefState {
    fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    fn stall_count(&self) -> u32 {
        self.stall_count.load(Ordering::Relaxed)
    }

    /// Record a stalled fetch at `height` and switch shedding on. Returns the
    /// consecutive stall count for that height and whether this call was the
    /// transition into load-shed mode (so the caller logs it once, not per pass).
    fn register_stall(&self, height: u64) -> (u32, bool) {
        let previous_height = self.stalled_height.swap(height, Ordering::Relaxed);
        let stalls = if previous_height == height {
            self.stall_count.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.stall_count.store(1, Ordering::Relaxed);
            1
        };
        // Progress must be re-earned from zero after every stall.
        self.committed_streak.store(0, Ordering::Relaxed);
        let entered = !self.active.swap(true, Ordering::Relaxed);
        (stalls, entered)
    }

    /// Record a pass that committed `blocks` with no stall. Returns true when the
    /// streak was long enough to leave load-shed mode (logged by the caller).
    fn register_progress(&self, blocks: u64) -> bool {
        self.stall_count.store(0, Ordering::Relaxed);
        if !self.is_active() || blocks == 0 {
            return false;
        }
        let blocks = u32::try_from(blocks).unwrap_or(u32::MAX);
        let streak = self
            .committed_streak
            .fetch_add(blocks, Ordering::Relaxed)
            .saturating_add(blocks);
        if streak < RELIEF_RECOVERY_COMMIT_BLOCKS {
            return false;
        }
        self.committed_streak.store(0, Ordering::Relaxed);
        self.active.swap(false, Ordering::Relaxed)
    }

    /// Leave load-shed mode outright. Used when there is nothing left to fetch:
    /// the recovery streak can never be earned while the chain produces no new
    /// blocks, so without this the worker would sit at the tip with shedding —
    /// and its paused RPC maintenance — latched on indefinitely. Returns true if
    /// this call was the transition back to normal.
    fn clear(&self) -> bool {
        self.stall_count.store(0, Ordering::Relaxed);
        self.committed_streak.store(0, Ordering::Relaxed);
        self.active.swap(false, Ordering::Relaxed)
    }
}

/// Is this failure ours (the database) rather than the node's?
///
/// Load shedding exists to spare a struggling node, so a Postgres outage must not
/// trigger it: collapsing the fetch window and parking RPC maintenance would slow
/// the recovery down and hide the component that actually needs attention. Every
/// other failure — node errors, unusable block payloads — is worth backing off on.
fn is_database_failure(error: &IngestionError) -> bool {
    matches!(error, IngestionError::Db(_) | IngestionError::Sqlx(_))
}

/// Does a mid-window failure keep the blocks committed so far and become a stall on
/// the current height, instead of failing the whole pass?
///
/// Both projection paths answer this the same way, and must: the height the worker
/// backs off on is derived from it. A pass that failed the whole window blocks on
/// its first height, so the caller can name that height itself; a pass that failed
/// after committing a prefix blocks somewhere in the middle, which only the path
/// itself knows — reporting that as a plain error would key the escalating backoff
/// on the wrong (already committed) height. Database failures are excluded because
/// they are not the node's problem and must not shed RPC load.
fn keeps_committed_prefix(projected_blocks: u64, error: &IngestionError) -> bool {
    projected_blocks > 0 && !is_database_failure(error)
}

/// How long to wait after a failed sync pass.
///
/// The generic escalation (5 s per consecutive failure, capped at 30 s) covers
/// ordinary flakiness; a stalled block fetch needs the far longer relief schedule,
/// so the two are combined by taking the longer one. A database failure is exempt:
/// relief exists to spare the NODE, and inheriting its up-to-five-minute wait for a
/// Postgres problem would idle the worker long after the database is back.
fn failed_pass_backoff(
    consecutive_failures: u32,
    stall_count: u32,
    database_failure: bool,
) -> Duration {
    let generic = Duration::from_secs(u64::from(consecutive_failures.min(6)) * 5);
    if database_failure {
        return generic;
    }
    generic.max(relief_stall_backoff(stall_count))
}

/// Wait out `backoff`, unless shutdown arrives first; returns true when it did.
///
/// Every backoff in the worker loop must be interruptible. A bare `sleep` leaves the
/// worker deaf to Ctrl+C and SIGTERM for the whole wait, and once a wait grows past
/// an orchestrator's stop grace period that means the process is hard-killed instead
/// of stopped.
async fn wait_or_shutdown(backoff: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    if backoff.is_zero() {
        return false;
    }
    tokio::select! {
        _ = sleep(backoff) => false,
        _ = shutdown.changed() => true,
    }
}

/// Result of projecting one fetch window.
struct WindowOutcome {
    projected_blocks: u64,
    cursor_height_after: BlockHeight,
    /// Lowest height the pass could not fetch. The contiguous prefix below it is
    /// committed; the window above it is abandoned (and its in-flight fetches
    /// aborted) because the cursor can only advance through that gap.
    stalled_height: Option<u64>,
}

/// Record a per-token NFT metadata fetch failure. A permanent RPC error (e.g.
/// `getNFT` "ID not found") would otherwise recur every maintenance cycle because
/// the candidate gate re-selects every NFT whose `chain_api_response` is still
/// NULL, so negative-cache it (mirrors the series error path). A transient
/// transport failure — already retried by `with_failover` — is only logged and
/// left to retry on the next pass so a node outage cannot poison resolvable
/// tokens.
fn record_nft_metadata_fetch_failure(
    upserts: &mut Vec<NftRpcMetadataUpsert>,
    symbol: &str,
    token_id: &str,
    error: &RpcError,
) {
    if explorer_rpc::is_transient_rpc_error(error) {
        warn!(%error, symbol, token_id, "single NFT RPC metadata fetch failed");
    } else {
        warn!(
            %error,
            symbol, token_id, "single NFT RPC metadata fetch failed; storing error response"
        );
        upserts.push(nft_error_to_metadata_upsert(symbol, token_id, error));
    }
}

impl BlockIngestionDriver {
    pub fn new(
        rpc: PhantasmaSdkClient,
        pool: PgPool,
        chain: ChainConfig,
        settings: WorkerConfig,
    ) -> Self {
        Self {
            rpc,
            pool,
            chain,
            settings,
            node_guard_checked: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            relief: Arc::new(RpcReliefState::default()),
        }
    }

    pub async fn startup_probe(&self) -> Result<StartupProbe, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        self.guard_node_matches_db(chain_id, cursor_height).await?;
        let window = plan_fetch_window(
            cursor_height,
            rpc_tip,
            &self.settings,
            self.relief.is_active(),
        )?;

        Ok(StartupProbe {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_endpoints: self.rpc.endpoint_urls(),
            sync_mode: self.settings.sync_mode.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            next_planned_height: window.as_ref().map(|window| window.from_height.value()),
            fetch_batch_size: self.settings.effective_fetch_batch_size(),
            fetch_concurrency: window
                .as_ref()
                .map(|window| window.concurrency)
                .unwrap_or(self.settings.effective_fetch_concurrency()),
            inter_block_delay_ms: duration_millis_u64(self.settings.inter_block_delay),
            batch_delay_ms: duration_millis_u64(self.settings.batch_delay),
        })
    }

    pub async fn fetch_and_persist_raw_block(
        &self,
        height: BlockHeight,
    ) -> Result<RawBlockRecord, IngestionError> {
        let payload = self
            .rpc
            .get_block_by_height_payload(&self.chain.chain, height)
            .await?;
        let payload_bytes =
            i32::try_from(payload.byte_len).map_err(|_| IngestionError::PayloadTooLarge {
                height: height.value(),
            })?;

        Ok(RawBlockRecord {
            id: uuid::Uuid::now_v7(),
            nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            height: i64::try_from(height.value()).map_err(|_| {
                IngestionError::BlockFieldOutOfRange {
                    height: height.value(),
                    field: "height",
                }
            })?,
            hash: extract_block_hash(&payload.value),
            rpc_node: payload.endpoint,
            payload_json: payload.raw_value,
            payload_bytes,
            fetched_at: chrono::Utc::now(),
        })
    }

    pub async fn project_raw_block(
        &self,
        height: BlockHeight,
    ) -> Result<BlockRecord, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        self.validate_projected_height(height)?;
        drop(conn);

        let block = self.fetch_decoded_block_for_projection(height).await?;
        let mut transaction = self.pool.begin().await?;
        let block_record = self
            .project_decoded_block(&mut transaction, height, &block)
            .await?;
        transaction.commit().await?;

        Ok(block_record)
    }

    async fn project_raw_block_and_advance_cursor(
        &self,
        height: BlockHeight,
    ) -> Result<(BlockRecord, BlockHeight), IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        self.validate_projected_height(height)?;
        drop(conn);

        let block = self.fetch_decoded_block_for_projection(height).await?;
        let mut transaction = self.pool.begin().await?;
        let block_record = self
            .project_decoded_block(&mut transaction, height, &block)
            .await?;
        let cursor_height_after =
            explorer_db::advance_cursor(&mut transaction, block_record.chain_id, height).await?;
        transaction.commit().await?;

        Ok((block_record, cursor_height_after))
    }

    async fn fetch_decoded_block_for_projection(
        &self,
        height: BlockHeight,
    ) -> Result<SdkBlockResult, IngestionError> {
        let mut last_incomplete_payload = None;
        for attempt in 1..=SPECIAL_RESOLUTION_REFETCH_ATTEMPTS {
            let payload = self
                .rpc
                .get_block_by_height_payload(&self.chain.chain, height)
                .await?;
            let mut block = decode_block_result(payload.raw_value)?;
            // A block can carry more than one incomplete extended payload; repair
            // each one, re-scanning after every fix, before projecting — rather than
            // returning after the first repair and leaving any later ones raw-only.
            let mut completed_tx_indexes = std::collections::HashSet::new();
            let unresolved = loop {
                let Some(incomplete) = incomplete_extended_payload(&block) else {
                    return Ok(block);
                };
                last_incomplete_payload = Some(incomplete);
                // Defensive: if a re-scan reports a tx we already replaced (e.g. a
                // predicate disagreement), stop instead of spinning forever.
                if completed_tx_indexes.contains(&incomplete.tx_index) {
                    break incomplete;
                }
                if self
                    .try_complete_transaction(height, incomplete.tx_index, &mut block)
                    .await?
                {
                    completed_tx_indexes.insert(incomplete.tx_index);
                    continue;
                }
                break incomplete;
            };

            if attempt < SPECIAL_RESOLUTION_REFETCH_ATTEMPTS {
                warn!(
                    height = height.value(),
                    tx_index = unresolved.tx_index,
                    event_kind = unresolved.event_kind,
                    attempt,
                    "RPC returned incomplete extended event payload; refetching block"
                );
                sleep(std::time::Duration::from_millis(
                    SPECIAL_RESOLUTION_REFETCH_DELAY_MS,
                ))
                .await;
                continue;
            }

            break;
        }

        Err(IngestionError::EventPayloadDecode {
            height: height.value(),
            transaction_index: last_incomplete_payload
                .map(|incomplete| incomplete.tx_index)
                .unwrap_or_default(),
            event_index: 0,
            event_kind: last_incomplete_payload
                .map(|incomplete| incomplete.event_kind.to_owned())
                .unwrap_or_else(|| "extended".to_owned()),
        })
    }

    async fn try_complete_transaction(
        &self,
        height: BlockHeight,
        tx_index: usize,
        block: &mut SdkBlockResult,
    ) -> Result<bool, IngestionError> {
        let Some(tx_hash) = block
            .txs
            .get(tx_index)
            .and_then(|transaction| non_empty_string(&transaction.hash))
        else {
            warn!(
                height = height.value(),
                tx_index,
                "RPC block response has incomplete extended payload and empty transaction hash"
            );
            return Ok(false);
        };

        let transaction = self.rpc.get_transaction(&tx_hash).await?;
        if transaction_has_incomplete_extended_payload(&transaction) {
            warn!(
                height = height.value(),
                tx_index,
                tx_hash,
                "RPC transaction response still has incomplete extended event payload"
            );
            return Ok(false);
        }

        warn!(
            height = height.value(),
            tx_index,
            tx_hash,
            "RPC block response had incomplete extended event payload; using transaction response"
        );
        block.txs[tx_index] = transaction;
        Ok(true)
    }

    async fn project_decoded_block(
        &self,
        conn: &mut PgConnection,
        height: BlockHeight,
        block: &SdkBlockResult,
    ) -> Result<BlockRecord, IngestionError> {
        let projection = block_result_to_projection(&self.chain.chain, height, block)?;
        let block_record = explorer_db::upsert_block(conn, projection).await?;
        let kcal_decimals = if block.txs.is_empty() {
            None
        } else {
            Some(
                explorer_db::get_token_decimals(
                    conn,
                    block_record.chain_id,
                    LEGACY_GAS_TOKEN_SYMBOL,
                )
                .await?,
            )
        };

        // One dimension cache per block: addresses/states/kinds/contracts are
        // resolved once on first encounter (in transaction/event order) and
        // reused across the block's transactions and events.
        let mut dimension_cache = explorer_db::ProjectionDimensionCache::new();
        let mut transaction_projections = Vec::with_capacity(block.txs.len());
        for (tx_index, transaction) in block.txs.iter().enumerate() {
            transaction_projections.push(transaction_result_to_projection(
                &block_record,
                tx_index,
                transaction,
                kcal_decimals.unwrap_or_default(),
            )?);
        }
        // Pre-resolve the block's transaction addresses in one batch so the per-tx
        // dimension resolution below hits the cache instead of doing a serial
        // round-trip per new address (the dominant per-block write cost; C# prefetches
        // the whole block's addresses up front the same way).
        let mut tx_addresses: Vec<String> = transaction_projections
            .iter()
            .flat_map(|transaction| {
                [
                    transaction.sender.clone(),
                    transaction.gas_payer.clone(),
                    transaction.gas_target.clone(),
                ]
            })
            .map(|address| address.unwrap_or_else(|| "NULL".to_owned()))
            .collect();
        tx_addresses.sort_unstable();
        tx_addresses.dedup();
        dimension_cache
            .prefetch_addresses(conn, block_record.chain_id, &tx_addresses)
            .await?;
        // Upsert the block's transactions set-based; records come back in tx order.
        let transaction_records = explorer_db::batch_upsert_transactions(
            conn,
            &mut dimension_cache,
            transaction_projections,
        )
        .await?;

        let mut transaction_ids = Vec::with_capacity(transaction_records.len());
        let mut event_batches = Vec::with_capacity(transaction_records.len());
        for ((tx_index, transaction), transaction_record) in
            block.txs.iter().enumerate().zip(transaction_records.iter())
        {
            let event_projections = transaction_events_to_projections(
                &block_record,
                transaction_record,
                tx_index,
                transaction,
            )?;
            transaction_ids.push(transaction_record.id);
            event_batches.push((transaction_record.id, event_projections));
        }
        // Write all of the block's events set-based, then apply each
        // transaction's stateful side effects in order, then link address
        // activity — all reading the rows just written.
        explorer_db::project_block_events(conn, &mut dimension_cache, &event_batches).await?;
        explorer_db::replace_address_transactions_for_block(conn, &transaction_ids).await?;
        explorer_db::mark_block_addresses_dirty(conn, block_record.id, height).await?;

        Ok(block_record)
    }

    pub async fn fetch_persist_and_project_block(
        &self,
        height: BlockHeight,
    ) -> Result<BlockRecord, IngestionError> {
        self.project_raw_block(height).await
    }

    pub async fn sync_once(&self) -> Result<SyncBatchReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        // Release the pooled connection before the (possibly long) projection
        // pass; the pipeline acquires its own per-block transactions.
        drop(conn);
        self.validate_zero_state_scope(cursor_height)?;
        self.guard_node_matches_db(chain_id, cursor_height).await?;
        let load_shed = self.relief.is_active();
        let Some(window) = plan_fetch_window(cursor_height, rpc_tip, &self.settings, load_shed)?
        else {
            // Caught up to the tip: nothing to fetch, and the node just answered
            // the tip probe. Shedding protects nothing here, while the recovery
            // streak it waits for cannot be earned until new blocks appear — so
            // on a quiet chain it would keep RPC maintenance parked for as long
            // as the chain stays quiet. Leave the mode instead.
            if self.relief.clear() {
                info!(
                    tip = rpc_tip.value(),
                    "leaving RPC load-shed mode: caught up to the tip and the node is responding"
                );
            }
            return Ok(SyncBatchReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_endpoints: self.rpc.endpoint_urls(),
                sync_mode: self.settings.sync_mode.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height_before: cursor_height.value(),
                from_height: None,
                to_height: None,
                projected_blocks: 0,
                cursor_height_after: cursor_height.value(),
                fetch_concurrency: 0,
                load_shed,
                stalled_height: None,
            });
        };

        // Normal mode runs the fetch/process pipeline: RPC fetch overlaps DB
        // writes (the Rust equivalent of the C# producer/consumer threads).
        // Sequential and Relief stay strictly serial — Sequential for
        // deterministic, reproducible ingestion and Relief for one-block,
        // load-shedding passes over difficult ranges. In every mode blocks are
        // written and the cursor advances in strict height order.
        let outcome = match self.settings.sync_mode {
            WorkerSyncMode::Normal => self.project_window_pipelined(&window, cursor_height).await,
            WorkerSyncMode::Sequential | WorkerSyncMode::Relief => {
                self.project_window_sequentially(&window, cursor_height)
                    .await
            }
        };

        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                // An error here means the pass committed nothing: both projection
                // paths keep their prefix and report a stall instead of failing
                // once anything was written (`keeps_committed_prefix`), so the
                // window's first height IS the height that blocks us. Shed load
                // before the retry: without this the same window (and the same
                // concurrent multi-megabyte fetches) is re-fired every poll until
                // the node dies. A database failure is not the node's fault, so it
                // keeps the plain retry path.
                if !is_database_failure(&error) {
                    self.note_fetch_stall(window.from_height.value());
                }
                return Err(error);
            }
        };

        match outcome.stalled_height {
            Some(height) => self.note_fetch_stall(height),
            None => self.note_fetch_progress(outcome.projected_blocks),
        }

        if !self.settings.batch_delay.is_zero() {
            sleep(self.settings.batch_delay).await;
        }

        let fetch_concurrency = match self.settings.sync_mode {
            WorkerSyncMode::Normal => window.concurrency,
            WorkerSyncMode::Sequential | WorkerSyncMode::Relief => 1,
        };
        Ok(SyncBatchReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_endpoints: self.rpc.endpoint_urls(),
            sync_mode: self.settings.sync_mode.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height_before: cursor_height.value(),
            from_height: Some(window.from_height.value()),
            to_height: Some(window.to_height.value()),
            projected_blocks: outcome.projected_blocks,
            cursor_height_after: outcome.cursor_height_after.value(),
            fetch_concurrency,
            load_shed,
            stalled_height: outcome.stalled_height,
        })
    }

    /// A fetch could not be served: switch to (or stay in) load-shed mode and log
    /// the escalating wait the worker loop will honour before retrying `height`.
    fn note_fetch_stall(&self, height: u64) {
        let (stalls, entered) = self.relief.register_stall(height);
        if entered {
            warn!(
                height,
                "entering RPC load-shed mode: fetching one block per pass until the node recovers"
            );
        }
        warn!(
            height,
            stalls,
            backoff_ms = relief_stall_backoff(stalls).as_millis(),
            "block fetch stalled; backing off before retrying this height"
        );
    }

    /// A pass committed blocks with no stall: clear the escalation and, once the
    /// recovery streak is met, restore the configured batch size and fan-out.
    fn note_fetch_progress(&self, projected_blocks: u64) {
        if self.relief.register_progress(projected_blocks) {
            info!(
                committed_blocks = RELIEF_RECOVERY_COMMIT_BLOCKS,
                "leaving RPC load-shed mode: node is serving blocks again"
            );
        }
    }

    // Zero-state protection (network-agnostic by design). The gen2 base — `main`
    // heights at or below the boundary — is the shared, immutable foundation for
    // EVERY network: mainnet, devnet, and testnet all restore the same zero-state
    // dump and only diverge ABOVE the boundary, each growing its own gen3 history.
    // The guard is therefore anchored on the boundary HEIGHT, not on the nexus
    // name: a `main` sync must never start below the boundary (that would
    // re-derive/overwrite the protected gen2 range). The nexus is deliberately NOT
    // gated here — devnet/testnet are legitimate forward-sync targets above the
    // same boundary, so locking to nexus == "mainnet" would block valid deployments
    // while adding no real protection (the boundary check below is the safeguard).
    fn validate_zero_state_scope(&self, cursor_height: BlockHeight) -> Result<(), IngestionError> {
        let cursor = cursor_height.value();
        if self.chain.chain.as_str() == "main" && cursor < MAIN_ZERO_STATE_BOUNDARY_HEIGHT {
            return Err(IngestionError::ProtectedZeroStateCursorBelowBoundary {
                chain: self.chain.chain.to_string(),
                cursor_height: cursor,
                boundary_height: MAIN_ZERO_STATE_BOUNDARY_HEIGHT,
            });
        }

        Ok(())
    }

    fn validate_projected_height(&self, height: BlockHeight) -> Result<(), IngestionError> {
        if self.chain.chain.as_str() == "main" && height.value() <= MAIN_ZERO_STATE_BOUNDARY_HEIGHT
        {
            return Err(IngestionError::ProtectedZeroStateBlock {
                chain: self.chain.chain.to_string(),
                height: height.value(),
                boundary_height: MAIN_ZERO_STATE_BOUNDARY_HEIGHT,
            });
        }

        Ok(())
    }

    /// Startup sanity guard against a wrong-network RPC. Once gen3 blocks are synced
    /// (cursor above the boundary), the node's block at the cursor height must
    /// hash-match our stored block; a mismatch means the configured RPC points at a
    /// different network than this DB holds, so we refuse rather than corrupt the DB
    /// above the boundary (the boundary guard alone cannot catch this). A fresh DB
    /// with nothing above the boundary cannot be checked here — deploy discipline
    /// (pair the devnet DB with the devnet RPC) covers that case.
    async fn guard_node_matches_db(
        &self,
        chain_id: i32,
        cursor_height: BlockHeight,
    ) -> Result<(), IngestionError> {
        // Checked once per process (after a confirmed match) to avoid an RPC per sync.
        if self
            .node_guard_checked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Ok(());
        }
        // A fresh DB with nothing above the boundary cannot be verified yet; don't
        // latch, so the check runs as soon as gen3 blocks exist.
        if cursor_height.value() <= MAIN_ZERO_STATE_BOUNDARY_HEIGHT {
            return Ok(());
        }
        let mut conn = self.pool.acquire().await?;
        let stored = explorer_db::block_hash_at_height(&mut conn, chain_id, cursor_height).await?;
        drop(conn);
        let Some(db_hash) = stored else {
            return Ok(());
        };
        let node_block = self
            .rpc
            .get_block_by_height(&self.chain.chain, cursor_height)
            .await?;
        if !db_hash.eq_ignore_ascii_case(&node_block.hash) {
            return Err(IngestionError::NodeChainMismatch {
                height: cursor_height.value(),
                db_hash,
                node_hash: node_block.hash,
                chain: self.chain.chain.to_string(),
                configured_nexus: self.chain.nexus.to_string(),
            });
        }
        self.node_guard_checked
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    async fn project_window_sequentially(
        &self,
        window: &FetchWindow,
        cursor_height: BlockHeight,
    ) -> Result<WindowOutcome, IngestionError> {
        let mut projected_blocks = 0;
        let mut cursor_height_after = cursor_height;
        for height in window.from_height.value()..=window.to_height.value() {
            match self
                .project_raw_block_and_advance_cursor(BlockHeight::new(height))
                .await
            {
                Ok((_, advanced_height)) => {
                    cursor_height_after = advanced_height;
                    projected_blocks += 1;
                }
                // A node-side failure after some blocks were committed is a stall on
                // THIS height, not a failed pass — the committed prefix stands and
                // the worker backs off on the stalled height instead of replaying
                // the whole window. Same contract as the pipelined path.
                Err(error) if keeps_committed_prefix(projected_blocks, &error) => {
                    warn!(
                        height,
                        %error,
                        "block projection stalled mid-window; kept the committed prefix"
                    );
                    return Ok(WindowOutcome {
                        projected_blocks,
                        cursor_height_after,
                        stalled_height: Some(height),
                    });
                }
                Err(error) => return Err(error),
            }

            if height < window.to_height.value() && !self.settings.inter_block_delay.is_zero() {
                sleep(self.settings.inter_block_delay).await;
            }
        }

        Ok(WindowOutcome {
            projected_blocks,
            cursor_height_after,
            stalled_height: None,
        })
    }

    /// Fetch/process pipeline: keep up to `window.concurrency` block fetches in
    /// flight while the writer drains them in strict height order, so RPC fetch
    /// overlaps DB writes (the Rust analogue of the C# fetch/process threads
    /// joined by a bounded channel). Throughput becomes `min(fetch_rate,
    /// write_rate)` instead of the old phased `fetch_all → write_all` (which left
    /// the RPC idle during writes and the DB idle during fetch).
    ///
    /// Ordering and crash recovery are preserved exactly as in the serial path:
    /// each block is written in its own transaction and the cursor advances only
    /// for the next contiguous height, so a crash or a mid-window fetch failure
    /// leaves a committed, gap-free prefix. Concurrent fetch completion is
    /// reordered through `ready` before writing, so insert order is identical to
    /// the sequential path.
    async fn project_window_pipelined(
        &self,
        window: &FetchWindow,
        cursor_height: BlockHeight,
    ) -> Result<WindowOutcome, IngestionError> {
        let from = window.from_height.value();
        let to = window.to_height.value();
        let concurrency = window.concurrency.max(1);
        // Never let fetching run more than this many blocks ahead of the writer.
        // Bounds in-flight + buffered decoded blocks in memory and applies
        // backpressure when the writer is slower than RPC — the Rust equivalent
        // of the C# bounded `Channel` capacity.
        let max_read_ahead = u64::try_from(self.settings.queue_capacity)
            .unwrap_or(u64::MAX)
            .max(concurrency as u64);

        let mut tasks: JoinSet<(u64, Result<SdkBlockResult, IngestionError>)> = JoinSet::new();
        // Abort handles of the in-flight fetches, keyed by height, so the look-ahead
        // above a failed height can be cancelled instead of downloading tens of
        // megabytes that are guaranteed to be discarded (see `record_fetched`).
        let mut in_flight: BTreeMap<u64, AbortHandle> = BTreeMap::new();
        let mut ready: BTreeMap<u64, SdkBlockResult> = BTreeMap::new();
        let mut next_to_spawn = from;
        let mut next_to_write = from;
        let mut projected_blocks = 0u64;
        let mut cursor_height_after = cursor_height;
        // Lowest height whose fetch failed. It stops new fetches; the
        // already-committed contiguous prefix below it stays valid and the cursor
        // reflects it.
        let mut fetch_error: Option<(u64, IngestionError)> = None;

        loop {
            // Top up the fetch pipeline: up to `concurrency` requests in flight,
            // capped to `max_read_ahead` blocks ahead of the writer.
            while fetch_error.is_none()
                && next_to_spawn <= to
                && tasks.len() < concurrency
                && next_to_spawn.saturating_sub(next_to_write) < max_read_ahead
            {
                let driver = self.clone();
                let height = BlockHeight::new(next_to_spawn);
                let handle = tasks.spawn(async move {
                    (
                        height.value(),
                        driver.fetch_decoded_block_for_projection(height).await,
                    )
                });
                in_flight.insert(next_to_spawn, handle);
                next_to_spawn += 1;
            }

            // Harvest every fetch that has already finished, without blocking, so
            // the writer can drain them in one tight batch and the freed slots
            // refill above. When the writer is the bottleneck (e.g. a low-latency
            // local node) `ready` fills toward `max_read_ahead`, so writes stay
            // batched instead of paying an await per block; when fetch is the
            // bottleneck `ready` stays near-empty and fetch overlaps the writes.
            while let Some(joined) = tasks.try_join_next() {
                Self::record_fetched(&mut ready, &mut fetch_error, &mut in_flight, joined)?;
            }

            // Write every block that is now contiguous from the cursor, in order.
            // Fetch tasks keep running in the background while these writes await
            // the DB, which is what overlaps fetch with write.
            while let Some(block) = ready.remove(&next_to_write) {
                let height = BlockHeight::new(next_to_write);
                match self.write_decoded_block(height, &block).await {
                    Ok(advanced_height) => {
                        cursor_height_after = advanced_height;
                        projected_blocks += 1;
                        next_to_write += 1;
                    }
                    // A write-side failure is treated exactly like a fetch-side one
                    // (and exactly like the sequential path): keep the committed
                    // prefix and report the stall on the height that actually
                    // blocks, so the escalating backoff is keyed on it rather than
                    // on the window's first — already committed — height.
                    Err(error) if keeps_committed_prefix(projected_blocks, &error) => {
                        warn!(
                            height = next_to_write,
                            %error,
                            "block projection stalled mid-window; kept the committed prefix"
                        );
                        return Ok(WindowOutcome {
                            projected_blocks,
                            cursor_height_after,
                            stalled_height: Some(next_to_write),
                        });
                    }
                    Err(error) => return Err(error),
                }

                if next_to_write <= to && !self.settings.inter_block_delay.is_zero() {
                    sleep(self.settings.inter_block_delay).await;
                }
            }

            if tasks.is_empty() && (fetch_error.is_some() || next_to_spawn > to) {
                break;
            }

            // The next contiguous block is not fetched yet — block until one more
            // in-flight fetch finishes, then loop back to spawn/harvest/write.
            if let Some(joined) = tasks.join_next().await {
                Self::record_fetched(&mut ready, &mut fetch_error, &mut in_flight, joined)?;
            }
        }

        if let Some((stalled_height, error)) = fetch_error {
            // No progress at all (the next height itself failed) → surface the
            // error so the worker loop backs off. Otherwise keep the committed
            // prefix and report the stall so the caller sheds load and backs off
            // on that height instead of replaying the window at full fan-out.
            if projected_blocks == 0 {
                return Err(error);
            }
            warn!(
                height = stalled_height,
                %error,
                "block fetch stalled mid-window; kept the committed prefix, retrying the failed height next pass"
            );
            return Ok(WindowOutcome {
                projected_blocks,
                cursor_height_after,
                stalled_height: Some(stalled_height),
            });
        }

        Ok(WindowOutcome {
            projected_blocks,
            cursor_height_after,
            stalled_height: None,
        })
    }

    /// Write one already-fetched block and advance the cursor to it, in a single
    /// transaction — the pipeline's writer step. Kept separate from the write loop
    /// so a failure is a value the loop can classify (stall vs. hard error) instead
    /// of a `?` that discards the pass and the height it failed on.
    async fn write_decoded_block(
        &self,
        height: BlockHeight,
        block: &SdkBlockResult,
    ) -> Result<BlockHeight, IngestionError> {
        let mut transaction = self.pool.begin().await?;
        let block_record = self
            .project_decoded_block(&mut transaction, height, block)
            .await?;
        let cursor_height_after =
            explorer_db::advance_cursor(&mut transaction, block_record.chain_id, height).await?;
        transaction.commit().await?;
        Ok(cursor_height_after)
    }

    /// Record a completed block fetch from the pipeline: buffer a fetched block
    /// for the writer, or remember the LOWEST failed height so the writer stops
    /// there with a gap-free committed prefix below it.
    ///
    /// On a failure it also aborts every fetch still running for a HIGHER height.
    /// Those blocks can no longer be committed in this pass (the cursor advances
    /// contiguously, so the gap blocks them) and their payloads would be decoded
    /// and thrown away — on the 100+ MB blocks that is exactly the pointless load
    /// that pushes a struggling node over the edge.
    ///
    /// Deliberate cancellations surface as cancelled joins and are not failures;
    /// any other join error is a real task panic and is propagated.
    fn record_fetched(
        ready: &mut BTreeMap<u64, SdkBlockResult>,
        fetch_error: &mut Option<(u64, IngestionError)>,
        in_flight: &mut BTreeMap<u64, AbortHandle>,
        joined: Result<(u64, Result<SdkBlockResult, IngestionError>), tokio::task::JoinError>,
    ) -> Result<(), IngestionError> {
        let (height, result) = match joined {
            Ok(joined) => joined,
            Err(join_error) if join_error.is_cancelled() => return Ok(()),
            Err(join_error) => return Err(join_error.into()),
        };
        in_flight.remove(&height);
        match result {
            Ok(block) => {
                ready.insert(height, block);
            }
            Err(error) => {
                let supersedes = match fetch_error {
                    Some((failed_height, _)) => height < *failed_height,
                    None => true,
                };
                if supersedes {
                    for (_, handle) in in_flight.split_off(&height) {
                        handle.abort();
                    }
                    *fetch_error = Some((height, error));
                }
            }
        }
        Ok(())
    }

    pub async fn mark_all_balances_dirty_once(
        &self,
    ) -> Result<BalanceDirtyMarkReport, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        let marked_addresses =
            explorer_db::mark_all_chain_addresses_dirty(&mut conn, chain_id, cursor_height).await?;

        Ok(BalanceDirtyMarkReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            cursor_height: cursor_height.value(),
            marked_addresses,
        })
    }

    pub async fn sync_contract_string_event_side_effects_once(
        &self,
    ) -> Result<ContractStringEventSideEffectSyncReport, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        let ContractStringEventSideEffectReport {
            upserted_contracts,
            linked_contract_creates,
        } = explorer_db::reconcile_contract_string_event_side_effects(
            &mut conn,
            chain_id,
            if self.chain.chain.as_str() == "main" {
                Some(BlockHeight::new(MAIN_ZERO_STATE_BOUNDARY_HEIGHT))
            } else {
                None
            },
        )
        .await?;

        Ok(ContractStringEventSideEffectSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            upserted_contracts,
            linked_contract_creates,
        })
    }

    pub async fn sync_contract_upgrade_methods_once(
        &self,
    ) -> Result<ContractUpgradeMethodSyncReport, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        let candidates = explorer_db::fetch_contract_upgrade_method_candidates(
            &mut conn,
            chain_id,
            BlockHeight::new(MAIN_ZERO_STATE_BOUNDARY_HEIGHT),
            CONTRACT_UPGRADE_METHOD_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let mut fetched_contracts = 0;
        let mut inserted_methods = 0;
        let mut linked_contracts = 0;
        let mut failed_contracts = 0;

        for candidate in &candidates {
            match self.fetch_contract_upgrade_method(candidate).await {
                Ok(Some(upsert)) => {
                    fetched_contracts += 1;
                    let mut transaction = self.pool.begin().await?;
                    let result = explorer_db::apply_contract_upgrade_method(
                        &mut transaction,
                        chain_id,
                        &upsert,
                    )
                    .await?;
                    transaction.commit().await?;
                    if result.inserted_method {
                        inserted_methods += 1;
                    }
                    if result.linked_contract {
                        linked_contracts += 1;
                    }
                }
                Ok(None) => {
                    fetched_contracts += 1;
                }
                Err(error) => {
                    failed_contracts += 1;
                    warn!(
                        contract_id = candidate.contract_id,
                        contract = candidate.name,
                        timestamp = candidate.timestamp_unix_seconds,
                        %error,
                        "contract upgrade method sync failed"
                    );
                }
            }
        }

        Ok(ContractUpgradeMethodSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            selected_upgrades: candidates.len(),
            fetched_contracts,
            inserted_methods,
            linked_contracts,
            failed_contracts,
        })
    }

    pub async fn sync_contract_rpc_metadata_once(
        &self,
    ) -> Result<ContractRpcMetadataSyncReport, IngestionError> {
        let now_unix_seconds = chrono::Utc::now().timestamp();
        let stale_before_unix_seconds =
            now_unix_seconds.saturating_sub(CONTRACT_RPC_METADATA_STALE_SECONDS);
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;
        let candidates = explorer_db::fetch_contract_rpc_metadata_candidates(
            &mut conn,
            chain_id,
            stale_before_unix_seconds,
            BlockHeight::new(MAIN_ZERO_STATE_BOUNDARY_HEIGHT),
            CONTRACT_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let mut fetched_contracts = 0;
        let mut updated_contracts = 0;
        let mut inserted_methods = 0;
        let mut failed_contracts = 0;

        for candidate in &candidates {
            match self.fetch_contract_rpc_metadata(candidate).await {
                Ok(upsert) => {
                    fetched_contracts += 1;
                    let mut transaction = self.pool.begin().await?;
                    let result = explorer_db::apply_contract_rpc_metadata(
                        &mut transaction,
                        chain_id,
                        &upsert,
                    )
                    .await?;
                    transaction.commit().await?;
                    if result.updated_contract {
                        updated_contracts += 1;
                    }
                    if result.inserted_method {
                        inserted_methods += 1;
                    }
                }
                Err(error) => {
                    failed_contracts += 1;
                    warn!(
                        contract_id = candidate.id,
                        contract = candidate.name,
                        %error,
                        "contract RPC metadata sync failed"
                    );
                }
            }
        }

        Ok(ContractRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            selected_contracts: candidates.len(),
            fetched_contracts,
            updated_contracts,
            inserted_methods,
            failed_contracts,
        })
    }

    async fn fetch_contract_rpc_metadata(
        &self,
        candidate: &ContractRpcMetadataCandidate,
    ) -> Result<ContractRpcMetadataUpsert, IngestionError> {
        let contract = self
            .rpc
            .get_contract(&self.chain.chain, &candidate.name)
            .await?;
        Ok(contract_result_to_rpc_metadata_upsert(
            candidate.id,
            &contract,
            candidate.insert_current_method,
            chrono::Utc::now().timestamp(),
        ))
    }

    async fn fetch_contract_upgrade_method(
        &self,
        candidate: &ContractUpgradeMethodCandidate,
    ) -> Result<Option<ContractUpgradeMethodUpsert>, IngestionError> {
        let contract = self
            .rpc
            .get_contract(&self.chain.chain, &candidate.name)
            .await?;
        Ok(contract_result_to_upgrade_method_upsert(
            candidate.contract_id,
            &contract,
            candidate.timestamp_unix_seconds,
        ))
    }

    /// Maintain the staking snapshot (Soul-Masters) daily/monthly series. Called from the balance
    /// sync after the tip daily is written, so the series is validated against the fresh
    /// `balance-sync.v1` overlap.
    async fn project_stake_snapshots_for_chain(
        &self,
        chain_id: i32,
    ) -> Result<explorer_db::StakeForwardBuildReport, IngestionError> {
        Ok(explorer_db::project_stake_snapshots_forward(&self.pool, chain_id).await?)
    }

    async fn project_stake_snapshots_once(
        &self,
    ) -> Result<explorer_db::StakeForwardBuildReport, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        drop(conn);
        self.project_stake_snapshots_for_chain(chain_id).await
    }

    /// One-time bootstrap of the per-address staking snapshot seed (see
    /// `explorer_db::capture_stake_boundary_slice`). Run once on a fully populated database.
    pub async fn capture_stake_boundary_slice_once(
        &self,
    ) -> Result<explorer_db::StakeBoundarySliceReport, IngestionError> {
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        drop(conn);
        Ok(explorer_db::capture_stake_boundary_slice(&self.pool, chain_id).await?)
    }

    pub async fn sync_dirty_balances_once(&self) -> Result<BalanceSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let dirty_before = explorer_db::count_dirty_addresses(&mut conn, chain_id).await?;
        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        if lag > BALANCE_SYNC_LAG_THRESHOLD {
            return Ok(BalanceSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                dirty_before,
                selected_addresses: 0,
                updated_accounts: 0,
                reset_dirty_flags: 0,
                skipped_catchup: dirty_before > 0,
            });
        }
        if dirty_before == 0 {
            return Ok(BalanceSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                dirty_before,
                selected_addresses: 0,
                updated_accounts: 0,
                reset_dirty_flags: 0,
                skipped_catchup: false,
            });
        }

        let dirty_addresses = explorer_db::fetch_dirty_address_batch(
            &mut conn,
            chain_id,
            balance_dirty_batch_size(dirty_before),
        )
        .await?;
        drop(conn);

        let accounts = self.fetch_balance_accounts(&dirty_addresses).await?;
        let updated_accounts = self
            .persist_balance_accounts(chain_id, &dirty_addresses, accounts)
            .await?;

        let mut transaction = self.pool.begin().await?;
        let updated_address_ids = updated_accounts
            .iter()
            .map(|address| address.id)
            .collect::<Vec<_>>();
        // Refresh live DAO membership only. The Soul-Masters curve is owned solely by
        // the forward projector (project_stake_snapshots_for_chain below); balance sync
        // no longer writes a `balance-sync.v1` snapshot.
        explorer_db::reconcile_stake_memberships(&mut transaction, &updated_address_ids).await?;
        let reset_dirty_flags =
            explorer_db::reset_dirty_balance_flags(&mut transaction, &updated_accounts).await?;
        transaction.commit().await?;

        Ok(BalanceSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            dirty_before,
            selected_addresses: dirty_addresses.len(),
            updated_accounts: updated_accounts.len(),
            reset_dirty_flags,
            skipped_catchup: false,
        })
    }

    async fn fetch_balance_accounts(
        &self,
        dirty_addresses: &[DirtyAddress],
    ) -> Result<Vec<FetchedBalanceAccount>, IngestionError> {
        if dirty_addresses.is_empty() {
            return Ok(Vec::new());
        }

        // Phase 1: account overviews (name + stake), one getAccountInfos batch
        // per <=100-address chunk, all read from a single node-side snapshot.
        // The batch is all-or-nothing — one malformed address rejects the whole
        // call — so a failed multi-address chunk degrades to per-address calls
        // and only the offending addresses stay dirty.
        let mut infos = Vec::with_capacity(dirty_addresses.len());
        for chunk in dirty_addresses.chunks(BALANCE_SYNC_CHUNK_SIZE) {
            let addresses = chunk
                .iter()
                .map(|address| address.address.clone())
                .collect::<Vec<_>>();

            match self.rpc.get_account_infos(&addresses, false).await {
                Ok(chunk_infos) => infos.extend(chunk_infos),
                Err(error) if addresses.len() > 1 => {
                    warn!(
                        %error,
                        count = addresses.len(),
                        "batch account info fetch failed; retrying addresses one by one"
                    );
                    for address in addresses {
                        match self.rpc.get_account_info(&address, false).await {
                            Ok(info) => infos.push(info),
                            Err(error) => warn!(
                                %error,
                                address,
                                "single account info fetch failed; keeping address dirty"
                            ),
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }

        // Phase 2: balance rows per address through the paginated endpoints,
        // with the per-address fan-out bounded so a large dirty batch cannot
        // flood the node's global concurrency cap. An address whose balance
        // fetch fails is dropped from the result and stays dirty for the next
        // pass; the others proceed.
        let mut accounts = Vec::with_capacity(infos.len());
        let mut fetches = tokio::task::JoinSet::new();
        let mut pending = infos.into_iter();
        loop {
            while fetches.len() < BALANCE_FETCH_CONCURRENCY {
                let Some(info) = pending.next() else { break };
                let rpc = self.rpc.clone();
                let chain = self.chain.chain.clone();
                fetches.spawn(async move {
                    let balances = fetch_address_balances(&rpc, &chain, &info.address).await;
                    (info, balances)
                });
            }
            let Some(joined) = fetches.join_next().await else {
                break;
            };
            match joined {
                Ok((info, Ok(balances))) => {
                    accounts.push(FetchedBalanceAccount { info, balances });
                }
                Ok((info, Err(error))) => warn!(
                    %error,
                    address = %info.address,
                    "address balance fetch failed; keeping address dirty"
                ),
                // A crashed fetch task (cancellation is never used here) only
                // loses that one address for this pass.
                Err(join_error) => warn!(
                    %join_error,
                    "address balance fetch task failed; keeping address dirty"
                ),
            }
        }

        Ok(accounts)
    }

    async fn persist_balance_accounts(
        &self,
        chain_id: i32,
        dirty_addresses: &[DirtyAddress],
        accounts: Vec<FetchedBalanceAccount>,
    ) -> Result<Vec<DirtyAddress>, IngestionError> {
        if accounts.is_empty() {
            return Ok(Vec::new());
        }

        let dirty_by_address = dirty_addresses
            .iter()
            .map(|address| (address.address.as_str(), address))
            .collect::<BTreeMap<_, _>>();

        let mut conn = self.pool.acquire().await?;
        let soul_decimals = explorer_db::get_token_decimals(&mut conn, chain_id, "SOUL").await?;
        let kcal_decimals = explorer_db::get_token_decimals(&mut conn, chain_id, "KCAL").await?;
        drop(conn);

        let now_unix_seconds = chrono::Utc::now().timestamp();
        let mut updated_dirty_addresses = Vec::new();
        let mut transaction = self.pool.begin().await?;

        for account in accounts {
            let Some(dirty_address) = dirty_by_address.get(account.info.address.as_str()) else {
                continue;
            };
            let account_upsert = account_info_to_upsert(
                dirty_address.id,
                &account,
                soul_decimals,
                kcal_decimals,
                now_unix_seconds,
            );
            let upsert_result =
                explorer_db::upsert_address_account(&mut transaction, chain_id, &account_upsert)
                    .await?;
            if upsert_result.missing_balance_symbols.is_empty() {
                updated_dirty_addresses.push((*dirty_address).clone());
            } else {
                warn!(
                    address = %account.info.address,
                    symbols = ?upsert_result.missing_balance_symbols,
                    "account balance sync returned balances for unknown tokens; keeping address dirty"
                );
            }
        }

        transaction.commit().await?;
        Ok(updated_dirty_addresses)
    }

    pub async fn sync_token_supplies_once(&self) -> Result<TokenSupplySyncReport, IngestionError> {
        let tokens = self.rpc.get_tokens(false).await?;
        let supplies = tokens
            .iter()
            .map(token_result_to_supply_upsert)
            .collect::<Vec<_>>();

        let mut transaction = self.pool.begin().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut transaction, &self.chain.chain).await?;
        let updated_tokens =
            explorer_db::update_token_supplies(&mut transaction, chain_id, &supplies).await?;
        transaction.commit().await?;

        Ok(TokenSupplySyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            fetched_tokens: tokens.len(),
            updated_tokens,
        })
    }

    /// Refreshes token prices from CoinGecko, mirroring the C# `Price.CoinGecko` plugin. It does
    /// two things: a live `/simple/price` refresh of `tokens.price_*`, then resumes the daily USD
    /// history backfill into
    /// `token_daily_prices`. The optional `EXPLORER_COINGECKO_API_KEY` is sent as the
    /// demo-key header (the free tier works without it); `EXPLORER_COINGECKO_BASE_URL`
    /// overrides the host for tests.
    pub async fn sync_token_prices_once(&self) -> Result<TokenPriceSyncReport, IngestionError> {
        let api_key = std::env::var("EXPLORER_COINGECKO_API_KEY")
            .ok()
            .filter(|key| !key.is_empty());
        let base_url = std::env::var("EXPLORER_COINGECKO_BASE_URL")
            .unwrap_or_else(|_| prices::COINGECKO_BASE_URL.to_owned());
        let client = prices::build_client()?;

        // Step 1: live prices. A single request that refreshes every fiat column.
        let live = prices::fetch_live_prices(&client, &base_url, api_key.as_deref()).await?;
        let mut transaction = self.pool.begin().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut transaction, &self.chain.chain).await?;
        let live_prices_updated =
            explorer_db::update_token_prices(&mut transaction, chain_id, &live).await?;
        let latest_daily =
            explorer_db::latest_token_daily_price_date(&mut transaction, chain_id).await?;
        transaction.commit().await?;

        // Step 2: daily history, resuming from the day after the latest stored close.
        let (daily_days_processed, daily_rows_inserted, daily_caught_up) = self
            .backfill_daily_prices(
                &client,
                &base_url,
                api_key.as_deref(),
                chain_id,
                latest_daily,
            )
            .await?;

        Ok(TokenPriceSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            live_prices_updated,
            daily_days_processed,
            daily_rows_inserted,
            daily_caught_up,
        })
    }

    /// Walks the daily USD history forward from the latest stored day to today,
    /// bounded per run, fetching one `/coins/{id}/history` per priced symbol per day.
    /// KCAL is skipped (its history needs a paid plan; the C# plugin marks it
    /// inactive) and GOATI is pegged to SOUL's USD price (it has no listing). A
    /// rate-limit response stops the run early; the rest resumes next tick. Returns
    /// `(days_processed, rows_inserted, caught_up)`.
    async fn backfill_daily_prices(
        &self,
        client: &reqwest::Client,
        base_url: &str,
        api_key: Option<&str>,
        chain_id: i32,
        latest_stored: Option<i64>,
    ) -> Result<(u64, u64, bool), IngestionError> {
        let today = chrono::Utc::now().date_naive();

        // Without an anchor day there is nothing to resume from, so daily backfill
        // is a no-op and already "caught up".
        let Some(latest) = latest_stored else {
            return Ok((0, 0, true));
        };
        let Some(latest_dt) = chrono::DateTime::from_timestamp(latest, 0) else {
            return Ok((0, 0, true));
        };
        let mut day = latest_dt.date_naive() + chrono::Duration::days(1);

        let mut days_processed: u64 = 0;
        let mut rows_inserted: u64 = 0;

        while day <= today && days_processed < TOKEN_PRICE_DAILY_BACKFILL_MAX_DAYS_PER_RUN {
            let date_param = day.format("%d-%m-%Y").to_string();
            let Some(day_unix) = day
                .and_hms_opt(0, 0, 0)
                .map(|datetime| datetime.and_utc().timestamp())
            else {
                break;
            };

            let mut day_rows: Vec<explorer_db::TokenDailyPriceUpsert> = Vec::new();
            let mut soul_usd: Option<f64> = None;
            let mut rate_limited = false;

            for symbol in prices::PRICED_SYMBOLS {
                let Some(coin_id) = prices::coingecko_id(symbol) else {
                    continue;
                };
                // KCAL's daily-history endpoint requires a paid plan; skip like C#.
                if coin_id == prices::KCAL_COINGECKO_ID {
                    continue;
                }

                match prices::fetch_daily_close(client, base_url, api_key, coin_id, &date_param)
                    .await?
                {
                    prices::DailyCloseOutcome::Price(usd) => {
                        if symbol == "SOUL" {
                            soul_usd = Some(usd);
                        }
                        day_rows.push(explorer_db::TokenDailyPriceUpsert {
                            symbol: symbol.to_owned(),
                            date_unix_seconds: day_unix,
                            price_usd: usd,
                        });
                    }
                    prices::DailyCloseOutcome::Missing => {}
                    prices::DailyCloseOutcome::RateLimited => {
                        rate_limited = true;
                        break;
                    }
                }

                sleep(std::time::Duration::from_millis(
                    TOKEN_PRICE_DAILY_REQUEST_DELAY_MS,
                ))
                .await;
            }

            // GOATI has no CoinGecko listing; C# pegs its daily price to SOUL's USD.
            if let Some(usd) = soul_usd {
                day_rows.push(explorer_db::TokenDailyPriceUpsert {
                    symbol: "GOATI".to_owned(),
                    date_unix_seconds: day_unix,
                    price_usd: usd,
                });
            }

            if !day_rows.is_empty() {
                let mut transaction = self.pool.begin().await?;
                rows_inserted +=
                    explorer_db::upsert_token_daily_prices(&mut transaction, chain_id, &day_rows)
                        .await?;
                transaction.commit().await?;
            }

            days_processed += 1;

            if rate_limited {
                // Stop this run gracefully; the remaining days resume next tick.
                return Ok((days_processed, rows_inserted, false));
            }

            day += chrono::Duration::days(1);
        }

        let caught_up = day > today;
        Ok((days_processed, rows_inserted, caught_up))
    }

    /// Fetches off-chain NFT metadata for one batch of TTRS NFTs from 22series and
    /// writes it — a port of the C# `Nft.TTRS` plugin's `LoadNfts`. Selects NFTs under the TTRS
    /// contract that
    /// still lack off-chain metadata, POSTs their ids, and patches
    /// `nfts.offchain_api_response` plus the display fields. One bounded batch per run
    /// (the backlog drains across near-tip ticks). The C# "delete System object NFT"
    /// path is intentionally NOT ported — this backend never deletes rows here.
    /// `EXPLORER_TTRS_API_URL` overrides the host for tests.
    pub async fn sync_ttrs_offchain_nfts_once(
        &self,
    ) -> Result<TtrsOffchainSyncReport, IngestionError> {
        let url = std::env::var("EXPLORER_TTRS_API_URL")
            .unwrap_or_else(|_| ttrs::TTRS_API_URL.to_owned());

        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let token_ids = explorer_db::list_contract_nfts_missing_offchain(
            &mut conn,
            chain_id,
            ttrs::TTRS_CONTRACT_NAME,
            TTRS_OFFCHAIN_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        if token_ids.is_empty() {
            return Ok(TtrsOffchainSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                selected: 0,
                fetched: 0,
                updated: 0,
            });
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .user_agent("phantasma-explorer-rs/ttrs-feed")
            .build()
            .map_err(ttrs::TtrsFeedError::ClientBuild)?;

        let records = ttrs::fetch_offchain_batch(&client, &url, &token_ids).await?;

        let mut transaction = self.pool.begin().await?;
        let updated = explorer_db::update_nft_offchain_metadata(
            &mut transaction,
            chain_id,
            ttrs::TTRS_CONTRACT_NAME,
            &records,
        )
        .await?;
        transaction.commit().await?;

        Ok(TtrsOffchainSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            selected: token_ids.len(),
            fetched: records.len(),
            updated,
        })
    }

    pub async fn sync_nft_rpc_metadata_once(
        &self,
    ) -> Result<NftRpcMetadataSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        if lag > BALANCE_SYNC_LAG_THRESHOLD {
            return Ok(NftRpcMetadataSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                selected_nfts: 0,
                fetched_nfts: 0,
                updated_nfts: 0,
                skipped_catchup: true,
            });
        }

        let min_mint_block_height = if self.chain.chain.as_str() == "main" {
            i64::try_from(MAIN_ZERO_STATE_BOUNDARY_HEIGHT).unwrap_or(i64::MAX)
        } else {
            0
        };
        let candidates = explorer_db::fetch_nft_rpc_metadata_candidates(
            &mut conn,
            chain_id,
            min_mint_block_height,
            NFT_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let upserts = self.fetch_nft_rpc_metadata(&candidates).await?;
        let updated_nfts = if upserts.is_empty() {
            0
        } else {
            let mut transaction = self.pool.begin().await?;
            let updated =
                explorer_db::apply_nft_rpc_metadata(&mut transaction, chain_id, &upserts).await?;
            transaction.commit().await?;
            updated
        };

        Ok(NftRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_nfts: candidates.len(),
            fetched_nfts: upserts.len(),
            updated_nfts,
            skipped_catchup: false,
        })
    }

    pub async fn repair_nft_rpc_metadata_once(
        &self,
    ) -> Result<NftRpcMetadataSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let min_mint_block_height = if self.chain.chain.as_str() == "main" {
            i64::try_from(MAIN_ZERO_STATE_BOUNDARY_HEIGHT).unwrap_or(i64::MAX)
        } else {
            0
        };
        let candidates = explorer_db::fetch_nft_rpc_metadata_repair_candidates(
            &mut conn,
            chain_id,
            min_mint_block_height,
            NFT_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let upserts = self.fetch_nft_rpc_metadata(&candidates).await?;
        let updated_nfts = if upserts.is_empty() {
            0
        } else {
            let mut transaction = self.pool.begin().await?;
            let updated =
                explorer_db::apply_nft_rpc_metadata(&mut transaction, chain_id, &upserts).await?;
            transaction.commit().await?;
            updated
        };

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        Ok(NftRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_nfts: candidates.len(),
            fetched_nfts: upserts.len(),
            updated_nfts,
            skipped_catchup: false,
        })
    }

    pub async fn sync_nft_rpc_metadata_for_mint_block(
        &self,
        height: BlockHeight,
    ) -> Result<NftRpcMetadataSyncReport, IngestionError> {
        self.validate_projected_height(height)?;
        let mint_block_height =
            i64::try_from(height.value()).map_err(|_| IngestionError::BlockFieldOutOfRange {
                height: height.value(),
                field: "height",
            })?;

        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let candidates = explorer_db::fetch_nft_rpc_metadata_candidates_for_mint_block(
            &mut conn,
            chain_id,
            mint_block_height,
            NFT_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let upserts = self.fetch_nft_rpc_metadata(&candidates).await?;
        let updated_nfts = if upserts.is_empty() {
            0
        } else {
            let mut transaction = self.pool.begin().await?;
            let updated =
                explorer_db::apply_nft_rpc_metadata(&mut transaction, chain_id, &upserts).await?;
            transaction.commit().await?;
            updated
        };

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        Ok(NftRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_nfts: candidates.len(),
            fetched_nfts: upserts.len(),
            updated_nfts,
            skipped_catchup: false,
        })
    }

    async fn fetch_nft_rpc_metadata(
        &self,
        candidates: &[NftRpcMetadataCandidate],
    ) -> Result<Vec<NftRpcMetadataUpsert>, IngestionError> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_symbol = BTreeMap::<String, Vec<String>>::new();
        for candidate in candidates {
            by_symbol
                .entry(candidate.symbol.clone())
                .or_default()
                .push(candidate.token_id.clone());
        }

        let mut upserts = Vec::new();
        for (symbol, token_ids) in by_symbol {
            match self.rpc.get_nfts(&symbol, &token_ids, true).await {
                Ok(nfts) => {
                    let mut seen = std::collections::BTreeSet::new();
                    for nft in nfts {
                        seen.insert(nft.id.clone());
                        if let Some(upsert) = nft_result_to_metadata_upsert(&symbol, &nft) {
                            upserts.push(upsert);
                        }
                    }

                    for token_id in token_ids
                        .iter()
                        .filter(|token_id| !seen.contains(*token_id))
                    {
                        match self.rpc.get_nft(&symbol, token_id, true).await {
                            Ok(nft) => {
                                if let Some(upsert) = nft_result_to_metadata_upsert(&symbol, &nft) {
                                    upserts.push(upsert);
                                }
                            }
                            Err(error) => record_nft_metadata_fetch_failure(
                                &mut upserts,
                                &symbol,
                                token_id,
                                &error,
                            ),
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        %error,
                        symbol,
                        count = token_ids.len(),
                        "batch NFT RPC metadata fetch failed; retrying one by one"
                    );
                    for token_id in token_ids {
                        match self.rpc.get_nft(&symbol, &token_id, true).await {
                            Ok(nft) => {
                                if let Some(upsert) = nft_result_to_metadata_upsert(&symbol, &nft) {
                                    upserts.push(upsert);
                                }
                            }
                            Err(error) => record_nft_metadata_fetch_failure(
                                &mut upserts,
                                &symbol,
                                &token_id,
                                &error,
                            ),
                        }
                    }
                }
            }
        }

        Ok(upserts)
    }

    pub async fn sync_series_rpc_metadata_once(
        &self,
    ) -> Result<SeriesRpcMetadataSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        if lag > BALANCE_SYNC_LAG_THRESHOLD {
            return Ok(SeriesRpcMetadataSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                selected_series: 0,
                fetched_series: 0,
                updated_series: 0,
                skipped_catchup: true,
            });
        }

        let min_event_block_height = if self.chain.chain.as_str() == "main" {
            i64::try_from(MAIN_ZERO_STATE_BOUNDARY_HEIGHT).unwrap_or(i64::MAX)
        } else {
            0
        };
        let candidates = explorer_db::fetch_series_rpc_metadata_candidates(
            &mut conn,
            chain_id,
            min_event_block_height,
            SERIES_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let upserts = self.fetch_series_rpc_metadata(&candidates).await?;
        let updated_series = if upserts.is_empty() {
            0
        } else {
            let mut transaction = self.pool.begin().await?;
            let updated =
                explorer_db::apply_series_rpc_metadata(&mut transaction, chain_id, &upserts)
                    .await?;
            transaction.commit().await?;
            updated
        };

        Ok(SeriesRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_series: candidates.len(),
            fetched_series: upserts.len(),
            updated_series,
            skipped_catchup: false,
        })
    }

    pub async fn repair_series_rpc_metadata_once(
        &self,
    ) -> Result<SeriesRpcMetadataSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        if lag > BALANCE_SYNC_LAG_THRESHOLD {
            return Ok(SeriesRpcMetadataSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                selected_series: 0,
                fetched_series: 0,
                updated_series: 0,
                skipped_catchup: true,
            });
        }

        let min_event_block_height = if self.chain.chain.as_str() == "main" {
            i64::try_from(MAIN_ZERO_STATE_BOUNDARY_HEIGHT).unwrap_or(i64::MAX)
        } else {
            0
        };
        let candidates = explorer_db::fetch_series_rpc_metadata_repair_candidates(
            &mut conn,
            chain_id,
            min_event_block_height,
            SERIES_RPC_METADATA_SYNC_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let upserts = self.fetch_series_rpc_metadata(&candidates).await?;
        let updated_series = if upserts.is_empty() {
            0
        } else {
            let mut transaction = self.pool.begin().await?;
            let updated =
                explorer_db::apply_series_rpc_metadata(&mut transaction, chain_id, &upserts)
                    .await?;
            transaction.commit().await?;
            updated
        };

        Ok(SeriesRpcMetadataSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_series: candidates.len(),
            fetched_series: upserts.len(),
            updated_series,
            skipped_catchup: false,
        })
    }

    async fn fetch_series_rpc_metadata(
        &self,
        candidates: &[SeriesRpcMetadataCandidate],
    ) -> Result<Vec<SeriesRpcMetadataUpsert>, IngestionError> {
        let mut upserts = Vec::new();
        for candidate in candidates {
            match self
                .rpc
                .get_token_series_by_id(&candidate.symbol, &candidate.series_id)
                .await
            {
                Ok(series) => {
                    if let Some(upsert) =
                        series_result_to_metadata_upsert(&candidate.symbol, &series)
                    {
                        upserts.push(upsert);
                    }
                }
                Err(error) => {
                    warn!(
                        %error,
                        symbol = candidate.symbol,
                        series_id = candidate.series_id,
                        "series RPC metadata fetch failed; storing error response"
                    );
                    upserts.push(series_error_to_metadata_upsert(candidate, &error));
                }
            }
        }

        Ok(upserts)
    }

    pub async fn sync_failed_transaction_debug_comments_once(
        &self,
    ) -> Result<FailedTransactionDebugSyncReport, IngestionError> {
        let rpc_tip = self.rpc.get_block_height(&self.chain.chain).await?;
        let mut conn = self.pool.acquire().await?;
        let chain_id = explorer_db::resolve_chain_id(&mut conn, &self.chain.chain).await?;
        let cursor_height = explorer_db::get_cursor_height(&mut conn, chain_id)
            .await?
            .unwrap_or_else(|| BlockHeight::new(0));
        self.validate_zero_state_scope(cursor_height)?;

        let lag = rpc_tip.value().saturating_sub(cursor_height.value());
        if lag > BALANCE_SYNC_LAG_THRESHOLD {
            return Ok(FailedTransactionDebugSyncReport {
                configured_nexus: self.chain.nexus.to_string(),
                chain: self.chain.chain.to_string(),
                rpc_tip_height: rpc_tip.value(),
                cursor_height: cursor_height.value(),
                lag,
                selected_transactions: 0,
                updated_transactions: 0,
                skipped_catchup: true,
            });
        }

        let cutoff_unix_seconds = chrono::Utc::now()
            .timestamp()
            .saturating_sub(FAILED_TX_DEBUG_SEED_WINDOW_SECONDS);
        let hashes = explorer_db::fetch_failed_transactions_missing_debug_comment(
            &mut conn,
            chain_id,
            cutoff_unix_seconds,
            FAILED_TX_DEBUG_BATCH_SIZE,
        )
        .await?;
        drop(conn);

        let mut updated_transactions = 0;
        for hash in &hashes {
            let transaction = match self.rpc.get_transaction(hash).await {
                Ok(transaction) => transaction,
                Err(error) => {
                    warn!(
                        %error,
                        hash,
                        "failed transaction debug-comment fetch failed"
                    );
                    continue;
                }
            };

            let Some(debug_comment) = transaction
                .debug_comment
                .as_deref()
                .and_then(non_empty_string)
            else {
                continue;
            };
            let result = non_empty_string(&transaction.result);

            let mut transaction_conn = self.pool.acquire().await?;
            let changed = explorer_db::update_failed_transaction_debug_comment(
                &mut transaction_conn,
                hash,
                result.as_deref(),
                &debug_comment,
            )
            .await?;
            if changed {
                updated_transactions += 1;
            }
        }

        Ok(FailedTransactionDebugSyncReport {
            configured_nexus: self.chain.nexus.to_string(),
            chain: self.chain.chain.to_string(),
            rpc_tip_height: rpc_tip.value(),
            cursor_height: cursor_height.value(),
            lag,
            selected_transactions: hashes.len(),
            updated_transactions,
            skipped_catchup: false,
        })
    }

    pub async fn run_until_shutdown(&self) -> Result<(), IngestionError> {
        // A deploy that restores an already-migrated database and skips
        // `explorer-migrate` carries no planner stats (pg_restore drops them), so
        // the first catch-up sync would crawl until autovacuum analyzes. Refresh
        // stats once at startup to close that window — the migrate path does the
        // same after applying migrations. Non-fatal: a failed ANALYZE must not
        // stop the worker from syncing.
        match explorer_db::analyze_database(&self.pool).await {
            Ok(()) => info!("database analyzed at startup"),
            Err(error) => warn!(%error, "startup database analyze failed; continuing"),
        }

        // Maintenance (balance/stake, token supply/price, NFT/series/contract
        // metadata, failed-tx debug) runs on its OWN tasks, concurrently with block
        // ingestion and with each other — mirroring the C# thread-per-job model. The
        // block loop below only projects blocks, so the tip is indexed within a poll
        // no matter how long a maintenance job takes. Each task gates itself on the
        // shared near-tip `lag` snapshot; balance is woken fire-and-forget after each
        // near-tip batch (the analogue of C#'s RequestBalanceSync semaphore).
        let lag = Arc::new(AtomicU64::new(u64::MAX));
        let balance_kick = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut maintenance = self.spawn_maintenance_tasks(&lag, &balance_kick, &shutdown_rx);

        // Register the OS shutdown signal listener EXACTLY ONCE, in a dedicated
        // task that fans the result out through the shutdown watch channel.
        //
        // Re-creating `wait_for_shutdown_signal()` inside `select!` on every loop
        // pass (as this loop used to) is unsound for Ctrl+C: tokio installs a
        // process-global SIGINT handler on first use, replacing the default
        // "terminate the process" disposition, and each freshly built future only
        // observes a signal that arrives while it is actively polled. Dropping and
        // rebuilding the listener every pass leaves gaps where a Ctrl+C is handled
        // by neither us nor the (already replaced) default handler, so the worker
        // stops responding to Ctrl+C. One long-lived listener plus the latched
        // watch channel (once true, stays true) removes that gap and also stops the
        // maintenance tasks the moment the signal arrives.
        {
            let signal_shutdown_tx = shutdown_tx.clone();
            tokio::spawn(async move {
                explorer_runtime::wait_for_shutdown_signal().await;
                let _ = signal_shutdown_tx.send(true);
            });
        }
        let mut shutdown_signal = shutdown_rx.clone();

        let mut ticker = interval(self.settings.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Observability state: count consecutive sync failures (for error-level
        // escalation + backoff) and track whether near-tip maintenance is paused
        // (so the pause/resume is logged instead of being silently invisible).
        let mut consecutive_sync_failures: u32 = 0;
        let mut maintenance_paused = false;
        // Suppress the per-poll "synced range=none blocks=0" spam once at the tip:
        // announce reaching the tip once, then stay quiet until new blocks arrive.
        let mut caught_up_logged = false;

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown_signal.changed() => {
                    info!("worker shutdown signal received");
                    break;
                }
            }

            let sync_result = tokio::select! {
                result = self.sync_once() => result,
                _ = shutdown_signal.changed() => {
                    info!("worker shutdown signal received; cancelling current sync batch");
                    break;
                }
            };

            match sync_result {
                Ok(report) => {
                    // Log only when something happened: a batch that wrote blocks, or a
                    // pass that is still behind the tip. Once caught up, say so once and
                    // then stay silent instead of logging every idle poll.
                    if report.projected_blocks > 0
                        || report.cursor_height_after < report.rpc_tip_height
                    {
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
                        caught_up_logged = false;
                    } else if !caught_up_logged {
                        info!("caught up to tip {}", report.rpc_tip_height);
                        caught_up_logged = true;
                    }

                    consecutive_sync_failures = 0;
                    // A pass that committed its prefix but stalled on a height is
                    // not a failure — it still must wait before touching that
                    // height again, on the escalating schedule (30s..5min), or the
                    // poll tick would re-request the same unservable block within
                    // seconds. This is the load the node feels most.
                    if report.stalled_height.is_some() {
                        let backoff = relief_stall_backoff(self.relief.stall_count());
                        if wait_or_shutdown(backoff, &mut shutdown_signal).await {
                            info!("worker shutdown signal received during the stall backoff");
                            break;
                        }
                    }
                    let current_lag = report
                        .rpc_tip_height
                        .saturating_sub(report.cursor_height_after);
                    // Publish the near-tip lag for the maintenance tasks' gate.
                    lag.store(current_lag, Ordering::Relaxed);
                    let near_tip = current_lag <= BALANCE_SYNC_LAG_THRESHOLD;
                    if near_tip {
                        if maintenance_paused {
                            info!(
                                cursor = report.cursor_height_after,
                                tip = report.rpc_tip_height,
                                "near-tip maintenance resumed"
                            );
                            maintenance_paused = false;
                        }
                        // Wake the balance task off the block path after a batch
                        // that wrote blocks (and therefore dirtied addresses). The
                        // wake is coalesced (at most one pending), mirroring C#'s
                        // RequestBalanceSync.
                        if report.projected_blocks > 0 {
                            balance_kick.notify_one();
                        }
                    } else if !maintenance_paused {
                        warn!(
                            lag = report
                                .rpc_tip_height
                                .saturating_sub(report.cursor_height_after),
                            threshold = BALANCE_SYNC_LAG_THRESHOLD,
                            "near-tip maintenance paused: sync is too far behind the tip"
                        );
                        maintenance_paused = true;
                    }
                }
                Err(error) => {
                    consecutive_sync_failures = consecutive_sync_failures.saturating_add(1);
                    error!(
                        %error,
                        consecutive_failures = consecutive_sync_failures,
                        "worker sync batch failed"
                    );
                    // Back off on repeated failures (e.g. the RPC node is down) so we
                    // neither hammer it nor log-spam; capped, on top of the poll tick.
                    // A stalled block fetch carries its own, much longer escalating
                    // wait, which a database failure does not inherit.
                    let backoff = failed_pass_backoff(
                        consecutive_sync_failures,
                        self.relief.stall_count(),
                        is_database_failure(&error),
                    );
                    if wait_or_shutdown(backoff, &mut shutdown_signal).await {
                        info!("worker shutdown signal received during the failure backoff");
                        break;
                    }
                }
            }
        }

        // Shutdown: tell the maintenance tasks to stop and wait for each to
        // finish its current job (graceful, like the C# threads stopping on
        // `_running = false`).
        let _ = shutdown_tx.send(true);
        while maintenance.join_next().await.is_some() {}
        Ok(())
    }

    /// Spawn the maintenance jobs as independent tasks (the Rust analogue of the
    /// C# thread-per-job model). Each gates itself on the shared near-tip `lag`;
    /// balance is woken fire-and-forget by the block loop. The block loop never
    /// awaits any of them, so block indexing stays decoupled from maintenance.
    fn spawn_maintenance_tasks(
        &self,
        lag: &Arc<AtomicU64>,
        balance_kick: &Arc<Notify>,
        shutdown: &watch::Receiver<bool>,
    ) -> JoinSet<()> {
        let mut tasks = JoinSet::new();
        {
            let driver = self.clone();
            let lag = lag.clone();
            let kick = balance_kick.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_balance_maintenance(lag, kick, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks
                .spawn(async move { driver.run_stake_projection_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_token_supply_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_token_price_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_ttrs_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move {
                driver
                    .run_contract_metadata_maintenance(lag, shutdown)
                    .await
            });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_nft_metadata_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_series_metadata_maintenance(lag, shutdown).await });
        }
        {
            let driver = self.clone();
            let lag = lag.clone();
            let shutdown = shutdown.clone();
            tasks.spawn(async move { driver.run_failed_tx_maintenance(lag, shutdown).await });
        }
        tasks
    }

    /// Balance/stake drain: runs when the block loop pokes `kick` (immediately
    /// after a near-tip batch) or on a poll-interval fallback (to drain leftover
    /// dirty addresses and advance daily stake snapshots while the chain is idle).
    async fn run_balance_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        kick: Arc<Notify>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut fallback = interval(self.settings.poll_interval);
        fallback.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = kick.notified() => {}
                _ = fallback.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_dirty_balances_once().await {
                Ok(balance_report) if balance_report.updated_accounts > 0 => info!(
                    "synced balances accounts={} reset_dirty={} dirty_before={} lag={}",
                    balance_report.updated_accounts,
                    balance_report.reset_dirty_flags,
                    balance_report.dirty_before,
                    balance_report.lag,
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "balance sync batch failed"),
            }
        }
    }

    /// Build the Soul-Masters curve forward on its own cadence, fully decoupled from
    /// balance sync. Idempotent: most ticks no-op cheaply via the projector's
    /// max-projected-day gate; a tick rebuilds the curve only when a new block has
    /// advanced the cursor's day. Near-tip gated like the other maintenance families.
    async fn run_stake_projection_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            STAKE_PROJECTION_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.project_stake_snapshots_once().await {
                Ok(report) if report.daily_upserted > 0 || report.monthly_upserted > 0 => info!(
                    "built soul-masters curve daily={} monthly={} boundary_masters={}",
                    report.daily_upserted, report.monthly_upserted, report.boundary_masters_count
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "stake projection failed"),
            }
        }
    }

    async fn run_token_supply_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            TOKEN_SUPPLY_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_token_supplies_once().await {
                // Only log when something actually changed, like the other
                // maintenance tasks — supplies rarely move, so logging every tick
                // would just spam an idle-tip worker once a minute.
                Ok(token_report) if token_report.updated_tokens > 0 => info!(
                    "synced token supplies fetched={} updated={}",
                    token_report.fetched_tokens, token_report.updated_tokens
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "token supply sync failed"),
            }
        }
    }

    async fn run_token_price_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            TOKEN_PRICE_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_token_prices_once().await {
                Ok(price_report)
                    if price_report.live_prices_updated > 0
                        || price_report.daily_rows_inserted > 0 =>
                {
                    info!(
                        "synced token prices live_updated={} daily_days={} daily_inserted={} daily_caught_up={}",
                        price_report.live_prices_updated,
                        price_report.daily_days_processed,
                        price_report.daily_rows_inserted,
                        price_report.daily_caught_up
                    )
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "token price sync failed"),
            }
        }
    }

    async fn run_ttrs_maintenance(&self, lag: Arc<AtomicU64>, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = interval(std::time::Duration::from_secs(
            TTRS_OFFCHAIN_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_ttrs_offchain_nfts_once().await {
                Ok(ttrs_report) if ttrs_report.updated > 0 => info!(
                    "synced TTRS off-chain NFTs selected={} fetched={} updated={}",
                    ttrs_report.selected, ttrs_report.fetched, ttrs_report.updated
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "TTRS off-chain NFT sync failed"),
            }
        }
    }

    async fn run_contract_metadata_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            CONTRACT_RPC_METADATA_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_contract_upgrade_methods_once().await {
                Ok(upgrade_report)
                    if upgrade_report.inserted_methods > 0
                        || upgrade_report.failed_contracts > 0 =>
                {
                    info!(
                        "synced contract upgrade methods selected={} fetched={} inserted_methods={} linked_contracts={} failed={}",
                        upgrade_report.selected_upgrades,
                        upgrade_report.fetched_contracts,
                        upgrade_report.inserted_methods,
                        upgrade_report.linked_contracts,
                        upgrade_report.failed_contracts
                    )
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "contract upgrade method sync failed"),
            }
            match self.sync_contract_rpc_metadata_once().await {
                Ok(contract_report)
                    if contract_report.updated_contracts > 0
                        || contract_report.failed_contracts > 0 =>
                {
                    info!(
                        "synced contract RPC metadata selected={} fetched={} updated={} inserted_methods={} failed={}",
                        contract_report.selected_contracts,
                        contract_report.fetched_contracts,
                        contract_report.updated_contracts,
                        contract_report.inserted_methods,
                        contract_report.failed_contracts
                    )
                }
                Ok(_) => {}
                Err(error) => warn!(%error, "contract RPC metadata sync failed"),
            }
        }
    }

    async fn run_nft_metadata_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            NFT_RPC_METADATA_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_nft_rpc_metadata_once().await {
                Ok(nft_report) if nft_report.updated_nfts > 0 => info!(
                    "synced NFT RPC metadata selected={} fetched={} updated={} lag={}",
                    nft_report.selected_nfts,
                    nft_report.fetched_nfts,
                    nft_report.updated_nfts,
                    nft_report.lag
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "NFT RPC metadata sync failed"),
            }
        }
    }

    async fn run_series_metadata_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            SERIES_RPC_METADATA_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_series_rpc_metadata_once().await {
                Ok(series_report) if series_report.updated_series > 0 => info!(
                    "synced series RPC metadata selected={} fetched={} updated={} lag={}",
                    series_report.selected_series,
                    series_report.fetched_series,
                    series_report.updated_series,
                    series_report.lag
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "series RPC metadata sync failed"),
            }
        }
    }

    async fn run_failed_tx_maintenance(
        &self,
        lag: Arc<AtomicU64>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let mut ticker = interval(std::time::Duration::from_secs(
            FAILED_TX_DEBUG_SYNC_INTERVAL_SECONDS,
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = shutdown.changed() => return,
            }
            if lag.load(Ordering::Relaxed) > BALANCE_SYNC_LAG_THRESHOLD {
                continue;
            }
            match self.sync_failed_transaction_debug_comments_once().await {
                Ok(debug_report) if debug_report.updated_transactions > 0 => info!(
                    "synced failed tx debug comments selected={} updated={} lag={}",
                    debug_report.selected_transactions,
                    debug_report.updated_transactions,
                    debug_report.lag
                ),
                Ok(_) => {}
                Err(error) => warn!(%error, "failed tx debug-comment sync failed"),
            }
        }
    }
}

/// Assembles the full balance-row set for one address from the paginated
/// account endpoints: every fungible balance page, then a per-token
/// `getTokenBalance` for each token surfaced by the owned-tokens index that the
/// fungible enumeration does not carry — that is, the NFT ownership counts,
/// whose only remaining source is the single-token balance row (its embedded
/// `ids` list is ignored). Bounded by construction: pages are capped at 100
/// rows and the per-token lookups run only for tokens the address actually
/// holds, so cost scales with the address's real portfolio, never with global
/// token/NFT growth.
async fn fetch_address_balances(
    rpc: &PhantasmaSdkClient,
    chain: &ChainName,
    address: &str,
) -> Result<Vec<explorer_rpc::SdkBalanceResult>, RpcError> {
    let mut balances = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = rpc
            .get_account_fungible_tokens(address, BALANCE_PAGE_SIZE, &cursor, false)
            .await?;
        if let Some(rows) = page.result {
            balances.extend(rows);
        }
        match page.cursor {
            Some(next) if !next.is_empty() => cursor = next,
            _ => break,
        }
    }

    let fungible_symbols = balances
        .iter()
        .map(|balance| balance.symbol.clone())
        .collect::<std::collections::HashSet<_>>();

    let mut owned_tokens = Vec::new();
    let mut cursor = String::new();
    loop {
        let page = rpc
            .get_account_owned_tokens(address, BALANCE_PAGE_SIZE, &cursor, false)
            .await?;
        if let Some(rows) = page.result {
            owned_tokens.extend(rows);
        }
        match page.cursor {
            Some(next) if !next.is_empty() => cursor = next,
            _ => break,
        }
    }

    for token in owned_tokens {
        if token.symbol.is_empty() || fungible_symbols.contains(&token.symbol) {
            continue;
        }
        let row = rpc
            .get_token_balance(address, &token.symbol, chain, false)
            .await?;
        balances.push(row);
    }

    Ok(balances)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shutdown must cut a backoff short. A worker that sits out a multi-minute
    /// wait after the signal outlives the stop grace period of every orchestrator
    /// we deploy under: it is killed rather than stopped.
    #[tokio::test(start_paused = true)]
    async fn shutdown_interrupts_a_pending_backoff() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        assert!(
            shutdown_tx.send(true).is_ok(),
            "the receiver is still alive"
        );

        assert!(
            wait_or_shutdown(Duration::from_secs(300), &mut shutdown_rx).await,
            "a pending shutdown must end the wait immediately"
        );
    }

    /// Without a shutdown the wait runs to completion, so the backoff still does its
    /// job of keeping the worker off a struggling node.
    #[tokio::test(start_paused = true)]
    async fn a_backoff_without_shutdown_runs_to_completion() {
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let backoff = Duration::from_secs(300);
        let started = tokio::time::Instant::now();
        assert!(!wait_or_shutdown(backoff, &mut shutdown_rx).await);
        assert!(started.elapsed() >= backoff);

        // A zero backoff is not a wait at all — it must not park the loop on a
        // channel that may never change.
        assert!(!wait_or_shutdown(Duration::ZERO, &mut shutdown_rx).await);
    }

    /// The escalating wait is what actually keeps a struggling node alive: every
    /// consecutive failure on the same height must buy it more room, up to the cap.
    #[test]
    fn stall_backoff_doubles_per_consecutive_stall_and_caps() {
        assert_eq!(relief_stall_backoff(0), Duration::ZERO);
        assert_eq!(relief_stall_backoff(1), Duration::from_secs(30));
        assert_eq!(relief_stall_backoff(2), Duration::from_secs(60));
        assert_eq!(relief_stall_backoff(3), Duration::from_secs(120));
        assert_eq!(relief_stall_backoff(4), Duration::from_secs(240));
        // 5th doubling would be 480s; the cap takes over and never grows again.
        assert_eq!(relief_stall_backoff(5), RELIEF_STALL_BACKOFF_MAX);
        assert_eq!(relief_stall_backoff(6), RELIEF_STALL_BACKOFF_MAX);
        assert_eq!(relief_stall_backoff(u32::MAX), RELIEF_STALL_BACKOFF_MAX);
    }

    /// The first stalled fetch must switch shedding on immediately (one block per
    /// pass), and only a streak of committed blocks may restore the full fan-out.
    #[test]
    fn first_stall_sheds_load_and_the_recovery_streak_restores_it() {
        let relief = RpcReliefState::default();
        assert!(!relief.is_active());

        let (stalls, entered) = relief.register_stall(8_736_257);
        assert_eq!(stalls, 1);
        assert!(entered, "the first stall must report the mode transition");
        assert!(relief.is_active());

        // Repeating the transition must not re-report it: the log line is once per
        // episode, not once per pass.
        let (stalls, entered) = relief.register_stall(8_736_257);
        assert_eq!(stalls, 2);
        assert!(!entered);

        // Blocks trickling in one at a time: shedding holds until the streak is met.
        for _ in 0..(RELIEF_RECOVERY_COMMIT_BLOCKS - 1) {
            assert!(!relief.register_progress(1));
            assert!(relief.is_active());
        }
        assert!(relief.register_progress(1), "streak met → back to normal");
        assert!(!relief.is_active());
        assert_eq!(relief.stall_count(), 0);
    }

    /// A stall on a different height is a different problem: it must start the
    /// escalation over instead of inheriting a 5-minute wait from an earlier one.
    #[test]
    fn stall_on_a_new_height_restarts_the_escalation() {
        let relief = RpcReliefState::default();
        relief.register_stall(8_736_257);
        relief.register_stall(8_736_257);
        assert_eq!(relief.stall_count(), 2);

        let (stalls, entered) = relief.register_stall(8_736_259);
        assert_eq!(stalls, 1);
        assert!(!entered, "already shedding; only the height changed");
        assert_eq!(relief.stall_count(), 1);
    }

    /// A stall in the middle of a recovery streak must void that streak, otherwise
    /// a flapping node would be handed the full fan-out again after two lucky passes.
    #[test]
    fn a_stall_voids_the_recovery_streak() {
        let relief = RpcReliefState::default();
        relief.register_stall(100);
        assert!(!relief.register_progress(RELIEF_RECOVERY_COMMIT_BLOCKS as u64 - 1));
        relief.register_stall(100);

        // Only one block short of the threshold if the streak had survived.
        assert!(!relief.register_progress(RELIEF_RECOVERY_COMMIT_BLOCKS as u64 - 1));
        assert!(relief.is_active());
    }

    /// Idle passes at the tip commit nothing and must not count as recovery — the
    /// node is only proven healthy by actually serving blocks.
    #[test]
    fn idle_passes_do_not_end_load_shedding() {
        let relief = RpcReliefState::default();
        relief.register_stall(100);

        for _ in 0..10 {
            assert!(!relief.register_progress(0));
        }
        assert!(relief.is_active());
    }

    /// Shedding must be reserved for the node: a Postgres outage would otherwise
    /// collapse the fetch window and park RPC maintenance while the database, not
    /// the node, is the thing to fix.
    #[test]
    fn database_failures_do_not_shed_rpc_load() {
        assert!(is_database_failure(&IngestionError::Sqlx(
            sqlx::Error::PoolClosed
        )));
        assert!(!is_database_failure(&IngestionError::EmptyFetchBatch));
        assert!(!is_database_failure(&IngestionError::PayloadTooLarge {
            height: 8_736_257
        }));
    }

    /// Reaching the tip must end shedding even though no recovery streak was
    /// earned: with no blocks left to fetch the streak can never be completed, and
    /// a latched mode would keep RPC maintenance parked while the chain is quiet.
    #[test]
    fn catching_up_to_the_tip_ends_load_shedding() {
        let relief = RpcReliefState::default();
        relief.register_stall(100);
        relief.register_progress(1); // one block short of the streak
        assert!(relief.is_active());

        assert!(relief.clear(), "the first clear reports the transition");
        assert!(!relief.is_active());
        assert_eq!(relief.stall_count(), 0);
        assert!(!relief.clear(), "already normal: nothing to report");

        // A later stall starts a fresh episode at the base backoff.
        let (stalls, entered) = relief.register_stall(100);
        assert_eq!(stalls, 1);
        assert!(entered);
    }

    /// A pass that commits a whole batch clears shedding in one go: the streak is
    /// counted in blocks, not in passes.
    #[test]
    fn a_committed_batch_ends_load_shedding_in_one_pass() {
        let relief = RpcReliefState::default();
        relief.register_stall(100);

        assert!(relief.register_progress(u64::from(RELIEF_RECOVERY_COMMIT_BLOCKS)));
        assert!(!relief.is_active());
    }

    /// A pass that committed nothing fails outright, so the caller can name the
    /// blocked height itself; once anything is committed the path must report the
    /// stall instead, because only it knows where the window actually broke.
    #[test]
    fn only_a_pass_with_committed_blocks_keeps_its_prefix() {
        let node_failure = IngestionError::PayloadTooLarge { height: 8_736_257 };
        assert!(!keeps_committed_prefix(0, &node_failure));
        assert!(keeps_committed_prefix(1, &node_failure));

        // Ours, not the node's: it must stay a hard error at any progress, or a
        // Postgres outage would masquerade as a stalled block fetch and shed load.
        let database_failure = IngestionError::Sqlx(sqlx::Error::PoolClosed);
        assert!(!keeps_committed_prefix(0, &database_failure));
        assert!(!keeps_committed_prefix(500, &database_failure));
    }

    /// The relief wait exists to spare the node. A database outage that happens to
    /// follow a stall must not inherit it — otherwise the worker idles for up to
    /// five minutes per pass while the component that needs attention is Postgres.
    #[test]
    fn a_database_failure_does_not_inherit_the_relief_backoff() {
        // Same inputs, both failure kinds: 3 consecutive failures = 15 s generic,
        // and a height that has stalled 5 times = the 300 s relief cap.
        assert_eq!(
            failed_pass_backoff(3, 5, false),
            RELIEF_STALL_BACKOFF_MAX,
            "a node failure takes the longer of the two schedules"
        );
        assert_eq!(
            failed_pass_backoff(3, 5, true),
            Duration::from_secs(15),
            "a database failure keeps the plain generic escalation"
        );

        // The generic escalation still wins while the relief wait is short or absent.
        assert_eq!(failed_pass_backoff(6, 0, false), Duration::from_secs(30));
        assert_eq!(failed_pass_backoff(0, 0, false), Duration::ZERO);
    }
}
