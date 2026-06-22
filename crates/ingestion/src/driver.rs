//! Block ingestion orchestrator. Drives the worker's sync passes: ordered block
//! projection, balance sync, contract/NFT/series RPC metadata hydration, token
//! supply sync, stake-snapshot projection, and failed-tx debug recovery. The
//! `BlockIngestionDriver` struct is defined in the crate root; this module holds
//! its inherent impl.
use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Notify, watch};

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
        let window = plan_fetch_window(cursor_height, rpc_tip, &self.settings)?;

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
        let Some(window) = plan_fetch_window(cursor_height, rpc_tip, &self.settings)? else {
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
            });
        };

        // Normal mode runs the fetch/process pipeline: RPC fetch overlaps DB
        // writes (the Rust equivalent of the C# producer/consumer threads).
        // Sequential and Relief stay strictly serial — Sequential for
        // deterministic, reproducible ingestion and Relief for one-block,
        // load-shedding passes over difficult ranges. In every mode blocks are
        // written and the cursor advances in strict height order.
        let (projected_blocks, cursor_height_after) = match self.settings.sync_mode {
            WorkerSyncMode::Normal => {
                self.project_window_pipelined(&window, cursor_height)
                    .await?
            }
            WorkerSyncMode::Sequential | WorkerSyncMode::Relief => {
                self.project_window_sequentially(&window, cursor_height)
                    .await?
            }
        };

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
            projected_blocks,
            cursor_height_after: cursor_height_after.value(),
            fetch_concurrency,
        })
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
    ) -> Result<(u64, BlockHeight), IngestionError> {
        let mut projected_blocks = 0;
        let mut cursor_height_after = cursor_height;
        for height in window.from_height.value()..=window.to_height.value() {
            let (_, advanced_height) = self
                .project_raw_block_and_advance_cursor(BlockHeight::new(height))
                .await?;
            cursor_height_after = advanced_height;
            projected_blocks += 1;

            if height < window.to_height.value() && !self.settings.inter_block_delay.is_zero() {
                sleep(self.settings.inter_block_delay).await;
            }
        }

        Ok((projected_blocks, cursor_height_after))
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
    ) -> Result<(u64, BlockHeight), IngestionError> {
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
        let mut ready: BTreeMap<u64, SdkBlockResult> = BTreeMap::new();
        let mut next_to_spawn = from;
        let mut next_to_write = from;
        let mut projected_blocks = 0u64;
        let mut cursor_height_after = cursor_height;
        // First fetch error stops new fetches; the already-committed contiguous
        // prefix stays valid and the cursor reflects it.
        let mut fetch_error: Option<IngestionError> = None;

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
                tasks.spawn(async move {
                    (
                        height.value(),
                        driver.fetch_decoded_block_for_projection(height).await,
                    )
                });
                next_to_spawn += 1;
            }

            // Harvest every fetch that has already finished, without blocking, so
            // the writer can drain them in one tight batch and the freed slots
            // refill above. When the writer is the bottleneck (e.g. a low-latency
            // local node) `ready` fills toward `max_read_ahead`, so writes stay
            // batched instead of paying an await per block; when fetch is the
            // bottleneck `ready` stays near-empty and fetch overlaps the writes.
            while let Some(joined) = tasks.try_join_next() {
                Self::record_fetched(&mut ready, &mut fetch_error, joined?);
            }

            // Write every block that is now contiguous from the cursor, in order.
            // Fetch tasks keep running in the background while these writes await
            // the DB, which is what overlaps fetch with write.
            while let Some(block) = ready.remove(&next_to_write) {
                let height = BlockHeight::new(next_to_write);
                let mut transaction = self.pool.begin().await?;
                let block_record = self
                    .project_decoded_block(&mut transaction, height, &block)
                    .await?;
                cursor_height_after =
                    explorer_db::advance_cursor(&mut transaction, block_record.chain_id, height)
                        .await?;
                transaction.commit().await?;
                projected_blocks += 1;
                next_to_write += 1;

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
                Self::record_fetched(&mut ready, &mut fetch_error, joined?);
            }
        }

        if let Some(error) = fetch_error {
            // No progress at all (the next height itself failed) → surface the
            // error so the worker loop backs off. Otherwise keep the committed
            // prefix and let the next pass retry the failed height.
            if projected_blocks == 0 {
                return Err(error);
            }
            warn!(
                height = next_to_write,
                %error,
                "block fetch stalled mid-window; kept the committed prefix, retrying the failed height next pass"
            );
        }

        Ok((projected_blocks, cursor_height_after))
    }

    /// Record a completed block fetch from the pipeline: buffer a fetched block
    /// for the writer, or remember the first fetch error so the writer stops at
    /// the first missing (failed) height with a gap-free committed prefix below.
    fn record_fetched(
        ready: &mut BTreeMap<u64, SdkBlockResult>,
        fetch_error: &mut Option<IngestionError>,
        joined: (u64, Result<SdkBlockResult, IngestionError>),
    ) {
        let (height, result) = joined;
        match result {
            Ok(block) => {
                ready.insert(height, block);
            }
            Err(error) if fetch_error.is_none() => *fetch_error = Some(error),
            Err(_) => {}
        }
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
    ) -> Result<Vec<SdkAccountResult>, IngestionError> {
        if dirty_addresses.is_empty() {
            return Ok(Vec::new());
        }

        let mut accounts = Vec::new();
        for chunk in dirty_addresses.chunks(BALANCE_SYNC_CHUNK_SIZE) {
            let addresses = chunk
                .iter()
                .map(|address| address.address.clone())
                .collect::<Vec<_>>();

            match self
                .rpc
                .get_accounts_checked(&addresses, false, false)
                .await
            {
                Ok(chunk_accounts) => accounts.extend(chunk_accounts),
                Err(error) if addresses.len() > 1 => {
                    warn!(
                        %error,
                        count = addresses.len(),
                        "batch account balance fetch failed; retrying addresses one by one"
                    );
                    for address in addresses {
                        match self.rpc.get_account_checked(&address, false, false).await {
                            Ok(account) => accounts.push(account),
                            Err(error) => warn!(
                                %error,
                                address,
                                "single account balance fetch failed; keeping address dirty"
                            ),
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }

        Ok(accounts)
    }

    async fn persist_balance_accounts(
        &self,
        chain_id: i32,
        dirty_addresses: &[DirtyAddress],
        accounts: Vec<SdkAccountResult>,
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
            let Some(dirty_address) = dirty_by_address.get(account.address.as_str()) else {
                continue;
            };
            let account_upsert = account_result_to_upsert(
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
                    address = %account.address,
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
                _ = explorer_runtime::wait_for_shutdown_signal() => {
                    info!("worker shutdown signal received");
                    break;
                }
            }

            let sync_result = tokio::select! {
                result = self.sync_once() => result,
                _ = explorer_runtime::wait_for_shutdown_signal() => {
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
                    let backoff_secs = u64::from(consecutive_sync_failures.min(6)) * 5;
                    if backoff_secs > 0 {
                        sleep(std::time::Duration::from_secs(backoff_secs)).await;
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
                Ok(token_report) => info!(
                    "synced token supplies fetched={} updated={}",
                    token_report.fetched_tokens, token_report.updated_tokens
                ),
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
