use explorer_config::RpcConfig;
use explorer_domain::{BlockHeight, ChainName};
pub use phantasma_sdk::{
    AccountResult as SdkAccountResult, BlockResult as SdkBlockResult,
    ContractResult as SdkContractResult, CursorPaginatedResult as SdkCursorPaginatedResult,
    EventExResult as SdkEventExResult, EventResult as SdkEventResult,
    TokenDataResult as SdkTokenDataResult, TokenPropertyResult as SdkTokenPropertyResult,
    TokenResult as SdkTokenResult, TokenSeriesResult as SdkTokenSeriesResult,
    TransactionResult as SdkTransactionResult,
};
use phantasma_sdk::{PhantasmaError, PhantasmaRpc, RpcCallResult};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

/// Failover rounds per RPC call (each round tries every endpoint once). Bounds the
/// retry of transient transport blips before giving up.
const MAX_RPC_ATTEMPTS: usize = 4;
const RPC_RETRY_BASE_DELAY_MS: u64 = 250;
const RPC_RETRY_MAX_DELAY_MS: u64 = 2_000;

/// Whether an RPC error is worth retrying. Only transport-level failures (timeouts,
/// connection resets, premature EOF, 5xx — the SDK maps these to `Http`) are
/// transient; an RPC-level error (`Rpc`, e.g. "ID not found") or a decode error is
/// permanent and retrying it only wastes time and node load.
pub fn is_transient_rpc_error(error: &RpcError) -> bool {
    matches!(error, RpcError::Sdk(PhantasmaError::Http(_)))
}

#[derive(Clone)]
pub struct PhantasmaSdkClient {
    inner: Arc<SdkClientInner>,
}

struct SdkClientInner {
    endpoints: Vec<SdkEndpoint>,
    next_endpoint: AtomicUsize,
}

#[derive(Clone)]
struct SdkEndpoint {
    url: String,
    rpc: PhantasmaRpc,
}

#[derive(Debug, Clone)]
pub struct SdkPayload<T> {
    pub value: T,
    pub raw_value: Value,
    pub byte_len: usize,
    pub endpoint: String,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("at least one Phantasma JSON-RPC endpoint must be configured")]
    MissingRpcEndpoints,
    #[error("Phantasma SDK RPC call failed: {0}")]
    Sdk(#[from] PhantasmaError),
    #[error("SDK JSON-RPC result could not be decoded as {target}")]
    Json {
        target: &'static str,
        source: serde_json::Error,
    },
}

impl PhantasmaSdkClient {
    pub fn new(config: &RpcConfig) -> Result<Self, RpcError> {
        if config.rpc_endpoints.is_empty() {
            return Err(RpcError::MissingRpcEndpoints);
        }

        let endpoints = config
            .rpc_endpoints
            .iter()
            .map(|endpoint| {
                let mut rpc = PhantasmaRpc::new(endpoint.as_str())
                    .with_timeout(config.timeout)
                    .with_max_response_bytes(config.max_response_bytes);
                // Send the API key as the `X-Api-Key` header when configured, so a
                // rate-limiting node maps us to our key's tier. Absent => no header
                // (anonymous), identical to the pre-1.2.0 SDK behaviour.
                if let Some(api_key) = &config.api_key {
                    rpc = rpc.with_api_key(api_key.clone());
                }
                SdkEndpoint {
                    url: endpoint.to_string(),
                    rpc,
                }
            })
            .collect();

        Ok(Self {
            inner: Arc::new(SdkClientInner {
                endpoints,
                next_endpoint: AtomicUsize::new(0),
            }),
        })
    }

    /// Runs an RPC call with round-robin failover AND bounded retry+backoff: each
    /// attempt tries every configured endpoint once (starting at the next round-robin
    /// slot); if the whole round failed with a TRANSIENT error (a transport/timeout
    /// blip) it backs off and retries, up to `MAX_RPC_ATTEMPTS` rounds. A permanent
    /// error (an RPC-level "not found" or a decode error) stops immediately, since
    /// retrying it only wastes time. Previously the call gave up after one round with
    /// no retry — a single network blip failed the whole call (the C# client retried
    /// up to 5 times with backoff). The round-robin start advances each round, so the
    /// retries also spread across endpoints.
    async fn with_failover<T, F, Fut>(&self, rpc_call: &'static str, call: F) -> Result<T, RpcError>
    where
        F: Fn(SdkEndpoint) -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError>>,
    {
        let mut last_error: Option<RpcError> = None;
        for attempt in 0..MAX_RPC_ATTEMPTS {
            if attempt > 0 {
                let delay_ms =
                    (RPC_RETRY_BASE_DELAY_MS << (attempt - 1)).min(RPC_RETRY_MAX_DELAY_MS);
                tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            }
            match self.try_endpoints_once(rpc_call, &call).await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let transient = is_transient_rpc_error(&error);
                    last_error = Some(error);
                    if !transient {
                        break;
                    }
                    if attempt + 1 < MAX_RPC_ATTEMPTS {
                        tracing::warn!(
                            rpc_call,
                            attempt = attempt + 1,
                            "transient RPC failure across all endpoints; retrying after backoff"
                        );
                    }
                }
            }
        }
        Err(last_error.unwrap_or(RpcError::MissingRpcEndpoints))
    }

    /// One failover round: try each endpoint at most once (round-robin start) and
    /// return the first success, so one dead endpoint does not break the call while
    /// healthy ones remain.
    async fn try_endpoints_once<T, F, Fut>(
        &self,
        rpc_call: &'static str,
        call: &F,
    ) -> Result<T, RpcError>
    where
        F: Fn(SdkEndpoint) -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError>>,
    {
        let endpoints = &self.inner.endpoints;
        let start = self.inner.next_endpoint.fetch_add(1, Ordering::Relaxed);
        let mut last_error: Option<RpcError> = None;
        for offset in 0..endpoints.len() {
            // Pass an owned endpoint so the per-call future borrows nothing — that
            // keeps the future `Send` (the worker spawns these) and avoids the
            // borrow-lifetime gymnastics of handing out `&SdkEndpoint`. Cloning is
            // cheap: the SDK client is internally reference-counted.
            let endpoint = endpoints[(start + offset) % endpoints.len()].clone();
            let url = endpoint.url.clone();
            match call(endpoint).await {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if endpoints.len() > 1 {
                        tracing::warn!(
                            rpc_call,
                            endpoint = %url,
                            error = %error,
                            "RPC endpoint call failed; trying next endpoint"
                        );
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or(RpcError::MissingRpcEndpoints))
    }

    pub async fn get_block_height(&self, chain: &ChainName) -> Result<BlockHeight, RpcError> {
        let height = self
            .with_failover("get_block_height", |endpoint| async move {
                endpoint
                    .rpc
                    .get_block_height(chain.as_str())
                    .await
                    .map_err(RpcError::Sdk)
            })
            .await?;
        Ok(BlockHeight::new(height))
    }

    pub async fn get_block_by_height(
        &self,
        chain: &ChainName,
        height: BlockHeight,
    ) -> Result<SdkBlockResult, RpcError> {
        self.with_failover("get_block_by_height", |endpoint| async move {
            endpoint
                .rpc
                .get_block_by_height(chain.as_str(), height.value())
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_block_by_height_payload(
        &self,
        chain: &ChainName,
        height: BlockHeight,
    ) -> Result<SdkPayload<SdkBlockResult>, RpcError> {
        let response = self
            .with_failover("get_block_by_height_payload", |endpoint| async move {
                endpoint
                    .rpc
                    .get_block_by_height_with_raw(chain.as_str(), height.value())
                    .await
                    .map_err(RpcError::Sdk)
            })
            .await?;
        Ok(payload_from_sdk_response(response))
    }

    pub async fn get_transaction(&self, hash: &str) -> Result<SdkTransactionResult, RpcError> {
        self.with_failover("get_transaction", |endpoint| async move {
            endpoint
                .rpc
                .get_transaction(hash)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_account_checked(
        &self,
        address: &str,
        extended: bool,
        check_address_reserved_byte: bool,
    ) -> Result<SdkAccountResult, RpcError> {
        self.with_failover("getAccount", |endpoint| async move {
            endpoint
                .rpc
                .call(
                    "getAccount",
                    vec![
                        json!(address),
                        json!(extended),
                        json!(check_address_reserved_byte),
                    ],
                )
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_accounts_checked(
        &self,
        addresses: &[String],
        extended: bool,
        check_address_reserved_byte: bool,
    ) -> Result<Vec<SdkAccountResult>, RpcError> {
        let account_text = addresses.join(",");
        // Borrow as &str so the multi-attempt `Fn` closure can capture it by Copy
        // (an owned String would be moved on the first attempt and unavailable on a
        // failover retry).
        let account_text = account_text.as_str();
        self.with_failover("getAccounts", |endpoint| async move {
            endpoint
                .rpc
                .call(
                    "getAccounts",
                    vec![
                        json!(account_text),
                        json!(extended),
                        json!(check_address_reserved_byte),
                    ],
                )
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_token(
        &self,
        symbol: &str,
        extended: bool,
    ) -> Result<SdkTokenResult, RpcError> {
        self.with_failover("get_token", |endpoint| async move {
            endpoint
                .rpc
                .get_token(symbol, extended)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_tokens(&self, extended: bool) -> Result<Vec<SdkTokenResult>, RpcError> {
        self.with_failover("get_tokens", |endpoint| async move {
            endpoint
                .rpc
                .get_tokens(extended)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_contract(
        &self,
        chain: &ChainName,
        contract_name: &str,
    ) -> Result<SdkContractResult, RpcError> {
        self.with_failover("get_contract", |endpoint| async move {
            endpoint
                .rpc
                .get_contract(contract_name, chain.as_str())
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_token_series_by_id(
        &self,
        symbol: &str,
        series_id: &str,
    ) -> Result<SdkTokenSeriesResult, RpcError> {
        self.with_failover("getTokenSeriesById", |endpoint| async move {
            endpoint
                .rpc
                .call(
                    "getTokenSeriesById",
                    vec![json!(symbol), json!("0"), json!(series_id), json!("0")],
                )
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_token_series_by_ids(
        &self,
        symbol: &str,
        carbon_token_id: u64,
        series_id: &str,
        carbon_series_id: u64,
    ) -> Result<SdkTokenSeriesResult, RpcError> {
        self.with_failover("getTokenSeriesById", |endpoint| async move {
            endpoint
                .rpc
                .call(
                    "getTokenSeriesById",
                    vec![
                        json!(symbol),
                        json!(carbon_token_id.to_string()),
                        json!(series_id),
                        json!(carbon_series_id.to_string()),
                    ],
                )
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_token_series(
        &self,
        symbol: &str,
        carbon_token_id: u64,
        page_size: u32,
        cursor: &str,
    ) -> Result<SdkCursorPaginatedResult<Vec<SdkTokenSeriesResult>>, RpcError> {
        self.with_failover("get_token_series", |endpoint| async move {
            endpoint
                .rpc
                .get_token_series(symbol, carbon_token_id, page_size, cursor)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_nft(
        &self,
        symbol: &str,
        token_id: &str,
        extended: bool,
    ) -> Result<SdkTokenDataResult, RpcError> {
        self.with_failover("get_nft", |endpoint| async move {
            endpoint
                .rpc
                .get_nft(symbol, token_id, extended)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_nfts_text(
        &self,
        symbol: &str,
        token_ids: &str,
        extended: bool,
    ) -> Result<Vec<SdkTokenDataResult>, RpcError> {
        self.with_failover("get_nfts_text", |endpoint| async move {
            endpoint
                .rpc
                .get_nfts_text(symbol, token_ids, extended)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_nfts<S: AsRef<str> + Sync>(
        &self,
        symbol: &str,
        token_ids: &[S],
        extended: bool,
    ) -> Result<Vec<SdkTokenDataResult>, RpcError> {
        self.with_failover("get_nfts", |endpoint| async move {
            endpoint
                .rpc
                .get_nfts(symbol, token_ids, extended)
                .await
                .map_err(RpcError::Sdk)
        })
        .await
    }

    pub async fn get_transaction_payload(
        &self,
        hash: &str,
    ) -> Result<SdkPayload<SdkTransactionResult>, RpcError> {
        let response = self
            .with_failover("get_transaction_payload", |endpoint| async move {
                endpoint
                    .rpc
                    .get_transaction_with_raw(hash)
                    .await
                    .map_err(RpcError::Sdk)
            })
            .await?;
        Ok(payload_from_sdk_response(response))
    }

    pub fn endpoint_urls(&self) -> Vec<String> {
        self.inner
            .endpoints
            .iter()
            .map(|endpoint| endpoint.url.clone())
            .collect()
    }
}

pub fn decode_block_result(raw_value: Value) -> Result<SdkBlockResult, RpcError> {
    decode_sdk_result("BlockResult", raw_value)
}

fn payload_from_sdk_response<T>(response: RpcCallResult<T>) -> SdkPayload<T> {
    SdkPayload {
        byte_len: response.canonical_result_bytes,
        raw_value: response.raw_result,
        value: response.value,
        endpoint: response.endpoint,
    }
}

fn decode_sdk_result<T>(target: &'static str, raw_value: Value) -> Result<T, RpcError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(raw_value).map_err(|source| RpcError::Json { target, source })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_sdk_block_result_from_raw_value() {
        // Raw archival still stores the SDK JSON-RPC result, but projection
        // must go through the SDK BlockResult contract instead of ad hoc field
        // extraction in Explorer.
        let block = decode_block_result(serde_json::json!({
            "hash": "ABC",
            "previousHash": "PREV",
            "height": 42,
            "timestamp": 123456,
            "chainAddress": "PCHAIN",
            "protocol": 18,
            "validatorAddress": "PVALIDATOR",
            "reward": "0",
            "txs": [{ "hash": "TX1" }]
        }));

        assert!(matches!(
            block,
            Ok(SdkBlockResult {
                hash,
                previous_hash,
                height: 42,
                protocol: 18,
                ..
            }) if hash == "ABC" && previous_hash == "PREV"
        ));
    }

    #[test]
    fn decodes_sdk_token_series_result_from_raw_value() {
        let series = decode_sdk_result::<SdkTokenSeriesResult>(
            "TokenSeriesResult",
            serde_json::json!({
                "seriesId": "123",
                "carbonTokenId": "7",
                "carbonSeriesId": "9",
                "ownerAddress": "Powner",
                "maxMint": "100",
                "mintCount": "4",
                "currentSupply": "4",
                "maxSupply": "100",
                "burnedSupply": "0",
                "mode": "0",
                "metadata": [{ "key": "name", "value": "Series name" }]
            }),
        );

        assert!(matches!(
            series,
            Ok(SdkTokenSeriesResult {
                series_id,
                owner_address,
                metadata,
                ..
            }) if series_id == "123"
                && owner_address == "Powner"
                && metadata.first().is_some_and(|property| {
                    property.key == "name" && property.value == "Series name"
                })
        ));
    }

    #[test]
    fn decodes_sdk_token_data_result_from_raw_value() {
        let nft = decode_sdk_result::<SdkTokenDataResult>(
            "TokenDataResult",
            serde_json::json!({
                "id": "456",
                "series": "123",
                "carbonTokenId": "7",
                "carbonSeriesId": "9",
                "carbonNftAddress": "0xabc",
                "mint": "4",
                "chainName": "main",
                "ownerAddress": "Powner",
                "creatorAddress": "Pcreator",
                "ram": "ram-bytes",
                "rom": "rom-bytes",
                "status": "Transferable",
                "infusion": [{ "key": "SOUL", "value": "100000000" }],
                "properties": [{ "key": "name", "value": "NFT name" }]
            }),
        );

        assert!(matches!(
            nft,
            Ok(SdkTokenDataResult {
                id,
                series,
                creator_address,
                properties,
                ..
            }) if id == "456"
                && series == "123"
                && creator_address == "Pcreator"
                && properties.first().is_some_and(|property| {
                    property.key == "name" && property.value == "NFT name"
                })
        ));
    }
}
