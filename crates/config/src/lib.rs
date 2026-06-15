use explorer_domain::{ChainName, DomainError, NexusName};
use serde::Deserialize;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use url::Url;

const CONFIG_FILE_ENV: &str = "EXPLORER_CONFIG_FILE";

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub service_name: String,
    pub database: DatabaseConfig,
    pub chain: ChainConfig,
    pub rpc: RpcConfig,
    pub worker: WorkerConfig,
    pub logging: LoggingConfig,
}

impl AppConfig {
    pub fn from_env(service_name: impl Into<String>) -> Result<Self, ConfigError> {
        Self::from_file_or_env(service_name, config_file_path_from_env().as_deref())
    }

    pub fn from_file_or_env(
        service_name: impl Into<String>,
        config_file_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let service_name = service_name.into();
        let file = ExplorerConfigFile::load_optional(config_file_path)?;
        Ok(Self {
            service_name,
            database: DatabaseConfig::from_file_or_env(file.database.as_ref())?,
            chain: ChainConfig::from_file_or_env(file.chain.as_ref())?,
            rpc: RpcConfig::from_file_or_env(file.rpc.as_ref())?,
            worker: WorkerConfig::from_file_or_env(file.worker.as_ref())?,
            logging: LoggingConfig::from_file_or_env(file.logging.as_ref())?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApiConfig {
    pub service_name: String,
    pub http: HttpConfig,
    pub database: DatabaseConfig,
    pub chain: ChainConfig,
    pub logging: LoggingConfig,
}

impl ApiConfig {
    pub fn from_env(service_name: impl Into<String>) -> Result<Self, ConfigError> {
        Self::from_file_or_env(service_name, config_file_path_from_env().as_deref())
    }

    pub fn from_file_or_env(
        service_name: impl Into<String>,
        config_file_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let file = ExplorerConfigFile::load_optional(config_file_path)?;
        Ok(Self {
            service_name: service_name.into(),
            http: HttpConfig::from_file_or_env(file.http.as_ref())?,
            database: DatabaseConfig::from_file_or_env(file.database.as_ref())?,
            chain: ChainConfig::from_file_or_env(file.chain.as_ref())?,
            logging: LoggingConfig::from_file_or_env(file.logging.as_ref())?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MigrationConfig {
    pub service_name: String,
    pub database: DatabaseConfig,
    pub logging: LoggingConfig,
}

impl MigrationConfig {
    pub fn from_env(service_name: impl Into<String>) -> Result<Self, ConfigError> {
        Self::from_file_or_env(service_name, config_file_path_from_env().as_deref())
    }

    pub fn from_file_or_env(
        service_name: impl Into<String>,
        config_file_path: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let file = ExplorerConfigFile::load_optional(config_file_path)?;
        Ok(Self {
            service_name: service_name.into(),
            database: DatabaseConfig::from_file_or_env(file.database.as_ref())?,
            logging: LoggingConfig::from_file_or_env(file.logging.as_ref())?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LoggingConfig {
    pub file: Option<PathBuf>,
    pub console: bool,
}

impl LoggingConfig {
    fn from_file_or_env(file: Option<&LoggingFileConfig>) -> Result<Self, ConfigError> {
        let file_path = non_empty_env("EXPLORER_LOG_FILE")
            .map(PathBuf::from)
            .or_else(|| file.and_then(|file| file.file.clone()));

        let console = env_or_file_or_default(
            "EXPLORER_LOG_CONSOLE",
            "logging.console",
            file.and_then(|file| file.console),
            true,
        )?;

        Ok(Self {
            file: file_path,
            console,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExplorerConfigFile {
    database: Option<DatabaseFileConfig>,
    http: Option<HttpFileConfig>,
    chain: Option<ChainFileConfig>,
    rpc: Option<RpcFileConfig>,
    worker: Option<WorkerFileConfig>,
    logging: Option<LoggingFileConfig>,
}

impl ExplorerConfigFile {
    fn load_optional(path: Option<&Path>) -> Result<Self, ConfigError> {
        let Some(path) = path else {
            return Ok(Self::default());
        };

        let content = fs::read_to_string(path).map_err(|source| ConfigError::ConfigFileRead {
            path: path.to_owned(),
            source,
        })?;

        toml::from_str(&content).map_err(|source| ConfigError::ConfigFileParse {
            path: path.to_owned(),
            source,
        })
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DatabaseFileConfig {
    url: Option<String>,
    max_connections: Option<u32>,
    acquire_timeout_seconds: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct HttpFileConfig {
    bind_addr: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ChainFileConfig {
    nexus: Option<String>,
    chain: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RpcFileConfig {
    endpoints: Option<Vec<String>>,
    timeout_seconds: Option<u64>,
    max_response_bytes: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WorkerFileConfig {
    poll_seconds: Option<u64>,
    queue_capacity: Option<usize>,
    fetch_batch_size: Option<u64>,
    fetch_concurrency: Option<usize>,
    project_concurrency: Option<usize>,
    sync_mode: Option<String>,
    inter_block_delay_ms: Option<u64>,
    batch_delay_ms: Option<u64>,
    height_limit: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LoggingFileConfig {
    file: Option<PathBuf>,
    console: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
}

impl HttpConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_file_or_env(None)
    }

    fn from_file_or_env(file: Option<&HttpFileConfig>) -> Result<Self, ConfigError> {
        Ok(Self {
            bind_addr: resolve_bind_addr(file.and_then(|file| file.bind_addr.as_deref()))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout: Duration,
}

impl DatabaseConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_file_or_env(None)
    }

    fn from_file_or_env(file: Option<&DatabaseFileConfig>) -> Result<Self, ConfigError> {
        Ok(Self {
            url: required_env_or_file(
                "EXPLORER_DATABASE_URL",
                "database.url",
                file.and_then(|file| file.url.as_deref()),
            )?,
            max_connections: env_or_file_or_default(
                "EXPLORER_DATABASE_MAX_CONNECTIONS",
                "database.max_connections",
                file.and_then(|file| file.max_connections),
                16,
            )?,
            acquire_timeout: Duration::from_secs(env_or_file_or_default(
                "EXPLORER_DATABASE_ACQUIRE_TIMEOUT_SECONDS",
                "database.acquire_timeout_seconds",
                file.and_then(|file| file.acquire_timeout_seconds),
                10,
            )?),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub nexus: NexusName,
    pub chain: ChainName,
}

impl ChainConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_file_or_env(None)
    }

    fn from_file_or_env(file: Option<&ChainFileConfig>) -> Result<Self, ConfigError> {
        Ok(Self {
            nexus: NexusName::new(env_or_file_str_or_default::<String>(
                "EXPLORER_NEXUS",
                "chain.nexus",
                file.and_then(|file| file.nexus.as_deref()),
                "mainnet".to_owned(),
            )?)?,
            chain: ChainName::new(env_or_file_str_or_default::<String>(
                "EXPLORER_CHAIN",
                "chain.chain",
                file.and_then(|file| file.chain.as_deref()),
                "main".to_owned(),
            )?)?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub rpc_endpoints: Vec<Url>,
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

impl RpcConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_file_or_env(None)
    }

    fn from_file_or_env(file: Option<&RpcFileConfig>) -> Result<Self, ConfigError> {
        let rpc_endpoints = if let Some((endpoint_env, raw_endpoints)) = rpc_endpoint_env() {
            parse_rpc_endpoint_list(endpoint_env, &raw_endpoints)?
        } else if let Some(endpoints) = file.and_then(|file| file.endpoints.as_ref()) {
            parse_rpc_endpoint_values("rpc.endpoints", endpoints)?
        } else {
            return Err(ConfigError::MissingConfig {
                name: "rpc.endpoints",
            });
        };

        Ok(Self {
            rpc_endpoints,
            timeout: Duration::from_secs(env_or_file_or_default(
                "EXPLORER_RPC_TIMEOUT_SECONDS",
                "rpc.timeout_seconds",
                file.and_then(|file| file.timeout_seconds),
                30,
            )?),
            max_response_bytes: env_or_file_or_default(
                "EXPLORER_RPC_MAX_RESPONSE_BYTES",
                "rpc.max_response_bytes",
                file.and_then(|file| file.max_response_bytes),
                64 * 1024 * 1024,
            )?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub poll_interval: Duration,
    pub queue_capacity: usize,
    pub fetch_batch_size: u64,
    pub fetch_concurrency: usize,
    pub project_concurrency: usize,
    pub sync_mode: WorkerSyncMode,
    pub inter_block_delay: Duration,
    pub batch_delay: Duration,
    pub height_limit: Option<u64>,
}

impl WorkerConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_file_or_env(None)
    }

    fn from_file_or_env(file: Option<&WorkerFileConfig>) -> Result<Self, ConfigError> {
        Ok(Self {
            poll_interval: Duration::from_secs(env_or_file_or_default(
                "EXPLORER_WORKER_POLL_SECONDS",
                "worker.poll_seconds",
                file.and_then(|file| file.poll_seconds),
                5,
            )?),
            queue_capacity: env_or_file_or_default(
                "EXPLORER_WORKER_QUEUE_CAPACITY",
                "worker.queue_capacity",
                file.and_then(|file| file.queue_capacity),
                500,
            )?,
            fetch_batch_size: env_or_file_or_default(
                "EXPLORER_WORKER_FETCH_BATCH_SIZE",
                "worker.fetch_batch_size",
                file.and_then(|file| file.fetch_batch_size),
                50,
            )?,
            fetch_concurrency: env_or_file_or_default(
                "EXPLORER_WORKER_FETCH_CONCURRENCY",
                "worker.fetch_concurrency",
                file.and_then(|file| file.fetch_concurrency),
                4,
            )?,
            project_concurrency: env_or_file_or_default(
                "EXPLORER_WORKER_PROJECT_CONCURRENCY",
                "worker.project_concurrency",
                file.and_then(|file| file.project_concurrency),
                1,
            )?,
            sync_mode: worker_sync_mode_from_env_or_file(
                file.and_then(|file| file.sync_mode.as_deref()),
            )?,
            inter_block_delay: Duration::from_millis(env_or_file_or_default(
                "EXPLORER_WORKER_INTER_BLOCK_DELAY_MS",
                "worker.inter_block_delay_ms",
                file.and_then(|file| file.inter_block_delay_ms),
                0_u64,
            )?),
            batch_delay: Duration::from_millis(env_or_file_or_default(
                "EXPLORER_WORKER_BATCH_DELAY_MS",
                "worker.batch_delay_ms",
                file.and_then(|file| file.batch_delay_ms),
                0_u64,
            )?),
            height_limit: optional_env_or_file(
                "EXPLORER_WORKER_HEIGHT_LIMIT",
                "worker.height_limit",
                file.and_then(|file| file.height_limit),
            )?,
        })
    }

    pub fn effective_fetch_batch_size(&self) -> u64 {
        match self.sync_mode {
            WorkerSyncMode::Relief => 1,
            WorkerSyncMode::Sequential | WorkerSyncMode::Normal => self.fetch_batch_size,
        }
    }

    pub fn effective_fetch_concurrency(&self) -> usize {
        match self.sync_mode {
            WorkerSyncMode::Relief => 1,
            WorkerSyncMode::Sequential | WorkerSyncMode::Normal => self.fetch_concurrency.max(1),
        }
    }

    pub fn effective_project_concurrency(&self) -> usize {
        match self.sync_mode {
            WorkerSyncMode::Sequential | WorkerSyncMode::Relief => 1,
            WorkerSyncMode::Normal => self.project_concurrency.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerSyncMode {
    /// Project one block at a time for deterministic, reproducible insert order.
    Sequential,
    /// Project blocks in parallel for higher throughput; the cursor still
    /// advances strictly in height order.
    Normal,
    /// Force single-block fetch/projection windows for difficult or RPC-heavy ranges.
    Relief,
}

impl WorkerSyncMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Normal => "normal",
            Self::Relief => "relief",
        }
    }
}

impl std::fmt::Display for WorkerSyncMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for WorkerSyncMode {
    type Err = WorkerSyncModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "sequential" => Ok(Self::Sequential),
            "normal" => Ok(Self::Normal),
            "relief" | "slow" | "single" => Ok(Self::Relief),
            _ => Err(WorkerSyncModeParseError {
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Error)]
#[error("unsupported worker sync mode {value:?}; expected sequential, normal, or relief")]
pub struct WorkerSyncModeParseError {
    value: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("required environment variable {name} is missing")]
    MissingEnv { name: &'static str },
    #[error("required configuration value {name} is missing")]
    MissingConfig { name: &'static str },
    #[error("environment variable {name} has invalid value {value:?}")]
    InvalidEnv {
        name: &'static str,
        value: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("configuration value {name} has invalid value {value:?}")]
    InvalidConfig {
        name: &'static str,
        value: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("environment variable {name} must contain at least one URL")]
    EmptyUrlList { name: &'static str },
    #[error(
        "environment variable {name} contains unsupported JSON-RPC endpoint path in {value:?}; use /rpc or a node root URL"
    )]
    UnsupportedRpcEndpointPath { name: &'static str, value: String },
    #[error("failed to read config file {path}")]
    ConfigFileRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}")]
    ConfigFileParse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error(transparent)]
    Domain(#[from] DomainError),
}

fn config_file_path_from_env() -> Option<PathBuf> {
    non_empty_env(CONFIG_FILE_ENV).map(PathBuf::from)
}

fn non_empty_env(name: &'static str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn required_env_or_file(
    env_name: &'static str,
    config_name: &'static str,
    file_value: Option<&str>,
) -> Result<String, ConfigError> {
    non_empty_env(env_name)
        .or_else(|| {
            file_value
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .ok_or(ConfigError::MissingConfig { name: config_name })
}

fn env_or_file_or_default<T>(
    env_name: &'static str,
    _config_name: &'static str,
    file_value: Option<T>,
    default: T,
) -> Result<T, ConfigError>
where
    T: FromStr + ToString,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Some(value) = non_empty_env(env_name) else {
        return Ok(file_value.unwrap_or(default));
    };

    value
        .parse::<T>()
        .map_err(|source| ConfigError::InvalidEnv {
            name: env_name,
            value,
            source: Box::new(source),
        })
}

/// Resolve the HTTP bind address from env (`EXPLORER_BIND_ADDR`), the config file
/// (`http.bind_addr`), or the default `127.0.0.1:9000`. Unlike `SocketAddr`'s
/// `FromStr` (which accepts only an IP literal), this resolves a hostname such as
/// `localhost:9000` through `to_socket_addrs`, so the documented `localhost`
/// configs start as written.
fn resolve_bind_addr(file_value: Option<&str>) -> Result<SocketAddr, ConfigError> {
    use std::net::ToSocketAddrs;

    let (raw, name) = if let Some(value) = non_empty_env("EXPLORER_BIND_ADDR") {
        (value, "EXPLORER_BIND_ADDR")
    } else if let Some(value) = file_value.map(str::trim).filter(|value| !value.is_empty()) {
        (value.to_owned(), "http.bind_addr")
    } else {
        return Ok(SocketAddr::from(([127, 0, 0, 1], 9000)));
    };

    // A hostname can resolve to several addresses; take the first.
    raw.to_socket_addrs()
        .map_err(|source| ConfigError::InvalidConfig {
            name,
            value: raw.clone(),
            source: Box::new(source),
        })?
        .next()
        .ok_or_else(|| ConfigError::InvalidConfig {
            name,
            value: raw,
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "host resolved to no socket addresses",
            )),
        })
}

fn env_or_file_str_or_default<T>(
    env_name: &'static str,
    config_name: &'static str,
    file_value: Option<&str>,
    default: T,
) -> Result<T, ConfigError>
where
    T: FromStr + ToString,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    if let Some(value) = non_empty_env(env_name) {
        return value
            .parse::<T>()
            .map_err(|source| ConfigError::InvalidEnv {
                name: env_name,
                value,
                source: Box::new(source),
            });
    }

    let Some(value) = file_value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(default);
    };

    value
        .parse::<T>()
        .map_err(|source| ConfigError::InvalidConfig {
            name: config_name,
            value: value.to_owned(),
            source: Box::new(source),
        })
}

fn optional_env_or_file<T>(
    env_name: &'static str,
    _config_name: &'static str,
    file_value: Option<T>,
) -> Result<Option<T>, ConfigError>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let Some(value) = non_empty_env(env_name) else {
        return Ok(file_value);
    };

    value
        .parse::<T>()
        .map(Some)
        .map_err(|source| ConfigError::InvalidEnv {
            name: env_name,
            value,
            source: Box::new(source),
        })
}

fn worker_sync_mode_from_env_or_file(
    file_value: Option<&str>,
) -> Result<WorkerSyncMode, ConfigError> {
    if let Some(value) = non_empty_env("EXPLORER_WORKER_SYNC_MODE") {
        return value
            .parse::<WorkerSyncMode>()
            .map_err(|source| ConfigError::InvalidEnv {
                name: "EXPLORER_WORKER_SYNC_MODE",
                value,
                source: Box::new(source),
            });
    }

    let Some(value) = file_value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(WorkerSyncMode::Sequential);
    };

    value
        .parse::<WorkerSyncMode>()
        .map_err(|source| ConfigError::InvalidConfig {
            name: "worker.sync_mode",
            value: value.to_owned(),
            source: Box::new(source),
        })
}

fn parse_url_list(name: &'static str, value: &str) -> Result<Vec<Url>, ConfigError> {
    let urls = value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            Url::parse(item).map_err(|source| ConfigError::InvalidEnv {
                name,
                value: item.to_owned(),
                source: Box::new(source),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    if urls.is_empty() {
        return Err(ConfigError::EmptyUrlList { name });
    }

    Ok(urls)
}

fn rpc_endpoint_env() -> Option<(&'static str, String)> {
    const RPC_ENDPOINTS: &str = "EXPLORER_RPC_ENDPOINTS";
    const LEGACY_REST_NODES: &str = "EXPLORER_REST_NODES";

    env::var(RPC_ENDPOINTS)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| (RPC_ENDPOINTS, value))
        .or_else(|| {
            env::var(LEGACY_REST_NODES)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(|value| (LEGACY_REST_NODES, value))
        })
}

fn parse_rpc_endpoint_list(name: &'static str, value: &str) -> Result<Vec<Url>, ConfigError> {
    parse_url_list(name, value)?
        .into_iter()
        .map(|url| normalize_rpc_endpoint(name, url))
        .collect()
}

fn parse_rpc_endpoint_values(
    name: &'static str,
    values: &[String],
) -> Result<Vec<Url>, ConfigError> {
    let urls = values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|item| {
            Url::parse(item).map_err(|source| ConfigError::InvalidConfig {
                name,
                value: item.to_owned(),
                source: Box::new(source),
            })
        })
        .map(|url| url.and_then(|url| normalize_rpc_endpoint(name, url)))
        .collect::<Result<Vec<_>, _>>()?;

    if urls.is_empty() {
        return Err(ConfigError::EmptyUrlList { name });
    }

    Ok(urls)
}

fn normalize_rpc_endpoint(name: &'static str, mut url: Url) -> Result<Url, ConfigError> {
    let normalized_path = url.path().trim_end_matches('/');
    match normalized_path {
        "" => {
            url.set_path("rpc");
            Ok(url)
        }
        "/rpc" => Ok(url),
        _ => Err(ConfigError::UnsupportedRpcEndpointPath {
            name,
            value: url.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_normalizes_comma_separated_rpc_endpoints() {
        // Multiple JSON-RPC endpoints are required for production failover and
        // round-robin load distribution in the worker hot path.
        let urls = parse_rpc_endpoint_list(
            "EXPLORER_RPC_ENDPOINTS",
            "https://rpc-a.example.invalid, https://rpc-b.example.invalid/rpc",
        );

        let urls = urls.unwrap_or_default();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "https://rpc-a.example.invalid/rpc");
        assert_eq!(urls[1].as_str(), "https://rpc-b.example.invalid/rpc");
    }

    #[test]
    fn rejects_non_rpc_paths() {
        // SDK-backed RPC calls post JSON-RPC bodies to `/rpc`; accepting REST
        // paths would make configuration errors surface later as node failures.
        let urls = parse_rpc_endpoint_list(
            "EXPLORER_RPC_ENDPOINTS",
            "https://rpc-a.example.invalid/api/v1",
        );

        assert!(matches!(
            urls,
            Err(ConfigError::UnsupportedRpcEndpointPath { .. })
        ));
    }
}
