use clap::Parser;
use explorer_config::ApiConfig;
use explorer_http_api::{ApiState, RateLimiter, router};
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Parser)]
#[command(version, about = "Explorer HTTP API")]
struct Args {
    /// TOML config file. Env vars still override values from the file.
    #[arg(long, env = "EXPLORER_CONFIG_FILE")]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = ApiConfig::from_file_or_env("explorer-api", args.config.as_deref())?;
    explorer_runtime::init_tracing_with_logging(
        config.logging.file.as_deref(),
        config.logging.console,
    );
    let bind_addr = config.http.bind_addr;
    let pool = explorer_db::connect(&config.database).await?;
    let rate_limiter = RateLimiter::new(&config.rate_limiting);
    let app = router(
        ApiState::new(config.service_name, pool, config.chain.chain),
        rate_limiter,
    );

    serve(bind_addr, app).await
}

async fn serve(bind_addr: SocketAddr, app: axum::Router) -> anyhow::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!(%bind_addr, "explorer API listening");

    // ConnectInfo<SocketAddr> is required for the rate limiter's per-IP fallback.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(explorer_runtime::wait_for_shutdown_signal())
    .await?;

    Ok(())
}
