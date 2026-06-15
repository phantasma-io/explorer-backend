use clap::Parser;
use std::path::PathBuf;
use tracing::info;

#[derive(Debug, Parser)]
#[command(version, about = "Apply Explorer SQL migrations")]
struct Args {
    /// TOML config file. Env vars still override values from the file.
    #[arg(long, env = "EXPLORER_CONFIG_FILE")]
    config: Option<PathBuf>,
    /// Directory containing sqlx migration files.
    #[arg(long, env = "EXPLORER_MIGRATIONS_DIR")]
    migrations_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = explorer_config::MigrationConfig::from_file_or_env(
        "explorer-migrate",
        args.config.as_deref(),
    )?;
    explorer_runtime::init_tracing_with_logging(
        config.logging.file.as_deref(),
        config.logging.console,
    );
    let pool = explorer_db::connect(&config.database).await?;
    let migrations_dir = args
        .migrations_dir
        .unwrap_or_else(explorer_db::default_migrations_dir);
    let report = explorer_db::run_migrations(&pool, &migrations_dir).await?;

    info!(?report, "migrations completed");
    Ok(())
}
