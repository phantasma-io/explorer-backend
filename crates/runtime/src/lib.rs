use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::OnceLock;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::writer::MakeWriterExt;

const LOG_FILE_ENV: &str = "EXPLORER_LOG_FILE";

static LOG_FILE_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

pub fn init_tracing() {
    let log_file_path = std::env::var_os(LOG_FILE_ENV).filter(|value| !value.is_empty());
    init_tracing_with_logging(log_file_path.as_deref().map(Path::new), true);
}

pub fn init_tracing_with_log_file(log_file_path: Option<&Path>) {
    init_tracing_with_logging(log_file_path, true);
}

pub fn init_tracing_with_logging(log_file_path: Option<&Path>, console: bool) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,explorer=debug"));

    if let Some(log_file_path) = log_file_path {
        match open_log_file(log_file_path) {
            Ok(log_file) => {
                let (file_writer, guard) = tracing_appender::non_blocking(log_file);
                let _ = LOG_FILE_GUARD.set(guard);
                if console {
                    tracing_subscriber::fmt()
                        .with_env_filter(filter)
                        .with_target(false)
                        .with_thread_ids(false)
                        .compact()
                        .with_writer(std::io::stdout.and(file_writer))
                        .init();
                } else {
                    tracing_subscriber::fmt()
                        .with_env_filter(filter)
                        .with_target(false)
                        .with_thread_ids(false)
                        .compact()
                        .with_writer(file_writer)
                        .init();
                }
                return;
            }
            Err(error) => {
                eprintln!(
                    "failed to open log file {}: {error}",
                    log_file_path.display()
                );
            }
        }
    }

    if console {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_thread_ids(false)
            .compact()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .with_thread_ids(false)
            .compact()
            .with_writer(std::io::sink)
            .init();
    }
}

fn open_log_file(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    OpenOptions::new().create(true).append(true).open(path)
}

/// Resolves when the process is asked to stop: Ctrl+C (SIGINT) on any platform, or
/// SIGTERM on Unix (how docker/systemd request a graceful shutdown). Without the
/// SIGTERM arm a containerized worker/API would be hard-killed mid-work instead of
/// shutting down cleanly.
pub async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut sigterm) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
