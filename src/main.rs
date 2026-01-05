mod config;
mod destination;
mod metrics;
mod pipeline_manager;
mod repository;
mod schema_cache;
mod store;
mod wal_monitor;

use config::{create_pool, run_migrations, ConfigDbSettings, PipelineManagerSettings, WalMonitorSettings};
use pipeline_manager::PipelineManager;
use wal_monitor::WalMonitor;
use std::error::Error;
use tokio::signal;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load .env file
    dotenvy::dotenv().ok();

    // Initialize tracing (structured logging)
    let _log_flusher = etl_telemetry::tracing::init_tracing("etl-stream")
        .expect("Failed to initialize tracing");

    // Initialize metrics (Prometheus endpoint on port 9000)
    etl_telemetry::metrics::init_metrics(Some("etl-stream"))
        .expect("Failed to initialize metrics");

    info!("ETL Stream starting...");
    info!("Metrics endpoint available at http://[::]:9000/metrics");

    // Load configuration database settings
    let db_settings = ConfigDbSettings::from_env()?;
    let manager_settings = PipelineManagerSettings::from_env();
    let wal_settings = WalMonitorSettings::from_env();

    info!("Connecting to configuration database: {}:{}/{}", 
        db_settings.host, db_settings.port, db_settings.database);

    // Create database connection pool
    let pool = create_pool(&db_settings).await?;

    info!("Connected to configuration database");

    // Run migrations to ensure tables exist
    info!("Running database migrations...");
    run_migrations(&pool).await?;
    info!("Database migrations complete");

    // Start WAL monitor
    let wal_monitor = WalMonitor::new(pool.clone(), wal_settings.clone());
    wal_monitor.start().await;
    info!(
        "WAL monitor started (warning: {}MB, danger: {}MB)",
        wal_settings.warning_wal_mb, wal_settings.danger_wal_mb
    );

    // Create and start pipeline manager
    let manager = PipelineManager::new(pool, manager_settings.poll_interval_secs);
    manager.start().await?;

    info!("Pipeline manager started. Polling for pipeline changes every {} seconds.", 
        manager_settings.poll_interval_secs);

    // Wait for shutdown signal
    info!("Press Ctrl+C to shutdown");
    signal::ctrl_c().await?;

    info!("Shutdown signal received");
    manager.shutdown().await;

    info!("ETL Stream shutdown complete");
    Ok(())
}