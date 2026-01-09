mod alert_manager;
mod config;
mod config_validation;
mod constants;
mod destination;
mod metrics;
mod pipeline_manager;
mod repository;
mod schema_cache;
mod store;
mod tracing_context;
mod wal_monitor;

use config::{create_pool, run_migrations, AlertSettings, ConfigDbSettings, PipelineManagerSettings, WalMonitorSettings};
use pipeline_manager::PipelineManager;
use wal_monitor::WalMonitor;
use std::error::Error;
use tokio::signal;
use tracing::info;
use figlet_rs::FIGfont;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load .env file
    dotenvy::dotenv().ok();
    
    // Try to load ASCII art font, but don't fail if it's missing
    if let Ok(font) = FIGfont::from_file("assets/fonts/Slant.flf") {
        if let Some(figure) = font.convert("ETL Stream") {
            println!("{}", figure);
        }
    }

    // Initialize tracing (structured logging)
    let _log_flusher = etl_telemetry::tracing::init_tracing("etl-stream")
        .map_err(|e| format!("Failed to initialize tracing: {}", e))?;

    // Initialize metrics (Prometheus endpoint on port 9000)
    etl_telemetry::metrics::init_metrics(Some("etl-stream"))
        .map_err(|e| format!("Failed to initialize metrics: {}", e))?;

    info!("ETL Stream starting...");
    info!("Metrics endpoint available at http://[::]:9000/metrics");

    // Load configuration database settings
    let db_settings = ConfigDbSettings::from_env()?;
    let manager_settings = PipelineManagerSettings::from_env()?;
    let wal_settings = WalMonitorSettings::from_env()?;
    let alert_settings = AlertSettings::from_env()?;

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
    let wal_monitor = WalMonitor::new(pool.clone(), wal_settings.clone(), alert_settings.clone());
    wal_monitor.start().await;
    info!(
        "WAL monitor started (warning: {}MB, danger: {}MB)",
        wal_settings.warning_wal_mb, wal_settings.danger_wal_mb
    );
    if alert_settings.is_enabled() {
        info!(
            "WAL alerts enabled (URL: {}, threshold: {} mins)",
            alert_settings.alert_wal_url.as_ref().expect("URL guaranteed by is_enabled()"),
            alert_settings.time_check_notification_mins
        );
    }

    // Create and start pipeline manager
    let manager = PipelineManager::new(pool, manager_settings.poll_interval_secs).await?;
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