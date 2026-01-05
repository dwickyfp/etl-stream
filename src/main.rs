mod config;
mod destination;
mod pipeline_manager;
mod repository;
mod schema_cache;
mod store;

use config::{create_pool, run_migrations, ConfigDbSettings, PipelineManagerSettings};
use pipeline_manager::PipelineManager;
use std::error::Error;
use tokio::signal;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Load .env file
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt::init();

    info!("ETL Stream starting...");

    // Load configuration database settings
    let db_settings = ConfigDbSettings::from_env()?;
    let manager_settings = PipelineManagerSettings::from_env();

    info!("Connecting to configuration database: {}:{}/{}", 
        db_settings.host, db_settings.port, db_settings.database);

    // Create database connection pool
    let pool = create_pool(&db_settings).await?;

    info!("Connected to configuration database");

    // Run migrations to ensure tables exist
    info!("Running database migrations...");
    run_migrations(&pool).await?;
    info!("Database migrations complete");

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