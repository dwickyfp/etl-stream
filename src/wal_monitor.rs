//! WAL Monitor module for tracking WAL size per PostgreSQL source.
//!
//! This module periodically queries each source's WAL directory size
//! and exposes it as a Prometheus metric.

use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::WalMonitorSettings;
use crate::metrics;
use crate::repository::source_repository::{Source, SourceRepository};

/// WAL Monitor that periodically checks WAL size for all sources
pub struct WalMonitor {
    config_pool: sqlx::PgPool,
    settings: WalMonitorSettings,
    running: Arc<RwLock<bool>>,
}

impl WalMonitor {
    pub fn new(config_pool: sqlx::PgPool, settings: WalMonitorSettings) -> Self {
        Self {
            config_pool,
            settings,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the WAL monitor background task
    pub async fn start(&self) {
        let mut running = self.running.write().await;
        if *running {
            warn!("WAL monitor already running");
            return;
        }
        *running = true;
        drop(running);

        info!(
            "Starting WAL monitor (poll interval: {}s, warning: {}MB, danger: {}MB)",
            self.settings.poll_interval_secs,
            self.settings.warning_wal_mb,
            self.settings.danger_wal_mb
        );

        let config_pool = self.config_pool.clone();
        let settings = self.settings.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(settings.poll_interval_secs));
            
            loop {
                interval.tick().await;
                
                // Check if still running
                if !*running.read().await {
                    break;
                }

                if let Err(e) = Self::check_all_sources(&config_pool, &settings).await {
                    error!("Error checking WAL sizes: {}", e);
                }
            }
        });
    }

    /// Check WAL size for all sources
    async fn check_all_sources(
        config_pool: &sqlx::PgPool,
        settings: &WalMonitorSettings,
    ) -> Result<(), String> {
        let sources = SourceRepository::get_all(config_pool)
            .await
            .map_err(|e| e.to_string())?;
        
        for source in sources {
            match Self::get_wal_size(&source).await {
                Ok(size_mb) => {
                    debug!("Source '{}' WAL size: {} MB", source.name, size_mb);
                    
                    // Record metric
                    metrics::pg_source_wal_size_mb(&source.name, size_mb);
                    
                    // Log warnings based on thresholds
                    if size_mb >= settings.danger_wal_mb as f64 {
                        warn!(
                            "DANGER: Source '{}' WAL size ({} MB) exceeds danger threshold ({} MB)",
                            source.name, size_mb, settings.danger_wal_mb
                        );
                    } else if size_mb >= settings.warning_wal_mb as f64 {
                        warn!(
                            "WARNING: Source '{}' WAL size ({} MB) exceeds warning threshold ({} MB)",
                            source.name, size_mb, settings.warning_wal_mb
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to get WAL size for source '{}': {}", source.name, e);
                    // Set to -1 to indicate error in metrics
                    metrics::pg_source_wal_size_mb(&source.name, -1.0);
                }
            }
        }
        
        Ok(())
    }

    /// Get WAL size for a single source in MB
    async fn get_wal_size(source: &Source) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Build connection URL for the source
        let url = match &source.pg_password {
            Some(password) => format!(
                "postgres://{}:{}@{}:{}/{}",
                source.pg_username, password, source.pg_host, source.pg_port, source.pg_database
            ),
            None => format!(
                "postgres://{}@{}:{}/{}",
                source.pg_username, source.pg_host, source.pg_port, source.pg_database
            ),
        };

        // Create a temporary pool for this query
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await?;

        // Query WAL size
        let row: (String,) = sqlx::query_as(
            "SELECT pg_size_pretty(sum(size)) AS total_wal_size FROM pg_ls_waldir()"
        )
        .fetch_one(&pool)
        .await?;

        let size_str = row.0;
        let size_mb = Self::parse_size_to_mb(&size_str)?;

        Ok(size_mb)
    }

    /// Parse size string like "1536 MB" or "1.5 GB" to MB
    fn parse_size_to_mb(size_str: &str) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let parts: Vec<&str> = size_str.trim().split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!("Invalid size format: {}", size_str).into());
        }

        let value: f64 = parts[0].parse()?;
        let unit = parts[1].to_uppercase();

        let mb = match unit.as_str() {
            "BYTES" => value / (1024.0 * 1024.0),
            "KB" => value / 1024.0,
            "MB" => value,
            "GB" => value * 1024.0,
            "TB" => value * 1024.0 * 1024.0,
            _ => return Err(format!("Unknown unit: {}", unit).into()),
        };

        Ok(mb)
    }

    /// Stop the WAL monitor
    #[allow(dead_code)]
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("WAL monitor stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size_to_mb() {
        assert!((WalMonitor::parse_size_to_mb("1536 MB").unwrap() - 1536.0).abs() < 0.001);
        assert!((WalMonitor::parse_size_to_mb("1.5 GB").unwrap() - 1536.0).abs() < 0.001);
        assert!((WalMonitor::parse_size_to_mb("1024 KB").unwrap() - 1.0).abs() < 0.001);
        assert!((WalMonitor::parse_size_to_mb("1048576 bytes").unwrap() - 1.0).abs() < 0.001);
    }
}
