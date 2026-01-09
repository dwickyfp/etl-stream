//! WAL Monitor module for tracking WAL size per PostgreSQL source.
//!
//! This module periodically queries each source's WAL directory size
//! and exposes it as a Prometheus metric.

use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::alert_manager::AlertManager;
use crate::config::{AlertSettings, WalMonitorSettings};
use crate::metrics;
use crate::repository::source_repository::{Source, SourceRepository};

/// WAL Monitor that periodically checks WAL size for all sources
pub struct WalMonitor {
    config_pool: sqlx::PgPool,
    settings: WalMonitorSettings,
    running: Arc<RwLock<bool>>,
    alert_manager: Option<Arc<AlertManager>>,
    source_pools: Arc<RwLock<HashMap<i32, sqlx::PgPool>>>,
}

impl WalMonitor {
    pub fn new(
        config_pool: sqlx::PgPool,
        settings: WalMonitorSettings,
        alert_settings: AlertSettings,
    ) -> Self {
        let alert_manager = AlertManager::new(alert_settings, settings.clone())
            .map(Arc::new);

        Self {
            config_pool,
            settings,
            running: Arc::new(RwLock::new(false)),
            alert_manager,
            source_pools: Arc::new(RwLock::new(HashMap::new())),
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
        let alert_manager = self.alert_manager.clone();
        let source_pools = self.source_pools.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(settings.poll_interval_secs));
            
            loop {
                interval.tick().await;
                
                // Check if still running
                if !*running.read().await {
                    break;
                }

                if let Err(e) = Self::check_all_sources(&config_pool, &settings, alert_manager.as_ref(), &source_pools).await {
                    error!("Error checking WAL sizes: {}", e);
                }
            }
        });
    }

    /// Check WAL size for all sources
    async fn check_all_sources(
        config_pool: &sqlx::PgPool,
        settings: &WalMonitorSettings,
        alert_manager: Option<&Arc<AlertManager>>,
        source_pools: &Arc<RwLock<HashMap<i32, sqlx::PgPool>>>,
    ) -> Result<(), String> {
        // Fetch active sources
        let sources = SourceRepository::get_all(config_pool)
            .await
            .map_err(|e| e.to_string())?;
        
        // Collect active source IDs for cleanup
        let active_source_ids: Vec<i32> = sources.iter().map(|s| s.id).collect();
        
        // Cleanup pools for removed sources FIRST (before processing)
        // This ensures we don't hold onto resources for deleted sources
        {
            let mut pools = source_pools.write().await;
            let pool_ids: Vec<i32> = pools.keys().copied().collect();
            let mut cleaned = 0;
            
            for id in pool_ids {
                if !active_source_ids.contains(&id) {
                    if let Some(pool) = pools.remove(&id) {
                        pool.close().await;
                        info!("Cleaned up connection pool for removed source_id: {}", id);
                        cleaned += 1;
                    }
                }
            }
            
            if cleaned > 0 {
                metrics::connection_pool_cleanup("wal_monitor", cleaned);
            }
            metrics::connection_pool_size("wal_monitor", pools.len());
        }
        
        // Process each source
        for source in sources {
            // Get or create pool for this source
            let pool = Self::get_or_create_pool(source_pools, &source).await
                .map_err(|e| format!("Failed to get pool for source {}: {}", source.name, e))?;

            match Self::get_wal_size(&source, &pool).await {
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

                    // Update alert manager for webhook notifications
                    if let Some(manager) = alert_manager {
                        manager.update_status(&source.name, size_mb).await;
                    }
                }
                Err(e) => {
                    error!("Failed to get WAL size for source '{}': {}", source.name, e);
                }
            }
        }

        Ok(())
    }

    /// Get or create a connection pool for a source
    async fn get_or_create_pool(
        source_pools: &Arc<RwLock<HashMap<i32, sqlx::PgPool>>>,
        source: &Source,
    ) -> Result<sqlx::PgPool, Box<dyn std::error::Error + Send + Sync>> {
        // Fast path: check if pool exists
        {
            let pools = source_pools.read().await;
            if let Some(pool) = pools.get(&source.id) {
                return Ok(pool.clone());
            }
        }

        // Slow path: create new pool and insert
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

        // Create a pool for this source
        let pool = PgPoolOptions::new()
            .max_connections(2) // Low max connections for monitoring
            .acquire_timeout(Duration::from_secs(10))
            .connect(&url)
            .await?;

        debug!("Created new monitoring pool for source '{}'", source.name);

        let mut pools = source_pools.write().await;
        pools.insert(source.id, pool.clone());
        
        Ok(pool)
    }

    /// Get WAL size for a single source in MB using provided pool
    async fn get_wal_size(_source: &Source, pool: &sqlx::PgPool) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Query WAL size
        let row: (String,) = sqlx::query_as(
            "SELECT pg_size_pretty(sum(size)) AS total_wal_size FROM pg_ls_waldir()"
        )
        .fetch_one(pool)
        .await?;

        let size_str = row.0;
        let size_mb = Self::parse_size_to_mb(&size_str)?;

        Ok(size_mb)
    }

    /// Parse size string like "1536 MB" or "1.5 GB" to MB
    /// Handles various formats and edge cases robustly
    fn parse_size_to_mb(size_str: &str) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        let trimmed = size_str.trim();
        
        // Handle empty or whitespace-only input
        if trimmed.is_empty() {
            return Err("Empty size string".into());
        }
        
        // Split by whitespace and filter empty parts
        let parts: Vec<&str> = trimmed.split_whitespace().filter(|s| !s.is_empty()).collect();
        
        if parts.is_empty() {
            return Err(format!("Invalid size format (no parts): '{}'", size_str).into());
        }
        
        if parts.len() != 2 {
            return Err(format!(
                "Invalid size format (expected 2 parts, got {}): '{}'",
                parts.len(),
                size_str
            ).into());
        }

        // Parse the numeric value with better error handling
        let value: f64 = parts[0].parse().map_err(|e| {
            format!("Failed to parse numeric value '{}': {}", parts[0], e)
        })?;
        
        // Validate the numeric value
        if !value.is_finite() || value < 0.0 {
            return Err(format!("Invalid numeric value: {} (must be finite and non-negative)", value).into());
        }
        
        // Parse unit (case-insensitive)
        let unit = parts[1].to_uppercase();

        // Convert to MB based on unit
        let mb = match unit.as_str() {
            "BYTES" | "B" => value / (1024.0 * 1024.0),
            "KB" | "K" => value / 1024.0,
            "MB" | "M" => value,
            "GB" | "G" => value * 1024.0,
            "TB" | "T" => value * 1024.0 * 1024.0,
            _ => return Err(format!("Unknown size unit: '{}' (expected BYTES, KB, MB, GB, or TB)", unit).into()),
        };
        
        // Validate result is reasonable
        if !mb.is_finite() {
            return Err(format!("Calculation resulted in invalid value for input: '{}'", size_str).into());
        }

        Ok(mb)
    }

    /// Stop the WAL monitor
    #[allow(dead_code)]
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("WAL monitor stopped");
    }

    /// Cleanup connection pools for sources that no longer exist (immediate)
    #[allow(dead_code)]
    async fn cleanup_removed_source_pools(
        source_pools: &Arc<RwLock<HashMap<i32, sqlx::PgPool>>>,
        active_source_ids: &[i32],
    ) {
        let mut pools = source_pools.write().await;
        let pool_ids: Vec<i32> = pools.keys().copied().collect();
        let mut cleaned = 0;
        
        for id in pool_ids {
            if !active_source_ids.contains(&id) {
                if let Some(pool) = pools.remove(&id) {
                    pool.close().await;
                    info!("Immediately cleaned up connection pool for removed source_id: {}", id);
                    cleaned += 1;
                }
            }
        }
        
        if cleaned > 0 {
            metrics::connection_pool_cleanup("wal_monitor", cleaned);
            metrics::connection_pool_size("wal_monitor", pools.len());
        }
    }
    
    /// Cleanup connection pools for sources that no longer exist
    #[allow(dead_code)]
    pub async fn cleanup_removed_sources(&self, active_source_ids: &[i32]) {
        let mut pools = self.source_pools.write().await;
        let current_ids: Vec<i32> = pools.keys().copied().collect();
        
        for id in current_ids {
            if !active_source_ids.contains(&id) {
                if let Some(pool) = pools.remove(&id) {
                    pool.close().await;
                    info!("Cleaned up connection pool for removed source_id: {}", id);
                }
            }
        }
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
