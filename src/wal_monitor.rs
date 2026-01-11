//! WAL Monitor module for tracking WAL size per PostgreSQL source.
//!
//! This module periodically queries each source's WAL directory size
//! and exposes it as a Prometheus metric.
//!
//! Performance optimizations (PERF-01 & PERF-02):
//! - Uses `DashMap` for lock-free concurrent access to connection pools
//! - Uses atomic timestamps for LRU tracking (no write lock needed)
//! - Uses `buffer_unordered` instead of `join_all` to limit concurrent checks
//! - Implements LRU-based connection pool management to prevent connection explosion

use dashmap::DashMap;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use futures::stream::{self, StreamExt};

use crate::alert_manager::AlertManager;
use crate::config::{AlertSettings, WalMonitorSettings};
use crate::metrics;
use crate::repository::source_repository::{Source, SourceRepository};

/// Get current time as milliseconds since UNIX epoch
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Entry in the LRU connection pool cache with atomic timestamp (PERF-01 fix)
struct PoolEntry {
    pool: sqlx::PgPool,
    /// Last used timestamp in millis (atomic for lock-free updates)
    last_used_ms: AtomicU64,
}

impl PoolEntry {
    fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            last_used_ms: AtomicU64::new(now_millis()),
        }
    }

    fn touch(&self) {
        self.last_used_ms.store(now_millis(), Ordering::Relaxed);
    }

    fn last_used(&self) -> u64 {
        self.last_used_ms.load(Ordering::Relaxed)
    }
}

/// WAL Monitor that periodically checks WAL size for all sources
/// 
/// PERF-01 fix: Uses DashMap for concurrent pool access without global locks
pub struct WalMonitor {
    config_pool: sqlx::PgPool,
    settings: WalMonitorSettings,
    running: Arc<RwLock<bool>>,
    alert_manager: Option<Arc<AlertManager>>,
    /// LRU-managed connection pools with atomic timestamp tracking (PERF-01 fix)
    /// Using DashMap instead of Arc<RwLock<HashMap>> for lock-free concurrent access
    source_pools: Arc<DashMap<i32, PoolEntry>>,
}

impl WalMonitor {
    pub fn new(
        config_pool: sqlx::PgPool,
        settings: WalMonitorSettings,
        alert_settings: AlertSettings,
    ) -> Self {
        let alert_manager = AlertManager::new(alert_settings, settings.clone())
            .map(Arc::new);

        info!(
            "WalMonitor configured with max_concurrent_checks={}, max_connection_pools={}",
            settings.max_concurrent_checks,
            settings.max_connection_pools
        );

        Self {
            config_pool,
            settings,
            running: Arc::new(RwLock::new(false)),
            alert_manager,
            source_pools: Arc::new(DashMap::new()),
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
            "Starting WAL monitor (poll interval: {}s, warning: {}MB, danger: {}MB, max_concurrent: {})",
            self.settings.poll_interval_secs,
            self.settings.warning_wal_mb,
            self.settings.danger_wal_mb,
            self.settings.max_concurrent_checks
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

    /// Check WAL size for all sources with limited concurrency (PERF-02 fix)
    async fn check_all_sources(
        config_pool: &sqlx::PgPool,
        settings: &WalMonitorSettings,
        alert_manager: Option<&Arc<AlertManager>>,
        source_pools: &Arc<DashMap<i32, PoolEntry>>,
    ) -> Result<(), String> {
        // Fetch active sources
        let sources = SourceRepository::get_all(config_pool)
            .await
            .map_err(|e| e.to_string())?;
        
        // Collect active source IDs for cleanup
        let active_source_ids: Vec<i32> = sources.iter().map(|s| s.id).collect();
        
        // Cleanup pools for removed sources FIRST (before processing)
        Self::cleanup_removed_pools(source_pools, &active_source_ids).await;
        
        // Evict excess pools using LRU strategy (PERF-01 fix)
        Self::evict_lru_pools(source_pools, settings.max_connection_pools).await;
        
        // Record current pool size metric
        metrics::connection_pool_size("wal_monitor", source_pools.len());

        // Process sources with LIMITED CONCURRENCY using buffer_unordered (PERF-02 fix)
        // This replaces join_all to prevent thundering herd
        let check_stream = stream::iter(sources.into_iter().map(|source| {
            let source_pools = source_pools.clone();
            let settings = settings.clone();
            let alert_manager = alert_manager.cloned();
            
            async move {
                // Get or create pool for this source (PERF-01: lock-free with DashMap)
                let pool = match Self::get_or_create_pool(&source_pools, &source, settings.max_connection_pools).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Failed to get pool for source {}: {}", source.name, e);
                        return;
                    }
                };

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
                        if let Some(ref manager) = alert_manager {
                            manager.update_status(&source.name, size_mb).await;
                        }
                    }
                    Err(e) => {
                        error!("Failed to get WAL size for source '{}': {}", source.name, e);
                    }
                }
            }
        }));

        // Execute with limited concurrency - key optimization for PERF-02
        check_stream
            .buffer_unordered(settings.max_concurrent_checks)
            .collect::<Vec<_>>()
            .await;

        Ok(())
    }

    /// Cleanup pools for sources that no longer exist (PERF-01: lock-free with DashMap)
    async fn cleanup_removed_pools(
        source_pools: &Arc<DashMap<i32, PoolEntry>>,
        active_source_ids: &[i32],
    ) {
        let pool_ids: Vec<i32> = source_pools.iter().map(|r| *r.key()).collect();
        let mut cleaned = 0;
        
        for id in pool_ids {
            if !active_source_ids.contains(&id) {
                if let Some((_, entry)) = source_pools.remove(&id) {
                    entry.pool.close().await;
                    info!("Cleaned up connection pool for removed source_id: {}", id);
                    cleaned += 1;
                }
            }
        }
        
        if cleaned > 0 {
            metrics::connection_pool_cleanup("wal_monitor", cleaned);
        }
    }

    /// Evict least recently used pools when exceeding max limit (PERF-01 fix)
    /// Uses DashMap for lock-free access, only sorting happens with collected data
    async fn evict_lru_pools(
        source_pools: &Arc<DashMap<i32, PoolEntry>>,
        max_pools: usize,
    ) {
        let current_len = source_pools.len();
        if current_len <= max_pools {
            return;
        }
        
        // Collect entries with timestamps (lock-free iteration)
        let mut entries: Vec<(i32, u64)> = source_pools
            .iter()
            .map(|r| (*r.key(), r.value().last_used()))
            .collect();
        
        // Sort by last_used ascending (oldest first)
        entries.sort_by_key(|(_, last_used)| *last_used);
        
        // Calculate how many to evict
        let to_evict = current_len - max_pools;
        let mut evicted = 0;
        
        for (id, _) in entries.into_iter().take(to_evict) {
            if let Some((_, entry)) = source_pools.remove(&id) {
                entry.pool.close().await;
                info!("LRU evicted connection pool for source_id: {}", id);
                evicted += 1;
            }
        }
        
        if evicted > 0 {
            metrics::connection_pool_cleanup("wal_monitor_lru", evicted);
            info!("LRU evicted {} connection pools (limit: {})", evicted, max_pools);
        }
    }

    /// Get or create a connection pool for a source (PERF-01 fix: lock-free with DashMap)
    async fn get_or_create_pool(
        source_pools: &Arc<DashMap<i32, PoolEntry>>,
        source: &Source,
        max_pools: usize,
    ) -> Result<sqlx::PgPool, Box<dyn std::error::Error + Send + Sync>> {
        // Fast path: check if pool exists and update last_used atomically (lock-free)
        if let Some(entry) = source_pools.get(&source.id) {
            entry.touch(); // Atomic timestamp update - no lock needed!
            return Ok(entry.pool.clone());
        }

        // Slow path: create new pool
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

        // Create a pool for this source with minimal connections
        let pool = PgPoolOptions::new()
            .max_connections(2) // Low max connections for monitoring
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300)) // Close idle connections after 5 mins
            .connect(&url)
            .await?;

        debug!("Created new monitoring pool for source '{}' (id: {})", source.name, source.id);

        // Insert with DashMap (lock-free)
        // Use entry API for atomic check-and-insert
        let entry = source_pools.entry(source.id).or_insert_with(|| PoolEntry::new(pool.clone()));
        
        // If another task created the pool while we were connecting, use that one
        if !std::ptr::eq(&entry.pool as *const _, &pool as *const _) {
            // Another task beat us, close our pool
            pool.close().await;
        }
        
        // Proactive LRU eviction if over limit
        if source_pools.len() > max_pools {
            // Find and evict the oldest pool that isn't the one we just inserted
            let oldest = source_pools
                .iter()
                .filter(|r| *r.key() != source.id)
                .min_by_key(|r| r.value().last_used())
                .map(|r| *r.key());
            
            if let Some(oldest_id) = oldest {
                if let Some((_, old_entry)) = source_pools.remove(&oldest_id) {
                    old_entry.pool.close().await;
                    debug!("Proactively evicted pool for source_id {} to make room", oldest_id);
                }
            }
        }
        
        Ok(entry.pool.clone())
    }

    /// Get WAL size for a single source in MB using provided pool
    async fn get_wal_size(_source: &Source, pool: &sqlx::PgPool) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        // Query WAL size as raw bytes
        // pg_ls_waldir() returns size in bytes, sum it up
        // Cast to bigint to ensure we handle large sizes correctly
        let total_bytes: Option<i64> = sqlx::query_scalar(
            "SELECT sum(size)::bigint FROM pg_ls_waldir()"
        )
        .fetch_one(pool)
        .await?;

        let bytes = total_bytes.unwrap_or(0);
        
        // Convert to MB
        let size_mb = bytes as f64 / (1024.0 * 1024.0);

        Ok(size_mb)
    }

    /// Stop the WAL monitor
    #[allow(dead_code)]
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("WAL monitor stopped");
    }

    /// Cleanup connection pools for sources that no longer exist
    #[allow(dead_code)]
    pub async fn cleanup_removed_sources(&self, active_source_ids: &[i32]) {
        let pool_ids: Vec<i32> = self.source_pools.iter().map(|r| *r.key()).collect();
        
        for id in pool_ids {
            if !active_source_ids.contains(&id) {
                if let Some((_, entry)) = self.source_pools.remove(&id) {
                    entry.pool.close().await;
                    info!("Cleaned up connection pool for removed source_id: {}", id);
                }
            }
        }
    }
}
