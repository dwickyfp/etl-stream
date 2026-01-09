use sqlx::PgPool;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig as EtlPipelineConfig, TlsConfig, TableSyncCopyConfig};
use etl::pipeline::Pipeline;

use crate::destination::{DestinationHandler, SnowflakeDestination};
use crate::destination::snowflake_destination::SnowflakeDestinationConfig;
use crate::metrics;
use crate::repository::destination_repository::{Destination, DestinationRepository};
use crate::repository::pipeline_repository::{PipelineRepository, PipelineRow, PipelineStatus};
use crate::repository::source_repository::{Source, SourceRepository};
use crate::schema_cache::SchemaCache;
use crate::store::redis_store::RedisStore;

/// Manages multiple ETL pipelines
pub struct PipelineManager {
    pool: PgPool,
    poll_interval_secs: u64,
    running_pipelines: Arc<RwLock<HashMap<i32, RunningPipeline>>>,
    // Schema caches per source_id - all pipelines from the same source share the same schema cache
    source_schema_caches: Arc<RwLock<HashMap<i32, SchemaCache>>>,
    // Track number of active pipelines per source for cleanup
    source_pipeline_counts: Arc<RwLock<HashMap<i32, usize>>>,
    redis_store: RedisStore,
}

struct RunningPipeline {
    name: String,
    #[allow(dead_code)]
    source_id: i32,
    publication_name: String,
    known_tables: std::collections::HashSet<String>,
    handle: tokio::task::JoinHandle<()>,
}

impl PipelineManager {
    pub async fn new(pool: PgPool, poll_interval_secs: u64) -> Result<Self, Box<dyn Error>> {
        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "127.0.0.1:6379".to_string());
        let redis_url = format!("redis://{}", redis_url);
        
        info!("Initializing Redis store at {}", redis_url);
        let redis_store = RedisStore::new(&redis_url).await
            .map_err(|e| format!("Failed to connect to Redis: {}", e))?;

        Ok(Self {
            pool,
            poll_interval_secs,
            running_pipelines: Arc::new(RwLock::new(HashMap::new())),
            source_schema_caches: Arc::new(RwLock::new(HashMap::new())),
            source_pipeline_counts: Arc::new(RwLock::new(HashMap::new())),
            redis_store,
        })
    }

    /// Start the pipeline manager
    pub async fn start(&self) -> Result<(), Box<dyn Error>> {
        info!("Starting pipeline manager");

        // Initial load of active pipelines
        self.sync_pipelines().await?;

        // Start background task to poll for changes
        let pool = self.pool.clone();
        let running_pipelines = self.running_pipelines.clone();
        let source_schema_caches = self.source_schema_caches.clone();
        let source_pipeline_counts = self.source_pipeline_counts.clone();
        let poll_interval = self.poll_interval_secs;
        let redis_store = self.redis_store.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(poll_interval));
            loop {
                interval.tick().await;
                
                // Track source health
                {
                    let running = running_pipelines.read().await;
                    for rp in running.values() {
                        // For now, we use a simple reachability check or just report status
                        // In a real app, we'd ping the DB or check the pipeline's health
                        metrics::pg_source_status(&rp.name, true); 
                    }
                }

                // Check for schema changes (new tables in publication)
                // We do this in a separate block to avoid holding locks during async db calls
                let check_list: Vec<(i32, i32, String, std::collections::HashSet<String>)> = {
                    let running = running_pipelines.read().await;
                    running.iter()
                        .map(|(id, rp)| (*id, rp.source_id, rp.publication_name.clone(), rp.known_tables.clone()))
                        .collect()
                };

                for (pid, sid, pub_name, known) in check_list {
                    // Get schema cache for this source
                    let cache = {
                        let caches = source_schema_caches.read().await;
                        caches.get(&sid).cloned()
                    };

                    if let Some(cache) = cache {
                        // Fetch current tables in publication
                        info!("Checking for schema changes in publication {} for pipeline (source_id: {})...", pub_name, sid);
                        match cache.get_publication_tables(&pub_name).await {
                            Ok(current_tables) => {
                                let current_set: std::collections::HashSet<String> = current_tables
                                    .into_iter()
                                    .map(|t| format!("{}.{}", t.schema_name, t.table_name))
                                    .collect();
                                
                                info!("Current tables in publication {}: {:?}", pub_name, current_set);
                                info!("Known tables for pipeline {}: {:?}", pid, known);

                                // Check if there are any new tables
                                let new_tables: Vec<String> = current_set.difference(&known).cloned().collect();
                                let has_new = !new_tables.is_empty();
                                
                                if has_new {
                                    info!(
                                        "Detected new tables in publication {} for pipeline (source_id: {}): {:?}. Restarting pipeline.", 
                                        pub_name, sid, new_tables
                                    );
                                    
                                    // Remove from running pipelines to trigger restart in sync_pipelines_internal
                                    let mut running = running_pipelines.write().await;
                                    if let Some(rp) = running.remove(&pid) {
                                        info!("Aborting pipeline {} for restart due to schema change", rp.name);
                                        rp.handle.abort();
                                        metrics::pipeline_stopped(&rp.name);
                                        metrics::pipeline_active_dec();
                                    }
                                } else {
                                    info!("No new tables detected for pipeline {}", pid);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to check publication tables for pipeline source {}: {}", sid, e);
                            }
                        }
                    }
                }

                if let Err(e) = Self::sync_pipelines_internal(&pool, &running_pipelines, &source_schema_caches, &source_pipeline_counts, &redis_store).await {
                    error!("Error syncing pipelines: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Sync pipelines with database state
    async fn sync_pipelines(&self) -> Result<(), Box<dyn Error>> {
        Self::sync_pipelines_internal(&self.pool, &self.running_pipelines, &self.source_schema_caches, &self.source_pipeline_counts, &self.redis_store).await
    }

    async fn sync_pipelines_internal(
        pool: &PgPool,
        running_pipelines: &Arc<RwLock<HashMap<i32, RunningPipeline>>>,
        source_schema_caches: &Arc<RwLock<HashMap<i32, SchemaCache>>>,
        source_pipeline_counts: &Arc<RwLock<HashMap<i32, usize>>>,
        redis_store: &RedisStore,
    ) -> Result<(), Box<dyn Error>> {
        let all_pipelines = PipelineRepository::get_all(pool).await?;

        let mut running = running_pipelines.write().await;

        for pipeline_row in &all_pipelines {
            let status: PipelineStatus = pipeline_row.status.clone().into();
            let is_running = running.contains_key(&pipeline_row.id);

            // trace!("Syncing pipeline {}: status={:?}, is_running={}", pipeline_row.name, status, is_running);

            match (status, is_running) {
                (PipelineStatus::Start, false) => {
                    info!("Pipeline {} is marked as START but not running. Starting...", pipeline_row.name);
                    // Start this pipeline
                    if let Err(e) = Self::start_pipeline(pool, pipeline_row, &mut running, source_schema_caches, source_pipeline_counts, redis_store).await {
                        error!("Failed to start pipeline {}: {}", pipeline_row.name, e);
                        // Record pipeline error metric
                        metrics::pipeline_error(&pipeline_row.name, "start_failed");
                    }
                }
                (PipelineStatus::Pause, true) => {
                    // Stop this pipeline
                    info!("Stopping pipeline: {}", pipeline_row.name);
                    if let Some(rp) = running.remove(&pipeline_row.id) {
                        rp.handle.abort();
                        
                        // Decrement source pipeline count and cleanup if needed
                        Self::decrement_source_count(source_schema_caches, source_pipeline_counts, rp.source_id).await;
                        
                        // Record pipeline stop metric
                        metrics::pipeline_stopped(&rp.name);
                        metrics::pipeline_active_dec();
                        info!("Pipeline {} stopped", rp.name);
                    }
                }
                _ => {
                    // No change needed
                }
            }
        }

        // Remove pipelines that no longer exist in the database
        let db_ids: std::collections::HashSet<i32> =
            all_pipelines.iter().map(|p| p.id).collect();
        let running_ids: Vec<i32> = running.keys().cloned().collect();
        for id in running_ids {
            if !db_ids.contains(&id) {
                if let Some(rp) = running.remove(&id) {
                    rp.handle.abort();
                    
                    // Decrement source pipeline count and cleanup if needed
                    Self::decrement_source_count(source_schema_caches, source_pipeline_counts, rp.source_id).await;
                    
                    // Record pipeline stop metric (removed from db)
                    metrics::pipeline_stopped(&rp.name);
                    metrics::pipeline_active_dec();
                    info!("Pipeline {} removed (deleted from database)", rp.name);
                }
            }
        }

        Ok(())
    }

    async fn start_pipeline(
        pool: &PgPool,
        pipeline_row: &PipelineRow,
        running: &mut HashMap<i32, RunningPipeline>,
        source_schema_caches: &Arc<RwLock<HashMap<i32, SchemaCache>>>,
        source_pipeline_counts: &Arc<RwLock<HashMap<i32, usize>>>,
        redis_store: &RedisStore,
    ) -> Result<(), Box<dyn Error>> {
        info!("Starting pipeline: {}", pipeline_row.name);

        // Fetch source and destination
        let source = SourceRepository::get_by_id(pool, pipeline_row.source_id)
            .await?
            .ok_or_else(|| format!("Source {} not found", pipeline_row.source_id))?;

        let destination = DestinationRepository::get_by_id(pool, pipeline_row.destination_id)
            .await?
            .ok_or_else(|| format!("Destination {} not found", pipeline_row.destination_id))?;

        // Get or create schema cache for this source using entry API (atomic)
        // This prevents race conditions from double-checked locking
        let schema_cache = {
            let mut caches = source_schema_caches.write().await;
            caches.entry(pipeline_row.source_id)
                .or_insert_with(|| {
                    info!("Creating new shared schema cache for source_id {}", pipeline_row.source_id);
                    // Create a connection pool to the source database for schema lookups
                    let source_pool = futures::executor::block_on(Self::create_source_pool(&source));
                    match source_pool {
                        Ok(pool) => {
                            info!("Source pool created for schema lookups (source_id: {})", pipeline_row.source_id);
                            SchemaCache::with_pool(pool)
                        }
                        Err(e) => {
                            warn!("Failed to create source pool for schema lookups: {}. Schema auto-fetch disabled.", e);
                            SchemaCache::new()
                        }
                    }
                })
                .clone()
        };
        
        // Increment source pipeline count
        {
            let mut counts = source_pipeline_counts.write().await;
            *counts.entry(pipeline_row.source_id).or_insert(0) += 1;
        }

        // Build pipeline config
        let config = Self::build_pipeline_config(pipeline_row, &source)?;

        // Create destination handler with shared schema cache
        let dest_handler = Self::create_destination_handler(&destination, schema_cache.clone())?;

        // For Snowflake destinations, pre-initialize all tables from publication
        let DestinationHandler::Snowflake(ref sf_dest) = dest_handler;
        {
            let publication_name = source.publication_name.clone();
            match sf_dest.init_tables_from_source(&publication_name).await {
                Ok(created) => {
                    if !created.is_empty() {
                        info!(
                            "Pre-created {} Snowflake tables for pipeline {}",
                            created.len(),
                            pipeline_row.name
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to pre-initialize Snowflake tables for pipeline {}: {}. Tables will be created on first data.",
                        pipeline_row.name, e
                    );
                }
            }
        }

        // Spawn pipeline task
        let pipeline_name = pipeline_row.name.clone();
        let pipeline_name_for_error = pipeline_row.name.clone();
        let source_id = pipeline_row.source_id;
        
        // Use shared Redis store
        let store = redis_store.clone();

        let handle = tokio::spawn(async move {
            let mut pipeline = Pipeline::new(config, store, dest_handler);  

            if let Err(e) = pipeline.start().await {
                error!("Pipeline {} failed to start: {}", pipeline_name, e);
                metrics::pipeline_error(&pipeline_name, "runtime_start_failed");
                return;
            }

            if let Err(e) = pipeline.wait().await {
                error!("Pipeline {} error: {}", pipeline_name, e);
                metrics::pipeline_error(&pipeline_name, "runtime_error");
            }
        });

        let publication_name = source.publication_name.clone();
        
        // Initial fetch of tables to track changes
        // Initial fetch of tables to track changes
        let initial_tables: std::collections::HashSet<String> = match schema_cache.get_publication_tables(&publication_name).await {
            Ok(tables) => tables.into_iter().map(|t| format!("{}.{}", t.schema_name, t.table_name)).collect(),
            Err(e) => {
                warn!("Failed to fetch initial publication tables: {}", e);
                std::collections::HashSet::new()
            }
        };

        running.insert(
            pipeline_row.id,
            RunningPipeline {
                name: pipeline_row.name.clone(),
                source_id,
                publication_name,
                known_tables: initial_tables,
                handle,
            },
        );

        // Record pipeline start metrics
        metrics::pipeline_started(&pipeline_name_for_error);
        metrics::pipeline_active_inc();

        info!("Pipeline {} started successfully (source_id: {})", pipeline_row.name, source_id);
        Ok(())
    }

    fn build_pipeline_config(
        pipeline_row: &PipelineRow,
        source: &Source,
    ) -> Result<EtlPipelineConfig, Box<dyn Error>> {
        // Validate and convert integer fields with overflow protection
        let id_pipeline = pipeline_row.id_pipeline
            .try_into()
            .map_err(|_| format!("Pipeline ID {} is negative or too large", pipeline_row.id_pipeline))?;
        
        let batch_max_size = usize::try_from(pipeline_row.batch_max_size)
            .map_err(|_| format!("batch_max_size {} is invalid (must be non-negative and fit in usize)", pipeline_row.batch_max_size))?;
        
        let batch_max_fill_ms = u64::try_from(pipeline_row.batch_max_fill_ms)
            .map_err(|_| format!("batch_max_fill_ms {} is invalid (must be non-negative)", pipeline_row.batch_max_fill_ms))?;
        
        let table_error_retry_delay_ms = u64::try_from(pipeline_row.table_error_retry_delay_ms)
            .map_err(|_| format!("table_error_retry_delay_ms {} is invalid (must be non-negative)", pipeline_row.table_error_retry_delay_ms))?;
        
        let table_error_retry_max_attempts = u32::try_from(pipeline_row.table_error_retry_max_attempts)
            .map_err(|_| format!("table_error_retry_max_attempts {} is invalid (must be non-negative)", pipeline_row.table_error_retry_max_attempts))?;
        
        let max_table_sync_workers = u16::try_from(pipeline_row.max_table_sync_workers)
            .map_err(|_| format!("max_table_sync_workers {} is invalid (must be non-negative and fit in u16)", pipeline_row.max_table_sync_workers))?;
        
        let pg_port = u16::try_from(source.pg_port)
            .map_err(|_| format!("Invalid PostgreSQL port: {} (must be 0-65535)", source.pg_port))?;
        
        Ok(EtlPipelineConfig {
            id: id_pipeline,
            publication_name: source.publication_name.clone(),
            pg_connection: PgConnectionConfig {
                host: source.pg_host.clone(),
                port: pg_port,
                name: source.pg_database.clone(),
                username: source.pg_username.clone(),
                password: source.pg_password.clone().map(|p| p.into()),
                tls: TlsConfig {
                    enabled: source.pg_tls_enabled,
                    trusted_root_certs: String::new(),
                },
                keepalive: None,
            },
            batch: BatchConfig {
                max_size: batch_max_size,
                max_fill_ms: batch_max_fill_ms,
            },
            table_error_retry_delay_ms,
            table_error_retry_max_attempts,
            max_table_sync_workers,
            table_sync_copy: TableSyncCopyConfig::SkipAllTables,
        })
    }

    fn create_destination_handler(destination: &Destination, schema_cache: SchemaCache) -> Result<DestinationHandler, Box<dyn Error>> {
        match destination.destination_type.as_str() {
            "snowflake" => {
                let config = SnowflakeDestinationConfig {
                    account: destination.snowflake_account.clone().ok_or("Missing snowflake_account")?,
                    user: destination.snowflake_user.clone().ok_or("Missing snowflake_user")?,
                    database: destination.snowflake_database.clone().ok_or("Missing snowflake_database")?,
                    schema: destination.snowflake_schema.clone().ok_or("Missing snowflake_schema")?,
                    warehouse: destination.snowflake_warehouse.clone().ok_or("Missing snowflake_warehouse")?,
                    private_key_path: destination.snowflake_private_key_path.clone().ok_or("Missing snowflake_private_key_path")?,
                    private_key_passphrase: destination.snowflake_private_key_passphrase.clone(),
                    role: destination.snowflake_role.clone(),
                    landing_schema: destination.snowflake_landing_schema.clone().unwrap_or_else(|| "ETL_SCHEMA".to_string()),
                    task_schedule_minutes: destination.snowflake_task_schedule_minutes.unwrap_or(60) as u64,
                    host: destination.snowflake_host.clone(),
                };

                let sf_dest = SnowflakeDestination::new(config, schema_cache)
                    .map_err(|e| e.to_string())?;
                Ok(DestinationHandler::Snowflake(sf_dest))
            }
            other => Err(format!("Unsupported destination type: {}", other).into()),
        }
    }

    /// Create a database connection pool to the source database for schema lookups
    async fn create_source_pool(source: &Source) -> Result<PgPool, Box<dyn Error>> {
        let url = format!(
            "postgres://{}:{}@{}:{}/{}",
            source.pg_username,
            source.pg_password.as_deref().unwrap_or(""),
            source.pg_host,
            source.pg_port,
            source.pg_database
        );
        
        // Log connection attempt with redacted password
        let safe_url = format!(
            "postgres://{}:***@{}:{}/{}",
            source.pg_username,
            source.pg_host,
            source.pg_port,
            source.pg_database
        );
        info!("Creating source pool: {}", safe_url);
        
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)  // Small pool, only for schema queries
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&url)
            .await
            .map_err(|e| {
                // Don't include URL in error message to avoid password leak
                error!("Failed to connect to source database {}: {}", source.name, e);
                Box::<dyn Error>::from(format!("Database connection failed for source '{}': {}", source.name, e))
            })?;
        Ok(pool)
    }

    /// Wait for all pipelines to complete
    #[allow(dead_code)]
    pub async fn wait(&self) {
        loop {
            let running = self.running_pipelines.read().await;
            if running.is_empty() {
                break;
            }
            drop(running);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    
    /// Decrement source pipeline count and cleanup schema cache if no more pipelines
    async fn decrement_source_count(
        source_schema_caches: &Arc<RwLock<HashMap<i32, SchemaCache>>>,
        source_pipeline_counts: &Arc<RwLock<HashMap<i32, usize>>>,
        source_id: i32,
    ) {
        let should_cleanup = {
            let mut counts = source_pipeline_counts.write().await;
            if let Some(count) = counts.get_mut(&source_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&source_id);
                    true
                } else {
                    false
                }
            } else {
                false
            }
        };
        
        // Cleanup schema cache if no more pipelines for this source
        if should_cleanup {
            let mut caches = source_schema_caches.write().await;
            if let Some(_) = caches.remove(&source_id) {
                info!("Cleaned up schema cache for source_id {} (no active pipelines)", source_id);
            }
        }
    }

    /// Shutdown all pipelines
    pub async fn shutdown(&self) {
        info!("Shutting down all pipelines");
        let mut running = self.running_pipelines.write().await;
        for (id, rp) in running.drain() {
            info!("Stopping pipeline {} (id: {})", rp.name, id);
            rp.handle.abort();
            
            // Decrement source count and cleanup
            Self::decrement_source_count(&self.source_schema_caches, &self.source_pipeline_counts, rp.source_id).await;
            
            // Record stop metrics for each pipeline
            metrics::pipeline_stopped(&rp.name);
            metrics::pipeline_active_dec();
        }
    }
}
