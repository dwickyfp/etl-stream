use sqlx::PgPool;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig as EtlPipelineConfig, TlsConfig, TableSyncCopyConfig};
use etl::pipeline::Pipeline;

use crate::destination::DestinationHandler;
use crate::destination::http_destination::HttpDestination;
use crate::destination::snowflake_destination::{SnowflakeDestination, SnowflakeDestinationConfig};
use crate::metrics;
use crate::repository::destination_repository::{Destination, DestinationRepository, HttpDestinationConfig};
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
}

struct RunningPipeline {
    name: String,
    #[allow(dead_code)]
    source_id: i32,
    handle: tokio::task::JoinHandle<()>,
}

impl PipelineManager {
    pub fn new(pool: PgPool, poll_interval_secs: u64) -> Self {
        Self {
            pool,
            poll_interval_secs,
            running_pipelines: Arc::new(RwLock::new(HashMap::new())),
            source_schema_caches: Arc::new(RwLock::new(HashMap::new())),
        }
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
        let poll_interval = self.poll_interval_secs;

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

                if let Err(e) = Self::sync_pipelines_internal(&pool, &running_pipelines, &source_schema_caches).await {
                    error!("Error syncing pipelines: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Sync pipelines with database state
    async fn sync_pipelines(&self) -> Result<(), Box<dyn Error>> {
        Self::sync_pipelines_internal(&self.pool, &self.running_pipelines, &self.source_schema_caches).await
    }

    async fn sync_pipelines_internal(
        pool: &PgPool,
        running_pipelines: &Arc<RwLock<HashMap<i32, RunningPipeline>>>,
        source_schema_caches: &Arc<RwLock<HashMap<i32, SchemaCache>>>,
    ) -> Result<(), Box<dyn Error>> {
        let all_pipelines = PipelineRepository::get_all(pool).await?;

        let mut running = running_pipelines.write().await;

        for pipeline_row in &all_pipelines {
            let status: PipelineStatus = pipeline_row.status.clone().into();
            let is_running = running.contains_key(&pipeline_row.id);

            match (status, is_running) {
                (PipelineStatus::Start, false) => {
                    // Start this pipeline
                    if let Err(e) = Self::start_pipeline(pool, pipeline_row, &mut running, source_schema_caches).await {
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
    ) -> Result<(), Box<dyn Error>> {
        info!("Starting pipeline: {}", pipeline_row.name);

        // Fetch source and destination
        let source = SourceRepository::get_by_id(pool, pipeline_row.source_id)
            .await?
            .ok_or_else(|| format!("Source {} not found", pipeline_row.source_id))?;

        let destination = DestinationRepository::get_by_id(pool, pipeline_row.destination_id)
            .await?
            .ok_or_else(|| format!("Destination {} not found", pipeline_row.destination_id))?;

        // Get or create schema cache for this source
        // We need to create the schema cache outside the async block since or_insert doesn't support async
        let schema_cache = {
            let mut caches = source_schema_caches.write().await;
            if !caches.contains_key(&pipeline_row.source_id) {
                info!("Creating new shared schema cache for source_id {}", pipeline_row.source_id);
                // Create a connection pool to the source database for schema lookups
                let source_pool = Self::create_source_pool(&source).await;
                let cache = match source_pool {
                    Ok(pool) => {
                        info!("Source pool created for schema lookups (source_id: {})", pipeline_row.source_id);
                        SchemaCache::with_pool(pool)
                    }
                    Err(e) => {
                        warn!("Failed to create source pool for schema lookups: {}. Schema auto-fetch disabled.", e);
                        SchemaCache::new()
                    }
                };
                caches.insert(pipeline_row.source_id, cache);
            }
            caches.get(&pipeline_row.source_id).unwrap().clone()
        };

        // Build pipeline config
        let config = Self::build_pipeline_config(pipeline_row, &source)?;

        // Create destination handler with shared schema cache
        let dest_handler = Self::create_destination_handler(&destination, schema_cache)?;

        // For Snowflake destinations, pre-initialize all tables from publication
        if let DestinationHandler::Snowflake(ref sf_dest) = dest_handler {
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
        let redis_url = std::env::var("REDIS_URL")
            .unwrap_or_else(|_| "127.0.0.1:6379".to_string());
        let redis_url = format!("redis://{}", redis_url);
        
        let handle = tokio::spawn(async move {
            let store = RedisStore::new(&redis_url).await.expect("Failed to connect to Redis");
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

        running.insert(
            pipeline_row.id,
            RunningPipeline {
                name: pipeline_row.name.clone(),
                source_id,
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
        Ok(EtlPipelineConfig {
            id: pipeline_row.id_pipeline as u64,
            publication_name: source.publication_name.clone(),
            pg_connection: PgConnectionConfig {
                host: source.pg_host.clone(),
                port: source.pg_port as u16,
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
                max_size: pipeline_row.batch_max_size as usize,
                max_fill_ms: pipeline_row.batch_max_fill_ms as u64,
            },
            table_error_retry_delay_ms: pipeline_row.table_error_retry_delay_ms as u64,
            table_error_retry_max_attempts: pipeline_row.table_error_retry_max_attempts as u32,
            max_table_sync_workers: pipeline_row.max_table_sync_workers as u16,
            table_sync_copy: TableSyncCopyConfig::SkipAllTables,
        })
    }

    fn create_destination_handler(destination: &Destination, schema_cache: SchemaCache) -> Result<DestinationHandler, Box<dyn Error>> {
        match destination.destination_type.as_str() {
            "http" => {
                let config: HttpDestinationConfig = serde_json::from_value(destination.config.clone())?;
                let http_dest = HttpDestination::new(config.url, schema_cache)
                    .map_err(|e| e.to_string())?;
                Ok(DestinationHandler::Http(http_dest))
            }
            "snowflake" => {
                let config: SnowflakeDestinationConfig = serde_json::from_value(destination.config.clone())?;
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
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)  // Small pool, only for schema queries
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&url)
            .await?;
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

    /// Shutdown all pipelines
    pub async fn shutdown(&self) {
        info!("Shutting down all pipelines");
        let mut running = self.running_pipelines.write().await;
        for (id, rp) in running.drain() {
            info!("Stopping pipeline {} (id: {})", rp.name, id);
            rp.handle.abort();
            // Record stop metrics for each pipeline
            metrics::pipeline_stopped(&rp.name);
            metrics::pipeline_active_dec();
        }
    }
}
