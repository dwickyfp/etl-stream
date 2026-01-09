use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use deadpool_redis::{redis::cmd, Config, Pool, Runtime};
use tokio::sync::Mutex;
use tracing::{error, info};

use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};

use crate::metrics;

/// Redis-backed store for ETL pipeline state.
/// 
/// This store uses a hybrid approach:
/// - In-memory storage for TableSchema and TableReplicationPhase (which don't implement Serialize)
/// - Redis for persistent storage of table mappings (strings)
/// 
/// This provides Redis-backed persistence for mappings while maintaining
/// compatibility with the etl crate's non-serializable types.
/// 
/// For full persistence of state across restarts, the states are also
/// stored in Redis as debug strings for informational purposes.
#[derive(Clone)]
pub struct RedisStore {
    pool: Pool,
    key_prefix: String,
    // In-memory storage (same as CustomStore) since types don't implement Serialize
    tables: Arc<Mutex<HashMap<TableId, TableEntry>>>,
}

#[derive(Debug, Clone, Default)]
struct TableEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

impl std::fmt::Debug for RedisStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisStore")
            .field("key_prefix", &self.key_prefix)
            .finish()
    }
}

impl RedisStore {
    /// Create a new RedisStore with the given Redis URL.
    /// 
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., "redis://127.0.0.1:6379")
    pub async fn new(redis_url: &str) -> Result<Self, String> {
        Self::with_prefix(redis_url, "etl").await
    }

    /// Create a new RedisStore with a custom key prefix.
    /// 
    /// # Arguments
    /// * `redis_url` - Redis connection URL
    /// * `prefix` - Key prefix for all Redis keys (default: "etl")
    pub async fn with_prefix(redis_url: &str, prefix: &str) -> Result<Self, String> {
        info!("Creating Redis store with URL: {} and prefix: {}", redis_url, prefix);
        
        let timer = metrics::Timer::start();
        
        let cfg = Config::from_url(redis_url);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .map_err(|e| {
                metrics::redis_error("connect");
                format!("Failed to create Redis pool: {}", e)
            })?;

        // Test connection
        let mut conn = pool
            .get()
            .await
            .map_err(|e| {
                metrics::redis_error("connect");
                format!("Failed to connect to Redis: {}", e)
            })?;

        let _: String = cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                metrics::redis_error("ping");
                format!("Redis PING failed: {}", e)
            })?;

        metrics::redis_operation("connect");
        metrics::redis_operation_duration("connect", timer.elapsed_secs());

        info!("Redis store connected successfully");

        Ok(Self {
            pool,
            key_prefix: prefix.to_string(),
            tables: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    fn mapping_key(&self, table_id: &TableId) -> String {
        format!("{}:mappings:{}", self.key_prefix, table_id.0)
    }

    fn state_key(&self, table_id: &TableId) -> String {
        format!("{}:states:{}", self.key_prefix, table_id.0)
    }

    fn mappings_pattern(&self) -> String {
        format!("{}:mappings:*", self.key_prefix)
    }

    fn extract_table_id_from_key(&self, key: &str) -> Option<TableId> {
        key.rsplit(':').next()?.parse::<u32>().ok().map(TableId)
    }

    /// Persist mapping to Redis
    async fn persist_mapping_to_redis(&self, table_id: &TableId, mapping: &str) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        
        // Get connection first
        let mut conn = self.pool.get().await.map_err(|e| {
            metrics::redis_error("connect");
            etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to get Redis connection", e.to_string())
        })?;

        let key = self.mapping_key(table_id);
        let result: Result<(), _> = cmd("SET")
            .arg(&key)
            .arg(mapping)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                metrics::redis_operation("set");
                metrics::redis_operation_duration("set", timer.elapsed_secs());
                Ok(())
            }
            Err(e) => {
                metrics::redis_error("set");
                Err(etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to persist mapping to Redis", e.to_string()))
            }
        }
    }

    /// Persist state info to Redis (for debugging/monitoring)
    async fn persist_state_to_redis(&self, table_id: &TableId, state: &TableReplicationPhase) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        
        let mut conn = self.pool.get().await.map_err(|e| {
            metrics::redis_error("connect");
            etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to get Redis connection", e.to_string())
        })?;

        let key = self.state_key(table_id);
        let state_str = format!("{:?}", state);
        let result: Result<(), _> = cmd("SET")
            .arg(&key)
            .arg(&state_str)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                metrics::redis_operation("set");
                metrics::redis_operation_duration("set", timer.elapsed_secs());
                Ok(())
            }
            Err(e) => {
                metrics::redis_error("set");
                Err(etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to persist state to Redis", e.to_string()))
            }
        }
    }

    /// Delete keys from Redis
    async fn delete_from_redis(&self, table_id: &TableId) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        
        let mut conn = self.pool.get().await.map_err(|e| {
            metrics::redis_error("connect");
            etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to get Redis connection", e.to_string())
        })?;

        let mapping_key = self.mapping_key(table_id);
        let state_key = self.state_key(table_id);
        let result: Result<(), _> = cmd("DEL")
            .arg(&mapping_key)
            .arg(&state_key)
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                metrics::redis_operation("del");
                metrics::redis_operation_duration("del", timer.elapsed_secs());
                Ok(())
            }
            Err(e) => {
                metrics::redis_error("del");
                Err(etl::etl_error!(etl::error::ErrorKind::Unknown, "Failed to delete from Redis", e.to_string()))
            }
        }
    }

    /// Load mappings from Redis into memory
    async fn load_mappings_from_redis(&self) -> usize {
        let timer = metrics::Timer::start();
        let mut count = 0;
        
        if let Ok(mut conn) = self.pool.get().await {
            let pattern = self.mappings_pattern();
            let keys: Result<Vec<String>, _> = cmd("KEYS")
                .arg(&pattern)
                .query_async(&mut conn)
                .await;

            if let Ok(keys) = keys {
                metrics::redis_operation("keys");
                
                let mut tables = self.tables.lock().await;
                for key in keys {
                    let result: Result<Option<String>, _> = cmd("GET")
                        .arg(&key)
                        .query_async(&mut conn)
                        .await;
                    
                    if let Ok(Some(mapping)) = result {
                        if let Some(table_id) = self.extract_table_id_from_key(&key) {
                            tables.entry(table_id).or_default().mapping = Some(mapping);
                            count += 1;
                            metrics::redis_operation("get");
                        }
                    }
                }
            } else {
                metrics::redis_error("keys");
            }
        }
        
        metrics::redis_operation_duration("load_mappings", timer.elapsed_secs());
        count
    }
}

impl SchemaStore for RedisStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let timer = metrics::Timer::start();
        let tables = self.tables.lock().await;
        let result = tables.get(table_id).and_then(|e| e.schema.clone());
        metrics::redis_operation("get_schema");
        metrics::redis_operation_duration("get_schema", timer.elapsed_secs());
        Ok(result)
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let timer = metrics::Timer::start();
        let tables = self.tables.lock().await;
        let result = tables.values().filter_map(|e| e.schema.clone()).collect();
        metrics::redis_operation("get_schemas");
        metrics::redis_operation_duration("get_schemas", timer.elapsed_secs());
        Ok(result)
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        // Schemas are in-memory only since TableSchema doesn't implement Serialize
        let tables = self.tables.lock().await;
        Ok(tables.values().filter(|e| e.schema.is_some()).count())
    }

    async fn store_table_schema(&self, schema: TableSchema) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        let mut tables = self.tables.lock().await;
        let id = schema.id;
        tables.entry(id).or_default().schema = Some(Arc::new(schema));
        metrics::redis_operation("store_schema");
        metrics::redis_operation_duration("store_schema", timer.elapsed_secs());
        info!("Stored schema for table {}", id.0);
        Ok(())
    }
}

impl StateStore for RedisStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(&table_id).and_then(|e| e.state.clone()))
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<BTreeMap<TableId, TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
            .collect())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        // States are in-memory only
        let tables = self.tables.lock().await;
        Ok(tables.values().filter(|e| e.state.is_some()).count())
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        info!("Table {} -> {:?}", table_id.0, state);
        
        // Store in memory FIRST, then persist to Redis
        // This ensures memory state is always consistent with last successful operation
        // If Redis fails, memory stays in previous consistent state
        let previous_state = {
            let mut tables = self.tables.lock().await;
            let entry = tables.entry(table_id).or_default();
            let prev = entry.state.clone();
            entry.state = Some(state.clone());
            prev
        };
        
        // Try to persist to Redis
        if let Err(e) = self.persist_state_to_redis(&table_id, &state).await {
            // Rollback memory state on Redis failure
            error!("Failed to persist state to Redis for table {}, rolling back memory state: {}", table_id.0, e);
            let mut tables = self.tables.lock().await;
            if let Some(entry) = tables.get_mut(&table_id) {
                entry.state = previous_state;
            }
            return Err(e);
        }

        metrics::redis_operation("update_state");
        metrics::redis_operation_duration("update_state", timer.elapsed_secs());
        
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        // Get current state from memory
        let tables = self.tables.lock().await;
        let current_state = tables.get(&table_id)
            .and_then(|e| e.state.clone())
            .ok_or_else(|| etl::etl_error!(
                etl::error::ErrorKind::Unknown,
                "No state to rollback for table",
                format!("Table {} has no stored state", table_id.0)
            ))?;
        
        // For now, return current state as rollback is not fully implemented
        // In a full implementation, this would restore from a previous checkpoint
        info!("Rollback requested for table {} - returning current state", table_id.0);
        Ok(current_state)
    }

    async fn get_table_mapping(&self, table_id: &TableId) -> EtlResult<Option<String>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(table_id).and_then(|e| e.mapping.clone()))
    }

    async fn get_table_mappings(&self) -> EtlResult<HashMap<TableId, String>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .iter()
            .filter_map(|(id, e)| e.mapping.clone().map(|m| (*id, m)))
            .collect())
    }

    async fn load_table_mappings(&self) -> EtlResult<usize> {
        // Load mappings from Redis into memory
        let count = self.load_mappings_from_redis().await;
        Ok(count)
    }

    async fn store_table_mapping(
        &self,
        table_id: TableId,
        mapping: String,
    ) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        
        // Store in memory FIRST, then persist to Redis
        // This ensures memory state is always consistent
        let previous_mapping = {
            let mut tables = self.tables.lock().await;
            let entry = tables.entry(table_id).or_default();
            let prev = entry.mapping.clone();
            entry.mapping = Some(mapping.clone());
            prev
        };

        // Try to persist to Redis
        if let Err(e) = self.persist_mapping_to_redis(&table_id, &mapping).await {
            // Rollback memory state on Redis failure
            error!("Failed to persist mapping to Redis for table {}, rolling back memory state: {}", table_id.0, e);
            let mut tables = self.tables.lock().await;
            if let Some(entry) = tables.get_mut(&table_id) {
                entry.mapping = previous_mapping;
            }
            return Err(e);
        }
        
        metrics::redis_operation("store_mapping");
        metrics::redis_operation_duration("store_mapping", timer.elapsed_secs());
        
        Ok(())
    }
}

impl CleanupStore for RedisStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let timer = metrics::Timer::start();
        
        // Remove from Redis FIRST
        self.delete_from_redis(&table_id).await?;

        // Then remove from memory
        {
            let mut tables = self.tables.lock().await;
            tables.remove(&table_id);
        }
        
        metrics::redis_operation("cleanup");
        metrics::redis_operation_duration("cleanup", timer.elapsed_secs());
        
        info!("Cleaned up table {} from memory and Redis", table_id.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests require a running Redis instance
    // Run with: cargo test redis -- --ignored
    
    #[tokio::test]
    #[ignore = "requires Redis"]
    async fn test_redis_store_connection() {
        let store = RedisStore::new("redis://127.0.0.1:6379").await;
        assert!(store.is_ok());
    }
}
