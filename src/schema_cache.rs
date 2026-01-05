use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

use etl::types::{TableId, TableSchema};

/// Global schema cache that can be shared across multiple destinations
/// This ensures that all pipelines from the same source share schema information
#[derive(Debug, Clone)]
pub struct SchemaCache {
    schemas: Arc<RwLock<HashMap<TableId, Arc<TableSchema>>>>,
    table_names: Arc<RwLock<HashMap<u32, String>>>,
    source_pool: Option<PgPool>,
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl SchemaCache {
    /// Create a new empty schema cache
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
            source_pool: None,
        }
    }

    /// Create a new schema cache with a source database pool for table name lookups
    pub fn with_pool(pool: PgPool) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
            source_pool: Some(pool),
        }
    }

    /// Store a schema for a table
    pub async fn store(&self, table_id: TableId, schema: TableSchema) {
        // Store the table name from the schema
        {
            let mut names = self.table_names.write().await;
            names.insert(table_id.0, schema.name.to_string());
            info!("Cached table name: {} -> {}", table_id.0, schema.name);
        }
        
        let mut schemas = self.schemas.write().await;
        schemas.insert(table_id, Arc::new(schema));
    }

    /// Get a schema for a table
    pub async fn get(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        let schemas = self.schemas.read().await;
        schemas.get(table_id).cloned()
    }

    /// Get a read lock on all schemas
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, HashMap<TableId, Arc<TableSchema>>> {
        self.schemas.read().await
    }

    /// Get table name from cache or query the database
    pub async fn get_table_name(&self, oid: u32) -> String {
        // First, check the cache
        {
            let names = self.table_names.read().await;
            if let Some(name) = names.get(&oid) {
                return name.clone();
            }
        }

        // If not in cache and we have a pool, query the database
        if let Some(pool) = &self.source_pool {
            match self.query_table_name(pool, oid).await {
                Ok(name) => {
                    // Cache the result
                    let mut names = self.table_names.write().await;
                    names.insert(oid, name.clone());
                    info!("Queried and cached table name: {} -> {}", oid, name);
                    return name;
                }
                Err(e) => {
                    warn!("Failed to query table name for OID {}: {}", oid, e);
                }
            }
        }

        // Fallback: return the OID as string
        oid.to_string()
    }

    /// Query the database to get the table name from an OID
    async fn query_table_name(&self, pool: &PgPool, oid: u32) -> Result<String, sqlx::Error> {
        let query = format!("SELECT {}::regclass::text", oid);
        let row: (String,) = sqlx::query_as(&query)
            .fetch_one(pool)
            .await?;
        Ok(row.0)
    }

    /// Store a table name directly (for cases where we already know the name)
    pub async fn store_table_name(&self, oid: u32, name: String) {
        let mut names = self.table_names.write().await;
        names.insert(oid, name);
    }
}
