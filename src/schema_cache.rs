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
    /// Lightweight column names cache for tables where we don't have full schema
    column_names: Arc<RwLock<HashMap<u32, Vec<String>>>>,
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
            column_names: Arc::new(RwLock::new(HashMap::new())),
            source_pool: None,
        }
    }

    /// Create a new schema cache with a source database pool for table name lookups
    pub fn with_pool(pool: PgPool) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
            column_names: Arc::new(RwLock::new(HashMap::new())),
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

    /// Get column names for a table, fetching from database if not cached
    /// This is a lightweight alternative to full schema when we just need column names for JSON
    pub async fn get_column_names(&self, table_id: TableId) -> Option<Vec<String>> {
        // First check if we have full schema
        {
            let schemas = self.schemas.read().await;
            if let Some(schema) = schemas.get(&table_id) {
                return Some(schema.column_schemas.iter().map(|c| c.name.clone()).collect());
            }
        }

        // Check column names cache
        {
            let col_names = self.column_names.read().await;
            if let Some(names) = col_names.get(&table_id.0) {
                return Some(names.clone());
            }
        }

        // Fetch from database if pool available
        self.fetch_column_names_from_db(table_id).await
    }

    /// Fetch column names from database for a given table OID
    /// This is called when schema is not available but we need column names for JSON output
    pub async fn fetch_column_names_from_db(&self, table_id: TableId) -> Option<Vec<String>> {
        // 1. Check if already cached (avoid redundant fetches)
        {
            let col_names = self.column_names.read().await;
            if let Some(names) = col_names.get(&table_id.0) {
                return Some(names.clone());
            }
        }

        // 2. If no pool available, return None
        let pool = self.source_pool.as_ref()?;

        // 3. Query pg_attribute to get column names
        match self.query_column_names(pool, table_id.0).await {
            Ok(names) => {
                info!("Fetched {} column names from database for table OID {}", names.len(), table_id.0);
                
                // Store in cache
                let mut col_names = self.column_names.write().await;
                col_names.insert(table_id.0, names.clone());
                
                // Also fetch and cache table name if not already cached
                {
                    let table_names = self.table_names.read().await;
                    if !table_names.contains_key(&table_id.0) {
                        drop(table_names);
                        let _ = self.get_table_name(table_id.0).await;
                    }
                }
                
                Some(names)
            }
            Err(e) => {
                warn!("Failed to fetch column names for OID {} from database: {}", table_id.0, e);
                None
            }
        }
    }

    /// Query PostgreSQL system catalogs to get column names
    async fn query_column_names(&self, pool: &PgPool, oid: u32) -> Result<Vec<String>, sqlx::Error> {
        let columns: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT 
                a.attname as column_name
            FROM pg_class c
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE c.oid = $1
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            "#
        )
        .bind(oid as i64)
        .fetch_all(pool)
        .await?;

        Ok(columns.into_iter().map(|(name,)| name).collect())
    }

    /// Get schema from cache, or fetch column names from database if not available
    pub async fn get_or_fetch(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        // First try to get full schema from cache
        if let Some(schema) = self.get(table_id).await {
            return Some(schema);
        }

        // If not in cache, try to fetch column names (we can't reconstruct full TableSchema
        // without tokio_postgres types, but we can cache column names for JSON output)
        let _ = self.fetch_column_names_from_db(*table_id).await;
        
        // Return None - caller should use get_column_names() as fallback
        None
    }
}
