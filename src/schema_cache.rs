//! Schema Cache module for caching PostgreSQL table schemas.
//!
//! This module provides a high-performance concurrent cache for table schemas,
//! table names, and column names. 
//!
//! Performance optimizations (PERF-03 & SEC-01):
//! - Uses `moka` crate for lock-free concurrent caching with built-in LRU
//! - Eliminates O(N log N) sorting inside lock for cache eviction
//! - Read operations are lock-free (no write lock for LRU timestamp updates)

use moka::future::Cache;
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

use etl::types::{TableId, TableSchema};

/// Maximum number of entries in LRU caches to prevent unbounded growth
const MAX_CACHE_ENTRIES: u64 = 10_000;

/// Default cache TTL in seconds (5 minutes)
const DEFAULT_TTL_SECS: u64 = 300;

/// Global schema cache that can be shared across multiple destinations.
/// 
/// This ensures that all pipelines from the same source share schema information.
/// 
/// PERF-03 & SEC-01 fix: Uses `moka` for high-performance concurrent caching:
/// - Lock-free reads with automatic LRU tracking
/// - No O(N log N) sorting for cache eviction (moka handles this efficiently)
/// - Automatic background cleanup of expired entries
#[derive(Clone)]
pub struct SchemaCache {
    /// Full table schemas (kept in RwLock as TableSchema doesn't need frequent updates)
    schemas: Arc<RwLock<HashMap<TableId, Arc<TableSchema>>>>,
    /// Table names cache: OID -> table name (PERF-03 & SEC-01 fix: uses moka)
    table_names: Cache<u32, String>,
    /// Column names cache: OID -> column names (PERF-03 fix: uses moka)
    column_names: Cache<u32, Vec<String>>,
    /// Source database pool for lookups
    source_pool: Option<PgPool>,
}

impl std::fmt::Debug for SchemaCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaCache")
            .field("table_names_count", &self.table_names.entry_count())
            .field("column_names_count", &self.column_names.entry_count())
            .field("has_source_pool", &self.source_pool.is_some())
            .finish()
    }
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
        let table_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(DEFAULT_TTL_SECS))
            .build();
            
        let column_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(DEFAULT_TTL_SECS))
            .build();
        
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names,
            column_names,
            source_pool: None,
        }
        // Note: No cleanup task needed - moka handles this automatically
    }

    /// Create a new schema cache with a source database pool for table name lookups
    pub fn with_pool(pool: PgPool) -> Self {
        let table_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(DEFAULT_TTL_SECS))
            .build();
            
        let column_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(DEFAULT_TTL_SECS))
            .build();
        
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names,
            column_names,
            source_pool: Some(pool),
        }
    }
    
    /// Create schema cache with custom TTL
    pub fn with_pool_and_ttl(pool: PgPool, ttl_secs: u64) -> Self {
        let table_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(ttl_secs))
            .build();
            
        let column_names = Cache::builder()
            .max_capacity(MAX_CACHE_ENTRIES)
            .time_to_live(Duration::from_secs(ttl_secs))
            .build();
        
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names,
            column_names,
            source_pool: Some(pool),
        }
    }

    /// Store a schema for a table
    pub async fn store(&self, table_id: TableId, schema: TableSchema) {
        // Store table name in moka cache (lock-free)
        self.table_names.insert(table_id.0, schema.name.to_string()).await;
        info!("Cached table name: {} -> {}", table_id.0, schema.name);
        
        // Store full schema (still uses RwLock as schemas are rarely updated)
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

    /// Get table name from cache or query the database (SEC-01 fix: lock-free)
    /// 
    /// This operation is now completely lock-free for cache hits.
    /// moka handles LRU tracking internally without requiring a write lock.
    pub async fn get_table_name(&self, oid: u32) -> String {
        // Fast path: lock-free cache lookup with automatic LRU update
        if let Some(name) = self.table_names.get(&oid).await {
            return name;
        }

        // Slow path: query database and cache result
        if let Some(pool) = &self.source_pool {
            match self.query_table_name(pool, oid).await {
                Ok(name) => {
                    // Cache the result (lock-free insert)
                    self.table_names.insert(oid, name.clone()).await;
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
        self.table_names.insert(oid, name).await;
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

        // Check column names cache (lock-free with moka)
        if let Some(names) = self.column_names.get(&table_id.0).await {
            return Some(names);
        }

        // Fetch from database if pool available
        self.fetch_column_names_from_db(table_id).await
    }

    /// Fetch column names from database for a given table OID
    /// This is called when schema is not available but we need column names for JSON output
    pub async fn fetch_column_names_from_db(&self, table_id: TableId) -> Option<Vec<String>> {
        // Check cache again (lock-free)
        if let Some(names) = self.column_names.get(&table_id.0).await {
            return Some(names);
        }

        // If no pool available, return None
        let pool = self.source_pool.as_ref()?;

        // Query pg_attribute to get column names
        match self.query_column_names(pool, table_id.0).await {
            Ok(names) => {
                info!("Fetched {} column names from database for table OID {}", names.len(), table_id.0);
                
                // Store in cache (lock-free)
                self.column_names.insert(table_id.0, names.clone()).await;
                
                // Also fetch and cache table name if not already cached
                if self.table_names.get(&table_id.0).await.is_none() {
                    let _ = self.get_table_name(table_id.0).await;
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

    /// Get all tables from a PostgreSQL publication
    /// This is used during pipeline startup to pre-initialize all Snowflake tables
    pub async fn get_publication_tables(&self, publication_name: &str) -> Result<Vec<PublicationTable>, sqlx::Error> {
        let pool = match &self.source_pool {
            Some(p) => p,
            None => {
                warn!("Cannot query publication tables: no source database pool available");
                return Ok(vec![]);
            }
        };

        let tables: Vec<(String, String, i64)> = sqlx::query_as(
            r#"
            SELECT 
                schemaname::text,
                tablename::text,
                (
                    SELECT c.oid::bigint
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = pt.schemaname AND c.relname = pt.tablename
                ) as table_oid
            FROM pg_publication_tables pt
            WHERE pubname = $1
            ORDER BY schemaname, tablename
            "#
        )
        .bind(publication_name)
        .fetch_all(pool)
        .await?;

        Ok(tables.into_iter().map(|(schema, name, oid)| PublicationTable {
            schema_name: schema,
            table_name: name,
            oid: oid as u32,
        }).collect())
    }

    /// Query column schema from database for a given table OID
    /// Returns full column information for table creation
    pub async fn query_table_schema(&self, oid: u32) -> Result<Vec<ColumnInfo>, sqlx::Error> {
        let pool = match &self.source_pool {
            Some(p) => p,
            None => return Ok(vec![]),
        };

        let columns: Vec<(String, i64, String, i32, bool, bool)> = sqlx::query_as(
            r#"
            SELECT 
                a.attname as column_name,
                a.atttypid as type_oid,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                a.atttypmod as modifier,
                NOT a.attnotnull as nullable,
                COALESCE(
                    (SELECT TRUE FROM pg_index i
                     WHERE i.indrelid = a.attrelid 
                       AND i.indisprimary 
                       AND a.attnum = ANY(i.indkey)),
                    FALSE
                ) as is_primary
            FROM pg_attribute a
            WHERE a.attrelid = $1
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            "#
        )
        .bind(oid as i64)
        .fetch_all(pool)
        .await?;

        Ok(columns.into_iter().map(|(name, type_oid, type_name, modifier, nullable, primary)| ColumnInfo {
            name,
            type_oid: type_oid as u32,
            type_name,
            modifier,
            nullable,
            primary,
        }).collect())
    }
    
    /// Get cache statistics for monitoring
    pub fn cache_stats(&self) -> (u64, u64) {
        (self.table_names.entry_count(), self.column_names.entry_count())
    }
}

/// Represents a table in a PostgreSQL publication
#[derive(Debug, Clone)]
pub struct PublicationTable {
    pub schema_name: String,
    pub table_name: String,
    pub oid: u32,
}

/// Column information for table creation
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub type_oid: u32,
    pub type_name: String,
    pub modifier: i32,
    pub nullable: bool,
    pub primary: bool,
}
