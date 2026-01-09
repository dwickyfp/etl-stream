use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{info, warn, debug};

use etl::types::{TableId, TableSchema};

/// Maximum number of entries in LRU caches to prevent unbounded growth
const MAX_CACHE_ENTRIES: usize = 10_000;

/// Cache entry with TTL tracking and LRU access time
#[derive(Debug, Clone)]
struct CacheEntry<T> {
    value: T,
    cached_at: Instant,
    last_accessed: Instant,
}

impl<T> CacheEntry<T> {
    fn new(value: T) -> Self {
        let now = Instant::now();
        Self {
            value,
            cached_at: now,
            last_accessed: now,
        }
    }
    
    fn is_expired(&self, ttl: Duration) -> bool {
        self.cached_at.elapsed() > ttl
    }
    
    fn touch(&mut self) {
        self.last_accessed = Instant::now();
    }
}

/// Global schema cache that can be shared across multiple destinations
/// This ensures that all pipelines from the same source share schema information
/// Uses LRU eviction to prevent unbounded memory growth
#[derive(Debug, Clone)]
pub struct SchemaCache {
    schemas: Arc<RwLock<HashMap<TableId, Arc<TableSchema>>>>,
    table_names: Arc<RwLock<HashMap<u32, CacheEntry<String>>>>,
    /// Lightweight column names cache for tables where we don't have full schema
    column_names: Arc<RwLock<HashMap<u32, CacheEntry<Vec<String>>>>>,
    source_pool: Option<PgPool>,
    /// TTL for cached column names (default: 5 minutes)
    cache_ttl: Duration,
    /// Maximum cache entries before LRU eviction
    max_entries: usize,
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
            cache_ttl: Duration::from_secs(300), // 5 minutes default
            max_entries: MAX_CACHE_ENTRIES,
        }
    }

    /// Create a new schema cache with a source database pool for table name lookups
    pub fn with_pool(pool: PgPool) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
            column_names: Arc::new(RwLock::new(HashMap::new())),
            source_pool: Some(pool),
            cache_ttl: Duration::from_secs(300), // 5 minutes default
            max_entries: MAX_CACHE_ENTRIES,
        }
    }
    
    /// Create schema cache with custom TTL
    pub fn with_pool_and_ttl(pool: PgPool, ttl_secs: u64) -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
            column_names: Arc::new(RwLock::new(HashMap::new())),
            source_pool: Some(pool),
            cache_ttl: Duration::from_secs(ttl_secs),
            max_entries: MAX_CACHE_ENTRIES,
        }
    }
    
    /// Cleanup expired cache entries and enforce LRU eviction if over capacity
    pub async fn cleanup_expired(&self) {
        let mut expired_count = 0;
        let mut evicted_count = 0;
        
        // Cleanup table names with LRU eviction
        {
            let mut names = self.table_names.write().await;
            let before = names.len();
            names.retain(|_, entry| !entry.is_expired(self.cache_ttl));
            expired_count += before - names.len();
            
            // LRU eviction if still over capacity
            if names.len() > self.max_entries {
                let to_evict = names.len() - self.max_entries;
                let mut entries: Vec<_> = names.iter().map(|(k, v)| (*k, v.last_accessed)).collect();
                entries.sort_by_key(|(_, accessed)| *accessed);
                
                for (key, _) in entries.into_iter().take(to_evict) {
                    names.remove(&key);
                    evicted_count += 1;
                }
            }
        }
        
        // Cleanup column names with LRU eviction
        {
            let mut cols = self.column_names.write().await;
            let before = cols.len();
            cols.retain(|_, entry| !entry.is_expired(self.cache_ttl));
            expired_count += before - cols.len();
            
            // LRU eviction if still over capacity
            if cols.len() > self.max_entries {
                let to_evict = cols.len() - self.max_entries;
                let mut entries: Vec<_> = cols.iter().map(|(k, v)| (*k, v.last_accessed)).collect();
                entries.sort_by_key(|(_, accessed)| *accessed);
                
                for (key, _) in entries.into_iter().take(to_evict) {
                    cols.remove(&key);
                    evicted_count += 1;
                }
            }
        }
        
        if expired_count > 0 {
            debug!("Cleaned up {} expired cache entries", expired_count);
        }
        if evicted_count > 0 {
            debug!("Evicted {} LRU cache entries", evicted_count);
        }
    }

    /// Store a schema for a table
    pub async fn store(&self, table_id: TableId, schema: TableSchema) {
        // Store the table name from the schema
        {
            let mut names = self.table_names.write().await;
            names.insert(table_id.0, CacheEntry::new(schema.name.to_string()));
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
        // First, check the cache and validate TTL
        {
            let mut names = self.table_names.write().await;
            if let Some(entry) = names.get_mut(&oid) {
                if !entry.is_expired(self.cache_ttl) {
                    entry.touch(); // Update LRU tracking
                    return entry.value.clone();
                }
            }
        }

        // If not in cache and we have a pool, query the database
        if let Some(pool) = &self.source_pool {
            match self.query_table_name(pool, oid).await {
                Ok(name) => {
                    // Cache the result with TTL
                    let mut names = self.table_names.write().await;
                    names.insert(oid, CacheEntry::new(name.clone()));
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
        names.insert(oid, CacheEntry::new(name));
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

        // Check column names cache with TTL validation
        {
            let col_names = self.column_names.read().await;
            if let Some(entry) = col_names.get(&table_id.0) {
                if !entry.is_expired(self.cache_ttl) {
                    return Some(entry.value.clone());
                } else {
                    debug!("Column cache expired for table OID {}", table_id.0);
                }
            }
        }

        // Fetch from database if pool available
        self.fetch_column_names_from_db(table_id).await
    }

    /// Fetch column names from database for a given table OID
    /// This is called when schema is not available but we need column names for JSON output
    pub async fn fetch_column_names_from_db(&self, table_id: TableId) -> Option<Vec<String>> {
        // 1. Check if already cached with TTL validation
        {
            let col_names = self.column_names.read().await;
            if let Some(entry) = col_names.get(&table_id.0) {
                if !entry.is_expired(self.cache_ttl) {
                    return Some(entry.value.clone());
                }
            }
        }

        // 2. If no pool available, return None
        let pool = self.source_pool.as_ref()?;

        // 3. Query pg_attribute to get column names
        match self.query_column_names(pool, table_id.0).await {
            Ok(names) => {
                info!("Fetched {} column names from database for table OID {}", names.len(), table_id.0);
                
                // Store in cache with TTL
                let mut col_names = self.column_names.write().await;
                col_names.insert(table_id.0, CacheEntry::new(names.clone()));
                
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
