// Schema Cache unit tests

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Mock TableId for testing (matches etl crate structure)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TableId(u32);

/// Mock TableSchema for testing
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct TableSchema {
    id: TableId,
    name: String,
    column_count: usize,
}

/// Test implementation of SchemaCache (mirrors src/schema_cache.rs)
#[derive(Debug, Clone)]
struct SchemaCache {
    schemas: Arc<RwLock<HashMap<TableId, Arc<TableSchema>>>>,
    table_names: Arc<RwLock<HashMap<u32, String>>>,
}

impl SchemaCache {
    fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            table_names: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn store(&self, table_id: TableId, schema: TableSchema) {
        {
            let mut names = self.table_names.write().await;
            names.insert(table_id.0, schema.name.clone());
        }
        let mut schemas = self.schemas.write().await;
        schemas.insert(table_id, Arc::new(schema));
    }

    async fn get(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        let schemas = self.schemas.read().await;
        schemas.get(table_id).cloned()
    }

    async fn get_table_name(&self, oid: u32) -> String {
        let names = self.table_names.read().await;
        names.get(&oid).cloned().unwrap_or_else(|| oid.to_string())
    }

    async fn store_table_name(&self, oid: u32, name: String) {
        let mut names = self.table_names.write().await;
        names.insert(oid, name);
    }
}

// ============================================================================
// Schema Cache Tests
// ============================================================================

#[tokio::test]
async fn test_store_and_get_schema() {
    let cache = SchemaCache::new();
    let table_id = TableId(12345);
    let schema = TableSchema {
        id: table_id,
        name: "users".to_string(),
        column_count: 2,
    };

    cache.store(table_id, schema.clone()).await;
    let retrieved = cache.get(&table_id).await;

    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.name, "users");
    assert_eq!(retrieved.column_count, 2);
}

#[tokio::test]
async fn test_get_nonexistent_schema() {
    let cache = SchemaCache::new();
    let table_id = TableId(99999);

    let result = cache.get(&table_id).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_table_name_caching() {
    let cache = SchemaCache::new();
    let table_id = TableId(54321);
    let schema = TableSchema {
        id: table_id,
        name: "orders".to_string(),
        column_count: 0,
    };

    cache.store(table_id, schema).await;

    // Table name should be cached automatically
    let name = cache.get_table_name(54321).await;
    assert_eq!(name, "orders");
}

#[tokio::test]
async fn test_get_table_name_from_cache() {
    let cache = SchemaCache::new();
    
    // Directly store a table name
    cache.store_table_name(11111, "products".to_string()).await;

    let name = cache.get_table_name(11111).await;
    assert_eq!(name, "products");
}

#[tokio::test]
async fn test_get_table_name_fallback_to_oid() {
    let cache = SchemaCache::new();

    // No schema or name stored for this OID
    let name = cache.get_table_name(77777).await;
    assert_eq!(name, "77777");
}

#[tokio::test]
async fn test_concurrent_schema_access() {
    let cache = SchemaCache::new();
    
    // Store initial schema
    let table_id = TableId(1);
    let schema = TableSchema {
        id: table_id,
        name: "test".to_string(),
        column_count: 0,
    };
    cache.store(table_id, schema).await;

    // Spawn multiple concurrent readers
    let cache_clone1 = cache.clone();
    let cache_clone2 = cache.clone();
    let cache_clone3 = cache.clone();

    let handle1 = tokio::spawn(async move {
        for _ in 0..100 {
            let _ = cache_clone1.get(&TableId(1)).await;
        }
    });

    let handle2 = tokio::spawn(async move {
        for _ in 0..100 {
            let _ = cache_clone2.get(&TableId(1)).await;
        }
    });

    let handle3 = tokio::spawn(async move {
        for _ in 0..100 {
            let _ = cache_clone3.get_table_name(1).await;
        }
    });

    // All should complete without deadlock
    handle1.await.unwrap();
    handle2.await.unwrap();
    handle3.await.unwrap();
}

#[tokio::test]
async fn test_schema_cache_clone_shares_data() {
    let cache1 = SchemaCache::new();
    let cache2 = cache1.clone();

    let table_id = TableId(999);
    let schema = TableSchema {
        id: table_id,
        name: "shared_table".to_string(),
        column_count: 0,
    };

    // Store through cache1
    cache1.store(table_id, schema).await;

    // Should be visible through cache2
    let result = cache2.get(&table_id).await;
    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "shared_table");
}

#[tokio::test]
async fn test_multiple_schemas_stored() {
    let cache = SchemaCache::new();

    for i in 1..=5 {
        let table_id = TableId(i);
        let schema = TableSchema {
            id: table_id,
            name: format!("table_{}", i),
            column_count: 0,
        };
        cache.store(table_id, schema).await;
    }

    for i in 1..=5 {
        let result = cache.get(&TableId(i)).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, format!("table_{}", i));
    }
}

#[tokio::test]
async fn test_schema_update_overwrites() {
    let cache = SchemaCache::new();
    let table_id = TableId(123);

    // Store initial schema
    let schema1 = TableSchema {
        id: table_id,
        name: "old_name".to_string(),
        column_count: 0,
    };
    cache.store(table_id, schema1).await;

    // Update with new schema
    let schema2 = TableSchema {
        id: table_id,
        name: "new_name".to_string(),
        column_count: 1,
    };
    cache.store(table_id, schema2).await;

    let result = cache.get(&table_id).await.unwrap();
    assert_eq!(result.name, "new_name");
    assert_eq!(result.column_count, 1);
}
