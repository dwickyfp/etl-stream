// Custom Store unit tests

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Mock TableId for testing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct TableId(u32);

/// Mock TableSchema for testing
#[derive(Debug, Clone)]
struct TableSchema {
    id: TableId,
    name: String,
}

/// Mock TableReplicationPhase for testing
#[derive(Debug, Clone, PartialEq)]
enum TableReplicationPhase {
    CopySnapshot,
    Streaming,
}

/// Test implementation of table entry
#[derive(Debug, Clone, Default)]
struct TableEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

/// Test implementation of CustomStore (mirrors src/store/custom_store.rs)
#[derive(Debug, Clone)]
struct CustomStore {
    tables: Arc<Mutex<HashMap<TableId, TableEntry>>>,
}

impl CustomStore {
    fn new() -> Self {
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // SchemaStore trait methods
    async fn get_table_schema(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        let tables = self.tables.lock().await;
        tables.get(table_id).and_then(|e| e.schema.clone())
    }

    async fn get_table_schemas(&self) -> Vec<Arc<TableSchema>> {
        let tables = self.tables.lock().await;
        tables.values().filter_map(|e| e.schema.clone()).collect()
    }

    async fn store_table_schema(&self, schema: TableSchema) {
        let mut tables = self.tables.lock().await;
        let id = schema.id;
        tables.entry(id).or_default().schema = Some(Arc::new(schema));
    }

    // StateStore trait methods
    async fn get_table_replication_state(&self, table_id: TableId) -> Option<TableReplicationPhase> {
        let tables = self.tables.lock().await;
        tables.get(&table_id).and_then(|e| e.state.clone())
    }

    async fn get_table_replication_states(&self) -> BTreeMap<TableId, TableReplicationPhase> {
        let tables = self.tables.lock().await;
        tables
            .iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
            .collect()
    }

    async fn update_table_replication_state(&self, table_id: TableId, state: TableReplicationPhase) {
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().state = Some(state);
    }

    async fn get_table_mapping(&self, table_id: &TableId) -> Option<String> {
        let tables = self.tables.lock().await;
        tables.get(table_id).and_then(|e| e.mapping.clone())
    }

    async fn get_table_mappings(&self) -> HashMap<TableId, String> {
        let tables = self.tables.lock().await;
        tables
            .iter()
            .filter_map(|(id, e)| e.mapping.clone().map(|m| (*id, m)))
            .collect()
    }

    async fn store_table_mapping(&self, table_id: TableId, mapping: String) {
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().mapping = Some(mapping);
    }

    // CleanupStore trait methods
    async fn cleanup_table_state(&self, table_id: TableId) {
        let mut tables = self.tables.lock().await;
        tables.remove(&table_id);
    }
}

// ============================================================================
// SchemaStore Trait Tests
// ============================================================================

#[tokio::test]
async fn test_store_and_get_table_schema() {
    let store = CustomStore::new();
    let table_id = TableId(100);
    let schema = TableSchema {
        id: table_id,
        name: "users".to_string(),
    };

    store.store_table_schema(schema.clone()).await;
    let result = store.get_table_schema(&table_id).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "users");
}

#[tokio::test]
async fn test_get_table_schemas() {
    let store = CustomStore::new();

    store.store_table_schema(TableSchema {
        id: TableId(1),
        name: "table1".to_string(),
    }).await;
    store.store_table_schema(TableSchema {
        id: TableId(2),
        name: "table2".to_string(),
    }).await;
    store.store_table_schema(TableSchema {
        id: TableId(3),
        name: "table3".to_string(),
    }).await;

    let schemas = store.get_table_schemas().await;
    assert_eq!(schemas.len(), 3);
}

#[tokio::test]
async fn test_get_nonexistent_schema() {
    let store = CustomStore::new();
    let result = store.get_table_schema(&TableId(999)).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_schema_update_replaces_old() {
    let store = CustomStore::new();
    let table_id = TableId(50);

    store.store_table_schema(TableSchema {
        id: table_id,
        name: "old_name".to_string(),
    }).await;

    store.store_table_schema(TableSchema {
        id: table_id,
        name: "new_name".to_string(),
    }).await;

    let result = store.get_table_schema(&table_id).await.unwrap();
    assert_eq!(result.name, "new_name");
}

// ============================================================================
// StateStore Trait Tests
// ============================================================================

#[tokio::test]
async fn test_update_and_get_replication_state() {
    let store = CustomStore::new();
    let table_id = TableId(200);

    store.update_table_replication_state(table_id, TableReplicationPhase::CopySnapshot).await;
    let result = store.get_table_replication_state(table_id).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap(), TableReplicationPhase::CopySnapshot);
}

#[tokio::test]
async fn test_get_replication_states() {
    let store = CustomStore::new();

    store.update_table_replication_state(TableId(1), TableReplicationPhase::CopySnapshot).await;
    store.update_table_replication_state(TableId(2), TableReplicationPhase::Streaming).await;

    let states = store.get_table_replication_states().await;
    assert_eq!(states.len(), 2);
    assert_eq!(states.get(&TableId(1)), Some(&TableReplicationPhase::CopySnapshot));
    assert_eq!(states.get(&TableId(2)), Some(&TableReplicationPhase::Streaming));
}

#[tokio::test]
async fn test_state_transition() {
    let store = CustomStore::new();
    let table_id = TableId(300);

    // Initial state
    store.update_table_replication_state(table_id, TableReplicationPhase::CopySnapshot).await;
    assert_eq!(
        store.get_table_replication_state(table_id).await,
        Some(TableReplicationPhase::CopySnapshot)
    );

    // Transition to streaming
    store.update_table_replication_state(table_id, TableReplicationPhase::Streaming).await;
    assert_eq!(
        store.get_table_replication_state(table_id).await,
        Some(TableReplicationPhase::Streaming)
    );
}

#[tokio::test]
async fn test_get_nonexistent_state() {
    let store = CustomStore::new();
    let result = store.get_table_replication_state(TableId(888)).await;
    assert!(result.is_none());
}

// ============================================================================
// Table Mapping Tests
// ============================================================================

#[tokio::test]
async fn test_store_and_get_table_mapping() {
    let store = CustomStore::new();
    let table_id = TableId(400);

    store.store_table_mapping(table_id, "public.users".to_string()).await;
    let result = store.get_table_mapping(&table_id).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap(), "public.users");
}

#[tokio::test]
async fn test_get_table_mappings() {
    let store = CustomStore::new();

    store.store_table_mapping(TableId(1), "schema1.table1".to_string()).await;
    store.store_table_mapping(TableId(2), "schema2.table2".to_string()).await;

    let mappings = store.get_table_mappings().await;
    assert_eq!(mappings.len(), 2);
    assert_eq!(mappings.get(&TableId(1)), Some(&"schema1.table1".to_string()));
    assert_eq!(mappings.get(&TableId(2)), Some(&"schema2.table2".to_string()));
}

#[tokio::test]
async fn test_get_nonexistent_mapping() {
    let store = CustomStore::new();
    let result = store.get_table_mapping(&TableId(777)).await;
    assert!(result.is_none());
}

// ============================================================================
// CleanupStore Trait Tests
// ============================================================================

#[tokio::test]
async fn test_cleanup_removes_all_table_data() {
    let store = CustomStore::new();
    let table_id = TableId(500);

    // Store schema, state, and mapping
    store.store_table_schema(TableSchema {
        id: table_id,
        name: "cleanup_test".to_string(),
    }).await;
    store.update_table_replication_state(table_id, TableReplicationPhase::Streaming).await;
    store.store_table_mapping(table_id, "test.cleanup".to_string()).await;

    // Verify all exist
    assert!(store.get_table_schema(&table_id).await.is_some());
    assert!(store.get_table_replication_state(table_id).await.is_some());
    assert!(store.get_table_mapping(&table_id).await.is_some());

    // Cleanup
    store.cleanup_table_state(table_id).await;

    // Verify all removed
    assert!(store.get_table_schema(&table_id).await.is_none());
    assert!(store.get_table_replication_state(table_id).await.is_none());
    assert!(store.get_table_mapping(&table_id).await.is_none());
}

#[tokio::test]
async fn test_cleanup_only_affects_specified_table() {
    let store = CustomStore::new();
    let table_id_1 = TableId(1);
    let table_id_2 = TableId(2);

    // Store data for both tables
    store.store_table_schema(TableSchema { id: table_id_1, name: "table1".to_string() }).await;
    store.store_table_schema(TableSchema { id: table_id_2, name: "table2".to_string() }).await;

    // Cleanup only table 1
    store.cleanup_table_state(table_id_1).await;

    // Table 1 should be gone
    assert!(store.get_table_schema(&table_id_1).await.is_none());

    // Table 2 should still exist
    assert!(store.get_table_schema(&table_id_2).await.is_some());
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[tokio::test]
async fn test_concurrent_store_operations() {
    let store = CustomStore::new();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let store_clone = store.clone();
            tokio::spawn(async move {
                store_clone.store_table_schema(TableSchema {
                    id: TableId(i),
                    name: format!("table_{}", i),
                }).await;
            })
        })
        .collect();

    for handle in handles {
        handle.await.unwrap();
    }

    let schemas = store.get_table_schemas().await;
    assert_eq!(schemas.len(), 10);
}

#[tokio::test]
async fn test_store_clone_shares_data() {
    let store1 = CustomStore::new();
    let store2 = store1.clone();

    store1.store_table_schema(TableSchema {
        id: TableId(999),
        name: "shared".to_string(),
    }).await;

    // Should be visible through store2
    let result = store2.get_table_schema(&TableId(999)).await;
    assert!(result.is_some());
    assert_eq!(result.unwrap().name, "shared");
}

// ============================================================================
// Mixed Operations Tests
// ============================================================================

#[tokio::test]
async fn test_mixed_operations_same_table() {
    let store = CustomStore::new();
    let table_id = TableId(600);

    // Interleaved operations on the same table
    store.store_table_schema(TableSchema { id: table_id, name: "test".to_string() }).await;
    store.update_table_replication_state(table_id, TableReplicationPhase::CopySnapshot).await;
    store.store_table_mapping(table_id, "public.test".to_string()).await;

    // All should exist
    assert!(store.get_table_schema(&table_id).await.is_some());
    assert_eq!(store.get_table_replication_state(table_id).await, Some(TableReplicationPhase::CopySnapshot));
    assert_eq!(store.get_table_mapping(&table_id).await, Some("public.test".to_string()));

    // Update state
    store.update_table_replication_state(table_id, TableReplicationPhase::Streaming).await;
    assert_eq!(store.get_table_replication_state(table_id).await, Some(TableReplicationPhase::Streaming));
}
