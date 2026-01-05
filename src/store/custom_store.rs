use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use etl::error::EtlResult;
use etl::state::table::TableReplicationPhase;
use etl::store::cleanup::CleanupStore;
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{TableId, TableSchema};

#[derive(Debug, Clone, Default)]
struct TableEntry {
    schema: Option<Arc<TableSchema>>,
    state: Option<TableReplicationPhase>,
    mapping: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CustomStore {
    tables: Arc<Mutex<HashMap<TableId, TableEntry>>>,
}

impl CustomStore {
    pub fn new() -> Self {
        info!("Creating custom store");
        Self {
            tables: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SchemaStore for CustomStore {
    async fn get_table_schema(&self, table_id: &TableId) -> EtlResult<Option<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(table_id).and_then(|e| e.schema.clone()))
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        let tables = self.tables.lock().await;
        Ok(tables.values().filter_map(|e| e.schema.clone()).collect())
    }

    async fn load_table_schemas(&self) -> EtlResult<usize> {
        Ok(0) // In-memory store, nothing to load
    }

    async fn store_table_schema(&self, schema: TableSchema) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        let id = schema.id;
        tables.entry(id).or_default().schema = Some(Arc::new(schema));
        Ok(())
    }
}

impl StateStore for CustomStore {
    async fn get_table_replication_state(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables.get(&table_id).and_then(|e| e.state.clone()))
    }

    async fn get_table_replication_states(
        &self,
    ) -> EtlResult<std::collections::BTreeMap<TableId, TableReplicationPhase>> {
        let tables = self.tables.lock().await;
        Ok(tables
            .iter()
            .filter_map(|(id, e)| e.state.clone().map(|s| (*id, s)))
            .collect())
    }

    async fn load_table_replication_states(&self) -> EtlResult<usize> {
        Ok(0)
    }

    async fn update_table_replication_state(
        &self,
        table_id: TableId,
        state: TableReplicationPhase,
    ) -> EtlResult<()> {
        info!("Table {} -> {:?}", table_id.0, state);
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().state = Some(state);
        Ok(())
    }

    async fn rollback_table_replication_state(
        &self,
        _table_id: TableId,
    ) -> EtlResult<TableReplicationPhase> {
        todo!("Implement rollback if needed")
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
        Ok(0)
    }

    async fn store_table_mapping(
        &self,
        table_id: TableId,
        mapping: String,
    ) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        tables.entry(table_id).or_default().mapping = Some(mapping);
        Ok(())
    }
}

impl CleanupStore for CustomStore {
    async fn cleanup_table_state(&self, table_id: TableId) -> EtlResult<()> {
        let mut tables = self.tables.lock().await;
        tables.remove(&table_id);
        Ok(())
    }
}