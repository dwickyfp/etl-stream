//! Core Snowflake destination implementation.
//!
//! Implements the ETL Destination trait for Snowflake using PyO3 to bridge
//! to Python's Snowpipe Streaming SDK.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::schema::SchemaStore;
use etl::store::state::StateStore;
use etl::types::{Cell, ColumnSchema, Event, TableId, TableRow, TableSchema, generate_sequence_number};
use etl::{bail, etl_error};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::snowflake::bridge::{ColumnDef, RowData, SnowflakePyBridge};
use crate::snowflake::config::SnowflakeConfig;
use crate::snowflake::error::{snowflake_error_to_etl_error, SnowflakeError};

/// CDC operation types for Snowflake landing tables.
#[derive(Debug, Clone, Copy)]
pub enum SnowflakeOperationType {
    Insert,
    Update,
    Delete,
}

impl std::fmt::Display for SnowflakeOperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnowflakeOperationType::Insert => write!(f, "INSERT"),
            SnowflakeOperationType::Update => write!(f, "UPDATE"),
            SnowflakeOperationType::Delete => write!(f, "DELETE"),
        }
    }
}

/// Internal state for Snowflake destination.
#[derive(Debug)]
struct Inner {
    /// Tables that have been initialized
    initialized_tables: HashSet<String>,
    /// Cached table schemas for schema evolution detection
    table_schemas: HashMap<TableId, Vec<ColumnDef>>,
}

/// Snowflake destination implementing the ETL Destination trait.
///
/// Uses Python's Snowpipe Streaming SDK via PyO3 for high-performance,
/// low-latency data ingestion into landing tables. A scheduled Snowflake
/// task merges data from landing tables into target tables.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination<S> {
    bridge: SnowflakePyBridge,
    config: SnowflakeConfig,
    store: S,
    inner: Arc<Mutex<Inner>>,
}

impl<S> SnowflakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    /// Creates a new Snowflake destination.
    pub fn new(config: SnowflakeConfig, store: S) -> EtlResult<Self> {
        let bridge = SnowflakePyBridge::new(config.clone())
            .map_err(snowflake_error_to_etl_error)?;

        Ok(Self {
            bridge,
            config,
            store,
            inner: Arc::new(Mutex::new(Inner {
                initialized_tables: HashSet::new(),
                table_schemas: HashMap::new(),
            })),
        })
    }

    /// Initializes the destination.
    ///
    /// Must be called before any write operations.
    pub async fn initialize(&self) -> EtlResult<()> {
        self.bridge
            .initialize()
            .await
            .map_err(snowflake_error_to_etl_error)?;
        
        info!("Snowflake destination initialized");
        Ok(())
    }

    /// Shuts down the destination and releases resources.
    pub async fn shutdown(&self) -> EtlResult<()> {
        self.bridge
            .close()
            .await
            .map_err(snowflake_error_to_etl_error)?;
        
        info!("Snowflake destination shut down");
        Ok(())
    }

    /// Prepares a table for streaming operations.
    async fn prepare_table_for_streaming(
        &self,
        table_id: TableId,
    ) -> EtlResult<(String, Vec<ColumnDef>, Vec<String>)> {
        let table_schema = self
            .store
            .get_table_schema(&table_id)
            .await?
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::MissingTableSchema,
                    "Table schema not found",
                    format!("No schema found for table {table_id}")
                )
            })?;

        let table_name = table_schema.name.name.clone();
        let columns = self.column_schemas_to_column_defs(&table_schema.column_schemas);
        let pk_columns: Vec<String> = table_schema
            .column_schemas
            .iter()
            .filter(|c| c.primary)
            .map(|c| c.name.clone())
            .collect();

        // Check for schema evolution
        let mut inner = self.inner.lock().await;
        
        if let Some(cached_columns) = inner.table_schemas.get(&table_id) {
            let new_columns = self.detect_new_columns(cached_columns, &columns);
            if !new_columns.is_empty() {
                info!(
                    "Schema evolution detected for table {}: {} new columns",
                    table_name,
                    new_columns.len()
                );
                
                // Handle schema evolution
                self.bridge
                    .handle_schema_evolution(&table_name, &new_columns, &columns, &pk_columns)
                    .await
                    .map_err(snowflake_error_to_etl_error)?;
                
                // Update cached schema
                inner.table_schemas.insert(table_id, columns.clone());
            }
        } else {
            // First time seeing this table
            if !inner.initialized_tables.contains(&table_name) {
                self.bridge
                    .ensure_table_initialized(&table_name, &columns, &pk_columns)
                    .await
                    .map_err(snowflake_error_to_etl_error)?;
                
                inner.initialized_tables.insert(table_name.clone());
            }
            inner.table_schemas.insert(table_id, columns.clone());
        }

        Ok((table_name, columns, pk_columns))
    }

    /// Converts ETL column schemas to bridge column definitions.
    fn column_schemas_to_column_defs(&self, schemas: &[ColumnSchema]) -> Vec<ColumnDef> {
        schemas
            .iter()
            .map(|s| ColumnDef {
                name: s.name.clone(),
                type_oid: s.typ.oid(),
                type_name: s.typ.name().to_string(),
                modifier: s.modifier,
                nullable: s.nullable,
                is_primary_key: s.primary,
            })
            .collect()
    }

    /// Detects new columns by comparing cached and current schemas.
    fn detect_new_columns(&self, cached: &[ColumnDef], current: &[ColumnDef]) -> Vec<ColumnDef> {
        let cached_names: HashSet<_> = cached.iter().map(|c| &c.name).collect();
        current
            .iter()
            .filter(|c| !cached_names.contains(&c.name))
            .cloned()
            .collect()
    }

    /// Converts table rows to row data for the bridge.
    fn table_rows_to_row_data(
        &self,
        table_rows: &[TableRow],
        columns: &[ColumnDef],
        operation: SnowflakeOperationType,
        start_lsn: Option<etl::types::Lsn>,
        commit_lsn: Option<etl::types::Lsn>,
    ) -> Vec<RowData> {
        table_rows
            .iter()
            .map(|row| {
                let mut values = HashMap::new();
                
                for (i, cell) in row.values.iter().enumerate() {
                    if let Some(col) = columns.get(i) {
                        let json_value = cell_to_json(cell);
                        values.insert(col.name.clone(), json_value);
                    }
                }
                
                RowData { values }
            })
            .collect()
    }

    /// Writes table rows during initial copy phase.
    async fn write_table_rows_impl(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let (table_name, columns, _pk_columns) =
            self.prepare_table_for_streaming(table_id).await?;

        let row_data =
            self.table_rows_to_row_data(&table_rows, &columns, SnowflakeOperationType::Insert, None, None);

        self.bridge
            .insert_rows(&table_name, row_data, "INSERT")
            .await
            .map_err(snowflake_error_to_etl_error)?;

        debug!(
            "Wrote {} rows to Snowflake table {}",
            table_rows.len(),
            table_name
        );

        Ok(())
    }

    /// Processes CDC events.
    async fn write_events_impl(&self, events: Vec<Event>) -> EtlResult<()> {
        let mut event_iter = events.into_iter().peekable();

        while event_iter.peek().is_some() {
            // Group events by table until we hit a truncate
            let mut table_id_to_rows: HashMap<TableId, Vec<(TableRow, SnowflakeOperationType, etl::types::Lsn, etl::types::Lsn)>> =
                HashMap::new();

            while let Some(event) = event_iter.peek() {
                if matches!(event, Event::Truncate(_)) {
                    break;
                }

                let event = event_iter.next().unwrap();
                match event {
                    Event::Insert(insert) => {
                        let entries = table_id_to_rows.entry(insert.table_id).or_default();
                        entries.push((
                            insert.table_row,
                            SnowflakeOperationType::Insert,
                            insert.start_lsn,
                            insert.commit_lsn,
                        ));
                    }
                    Event::Update(update) => {
                        let entries = table_id_to_rows.entry(update.table_id).or_default();
                        entries.push((
                            update.table_row,
                            SnowflakeOperationType::Update,
                            update.start_lsn,
                            update.commit_lsn,
                        ));
                    }
                    Event::Delete(delete) => {
                        if let Some((_, old_row)) = delete.old_table_row {
                            let entries = table_id_to_rows.entry(delete.table_id).or_default();
                            entries.push((
                                old_row,
                                SnowflakeOperationType::Delete,
                                delete.start_lsn,
                                delete.commit_lsn,
                            ));
                        } else {
                            debug!("Delete event has no old row, skipping");
                        }
                    }
                    event => {
                        debug!(event_type = %event.event_type(), "Skipping unsupported event type");
                    }
                }
            }

            // Process accumulated events
            for (table_id, rows_with_ops) in table_id_to_rows {
                let (table_name, columns, _pk_columns) =
                    self.prepare_table_for_streaming(table_id).await?;

                // Group by operation type for efficient insertion
                for (row, op, start_lsn, commit_lsn) in rows_with_ops {
                    let mut values = HashMap::new();
                    
                    for (i, cell) in row.values.iter().enumerate() {
                        if let Some(col) = columns.get(i) {
                            values.insert(col.name.clone(), cell_to_json(cell));
                        }
                    }
                    
                    // Add sequence number based on LSN
                    let sequence = generate_sequence_number(start_lsn, commit_lsn);
                    values.insert("_etl_sequence".to_string(), serde_json::Value::String(sequence));
                    
                    let row_data = vec![RowData { values }];
                    
                    self.bridge
                        .insert_rows(&table_name, row_data, &op.to_string())
                        .await
                        .map_err(snowflake_error_to_etl_error)?;
                }
            }

            // Handle truncate events
            let mut truncate_table_ids = HashSet::new();
            
            while let Some(Event::Truncate(_)) = event_iter.peek() {
                if let Some(Event::Truncate(truncate_event)) = event_iter.next() {
                    for table_id in truncate_event.rel_ids {
                        truncate_table_ids.insert(TableId::new(table_id));
                    }
                }
            }

            for table_id in truncate_table_ids {
                self.truncate_table_impl(table_id, true).await?;
            }
        }

        Ok(())
    }

    /// Truncates a table.
    async fn truncate_table_impl(&self, table_id: TableId, is_cdc: bool) -> EtlResult<()> {
        let table_schema = self.store.get_table_schema(&table_id).await?;
        
        let table_schema = match table_schema {
            Some(s) => s,
            None => {
                if is_cdc {
                    bail!(
                        ErrorKind::MissingTableSchema,
                        "Table schema not found for truncate",
                        format!("No schema found for table {table_id}")
                    );
                } else {
                    warn!(%table_id, "Table schema not found during truncate, skipping");
                    return Ok(());
                }
            }
        };

        let table_name = &table_schema.name.name;

        self.bridge
            .truncate_table(table_name)
            .await
            .map_err(snowflake_error_to_etl_error)?;

        // Remove from initialized tables so it will be recreated
        let mut inner = self.inner.lock().await;
        inner.initialized_tables.remove(table_name);
        inner.table_schemas.remove(&table_id);

        info!("Truncated Snowflake table: {}", table_name);
        Ok(())
    }
}

impl<S> Destination for SnowflakeDestination<S>
where
    S: StateStore + SchemaStore + Send + Sync,
{
    fn name() -> &'static str {
        "snowflake"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        self.truncate_table_impl(table_id, false).await
    }

    async fn write_table_rows(
        &self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        self.write_table_rows_impl(table_id, table_rows).await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        self.write_events_impl(events).await
    }

    async fn shutdown(&self) {
        if let Err(e) = self.shutdown().await {
            warn!("Error during Snowflake destination shutdown: {}", e);
        }
    }
}

/// Converts a Cell value to a JSON value.
fn cell_to_json(cell: &Cell) -> serde_json::Value {
    match cell {
        Cell::Null => serde_json::Value::Null,
        Cell::Bool(b) => serde_json::Value::Bool(*b),
        Cell::I16(i) => serde_json::Value::Number((*i).into()),
        Cell::I32(i) => serde_json::Value::Number((*i).into()),
        Cell::I64(i) => serde_json::Value::Number((*i).into()),
        Cell::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Cell::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Cell::String(s) => serde_json::Value::String(s.clone()),
        Cell::Bytes(b) => {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(b);
            serde_json::Value::String(encoded)
        }
        Cell::Date(d) => serde_json::Value::String(d.to_string()),
        Cell::Time(t) => serde_json::Value::String(t.to_string()),
        Cell::Timestamp(ts) => serde_json::Value::String(ts.to_string()),
        Cell::Uuid(u) => serde_json::Value::String(u.to_string()),
        Cell::Json(j) => j.clone(),
        Cell::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(cell_to_json).collect())
        }
        Cell::Numeric(n) => serde_json::Value::String(n.to_string()),
        Cell::Unsupported(s) => serde_json::Value::String(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_type_display() {
        assert_eq!(SnowflakeOperationType::Insert.to_string(), "INSERT");
        assert_eq!(SnowflakeOperationType::Update.to_string(), "UPDATE");
        assert_eq!(SnowflakeOperationType::Delete.to_string(), "DELETE");
    }

    #[test]
    fn test_cell_to_json() {
        assert_eq!(cell_to_json(&Cell::Null), serde_json::Value::Null);
        assert_eq!(cell_to_json(&Cell::Bool(true)), serde_json::json!(true));
        assert_eq!(cell_to_json(&Cell::I32(42)), serde_json::json!(42));
        assert_eq!(
            cell_to_json(&Cell::String("test".to_string())),
            serde_json::json!("test")
        );
    }
}
