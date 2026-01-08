//! Snowflake destination handler for the ETL pipeline.
//!
//! Uses PyO3 to bridge Rust with Python's Snowpipe Streaming SDK.

use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::types::{ArrayCell, Cell, Event, TableId, TableRow};
use etl::etl_error;

use crate::metrics;
use crate::schema_cache::SchemaCache;

/// Configuration for Snowflake destination from database.
#[derive(Debug, Clone, Deserialize)]
pub struct SnowflakeDestinationConfig {
    /// Snowflake account identifier (e.g., "xy12345.us-east-1")
    pub account: String,
    /// Snowflake username
    pub user: String,
    /// Target database name
    pub database: String,
    /// Target schema name for tables
    pub schema: String,
    /// Compute warehouse name
    pub warehouse: String,
    /// Path to private key file (.p8)
    pub private_key_path: String,
    /// Optional passphrase for encrypted private key
    #[serde(default)]
    pub private_key_passphrase: Option<String>,
    /// Optional Snowflake role to use
    #[serde(default)]
    pub role: Option<String>,
    /// Schema for landing tables (default: "ETL_SCHEMA")
    #[serde(default = "default_landing_schema")]
    pub landing_schema: String,
    /// Task schedule interval in minutes (default: 60)
    #[serde(default = "default_task_schedule")]
    pub task_schedule_minutes: u64,
}

fn default_landing_schema() -> String {
    "ETL_SCHEMA".to_string()
}

fn default_task_schedule() -> u64 {
    60
}

/// Internal state for tracking initialized tables.
#[derive(Debug, Default)]
struct Inner {
    initialized_tables: std::collections::HashSet<String>,
}

/// Snowflake destination using Python via PyO3.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    config: SnowflakeDestinationConfig,
    schema_cache: SchemaCache,
    inner: Arc<Mutex<Inner>>,
    py_client: Arc<Mutex<Option<pyo3::Py<pyo3::PyAny>>>>,
}

impl SnowflakeDestination {
    /// Creates a new Snowflake destination.
    pub fn new(config: SnowflakeDestinationConfig, schema_cache: SchemaCache) -> EtlResult<Self> {
        Ok(Self {
            config,
            schema_cache,
            inner: Arc::new(Mutex::new(Inner::default())),
            py_client: Arc::new(Mutex::new(None)),
        })
    }

    /// Initializes the Python client (lazy initialization on first use).
    async fn ensure_initialized(&self) -> EtlResult<()> {
        let mut client_guard = self.py_client.lock().await;
        if client_guard.is_some() {
            return Ok(());
        }

        let config = self.config.clone();

        // Initialize Python client in a blocking task
        let client = tokio::task::spawn_blocking(move || {
            pyo3::Python::with_gil(|py| -> EtlResult<pyo3::Py<pyo3::PyAny>> {
                use pyo3::types::PyAnyMethods;
                
                // Import etl_snowflake module
                let etl_snowflake = py
                    .import_bound("etl_snowflake")
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python import error", e.to_string()))?;

                // Create configuration
                let config_class = etl_snowflake
                    .getattr("SnowflakeConfig")
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;

                let py_config = config_class
                    .call1((
                        config.account.as_str(),
                        config.user.as_str(),
                        config.database.as_str(),
                        config.schema.as_str(),
                        config.warehouse.as_str(),
                        config.private_key_path.as_str(),
                        config.private_key_passphrase.as_deref(),
                        config.role.as_deref(),
                        config.landing_schema.as_str(),
                        config.task_schedule_minutes as i64,
                    ))
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python config error", e.to_string()))?;

                // Create client
                let client_class = etl_snowflake
                    .getattr("SnowflakeClient")
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;

                let client = client_class
                    .call1((py_config,))
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python client error", e.to_string()))?;

                // Initialize
                client
                    .call_method0("initialize")
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python init error", e.to_string()))?;

                info!("Snowflake Python client initialized");
                Ok(client.unbind())
            })
        })
        .await
        .map_err(|e| etl_error!(ErrorKind::Unknown, "Task join error", e.to_string()))??;

        *client_guard = Some(client);
        Ok(())
    }

    /// Converts a Cell to a JSON value for Python.
    fn cell_to_json(cell: &Cell) -> JsonValue {
        match cell {
            Cell::Null => JsonValue::Null,
            Cell::Bool(b) => JsonValue::Bool(*b),
            Cell::I16(i) => serde_json::json!(*i),
            Cell::I32(i) => serde_json::json!(*i),
            Cell::I64(i) => serde_json::json!(*i),
            Cell::F32(f) => serde_json::Number::from_f64(*f as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Cell::F64(f) => serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Cell::String(s) => JsonValue::String(s.clone()),
            Cell::Bytes(b) => {
                use base64::Engine;
                JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b))
            }
            Cell::Date(d) => JsonValue::String(d.to_string()),
            Cell::Time(t) => JsonValue::String(t.to_string()),
            Cell::Timestamp(ts) => JsonValue::String(ts.to_string()),
            Cell::Uuid(u) => JsonValue::String(u.to_string()),
            Cell::Json(j) => j.clone(),
            Cell::Array(arr) => Self::array_cell_to_json(arr),
            Cell::Numeric(n) => JsonValue::String(n.to_string()),
            Cell::U32(u) => serde_json::json!(*u),
            Cell::TimestampTz(ts) => JsonValue::String(ts.to_rfc3339()),
        }
    }

    /// Converts an ArrayCell to a JSON array value.
    /// Properly handles all ArrayCell variants for Snowflake compatibility.
    fn array_cell_to_json(arr: &ArrayCell) -> JsonValue {
        match arr {
            ArrayCell::Bool(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.map(JsonValue::Bool).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::String(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.as_ref()
                            .map(|s| JsonValue::String(s.clone()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::I16(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.map(|v| serde_json::json!(v)).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::I32(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.map(|v| serde_json::json!(v)).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::U32(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.map(|v| serde_json::json!(v)).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::I64(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.map(|v| serde_json::json!(v)).unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::F32(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.and_then(|v| serde_json::Number::from_f64(v as f64))
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::F64(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.and_then(|v| serde_json::Number::from_f64(v))
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Numeric(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.as_ref()
                            .map(|n| JsonValue::String(n.to_string()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Date(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.map(|d| JsonValue::String(d.to_string()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Time(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.map(|t| JsonValue::String(t.to_string()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Timestamp(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.map(|ts| JsonValue::String(ts.to_string()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::TimestampTz(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.map(|ts| JsonValue::String(ts.to_rfc3339()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Uuid(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| {
                        opt.map(|u| JsonValue::String(u.to_string()))
                            .unwrap_or(JsonValue::Null)
                    })
                    .collect(),
            ),
            ArrayCell::Json(vec) => JsonValue::Array(
                vec.iter()
                    .map(|opt| opt.clone().unwrap_or(JsonValue::Null))
                    .collect(),
            ),
            ArrayCell::Bytes(vec) => {
                use base64::Engine;
                JsonValue::Array(
                    vec.iter()
                        .map(|opt| {
                            opt.as_ref()
                                .map(|b| {
                                    JsonValue::String(
                                        base64::engine::general_purpose::STANDARD.encode(b),
                                    )
                                })
                                .unwrap_or(JsonValue::Null)
                        })
                        .collect(),
                )
            }
        }
    }

    /// Get table name from schema cache or generate from table ID.
    /// Strips schema prefix if present (e.g., "public.sales" -> "sales").
    async fn get_table_name(&self, table_id: TableId) -> String {
        let full_name = self.schema_cache.get_table_name(table_id.0).await;
        
        // Strip schema prefix if present (e.g., "public.sales" -> "sales")
        if let Some(dot_pos) = full_name.rfind('.') {
            full_name[dot_pos + 1..].to_string()
        } else {
            full_name
        }
    }

    /// Insert rows into Snowflake landing table.
    async fn insert_rows_internal(
        &self,
        table_name: &str,
        rows: Vec<HashMap<String, JsonValue>>,
        operation: &str,
    ) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let timer = metrics::Timer::start();
        let row_count = rows.len() as u64;

        self.ensure_initialized().await?;

        let client_arc = self.py_client.clone();
        let table_name_str = table_name.to_string();
        let table_name_for_closure = table_name.to_string();
        let operation = operation.to_string();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::with_gil(|py| -> EtlResult<()> {
                use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};

                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;

                // Convert rows to Python list of dicts
                let py_rows = PyList::empty_bound(py);
                for row in &rows {
                    let row_dict = PyDict::new_bound(py);
                    for (key, value) in row {
                        let py_value = json_to_py(py, value)?;
                        row_dict.set_item(key, py_value)
                            .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                    }
                    py_rows.append(row_dict)
                        .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                }

                client
                    .call_method_bound(py, "insert_rows", (&table_name_for_closure, &py_rows, &operation), None)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Insert failed", e.to_string()))?;

                debug!("Inserted {} rows to {}", rows.len(), table_name_for_closure);
                Ok(())
            })
        })
        .await
        .map_err(|e| {
            metrics::snowflake_error("insert");
            etl_error!(ErrorKind::Unknown, "Task error", e.to_string())
        })??;

        // Record Snowflake metrics
        metrics::snowflake_request("success");
        metrics::snowflake_request_duration(timer.elapsed_secs());
        metrics::snowflake_rows_inserted(&table_name_str, row_count);

        Ok(())
    }

    /// Ensure table is initialized in Snowflake.
    /// Uses query_table_schema to get accurate column info including nullable constraints,
    /// which are not included in PostgreSQL logical replication Relation messages.
    async fn ensure_table_initialized(
        &self,
        table_name: &str,
        table_id: TableId,
    ) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;
        if inner.initialized_tables.contains(table_name) {
            return Ok(());
        }

        self.ensure_initialized().await?;

        // First, try to get accurate column info from pg_attribute
        // This is preferred because it includes nullable constraints
        // which are NOT included in PostgreSQL logical replication Relation messages
        let columns: Vec<HashMap<String, JsonValue>> = match self.schema_cache
            .query_table_schema(table_id.0)
            .await
        {
            Ok(db_columns) if !db_columns.is_empty() => {
                debug!(
                    "Using column info from pg_attribute for table {} (includes correct nullable info)",
                    table_name
                );
                db_columns
                    .iter()
                    .map(|col| {
                        let mut map = HashMap::new();
                        map.insert("name".to_string(), serde_json::json!(col.name));
                        map.insert("type_oid".to_string(), serde_json::json!(col.type_oid));
                        map.insert("type_name".to_string(), serde_json::json!(col.type_name));
                        map.insert("modifier".to_string(), serde_json::json!(col.modifier));
                        map.insert("nullable".to_string(), serde_json::json!(col.nullable));
                        map
                    })
                    .collect()
            }
            _ => {
                // Fallback to cached schema if database query fails
                // Note: Relation events may not have accurate nullable info
                let schemas = self.schema_cache.read().await;
                if let Some(s) = schemas.get(&table_id) {
                    warn!(
                        "Using cached schema for table {} - nullable constraints may be inaccurate",
                        table_name
                    );
                    s.column_schemas
                        .iter()
                        .map(|col| {
                            let mut map = HashMap::new();
                            map.insert("name".to_string(), serde_json::json!(col.name));
                            map.insert("type_oid".to_string(), serde_json::json!(col.typ.oid()));
                            map.insert("type_name".to_string(), serde_json::json!(col.typ.name()));
                            map.insert("modifier".to_string(), serde_json::json!(col.modifier));
                            // Note: col.nullable from Relation events may not be accurate
                            // Default to true (nullable) as safe default
                            map.insert("nullable".to_string(), serde_json::json!(true));
                            map
                        })
                        .collect()
                } else {
                    // No schema from Rust cache - Python client will auto-create table from row data
                    debug!("No schema in cache for table {}, Python will infer schema from data", table_name);
                    return Ok(());
                }
            }
        };

        // Get primary key columns from database query
        let pk_columns: Vec<String> = match self.schema_cache
            .query_table_schema(table_id.0)
            .await
        {
            Ok(db_columns) => db_columns
                .iter()
                .filter(|c| c.primary)
                .map(|c| c.name.clone())
                .collect(),
            _ => {
                // Fallback to cached schema for PK info
                let schemas = self.schema_cache.read().await;
                schemas.get(&table_id)
                    .map(|s| s.column_schemas
                        .iter()
                        .filter(|c| c.primary)
                        .map(|c| c.name.clone())
                        .collect())
                    .unwrap_or_default()
            }
        };

        // Call Python to initialize table
        let client_arc = self.py_client.clone();
        let table_name_clone = table_name.to_string();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::with_gil(|py| -> EtlResult<()> {
                use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};

                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;

                let py_columns = PyList::empty_bound(py);
                for col in &columns {
                    let col_dict = PyDict::new_bound(py);
                    for (key, value) in col {
                        let py_value = json_to_py(py, value)?;
                        col_dict.set_item(key, py_value)
                            .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                    }
                    py_columns.append(col_dict)
                        .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                }

                let py_pk_columns = PyList::new_bound(py, &pk_columns);

                client
                    .call_method_bound(
                        py,
                        "ensure_table_initialized",
                        (&table_name_clone, &py_columns, &py_pk_columns),
                        None,
                    )
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Table init failed", e.to_string()))?;

                Ok(())
            })
        })
        .await
        .map_err(|e| etl_error!(ErrorKind::Unknown, "Task error", e.to_string()))??;

        inner.initialized_tables.insert(table_name.to_string());
        metrics::snowflake_table_initialized(table_name);
        info!("Snowflake table initialized: {}", table_name);
        Ok(())
    }

    /// Initialize all tables from a publication at pipeline startup.
    /// This queries the source PostgreSQL for all tables in the publication
    /// and ensures they exist in Snowflake before processing any events.
    pub async fn init_tables_from_source(&self, publication_name: &str) -> EtlResult<Vec<String>> {
        info!("Pre-initializing Snowflake tables from publication: {}", publication_name);
        
        self.ensure_initialized().await?;

        // Query publication tables from source database
        let pub_tables = self.schema_cache
            .get_publication_tables(publication_name)
            .await
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to query publication tables", e.to_string()))?;

        if pub_tables.is_empty() {
            warn!("No tables found in publication: {}", publication_name);
            return Ok(vec![]);
        }

        info!("Found {} tables in publication {}", pub_tables.len(), publication_name);

        // Build table definitions with column schema
        let mut table_defs: Vec<HashMap<String, serde_json::Value>> = Vec::new();

        for pub_table in &pub_tables {
            debug!(
                "Processing publication table: {}.{} (oid: {})",
                pub_table.schema_name, pub_table.table_name, pub_table.oid
            );
            let columns = self.schema_cache
                .query_table_schema(pub_table.oid)
                .await
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to query table schema", e.to_string()))?;

            if columns.is_empty() {
                warn!("No columns found for table {}.{}, skipping", pub_table.schema_name, pub_table.table_name);
                continue;
            }

            // Build column definitions for Python
            let mut col_defs: Vec<HashMap<String, serde_json::Value>> = Vec::new();
            let mut pk_columns: Vec<String> = Vec::new();

            for col in &columns {
                let mut col_def = HashMap::new();
                col_def.insert("name".to_string(), serde_json::json!(col.name));
                col_def.insert("type_oid".to_string(), serde_json::json!(col.type_oid));
                col_def.insert("type_name".to_string(), serde_json::json!(col.type_name));
                col_def.insert("modifier".to_string(), serde_json::json!(col.modifier));
                col_def.insert("nullable".to_string(), serde_json::json!(col.nullable));
                col_defs.push(col_def);

                if col.primary {
                    pk_columns.push(col.name.clone());
                }
            }

            let mut table_def = HashMap::new();
            table_def.insert("name".to_string(), serde_json::json!(pub_table.table_name));
            table_def.insert("columns".to_string(), serde_json::json!(col_defs));
            table_def.insert("primary_key_columns".to_string(), serde_json::json!(pk_columns));
            table_defs.push(table_def);

            // Cache the table name for future lookups
            self.schema_cache.store_table_name(pub_table.oid, pub_table.table_name.clone()).await;
        }

        // Call Python to ensure all tables exist
        let client_arc = self.py_client.clone();
        let inner_arc = self.inner.clone();
        let total_tables = table_defs.len();

        let created_tables = tokio::task::spawn_blocking(move || {
            pyo3::Python::with_gil(|py| -> EtlResult<Vec<String>> {
                use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};

                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| etl_error!(ErrorKind::Unknown, "Python client not initialized"))?;

                // Convert table definitions to Python list of dicts
                let py_tables = PyList::empty_bound(py);
                for table_def in &table_defs {
                    let table_dict = PyDict::new_bound(py);
                    for (key, value) in table_def {
                        let py_value = json_to_py(py, value)?;
                        table_dict.set_item(key, py_value)
                            .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                    }
                    py_tables.append(table_dict)
                        .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                }

                // Call ensure_tables_from_publication
                let result = client
                    .call_method_bound(py, "ensure_tables_from_publication", (&py_tables,), None)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python call failed", e.to_string()))?;

                // Extract created table names
                use pyo3::types::PyAnyMethods;
                let py_list = result.bind(py);
                let mut created: Vec<String> = Vec::new();
                
                if let Ok(iter) = py_list.iter() {
                    for item in iter {
                        if let Ok(py_item) = item {
                            if let Ok(s) = py_item.extract::<String>() {
                                created.push(s);
                            }
                        }
                    }
                }

                // Update inner state with all tables (created + existing)
                let mut inner = futures::executor::block_on(inner_arc.lock());
                for table_def in &table_defs {
                    if let Some(serde_json::Value::String(name)) = table_def.get("name") {
                        inner.initialized_tables.insert(name.clone());
                    }
                }

                Ok(created)
            })
        })
        .await
        .map_err(|e| etl_error!(ErrorKind::Unknown, "Task error", e.to_string()))??;

        info!(
            "Snowflake table pre-initialization complete: {} tables created, {} already existed",
            created_tables.len(),
            total_tables - created_tables.len()
        );

        Ok(created_tables)
    }
}

impl Destination for SnowflakeDestination {
    fn name() -> &'static str {
        "snowflake"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        let table_name = self.get_table_name(table_id).await;
        let table_name_for_remove = table_name.clone();
        info!("Truncating Snowflake landing table for: {}", table_name);

        self.ensure_initialized().await?;

        let client_arc = self.py_client.clone();

        tokio::task::spawn_blocking(move || {
            pyo3::Python::with_gil(|py| -> EtlResult<()> {
                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;

                client
                    .call_method_bound(py, "truncate_table", (&table_name,), None)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Truncate failed", e.to_string()))?;

                Ok(())
            })
        })
        .await
        .map_err(|e| etl_error!(ErrorKind::Unknown, "Task error", e.to_string()))??;

        // Clear from initialized tables to force re-initialization check
        let mut inner = self.inner.lock().await;
        inner.initialized_tables.remove(&table_name_for_remove);

        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let table_name = self.get_table_name(table_id).await;
        info!("Writing {} rows to Snowflake table {}", rows.len(), table_name);

        // Record batch metrics
        metrics::events_batch_size(rows.len());

        // Ensure table is initialized
        self.ensure_table_initialized(&table_name, table_id).await?;

        // Get schema for column names
        let schemas = self.schema_cache.read().await;
        let schema = schemas.get(&table_id);

        let col_names: Vec<String> = schema
            .map(|s| s.column_schemas.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default();

        drop(schemas);

        // Convert rows to JSON
        let json_rows: Vec<HashMap<String, JsonValue>> = rows
            .iter()
            .map(|row| {
                let mut map = HashMap::new();
                for (i, cell) in row.values.iter().enumerate() {
                    let col_name = col_names
                        .get(i)
                        .cloned()
                        .unwrap_or_else(|| format!("column_{}", i));
                    map.insert(col_name, Self::cell_to_json(cell));
                }
                map
            })
            .collect();

        // Estimate bytes processed for throughput
        let bytes: usize = json_rows.iter().map(|r| format!("{:?}", r).len()).sum();
        metrics::events_bytes_processed("insert", bytes as u64);
        metrics::snowflake_bytes_processed(bytes as u64);
        metrics::events_processed("insert", rows.len() as u64);

        self.insert_rows_internal(&table_name, json_rows, "INSERT").await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let timer = metrics::Timer::start();
        let batch_size = events.len();

        // Record batch size
        metrics::events_batch_size(batch_size);

        // Process Relation events first
        for event in &events {
            if let Event::Relation(r) = event {
                self.schema_cache
                    .store(r.table_schema.id, r.table_schema.clone())
                    .await;
            }
        }

        // Collect table IDs that need schema lookup
        let schemas = self.schema_cache.read().await;
        let mut missing_schema_ids: Vec<TableId> = Vec::new();
        for event in &events {
            let table_id = match event {
                Event::Insert(i) => Some(i.table_id),
                Event::Update(u) => Some(u.table_id),
                Event::Delete(d) => Some(d.table_id),
                _ => None,
            };
            if let Some(id) = table_id {
                if !schemas.contains_key(&id) && !missing_schema_ids.contains(&id) {
                    missing_schema_ids.push(id);
                }
            }
        }
        drop(schemas);

        // Fetch column names from source database for tables without schema
        for table_id in &missing_schema_ids {
            let _ = self.schema_cache.fetch_column_names_from_db(*table_id).await;
        }

        let schemas = self.schema_cache.read().await;

        // Group events by table
        let mut table_rows: HashMap<TableId, Vec<(HashMap<String, JsonValue>, String)>> =
            HashMap::new();

        for event in &events {
            match event {
                Event::Insert(i) => {
                    let schema = schemas.get(&i.table_id);
                    // Get column names from schema or from column_names cache (populated by fetch_column_names_from_db)
                    let col_names: Vec<String> = if let Some(s) = schema {
                        s.column_schemas.iter().map(|c| c.name.clone()).collect()
                    } else if let Some(names) = self.schema_cache.get_column_names(i.table_id).await {
                        names
                    } else {
                        vec![]
                    };

                    let mut row = HashMap::new();
                    for (idx, cell) in i.table_row.values.iter().enumerate() {
                        let col_name = col_names
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("column_{}", idx));
                        row.insert(col_name, Self::cell_to_json(cell));
                    }

                    table_rows
                        .entry(i.table_id)
                        .or_default()
                        .push((row, "INSERT".to_string()));
                }
                Event::Update(u) => {
                    let schema = schemas.get(&u.table_id);
                    // Get column names from schema or from column_names cache
                    let col_names: Vec<String> = if let Some(s) = schema {
                        s.column_schemas.iter().map(|c| c.name.clone()).collect()
                    } else if let Some(names) = self.schema_cache.get_column_names(u.table_id).await {
                        names
                    } else {
                        vec![]
                    };

                    let mut row = HashMap::new();
                    for (idx, cell) in u.table_row.values.iter().enumerate() {
                        let col_name = col_names
                            .get(idx)
                            .cloned()
                            .unwrap_or_else(|| format!("column_{}", idx));
                        row.insert(col_name, Self::cell_to_json(cell));
                    }

                    table_rows
                        .entry(u.table_id)
                        .or_default()
                        .push((row, "UPDATE".to_string()));
                }
                Event::Delete(d) => {
                    if let Some((_, old_row)) = &d.old_table_row {
                        let schema = schemas.get(&d.table_id);
                        // Get column names from schema or from column_names cache
                        let col_names: Vec<String> = if let Some(s) = schema {
                            s.column_schemas.iter().map(|c| c.name.clone()).collect()
                        } else if let Some(names) = self.schema_cache.get_column_names(d.table_id).await {
                            names
                        } else {
                            vec![]
                        };

                        let mut row = HashMap::new();
                        for (idx, cell) in old_row.values.iter().enumerate() {
                            let col_name = col_names
                                .get(idx)
                                .cloned()
                                .unwrap_or_else(|| format!("column_{}", idx));
                            row.insert(col_name, Self::cell_to_json(cell));
                        }

                        table_rows
                            .entry(d.table_id)
                            .or_default()
                            .push((row, "DELETE".to_string()));
                    }
                }
                Event::Truncate(t) => {
                    for rel_id in &t.rel_ids {
                        let tid = TableId::new(*rel_id);
                        if let Err(e) = self.truncate_table(tid).await {
                            warn!("Truncate failed for table {}: {}", rel_id, e);
                        }
                    }
                }
                _ => {}
            }
        }

        drop(schemas);

        // Process grouped events
        for (table_id, rows_with_ops) in table_rows {
            let table_name = self.get_table_name(table_id).await;

            // Ensure table is initialized
            self.ensure_table_initialized(&table_name, table_id).await?;

            // Group by operation type
            let mut insert_rows = Vec::new();
            let mut update_rows = Vec::new();
            let mut delete_rows = Vec::new();

            for (row, op) in rows_with_ops {
                match op.as_str() {
                    "INSERT" => insert_rows.push(row),
                    "UPDATE" => update_rows.push(row),
                    "DELETE" => delete_rows.push(row),
                    _ => {}
                }
            }

            if !insert_rows.is_empty() {
                // Estimate bytes for throughput
                let bytes: usize = insert_rows.iter().map(|r| format!("{:?}", r).len()).sum();
                metrics::events_bytes_processed("insert", bytes as u64);
                metrics::snowflake_bytes_processed(bytes as u64);
                metrics::events_processed("insert", insert_rows.len() as u64);
                self.insert_rows_internal(&table_name, insert_rows, "INSERT")
                    .await?;
            }
            if !update_rows.is_empty() {
                // Estimate bytes for throughput
                let bytes: usize = update_rows.iter().map(|r| format!("{:?}", r).len()).sum();
                metrics::events_bytes_processed("update", bytes as u64);
                metrics::snowflake_bytes_processed(bytes as u64);
                metrics::events_processed("update", update_rows.len() as u64);
                self.insert_rows_internal(&table_name, update_rows, "UPDATE")
                    .await?;
            }
            if !delete_rows.is_empty() {
                // Estimate bytes for throughput
                let bytes: usize = delete_rows.iter().map(|r| format!("{:?}", r).len()).sum();
                metrics::events_bytes_processed("delete", bytes as u64);
                metrics::snowflake_bytes_processed(bytes as u64);
                metrics::events_processed("delete", delete_rows.len() as u64);
                self.insert_rows_internal(&table_name, delete_rows, "DELETE")
                    .await?;
            }
        }

        metrics::events_processing_duration(timer.elapsed_secs());
        Ok(())
    }
}

/// Convert JsonValue to Python object.
fn json_to_py(py: pyo3::Python<'_>, value: &JsonValue) -> EtlResult<pyo3::PyObject> {
    use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};
    use pyo3::ToPyObject;

    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => Ok(b.to_object(py)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.to_object(py))
            } else if let Some(f) = n.as_f64() {
                Ok(f.to_object(py))
            } else {
                Ok(n.to_string().to_object(py))
            }
        }
        JsonValue::String(s) => Ok(s.to_object(py)),
        JsonValue::Array(arr) => {
            let py_list = PyList::empty_bound(py);
            for item in arr {
                py_list.append(json_to_py(py, item)?)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }
            Ok(py_list.unbind().into())
        }
        JsonValue::Object(obj) => {
            let py_dict = PyDict::new_bound(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_to_py(py, v)?)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }
            Ok(py_dict.unbind().into())
        }
    }
}
