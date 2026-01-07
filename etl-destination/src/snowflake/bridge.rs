//! PyO3 bridge to Python Snowflake module.
//!
//! Provides Rust bindings to call Python's etl_snowflake module for
//! Snowpipe Streaming operations.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use crate::snowflake::config::SnowflakeConfig;
use crate::snowflake::error::{SnowflakeError, SnowflakeErrorKind};

/// Column definition for table creation.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub type_oid: u32,
    pub type_name: String,
    pub modifier: i32,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// Row data for insertion.
#[derive(Debug, Clone)]
pub struct RowData {
    pub values: HashMap<String, serde_json::Value>,
}

/// PyO3 bridge to Python Snowflake client.
///
/// Manages the Python interpreter lifecycle and provides async-safe
/// methods for calling Python Snowflake operations.
pub struct SnowflakePyBridge {
    config: SnowflakeConfig,
    client: Arc<Mutex<Option<Py<PyAny>>>>,
    initialized: Arc<Mutex<bool>>,
}

impl SnowflakePyBridge {
    /// Creates a new bridge instance.
    pub fn new(config: SnowflakeConfig) -> Result<Self, SnowflakeError> {
        config
            .validate()
            .map_err(|e| SnowflakeError::new(SnowflakeErrorKind::ConfigError, e))?;

        Ok(Self {
            config,
            client: Arc::new(Mutex::new(None)),
            initialized: Arc::new(Mutex::new(false)),
        })
    }

    /// Initializes the Python client.
    ///
    /// Must be called before any other operations.
    pub async fn initialize(&self) -> Result<(), SnowflakeError> {
        let mut initialized = self.initialized.lock().await;
        if *initialized {
            return Ok(());
        }

        let config = self.config.clone();
        let client_arc = self.client.clone();

        // Run Python initialization in a blocking task
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<(), SnowflakeError> {
                // Import the etl_snowflake module
                let etl_snowflake = py
                    .import("etl_snowflake")
                    .map_err(|e| SnowflakeError::python(format!("Failed to import etl_snowflake: {e}")))?;

                // Create configuration
                let config_class = etl_snowflake
                    .getattr("SnowflakeConfig")
                    .map_err(|e| SnowflakeError::python(format!("Failed to get SnowflakeConfig: {e}")))?;

                let py_config = config_class
                    .call1((
                        config.account.as_str(),
                        config.user.as_str(),
                        config.database.as_str(),
                        config.schema.as_str(),
                        config.warehouse.as_str(),
                        config.private_key_path.to_str().unwrap_or(""),
                        config.private_key_passphrase.as_deref(),
                        config.landing_schema.as_str(),
                        config.task_schedule_minutes as i64,
                    ))
                    .map_err(|e| SnowflakeError::python(format!("Failed to create config: {e}")))?;

                // Create client
                let client_class = etl_snowflake
                    .getattr("SnowflakeClient")
                    .map_err(|e| SnowflakeError::python(format!("Failed to get SnowflakeClient: {e}")))?;

                let client = client_class
                    .call1((py_config,))
                    .map_err(|e| SnowflakeError::python(format!("Failed to create client: {e}")))?;

                // Initialize the client
                client
                    .call_method0("initialize")
                    .map_err(|e| SnowflakeError::python(format!("Failed to initialize client: {e}")))?;

                // Store the client
                let mut client_guard = futures::executor::block_on(client_arc.lock());
                *client_guard = Some(client.into());

                info!("Python Snowflake client initialized successfully");
                Ok(())
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))??;

        *initialized = true;
        Ok(())
    }

    /// Ensures a table is initialized with landing table, target table, and task.
    pub async fn ensure_table_initialized(
        &self,
        table_name: &str,
        columns: &[ColumnDef],
        primary_key_columns: &[String],
    ) -> Result<(), SnowflakeError> {
        self.ensure_initialized().await?;

        let table_name = table_name.to_string();
        let columns = columns.to_vec();
        let pk_columns = primary_key_columns.to_vec();
        let client_arc = self.client.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<(), SnowflakeError> {
                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| SnowflakeError::python("Client not initialized"))?;

                // Convert columns to Python list of dicts
                let py_columns = PyList::empty(py);
                for col in &columns {
                    let col_dict = PyDict::new(py);
                    col_dict.set_item("name", &col.name)?;
                    col_dict.set_item("type_oid", col.type_oid)?;
                    col_dict.set_item("type_name", &col.type_name)?;
                    col_dict.set_item("modifier", col.modifier)?;
                    col_dict.set_item("nullable", col.nullable)?;
                    py_columns.append(col_dict)?;
                }

                // Convert PK columns to Python list
                let py_pk_columns = PyList::new(py, &pk_columns)?;

                client
                    .bind(py)
                    .call_method1(
                        "ensure_table_initialized",
                        (&table_name, py_columns, py_pk_columns),
                    )
                    .map_err(|e| {
                        SnowflakeError::ddl(format!("Failed to initialize table {table_name}: {e}"))
                    })?;

                debug!("Table initialized: {}", table_name);
                Ok(())
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))?
    }

    /// Inserts rows into a landing table.
    pub async fn insert_rows(
        &self,
        table_name: &str,
        rows: Vec<RowData>,
        operation: &str,
    ) -> Result<String, SnowflakeError> {
        if rows.is_empty() {
            return Ok(String::new());
        }

        self.ensure_initialized().await?;

        let table_name = table_name.to_string();
        let operation = operation.to_string();
        let client_arc = self.client.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<String, SnowflakeError> {
                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| SnowflakeError::python("Client not initialized"))?;

                // Convert rows to Python list of dicts
                let py_rows = PyList::empty(py);
                for row in &rows {
                    let row_dict = PyDict::new(py);
                    for (key, value) in &row.values {
                        let py_value = json_value_to_py(py, value)?;
                        row_dict.set_item(key, py_value)?;
                    }
                    py_rows.append(row_dict)?;
                }

                let result = client
                    .bind(py)
                    .call_method1("insert_rows", (&table_name, py_rows, &operation))
                    .map_err(|e| {
                        SnowflakeError::streaming(format!(
                            "Failed to insert rows to {table_name}: {e}"
                        ))
                    })?;

                let offset: String = result.extract().unwrap_or_default();
                debug!("Inserted {} rows to {}", rows.len(), table_name);
                Ok(offset)
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))?
    }

    /// Handles schema evolution by adding new columns.
    pub async fn handle_schema_evolution(
        &self,
        table_name: &str,
        new_columns: &[ColumnDef],
        all_columns: &[ColumnDef],
        primary_key_columns: &[String],
    ) -> Result<(), SnowflakeError> {
        if new_columns.is_empty() {
            return Ok(());
        }

        self.ensure_initialized().await?;

        let table_name = table_name.to_string();
        let new_columns = new_columns.to_vec();
        let all_columns = all_columns.to_vec();
        let pk_columns = primary_key_columns.to_vec();
        let client_arc = self.client.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<(), SnowflakeError> {
                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| SnowflakeError::python("Client not initialized"))?;

                let py_new_columns = columns_to_py_list(py, &new_columns)?;
                let py_all_columns = columns_to_py_list(py, &all_columns)?;
                let py_pk_columns = PyList::new(py, &pk_columns)?;

                client
                    .bind(py)
                    .call_method1(
                        "handle_schema_evolution",
                        (&table_name, py_new_columns, py_all_columns, py_pk_columns),
                    )
                    .map_err(|e| {
                        SnowflakeError::new(
                            SnowflakeErrorKind::SchemaEvolutionError,
                            format!("Schema evolution failed for {table_name}: {e}"),
                        )
                    })?;

                info!("Schema evolution completed for: {}", table_name);
                Ok(())
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))?
    }

    /// Truncates a table (drops landing and target tables).
    pub async fn truncate_table(&self, table_name: &str) -> Result<(), SnowflakeError> {
        self.ensure_initialized().await?;

        let table_name = table_name.to_string();
        let client_arc = self.client.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<(), SnowflakeError> {
                let client_guard = futures::executor::block_on(client_arc.lock());
                let client = client_guard
                    .as_ref()
                    .ok_or_else(|| SnowflakeError::python("Client not initialized"))?;

                client
                    .bind(py)
                    .call_method1("truncate_table", (&table_name,))
                    .map_err(|e| {
                        SnowflakeError::ddl(format!("Failed to truncate {table_name}: {e}"))
                    })?;

                info!("Truncated table: {}", table_name);
                Ok(())
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))?
    }

    /// Closes the client and releases resources.
    pub async fn close(&self) -> Result<(), SnowflakeError> {
        let client_arc = self.client.clone();

        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py| -> Result<(), SnowflakeError> {
                let mut client_guard = futures::executor::block_on(client_arc.lock());
                if let Some(client) = client_guard.take() {
                    client
                        .bind(py)
                        .call_method0("close")
                        .map_err(|e| SnowflakeError::python(format!("Failed to close client: {e}")))?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| SnowflakeError::python(format!("Task join error: {e}")))?;

        let mut initialized = self.initialized.lock().await;
        *initialized = false;

        info!("Python Snowflake client closed");
        Ok(())
    }

    /// Ensures the client is initialized.
    async fn ensure_initialized(&self) -> Result<(), SnowflakeError> {
        let initialized = self.initialized.lock().await;
        if !*initialized {
            return Err(SnowflakeError::python("Client not initialized. Call initialize() first."));
        }
        Ok(())
    }
}

/// Converts a serde_json::Value to a Python object.
fn json_value_to_py(py: Python<'_>, value: &serde_json::Value) -> Result<PyObject, SnowflakeError> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py).map_err(|e| SnowflakeError::python(e.to_string()))?.into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py).map_err(|e| SnowflakeError::python(e.to_string()))?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py).map_err(|e| SnowflakeError::python(e.to_string()))?.into_any().unbind())
            } else {
                Ok(n.to_string().into_pyobject(py).map_err(|e| SnowflakeError::python(e.to_string()))?.into_any().unbind())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py).map_err(|e| SnowflakeError::python(e.to_string()))?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(json_value_to_py(py, item)?)?;
            }
            Ok(py_list.into())
        }
        serde_json::Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_value_to_py(py, v)?)?;
            }
            Ok(py_dict.into())
        }
    }
}

/// Converts a slice of ColumnDef to a Python list of dicts.
fn columns_to_py_list<'py>(
    py: Python<'py>,
    columns: &[ColumnDef],
) -> Result<Bound<'py, PyList>, SnowflakeError> {
    let py_list = PyList::empty(py);
    for col in columns {
        let col_dict = PyDict::new(py);
        col_dict.set_item("name", &col.name)?;
        col_dict.set_item("type_oid", col.type_oid)?;
        col_dict.set_item("type_name", &col.type_name)?;
        col_dict.set_item("modifier", col.modifier)?;
        col_dict.set_item("nullable", col.nullable)?;
        py_list.append(col_dict)?;
    }
    Ok(py_list)
}

impl Clone for SnowflakePyBridge {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            client: self.client.clone(),
            initialized: self.initialized.clone(),
        }
    }
}

impl std::fmt::Debug for SnowflakePyBridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnowflakePyBridge")
            .field("config", &self.config)
            .field("initialized", &self.initialized)
            .finish()
    }
}
