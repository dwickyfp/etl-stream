//! Snowflake destination handler for the ETL pipeline.
//!
//! Uses PyO3 to bridge Rust with Python's Snowpipe Streaming SDK.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use chrono::Timelike;
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tokio::sync::{mpsc, oneshot};
// Use tokio::sync::Mutex for async-safe state (inner)
// Python client managed by Actor in separate thread/task
use tracing::{debug, error, info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::types::{ArrayCell, Cell, Event, TableId, TableRow};
use etl::etl_error;

use crate::circuit_breaker::CircuitBreaker;
use crate::metrics;
use crate::schema_cache::SchemaCache;

// Timeout for Python operations to prevent hanging
const PYTHON_OPERATION_TIMEOUT: Duration = Duration::from_secs(300); // 5 minutes

/// Maximum concurrent Python flush operations to prevent thread pool saturation
/// This limits simultaneous spawn_blocking calls for Python I/O operations
const MAX_CONCURRENT_FLUSHES: usize = 32;

// Arrow imports for zero-copy Rust-Python bridge
use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, RecordBatch, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder, UInt32Builder
};
use arrow::datatypes::{Field, Schema};
use arrow::pyarrow::ToPyArrow;

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
    /// Optional Snowflake host URL (e.g., "account.snowflakecomputing.com")
    #[serde(default)]
    pub host: Option<String>,
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

/// Messages sent to the Snowflake Actor
enum ActorMessage {
    Initialize(SnowflakeDestinationConfig, oneshot::Sender<EtlResult<()>>),
    InsertArrowBatch {
        table_name: String,
        batch: RecordBatch,
        operation: String,
        respond_to: oneshot::Sender<EtlResult<()>>,
    },
    EnsureTableInitialized {
        table_name: String,
        columns: Vec<HashMap<String, JsonValue>>,
        pk_columns: Vec<String>,
        respond_to: oneshot::Sender<EtlResult<()>>,
    },
    EnsureTablesFromPublication {
        table_defs: Vec<HashMap<String, JsonValue>>,
        respond_to: oneshot::Sender<EtlResult<Vec<String>>>,
    },
    Shutdown,
}

struct SnowflakeActor {
    client: Option<pyo3::Py<pyo3::PyAny>>,
    receiver: mpsc::Receiver<ActorMessage>,
}

impl SnowflakeActor {
    fn new(receiver: mpsc::Receiver<ActorMessage>) -> Self {
        Self {
            client: None,
            receiver,
        }
    }

    /// Extract human-readable message from panic payload
    fn panic_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
        if let Some(s) = payload.downcast_ref::<&str>() {
            format!("Panic: {}", s)
        } else if let Some(s) = payload.downcast_ref::<String>() {
            format!("Panic: {}", s)
        } else {
            "Unknown panic".to_string()
        }
    }

    fn run(mut self) {
        // This runs in a dedicated thread or blocking task
        while let Some(msg) = self.receiver.blocking_recv() {
            match msg {
                ActorMessage::Shutdown => {
                    if let Some(client) = self.client.take() {
                        let _ = pyo3::Python::attach(|py| client.call_method0(py, "close"));
                        info!("Snowflake Python client closed");
                    }
                    break;
                }
                ActorMessage::Initialize(config, respond_to) => {
                    // Catch panics during initialization to report error instead of killing the actor thread silentl
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.handle_initialize(config)
                    }));
                    
                    match result {
                        Ok(etl_result) => {
                             let _ = respond_to.send(etl_result);
                        }
                        Err(payload) => {
                             // Try to extract panic message
                             let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                                 format!("Panic: {}", s)
                             } else if let Some(s) = payload.downcast_ref::<String>() {
                                 format!("Panic: {}", s)
                             } else {
                                 "Unknown panic".to_string()
                             };
                             error!("SnowflakeActor panicked during initialization: {}", msg);
                             let _ = respond_to.send(Err(etl_error!(ErrorKind::Unknown, "Actor panic", msg)));
                        }
                    }
                }
                ActorMessage::InsertArrowBatch { table_name, batch, operation, respond_to } => {
                     let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                         self.handle_insert(table_name, batch, operation)
                     }));
                     match result {
                         Ok(etl_result) => {
                              let _ = respond_to.send(etl_result);
                         }
                         Err(payload) => {
                              let msg = Self::panic_to_string(payload);
                              error!("SnowflakeActor panicked during insert: {}", msg);
                              let _ = respond_to.send(Err(etl_error!(ErrorKind::Unknown, "Actor panic", msg)));
                         }
                     }
                }
                ActorMessage::EnsureTableInitialized { table_name, columns, pk_columns, respond_to } => {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.handle_ensure_table(table_name, columns, pk_columns)
                    }));
                    match result {
                        Ok(etl_result) => {
                             let _ = respond_to.send(etl_result);
                        }
                        Err(payload) => {
                             let msg = Self::panic_to_string(payload);
                             error!("SnowflakeActor panicked during ensure_table: {}", msg);
                             let _ = respond_to.send(Err(etl_error!(ErrorKind::Unknown, "Actor panic", msg)));
                        }
                    }
                }
                ActorMessage::EnsureTablesFromPublication { table_defs, respond_to } => {
                     let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                         self.handle_ensure_tables_batch(table_defs)
                     }));
                     match result {
                         Ok(etl_result) => {
                              let _ = respond_to.send(etl_result);
                         }
                         Err(payload) => {
                              let msg = Self::panic_to_string(payload);
                              error!("SnowflakeActor panicked during ensure_tables_batch: {}", msg);
                              let _ = respond_to.send(Err(etl_error!(ErrorKind::Unknown, "Actor panic", msg.clone())));
                         }
                     }
                }
            }
        }
    }

    fn handle_initialize(&mut self, config: SnowflakeDestinationConfig) -> EtlResult<()> {
        if self.client.is_some() {
            return Ok(());
        }

        pyo3::Python::attach(|py| -> EtlResult<()> {
            use pyo3::types::PyAnyMethods;
            
            // Import etl_snowflake module
            let etl_snowflake = py
                .import("etl_snowflake")
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
                    config.host.as_deref(),
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
            self.client = Some(client.unbind());
            Ok(())
        })
    }

    fn handle_insert(&self, table_name: String, batch: RecordBatch, operation: String) -> EtlResult<()> {
        let client_obj = self.client.as_ref().ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;
        
        pyo3::Python::attach(|py| -> EtlResult<()> {
             // Convert RecordBatch to PyArrow RecordBatch (zero-copy via Arrow C Data Interface)
             // Note: batch.to_pyarrow needs to happen inside GIL
             let py_batch_obj = batch.to_pyarrow(py)
                 .map_err(|e| etl_error!(ErrorKind::Unknown, "PyArrow conversion error", e.to_string()))?;

             client_obj
                 .call_method(py, "insert_arrow_batch", (table_name, &py_batch_obj, operation), None)
                 .map_err(|e| etl_error!(ErrorKind::Unknown, "Insert failed", e.to_string()))?;
             Ok(())
        })
    }

    fn handle_ensure_table(&self, table_name: String, columns: Vec<HashMap<String, JsonValue>>, pk_columns: Vec<String>) -> EtlResult<()> {
         let client_obj = self.client.as_ref().ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;
         
         pyo3::Python::attach(|py| -> EtlResult<()> {
            use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};

            let py_columns = PyList::empty(py);
            for col in &columns {
                let col_dict = PyDict::new(py);
                for (key, value) in col {
                    let py_value = json_to_py(py, value)?;
                    col_dict.set_item(key, py_value)
                        .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                }
                py_columns.append(col_dict)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }

            let py_pk_columns = PyList::new(py, &pk_columns)
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;

            client_obj
                .call_method(
                    py,
                    "ensure_table_initialized",
                    (table_name, &py_columns, &py_pk_columns),
                    None,
                )
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Table init failed", e.to_string()))?;
            Ok(())
         })
    }

    fn handle_ensure_tables_batch(&self, table_defs: Vec<HashMap<String, JsonValue>>) -> EtlResult<Vec<String>> {
        let client_obj = self.client.as_ref().ok_or_else(|| etl_error!(ErrorKind::Unknown, "Client not initialized"))?;
        
        pyo3::Python::attach(|py| -> EtlResult<Vec<String>> {
            use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods, PyAnyMethods};

            let py_tables = PyList::empty(py);
            for table_def in &table_defs {
                let table_dict = PyDict::new(py);
                for (key, value) in table_def {
                    let py_value = json_to_py(py, value)?;
                    table_dict.set_item(key, py_value)
                        .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
                }
                py_tables.append(table_dict)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }

            let result = client_obj
                .call_method(py, "ensure_tables_from_publication", (&py_tables,), None)
                .map_err(|e| etl_error!(ErrorKind::Unknown, "Python call failed", e.to_string()))?;

            let py_list = result.bind(py);
            let mut created: Vec<String> = Vec::new();
            
            if let Ok(list) = py_list.downcast::<pyo3::types::PyList>() {
                for item in list.iter() {
                    if let Ok(s) = item.extract::<String>() {
                        created.push(s);
                    }
                }
            }
            Ok(created)
        })
    }

}

/// Snowflake destination using Python via PyO3.
#[derive(Debug, Clone)]
pub struct SnowflakeDestination {
    config: SnowflakeDestinationConfig,
    schema_cache: SchemaCache,
    inner: Arc<tokio::sync::Mutex<Inner>>,
    /// Channel to send messages to the actor
    actor_tx: mpsc::Sender<ActorMessage>,
    circuit_breaker: CircuitBreaker,
    /// Semaphore to limit concurrent Python flush operations
    /// Prevents spawn_blocking thread pool saturation under high load
    flush_semaphore: Arc<Semaphore>,
}

// NOTE: We no longer implement Drop with Shutdown.
// The actor will exit naturally when all mpsc::Sender clones are dropped
// (which happens when the last SnowflakeDestination is dropped).
// This avoids the race condition where cloning SnowflakeDestination
// would cause premature Shutdown when one clone was dropped.

impl SnowflakeDestination {
    /// Creates a new Snowflake destination.
    pub fn new(config: SnowflakeDestinationConfig, schema_cache: SchemaCache) -> EtlResult<Self> {
        let circuit_breaker = CircuitBreaker::new(format!("snowflake_{}", config.account));
        let (tx, rx) = mpsc::channel(64); // Buffer up to 64 operations

        // Spawn the actor in a dedicated thread to avoid blocking Tokio workers
        // Since it holds the Python client and GIL often, a dedicated thread is safer
        // than spawn_blocking which uses a thread pool that can be exhausted.
        std::thread::Builder::new()
             .name("snowflake-actor".to_string())
             .spawn(move || {
                  info!("SnowflakeActor thread starting");
                  let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                      let actor = SnowflakeActor::new(rx);
                      actor.run();
                  }));
                  match result {
                      Ok(()) => info!("SnowflakeActor thread exited normally"),
                      Err(payload) => {
                          let msg = if let Some(s) = payload.downcast_ref::<&str>() {
                              format!("Panic: {}", s)
                          } else if let Some(s) = payload.downcast_ref::<String>() {
                              format!("Panic: {}", s)
                          } else {
                              "Unknown panic".to_string()
                          };
                          error!("SnowflakeActor thread CRASHED: {}", msg);
                      }
                  }
             })
             .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to spawn actor thread", e.to_string()))?;
        
        Ok(Self {
            config,
            schema_cache,
            inner: Arc::new(tokio::sync::Mutex::new(Inner::default())),
            actor_tx: tx,
            circuit_breaker,
            flush_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_FLUSHES)),
        })
    }

    /// Initializes the Python client via the Actor.
    async fn ensure_initialized(&self) -> EtlResult<()> {
        // Fast path check without messaging
        // NOTE: We cannot check actor state directly. 
        // We rely on the fact that if we successfully sent Initialize, it's done or in progress.
        // But for idempotency, our Actor handles repeated Initialize calls.
        
        // We can just call Initialize every time? No, overhead.
        // We should track locally if we THOUGHT we initialized it.
        // But `Inner` tracks tables. 
        // Let's assume initialized if we can send the message?
        // Actually, let's keep it simple: Just send Initialize. The actor will ignore if already done.
        // But we want to avoid message overhead.
        // For now, let's trust the actor to be fast on repeated init.
        // OR better: Assume initialized if we have processed at least 1 thing?
        // Let's stick to: Always send Initialize for now, optimize later if needed.
        // WAIT: heavily used? called once per batch.
        
        // Optimally, we can check a local AtomicBool.
        // let initialized = self.initialized.load(Ordering::Relaxed);
        // if initialized { return Ok(()); }

        // Check circuit breaker before attempting connection
        if !self.circuit_breaker.should_allow_request().await {
            metrics::circuit_breaker_rejected("snowflake");
            return Err(etl_error!(ErrorKind::Unknown, "Circuit breaker open", "Snowflake service unavailable"));
        }

        let (tx, rx) = oneshot::channel();
        self.actor_tx.send(ActorMessage::Initialize(self.config.clone(), tx)).await
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Actor closed", e.to_string()))?;

        match tokio::time::timeout(PYTHON_OPERATION_TIMEOUT, rx).await {
             Ok(Ok(Ok(()))) => {
                 self.circuit_breaker.record_success().await;
                 Ok(())
             }
             Ok(Ok(Err(e))) => {
                 self.circuit_breaker.record_failure().await;
                 Err(e)
             }
             Ok(Err(_)) => {
                 self.circuit_breaker.record_failure().await;
                 Err(etl_error!(ErrorKind::Unknown, "Actor dropped response channel"))
             }
             Err(_) => {
                 self.circuit_breaker.record_failure().await;
                  metrics::snowflake_error("init_timeout");
                 Err(etl_error!(ErrorKind::Unknown, "Initialization timeout"))
             }
        }
    }

    /// Convert TableRows to Arrow RecordBatch for zero-copy transfer to Python.
    /// This eliminates the double allocation issue where data was converted to 
    /// HashMap<String, JsonValue> in Rust and then again to PyDict row-by-row.
    /// Convert TableRows to Arrow RecordBatch for zero-copy transfer to Python.
    /// This eliminates the double allocation issue where data was converted to 
    /// HashMap<String, JsonValue> in Rust and then again to PyDict row-by-row.
    fn to_arrow_batch(
        rows: &[TableRow],
        col_names: &[String],
    ) -> EtlResult<RecordBatch> {
        let num_rows = rows.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(col_names.len());
        let mut fields: Vec<Field> = Vec::with_capacity(col_names.len());

        for (i, col_name) in col_names.iter().enumerate() {
            // Determine column type based on first non-null value
            let first_non_null = rows.iter().find_map(|r| {
                match r.values.get(i) {
                    Some(Cell::Null) | None => None,
                    Some(cell) => Some(cell),
                }
            });

            // Create appropriate builder based on data type
            let array: ArrayRef = match first_non_null {
                Some(Cell::I16(_)) => {
                    let mut builder = Int16Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::I16(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::I32(_)) => {
                    let mut builder = Int32Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::I32(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::I64(_)) => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::I64(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::U32(_)) => {
                    let mut builder = UInt32Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::U32(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::F32(_)) => {
                    let mut builder = Float32Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::F32(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::F64(_)) => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::F64(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::Bool(_)) => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::Bool(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::Date(_)) => {
                    let mut builder = Date32Builder::with_capacity(num_rows);
                    // Epoch is 1970-01-01
                    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::Date(v)) => {
                                let days = v.signed_duration_since(epoch).num_days() as i32;
                                builder.append_value(days);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::Timestamp(_)) => {
                    let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::Timestamp(v)) => builder.append_value(v.and_utc().timestamp_micros()),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                Some(Cell::Time(_)) => {
                    // Time64Microsecond stores time as microseconds since midnight
                    let mut builder = Time64MicrosecondBuilder::with_capacity(num_rows);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::Time(v)) => {
                                let micros = v.num_seconds_from_midnight() as i64 * 1_000_000 + v.nanosecond() as i64 / 1000;
                                builder.append_value(micros);
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                /*
                Some(Cell::Bytes(_)) => {
                     let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
                     for row in rows {
                         match row.values.get(i) {
                             Some(Cell::Bytes(v)) => builder.append_value(v),
                             _ => builder.append_null(),
                         }
                     }
                     Arc::new(builder.finish())
                }
                */
                // For strings, complex types, and mixed types, fallback to string serialization
                // Optimized to avoid intermediate serde_json::Value allocation
                _ => {
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for row in rows {
                        match row.values.get(i) {
                            Some(Cell::Null) | None => builder.append_null(),
                            Some(cell) => {
                                match cell {
                                    Cell::String(s) => builder.append_value(s),
                                    Cell::Uuid(u) => builder.append_value(u.to_string()), 
                                    Cell::Numeric(n) => builder.append_value(n.to_string()),
                                    Cell::Json(j) => builder.append_value(j.to_string()),
                                    Cell::Array(_) => {
                                        // Array still needs JSON conversion for now, or recursive builder?
                                        // Keeping it simple: fallback to full JSON stringify for arrays
                                        let json = Self::cell_to_json(cell);
                                        match json {
                                            JsonValue::String(s) => builder.append_value(&s),
                                            _ => builder.append_value(json.to_string()),
                                        }
                                    }
                                    _ => {
                                        let json = Self::cell_to_json(cell);
                                        match json {
                                            JsonValue::String(s) => builder.append_value(&s),
                                            JsonValue::Null => builder.append_null(),
                                            other => builder.append_value(other.to_string()),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Arc::new(builder.finish())
                }
            };

            fields.push(Field::new(col_name, array.data_type().clone(), true));
            columns.push(array);
        }

        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), columns)
            .map_err(|e| etl_error!(ErrorKind::Unknown, "Failed to create Arrow batch", e.to_string()))
    }

    /// Insert Arrow batch into Snowflake landing table (zero-copy to Python).
    /// Insert Arrow batch into Snowflake landing table (zero-copy to Python).
    async fn insert_arrow_batch(
        &self,
        table_name: &str,
        batch: RecordBatch,
        operation: &str,
    ) -> EtlResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let timer = metrics::Timer::start();
        let row_count = batch.num_rows() as u64;
        let batch_bytes = batch.get_array_memory_size() as u64;

        self.ensure_initialized().await?;

        // Acquire semaphore permit to limit concurrent Python flushes
        // This prevents spawn_blocking thread pool saturation under high load
        let _permit = self.flush_semaphore.acquire().await
            .map_err(|_| etl_error!(ErrorKind::Unknown, "Semaphore closed"))?;

        let (tx, rx) = oneshot::channel();
        self.actor_tx.send(ActorMessage::InsertArrowBatch {
            table_name: table_name.to_string(),
            batch,
            operation: operation.to_string(),
            respond_to: tx,
        }).await.map_err(|e| etl_error!(ErrorKind::Unknown, "Actor closed", e.to_string()))?;

        // Wrap response wait with timeout
        // Wrap response wait with timeout
        match tokio::time::timeout(PYTHON_OPERATION_TIMEOUT, rx).await {
            Ok(Ok(Ok(()))) => {
                 // Success
                 self.circuit_breaker.record_success().await;
            }
            Ok(Ok(Err(e))) => {
                 self.circuit_breaker.record_failure().await;
                 metrics::snowflake_error("insert_arrow");
                 return Err(e);
            }
            Ok(Err(_)) => {
                 self.circuit_breaker.record_failure().await;
                 metrics::snowflake_error("insert_arrow_dropped");
                 return Err(etl_error!(ErrorKind::Unknown, "Actor dropped response channel"));
            }
            Err(_) => {
                self.circuit_breaker.record_failure().await;
                metrics::snowflake_error("insert_arrow_timeout");
                return Err(etl_error!(ErrorKind::Unknown, "Python operation timeout", format!("Insert timed out after {:?}", PYTHON_OPERATION_TIMEOUT)));
            }
        }

        // Record Snowflake metrics
        metrics::snowflake_request("success");
        metrics::snowflake_request_duration(timer.elapsed_secs());
        metrics::snowflake_rows_inserted(table_name, row_count);
        metrics::snowflake_bytes_processed(batch_bytes);

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

    /// Ensure table is initialized in Snowflake.
    /// Uses query_table_schema to get accurate column info including nullable constraints,
    /// which are not included in PostgreSQL logical replication Relation messages.
    async fn ensure_table_initialized(
        &self,
        table_name: &str,
        table_id: TableId,
    ) -> EtlResult<()> {
        // Fast path: check if already initialized (holding lock only briefly)
        {
            let inner = self.inner.lock().await;
            if inner.initialized_tables.contains(table_name) {
                return Ok(());
            }
        }
        // Lock is dropped here, allowing other threads to check status for other tables
        // or the same table (idempotency handled by downstream systems or accepted race)

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
        let (tx, rx) = oneshot::channel();
        self.actor_tx.send(ActorMessage::EnsureTableInitialized {
            table_name: table_name.to_string(),
            columns,
            pk_columns,
            respond_to: tx,
        }).await.map_err(|e| etl_error!(ErrorKind::Unknown, "Actor closed", e.to_string()))?;

        match tokio::time::timeout(PYTHON_OPERATION_TIMEOUT, rx).await {
            Ok(Ok(Ok(()))) => {
                 let mut inner = self.inner.lock().await;
                 inner.initialized_tables.insert(table_name.to_string());
                 metrics::snowflake_table_initialized(table_name);
                 info!("Snowflake table initialized: {}", table_name);
                 self.circuit_breaker.record_success().await;
                 Ok(())
            }
            Ok(Ok(Err(e))) => {
                self.circuit_breaker.record_failure().await;
                Err(e)
            },
            Ok(Err(_)) => {
                self.circuit_breaker.record_failure().await;
                Err(etl_error!(ErrorKind::Unknown, "Actor dropped response channel"))
            },
            Err(_) => {
                self.circuit_breaker.record_failure().await;
                metrics::snowflake_error("table_init_timeout");
                Err(etl_error!(ErrorKind::Unknown, "Initialization timeout"))
            },
        }
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
            info!(
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
            } else {
                debug!("Found {} columns for table {}.{}", columns.len(), pub_table.schema_name, pub_table.table_name);
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

        let total_tables = table_defs.len();
        let all_table_names: Vec<String> = table_defs.iter()
            .filter_map(|t| t.get("name").and_then(|n| n.as_str()).map(String::from))
            .collect();

        // Call Actor to initialize tables
        let (tx, rx) = oneshot::channel();
        self.actor_tx.send(ActorMessage::EnsureTablesFromPublication {
            table_defs,
            respond_to: tx,
        }).await.map_err(|e| etl_error!(ErrorKind::Unknown, "Actor closed", e.to_string()))?;

        // Wait for response with timeout
        let created_tables = tokio::time::timeout(PYTHON_OPERATION_TIMEOUT, rx)
            .await
            .map_err(|_| etl_error!(ErrorKind::Unknown, "Batch init timeout"))?
            .map_err(|_| etl_error!(ErrorKind::Unknown, "Actor dropped channel"))??;

        // Update inner state
        {
            let mut inner = self.inner.lock().await;
            for name in &all_table_names {
                inner.initialized_tables.insert(name.clone());
            }
        }

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

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        info!("Truncate requested but disabled by user configuration - skipping truncation.");
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let table_name = self.get_table_name(table_id).await;
        info!("Writing {} rows to Snowflake table {} (Arrow)", rows.len(), table_name);

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

        // Generate column names if missing
        let effective_col_names = if col_names.is_empty() {
            if let Some(first) = rows.first() {
                (0..first.values.len())
                    .map(|i| format!("column_{}", i))
                    .collect()
            } else {
                vec![]
            }
        } else {
            col_names
        };

        // Convert to Arrow RecordBatch (zero-copy to Python)
        let batch = Self::to_arrow_batch(&rows, &effective_col_names)?;
        
        let batch_size_bytes = batch.get_array_memory_size() as u64;
        metrics::events_bytes_processed("insert", batch_size_bytes);
        metrics::events_processed("insert", rows.len() as u64);

        self.insert_arrow_batch(&table_name, batch, "INSERT").await
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        let timer = metrics::Timer::start();
        let batch_size = events.len();

        // Record batch size
        metrics::events_batch_size(batch_size);

        // Process Relation events first - these contain schema info for new tables
        for event in &events {
            if let Event::Relation(r) = event {
                info!(
                    "Received Relation event for table '{}' (OID: {}, columns: {})",
                    r.table_schema.name,
                    r.table_schema.id.0,
                    r.table_schema.column_schemas.len()
                );
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

        // Group events by table - using TableRow directly for zero-copy Arrow conversion
        // Key: TableId, Value: Vec<(TableRow, operation_type)>
        let mut table_rows: HashMap<TableId, Vec<(TableRow, String)>> = HashMap::new();
        
        // Also collect column names per table for Arrow batch creation
        let mut table_col_names: HashMap<TableId, Vec<String>> = HashMap::new();

        for event in &events {
            match event {
                Event::Insert(i) => {
                    // Cache column names if not already cached
                    if !table_col_names.contains_key(&i.table_id) {
                        let col_names: Vec<String> = if let Some(s) = schemas.get(&i.table_id) {
                            s.column_schemas.iter().map(|c| c.name.clone()).collect()
                        } else if let Some(names) = self.schema_cache.get_column_names(i.table_id).await {
                            names
                        } else {
                            // Generate column names based on row values count
                            (0..i.table_row.values.len())
                                .map(|idx| format!("column_{}", idx))
                                .collect()
                        };
                        table_col_names.insert(i.table_id, col_names);
                    }

                    table_rows
                        .entry(i.table_id)
                        .or_default()
                        .push((i.table_row.clone(), "INSERT".to_string()));
                }
                Event::Update(u) => {
                    // Cache column names if not already cached
                    if !table_col_names.contains_key(&u.table_id) {
                        let col_names: Vec<String> = if let Some(s) = schemas.get(&u.table_id) {
                            s.column_schemas.iter().map(|c| c.name.clone()).collect()
                        } else if let Some(names) = self.schema_cache.get_column_names(u.table_id).await {
                            names
                        } else {
                            (0..u.table_row.values.len())
                                .map(|idx| format!("column_{}", idx))
                                .collect()
                        };
                        table_col_names.insert(u.table_id, col_names);
                    }

                    table_rows
                        .entry(u.table_id)
                        .or_default()
                        .push((u.table_row.clone(), "UPDATE".to_string()));
                }
                Event::Delete(d) => {
                    if let Some((_, old_row)) = &d.old_table_row {
                        // Cache column names if not already cached
                        if !table_col_names.contains_key(&d.table_id) {
                            let col_names: Vec<String> = if let Some(s) = schemas.get(&d.table_id) {
                                s.column_schemas.iter().map(|c| c.name.clone()).collect()
                            } else if let Some(names) = self.schema_cache.get_column_names(d.table_id).await {
                                names
                            } else {
                                (0..old_row.values.len())
                                    .map(|idx| format!("column_{}", idx))
                                    .collect()
                            };
                            table_col_names.insert(d.table_id, col_names);
                        }

                        table_rows
                            .entry(d.table_id)
                            .or_default()
                            .push((old_row.clone(), "DELETE".to_string()));
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

        // Process grouped events in parallel for better throughput using Arrow zero-copy
        let table_count = table_rows.len();
        if table_count > 0 {
            info!("Processing {} tables in parallel (Arrow zero-copy)", table_count);
            let parallel_timer = metrics::Timer::start();
            
            use futures::future::try_join_all;
            
            let futures: Vec<_> = table_rows
                .into_iter()
                .map(|(table_id, rows_with_ops)| {
                    let self_clone = self.clone();
                    let col_names = table_col_names.get(&table_id).cloned().unwrap_or_default();
                    
                    async move {
                        let table_name = self_clone.get_table_name(table_id).await;

                        // Ensure table is initialized
                        self_clone.ensure_table_initialized(&table_name, table_id).await?;

                        // Group by operation type - collect TableRows directly
                        let mut insert_rows: Vec<TableRow> = Vec::new();
                        let mut update_rows: Vec<TableRow> = Vec::new();
                        let mut delete_rows: Vec<TableRow> = Vec::new();

                        for (row, op) in rows_with_ops {
                            match op.as_str() {
                                "INSERT" => insert_rows.push(row),
                                "UPDATE" => update_rows.push(row),
                                "DELETE" => delete_rows.push(row),
                                _ => {}
                            }
                        }

                        // Process INSERTs with Arrow zero-copy
                        if !insert_rows.is_empty() {
                            let batch = Self::to_arrow_batch(&insert_rows, &col_names)?;
                            let batch_bytes = batch.get_array_memory_size() as u64;
                            metrics::events_bytes_processed("insert", batch_bytes);
                            metrics::snowflake_bytes_processed(batch_bytes);
                            metrics::events_processed("insert", insert_rows.len() as u64);
                            self_clone.insert_arrow_batch(&table_name, batch, "INSERT").await?;
                        }
                        
                        // Process UPDATEs with Arrow zero-copy
                        if !update_rows.is_empty() {
                            let batch = Self::to_arrow_batch(&update_rows, &col_names)?;
                            let batch_bytes = batch.get_array_memory_size() as u64;
                            metrics::events_bytes_processed("update", batch_bytes);
                            metrics::snowflake_bytes_processed(batch_bytes);
                            metrics::events_processed("update", update_rows.len() as u64);
                            self_clone.insert_arrow_batch(&table_name, batch, "UPDATE").await?;
                        }
                        
                        // Process DELETEs with Arrow zero-copy
                        if !delete_rows.is_empty() {
                            let batch = Self::to_arrow_batch(&delete_rows, &col_names)?;
                            let batch_bytes = batch.get_array_memory_size() as u64;
                            metrics::events_bytes_processed("delete", batch_bytes);
                            metrics::snowflake_bytes_processed(batch_bytes);
                            metrics::events_processed("delete", delete_rows.len() as u64);
                            self_clone.insert_arrow_batch(&table_name, batch, "DELETE").await?;
                        }
                        
                        Ok::<(), etl::error::EtlError>(())
                    }
                })
                .collect();
            
            // Wait for all table operations to complete
            try_join_all(futures).await?;
            
            let parallel_elapsed = parallel_timer.elapsed_secs();
            metrics::parallel_batch_processing(table_count, parallel_elapsed);
            info!("Parallel batch processing completed in {:.2}s for {} tables (Arrow zero-copy)", parallel_elapsed, table_count);
        }

        metrics::events_processing_duration(timer.elapsed_secs());
        Ok(())
    }
}

/// Convert JsonValue to Python object.
fn json_to_py(py: pyo3::Python<'_>, value: &JsonValue) -> EtlResult<pyo3::Py<pyo3::PyAny>> {
    use pyo3::types::{PyDict, PyList, PyDictMethods, PyListMethods};
    use pyo3::IntoPyObject;

    match value {
        JsonValue::Null => Ok(py.None()),
        JsonValue::Bool(b) => {
            let py_bool = b.into_pyobject(py).unwrap();
            Ok(py_bool.to_owned().into_any().unbind())
        }
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                let py_int = i.into_pyobject(py).unwrap();
                Ok(py_int.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                let py_float = f.into_pyobject(py).unwrap();
                Ok(py_float.into_any().unbind())
            } else {
                let py_str = n.to_string().into_pyobject(py).unwrap();
                Ok(py_str.into_any().unbind())
            }
        }
        JsonValue::String(s) => {
            let py_str = s.clone().into_pyobject(py).unwrap();
            Ok(py_str.into_any().unbind())
        }
        JsonValue::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                py_list.append(json_to_py(py, item)?)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }
            Ok(py_list.unbind().into())
        }
        JsonValue::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                py_dict.set_item(k, json_to_py(py, v)?)
                    .map_err(|e| etl_error!(ErrorKind::Unknown, "Python error", e.to_string()))?;
            }
            Ok(py_dict.unbind().into())
        }
    }
}
