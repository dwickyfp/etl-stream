# Data Flow Journey

This document details how data moves from the source PostgreSQL database to the destination Snowflake warehouse.

## 1. Traceability & CDC Capture
The system relies on PostgreSQL's **Logical Replication** feature.
1.  **Change Occurs**: An `INSERT`, `UPDATE`, or `DELETE` happens in the source PostgreSQL database.
2.  **WAL Entry**: PostgreSQL writes this change to its Write-Ahead Log (WAL).
3.  **pgoutput plugin**: The `pgoutput` plugin decodes this WAL entry into a logical replication stream.

## 2. Ingestion (Rust Layer)
The `PipelineManager` manages a connection to the replication slot.
1.  **Startup**: When a pipeline starts, it connects to the specific replication slot defined in the configuration.
2.  **Streaming**: It continuously polls the replication stream.
3.  **Decoding**: The binary stream is decoded into internal `TableRow` structs.

## 3. Buffering & Batching
To ensure high throughput, data is not sent row-by-row.
1.  **Batch Configuration**: Each pipeline has `batch_max_size` (rows) and `batch_max_fill_ms` (latency) settings.
2.  **Accumulation**: Rows are accumulated in an in-memory buffer.
3.  **Trigger**: When either the size limit or time limit is reached, the batch is "sealed" for processing.

## 4. Rust-Python Bridge (Zero-Copy)
This is a critical performance optimization step.
1.  **Arrow Conversion**: The accumulated `TableRow` structs are converted into an **Apache Arrow RecordBatch** directly in Rust.
2.  **Memory Sharing**: This RecordBatch is passed to the Python runtime via `pyo3`. Because Arrow memory layout is standardized, this operation is **Zero-Copy**—the data is not serialized/deserialized, just the pointer is shared.

## 5. Snowflake Loading (Python Layer)
The `SnowflakeDestination` (Rust) delegates the actual upload to the embedded Python environment.
1.  **Actor Pool**: The request is dispatched to available `SnowflakeActor` (running in a dedicated thread).
2.  **Snowpipe Streaming**: The Python code uses the **Snowflake Ingest SDK (Snowpipe Streaming)**.
3.  **Upload**: The Arrow batch is sent directly to Snowflake's internal stages/tables.
4.  **Commit**: Snowflake acknowledges the write.

## 6. State Commitment
Once Snowflake confirms the write:
1.  **Updates State**: The `PipelineManager` updates the Local/Redis state with the new LSN (Log Sequence Number).
2.  **Feedback**: The new LSN is sent back to PostgreSQL to acknowledge "consumption".
3.  **WAL Cleanup**: PostgreSQL can now safely delete the older WAL files (preventing disk growth).

## 7. Error Handling & Retry
*   **Transient Errors**: (e.g., Network blip) - The system retries with exponential backoff.
*   **Schema Mismatch**: If the destination table is missing columns, the system attempts to **Self-Heal** by altering the Snowflake table (adding columns) and then retrying the batch.
*   **Hard Failures**: If retries fail effectively, the circuit breaker opens to protect the system.
