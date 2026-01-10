# Codebase Audit & Performance Optimization Directive

## 1. Executive Summary

The `etl-stream` repository demonstrates a solid architectural foundation using modern Rust async patterns (`tokio`, `sqlx`) and a bridging strategy to Python for Snowflake integration. The use of `arrow-rs` for zero-copy data transfer is a significant performance asset.

However, the system faces **critical security risks** regarding private key handling and **significant performance bottlenecks** due to lock contention in the pipeline manager and potential resource leaks in the WAL monitor.

**Codebase Health Score:**
- **Performance**: 7/10 (Good async baselines, but heavy lock contention and Python I/O overhead)
- **Security**: 4/10 (CRITICAL: Private keys written to disk)
- **Maintainability**: 8/10 (Clean modular structure, strong typing, good metrics)

---

## 2. Critical Issues (Blockers)

### 🚨 [SECURITY] Private Key Written to Disk
- **Severity**: **CRITICAL**
- **Location**: `etl-snowflake-py/etl_snowflake/client.py` lines 140-200 (`_create_profile_json`)
- **Impact**: Compromise of Snowflake credentials. The application writes unencrypted private keys to a `profile.json` file on disk to satisfy the Snowpipe Streaming SDK. This creates a massive attack surface (leak via backups, logging, or file system access).

### 🐢 [PERFORMANCE] Global Lock Contention in Pipeline Sync
- **Severity**: **HIGH**
- **Location**: `src/pipeline_manager.rs` lines 177-180 & 209-225
- **Impact**: System-wide stalls. The `sync_pipelines` function holds a read lock on `running_pipelines` while performing iterations that may involve logging or other overheads. More importantly, write locks for starting/stopping pipelines block all readers, potentially stalling health checks and metrics gathering.

### 🩸 [RELIABILITY] Connection Pool Leaks in WAL Monitor
- **Severity**: **MEDIUM**
- **Location**: `src/wal_monitor.rs` lines 180-218 (`get_or_create_pool`)
- **Impact**: Database resource exhaustion. The monitor creates new connection pools dynamically. If the cleanup logic in `cleanup_removed_source_pools` is not reached (e.g., due to an early error return in `check_all_sources`), pools remain active, leaking connections until the service restarts.

---

## 3. Detailed Technical Audit

### [PERF-01] Blocking Operations in Async Locking
- **Location**: `src/pipeline_manager.rs` (Various)
- **Category**: Concurrency Model
- **Severity**: Medium
- **Risk Analysis**: While `tokio::sync::RwLock` is used, the critical sections inside `sync_pipelines` iterate over all pipelines. As the number of pipelines grows, the efficient `join_all` strategies are bottlenecked by the initial lock acquisition and scalar processing.
- **Remediation**: 
    - Shard the `running_pipelines` map (e.g., `DashMap` or multiple `RwLock` buckets) to reduce contention.
    - Snapshot the state for iteration instead of holding the lock during logic processing.

### [PERF-02] Python I/O Overhead in Sync Wrappers
- **Location**: `src/destination/snowflake_destination.rs` & `etl_snowflake/client.py`
- **Category**: Foreign Function Interface (FFI) Performance
- **Severity**: Medium
- **Risk Analysis**: The Rust code correctly uses `task::spawn_blocking` to wrap Python calls. However, `client.py` methods like `insert_rows` perform synchronous network I/O. If many pipelines flush simultaneously, the `spawn_blocking` thread pool (default 512 threads) could become saturated, leading to thread exhaustion and increased latency.
- **Remediation**: ensure the Python side uses async-capable libraries where possible, or strictly limit concurrent flushes in Rust via a `Semaphore` that matches the blocking thread pool capacity.

### [PERF-03] Inefficient JSON Fallback
- **Location**: `src/destination/snowflake_destination.rs` lines 477-505 (`cell_to_json`)
- **Category**: Allocation Overhead
- **Severity**: Low
- **Risk Analysis**: The system has an excellent Arrow zero-copy path. However, the fallback `insert_rows_internal` converts every `Cell` to a `serde_json::Value` (heap allocation) and then to a Python object (another allocation).
- **Remediation**: Deprecate `insert_rows_internal` entirely. Enforce Arrow-based ingestion for all data paths to guarantee zero-copy performance.

### [SEC-01] Unencrypted Key Material in Memory
- **Location**: `src/config.rs` & `Memory`
- **Category**: Data Protection
- **Severity**: High
- **Risk Analysis**: Private keys are loaded into strings and passed around. If a core dump occurs or swap is used, keys are visible.
- **Remediation**: Use `secrecy` crate (or `Zeroize` trait) for `private_key` and contents to ensure they are zeroed out when dropped and not printed in debug logs.

### [LOGIC-01] Potential Race in Schema Cache Creation
- **Location**: `src/pipeline_manager.rs` line 345
- **Category**: Race Condition
- **Severity**: Low
- **Risk Analysis**: The double-checked locking pattern is implemented manually. While valid, it is complex and prone to subtle bugs if the lock dropping/re-acquiring order is changed during refactoring.
- **Remediation**: Use `arc_swap` or `once_cell` patterns for cleaner lazy initialization of shared resources.

---

## 4. Refactoring Roadmap

To maximize ROI on performance and safety, proceed in this order:

1.  **[P0] Fix Security Criticals**:
    -   Modify `SnowflakeClient` in Python to accept private keys as bytes/buffer effectively, or use a named pipe/memfd if the SDK absolutely demands a file path (to avoid writing to disk). *Note: The Snowflake Ingest SDK might require a file path, requiring a patch or a temporary secure file harness.*

2.  **[P1] Optimize Pipeline Manager Locking**:
    -   Refactor `PipelineManager` to use a detailed "Command Pattern" or "Actor Model" (via channels) for pipeline updates instead of a giant shared `RwLock` map. This removes the contention point entirely.

3.  **[P2] Harden WAL Monitor**:
    -   Implement RAII wrappers for `PgPool` in the monitor to ensure connections are closed automatically when the source is removed from the configuration, regardless of control flow errors.

4.  **[P3] Enforce Zero-Copy**:
    -   Remove `insert_rows_internal` and `cell_to_json`. Consolidate all ingestion on the Arrow path.
