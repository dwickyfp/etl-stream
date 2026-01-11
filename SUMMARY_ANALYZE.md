# Executive Summary

The `etl-stream` codebase represents a sophisticated hybrid Rust/Python ELT system designed for high throughput. It correctly utilizes asynchronous Rust (Tokio) for orchestration and Python (PyO3) for Snowpipe Streaming integration.

However, the system is currently severely limited by **concurrency anti-patterns** that negate the benefits of the async runtime. Critical critical paths are serialized by global write locks, and the "zero-copy" architecture is compromised by naive caching strategies.

**Overall Health Score:**
*   **Performance: 4/10** (Severe contention on hot paths)
*   **Security: 8/10** (Good secret management, secure defaults)
*   **Maintainability: 7/10** (Clean modular structure, decent error propagation)

---

# Critical Issues (Blockers)

1.  **Global Lock Contention (DoS Risk)**: The `WAL Monitor`, `Circuit Breaker`, and `Schema Cache` implementation impose global write locks on read-heavy hot paths. This effectively serializes the entire pipeline under load.
2.  **Resource Exhaustion (Memory/Connections)**: The `Pipeline Manager` defines a global connection semaphore but **never uses it**, allowing unbounded connection growth per source.
3.  **Algorithmic Complexity in Cache Eviction**: `SchemaCache` performs an `O(N log N)` sort operation *inside* a global write lock during cleanup, which will stall the runtime as cache size grows.

---

# Detailed Technical Audit

## Performance & Latency

### [PERF-01] Global Write Lock in WAL Monitor
**Location:** `src/wal_monitor.rs` (Lines 261, 292, 129)  
**Category:** Lock Contention  
**Severity:** **Critical**  
**Risk Analysis:**  
The `WalMonitor` uses `source_pools: Arc<RwLock<HashMap...>>` to manage connection pools.
*   `get_or_create_pool` takes a **write lock** on the *entire map* to check for existence or update LRU timestamps.
*   Since `check_all_sources` runs periodically for *all* sources, every source check contends for this single global lock.
*   Even "read" operations (fetching an existing pool) become "write" operations because of the naive LRU timestamp update.
**Business Impact:**  
As the number of sources increases, the WAL monitor execution time will grow exponentially due to lock contention, potentially causing it to miss polling intervals and failing to detect critical replication lag.
**Remediation:**  
1.  Use `DashMap` (concurrent hash map) for `source_pools` to allow concurrent access.
2.  Use atomic integers for LRU timestamps or use a specialized concurrent LRU cache (e.g., `moka`) instead of rolling a manual `HashMap` + timestamp solution.

### [PERF-02] Circuit Breaker Serializes All Requests
**Location:** `src/circuit_breaker.rs` (Line 94)  
**Category:** Lock Contention  
**Severity:** **High**  
**Risk Analysis:**  
`should_allow_request` acquires a **write lock** to check the state. This is the absolute hot path for every single data insertion.
*   Even in the healthy `Closed` state, every request forces exclusive access to the state struct.
*   This acts as a global mutex for the entire data plane.
**Business Impact:**  
Limits maximum throughput to the speed of lock acquisition/release, regardless of CPU cores or network bandwidth.
**Remediation:**  
1.  Use `AtomicUsize` for state tracking to allow lock-free checks in the `Closed` state.
2.  Only acquire a lock when transitioning states (failure detected).

### [PERF-03] O(N log N) Cache Eviction Inside Lock
**Location:** `src/schema_cache.rs` (Line 144)  
**Category:** Algorithmic Inefficiency  
**Severity:** **High**  
**Risk Analysis:**  
The `cleanup_expired` method sorts the entire cache access history (`MAX_CACHE_ENTRIES = 10,000`) to find LRU items.
*   It does this *while holding the global write lock*.
*   Sorting 10k items takes non-trivial CPU time, during which *no schema lookups can proceed*.
**Business Impact:**  
Periodic latency spikes (jitter) in the pipeline whenever cache cleanup triggers (every 5 minutes).
**Remediation:**  
1.  Replace the manual `HashMap` + `Vec` sorting with a proper Linked Hash Map or a dedicated caching crate like `moka` or `lru`.

### [PERF-04] Redis Store Lock Granularity
**Location:** `src/store/redis_store.rs` (Line 33)  
**Category:** Lock Contention  
**Severity:** **Medium**  
**Risk Analysis:**  
`tables` is protected by `Arc<Mutex<HashMap...>>`. A single Mutex guards the state of *all* tables.
*   While `RedisStore` is mostly for metadata, frequent updates from many pipelines will contend.
**Business Impact:**  
Increased latency for state updates.
**Remediation:**  
1.  Switch `Mutex` to `RwLock`.
2.  Shard the lock (e.g., `DashMap`, `scc::HashMap`) if table count is high (>1000).

## Resource Management & Safety

### [RES-01] Unenforced Global Connection Limit
**Location:** `src/pipeline_manager.rs` (Line 63)  
**Category:** Resource Exhaustion  
**Severity:** **Critical**  
**Risk Analysis:**  
`connection_semaphore` is initialized `Arc::new(Semaphore::new(100))` but is marked `#[allow(dead_code)]` and never used.
*   `create_source_pool` (Line 544) creates a new `PgPool` for every source.
*   Nothing limits the total number of sources or pools.
**Business Impact:**  
The application can easily exhaust file descriptors or database connection limits on the PostgreSQL server, causing a Denial of Service (DoS).
**Remediation:**  
1.  Pass the `connection_semaphore` into `start_pipeline`.
2.  Acquire a permit before calling `create_source_pool`.

### [RES-02] Python GIL Contention in Actor Pool
**Location:** `src/destination/snowflake_destination.rs` (Line 32)  
**Category:** Concurrency Model  
**Severity:** **Medium**  
**Risk Analysis:**  
The system spawns multiple threads (Actors) for Snowflake interaction, but they all share a single Python interpreter instance (via PyO3).
*   Python's Global Interpreter Lock (GIL) ensures only one thread executes Python bytecode at a time.
*   While I/O (network requests) releases the GIL, any data processing (schema inference, dict creation) is serialized.
**Business Impact:**  
Adding more actors might not scale throughput linearly and could increase context switching overhead.
**Remediation:**  
1.  This is a known architectural limit of PyO3. Ensure as much logic as possible (like Arrow conversion, which you have done) stays in Rust / released GIL.
2.  For true parallelism, use multiple processes or Python sub-interpreters (experimental).

## Security

### [SEC-01] Global Write Lock on Schema Cache Lookup
**Location:** `src/schema_cache.rs` (Line 212)  
**Category:** Denial of Service  
**Severity:** **Medium**  
**Risk Analysis:**  
`get_table_name` takes a write lock to update the LRU timestamp ("touch").
*   An attacker (or just high load) triggering many lookups will stall the system due to write-lock serialization.
**Remediation:**  
Use a distinct mutex for LRU tracking or use `AtomicU64` for last access time, allowing the data map to be accessed via Read lock.

---

# Refactoring Roadmap

This roadmap orders tasks by **Return on Investment (ROI)**—fixing the biggest blockers with the least effort first.

1.  **Fix Circuit Breaker & WalMonitor Locks (Day 1)**
    *   **Goal:** Restore parallelism to the hot path.
    *   **Action:**
        *   Change `CircuitBreaker` to use `AtomicUsize` or `RwLock` correctly (read first, write only on failure).
        *   Change `WalMonitor` to use `DashMap` or separate locks for pool access vs. pool creation.

2.  **Enforce Connection Limits (Day 1)**
    *   **Goal:** Prevent database crash from connection exhaustion.
    *   **Action:** Hook up the unused `connection_semaphore` in `PipelineManager`.

3.  **Optimize Cache Eviction (Day 2)**
    *   **Goal:** Eliminate periodic latency spikes.
    *   **Action:** Replace `SchemaCache` internals with `moka` crate (high performance concurrent cache) or remove the sorting logic in favor of a simpler random eviction or approximate LRU.

4.  **Enhance Redis Store Concurrency (Day 3)**
    *   **Goal:** Scale state management.
    *   **Action:** Replace `Mutex` with `RwLock` or `DashMap`.

5.  **Python Actor Optimization (Week 2)**
    *   **Goal:** Maximize throughput.
    *   **Action:** Profile GIL usage. Move `_infer_snowflake_type` or other heavy logic from Python to Rust if possible.
