//! Circuit breaker implementation for fault tolerance.
//!
//! Protects against cascading failures by monitoring error rates and
//! temporarily halting requests when a service is failing.
//!
//! Performance optimization (PERF-02):
//! - Uses atomic state for lock-free checks in the Closed state
//! - Only acquires locks when transitioning states (failure detected)

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Circuit breaker states (atomic-friendly representation)
const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
const STATE_HALF_OPEN: u8 = 2;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests flow normally
    Closed,
    /// Circuit is open - requests are immediately rejected
    Open,
    /// Circuit is half-open - testing if service recovered
    HalfOpen,
}

impl From<u8> for CircuitState {
    fn from(val: u8) -> Self {
        match val {
            STATE_CLOSED => CircuitState::Closed,
            STATE_OPEN => CircuitState::Open,
            STATE_HALF_OPEN => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

impl From<CircuitState> for u8 {
    fn from(state: CircuitState) -> Self {
        match state {
            CircuitState::Closed => STATE_CLOSED,
            CircuitState::Open => STATE_OPEN,
            CircuitState::HalfOpen => STATE_HALF_OPEN,
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,
    /// Time window for counting failures (seconds)
    pub failure_window_secs: u64,
    /// How long to wait before attempting recovery (seconds)
    pub recovery_timeout_secs: u64,
    /// Number of successful requests needed in half-open state
    pub success_threshold: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            failure_window_secs: 60,
            recovery_timeout_secs: 30,
            success_threshold: 2,
        }
    }
}

impl CircuitBreakerConfig {
    /// Helper to get recovery timeout as Duration
    #[allow(dead_code)]
    pub fn recovery_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.recovery_timeout_secs)
    }
}

/// Get current time as milliseconds since UNIX epoch
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Atomic circuit breaker state (PERF-02 optimization)
/// All fields are atomic for lock-free access in the hot path
struct AtomicCircuitState {
    /// Current state (0=Closed, 1=Open, 2=HalfOpen)
    state: AtomicU8,
    /// Number of consecutive failures
    failure_count: AtomicU32,
    /// Number of successes in half-open state
    success_count: AtomicU32,
    /// Timestamp of last failure (millis since epoch)
    last_failure_time_ms: AtomicU64,
    /// Timestamp of last state change (millis since epoch)
    last_state_change_ms: AtomicU64,
    /// Reference instant for calculating elapsed time
    #[allow(dead_code)]
    start_instant: Instant,
    /// Start time in millis for correlation
    #[allow(dead_code)]
    start_millis: u64,
}

impl AtomicCircuitState {
    fn new() -> Self {
        let now = now_millis();
        Self {
            state: AtomicU8::new(STATE_CLOSED),
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            last_failure_time_ms: AtomicU64::new(0),
            last_state_change_ms: AtomicU64::new(now),
            start_instant: Instant::now(),
            start_millis: now,
        }
    }

    /// Get elapsed seconds since last state change
    fn elapsed_since_state_change(&self) -> u64 {
        let last_change = self.last_state_change_ms.load(Ordering::Acquire);
        let now = now_millis();
        if now > last_change {
            (now - last_change) / 1000
        } else {
            0
        }
    }

    /// Get elapsed seconds since last failure
    fn elapsed_since_last_failure(&self) -> u64 {
        let last_failure = self.last_failure_time_ms.load(Ordering::Acquire);
        if last_failure == 0 {
            return u64::MAX; // Never failed
        }
        let now = now_millis();
        if now > last_failure {
            (now - last_failure) / 1000
        } else {
            0
        }
    }
}

/// Circuit breaker implementation with atomic state (PERF-02 fix)
/// 
/// Uses lock-free operations for the hot path (Closed state checks)
/// Only uses atomic compare-and-swap for state transitions
#[derive(Clone)]
pub struct CircuitBreaker {
    state: std::sync::Arc<AtomicCircuitState>,
    config: CircuitBreakerConfig,
    name: String,
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field("name", &self.name)
            .field("state", &self.get_state_sync())
            .field("failure_count", &self.state.failure_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration
    pub fn with_config(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            state: std::sync::Arc::new(AtomicCircuitState::new()),
            config,
            name: name.into(),
        }
    }

    /// Get current state synchronously (for debugging)
    fn get_state_sync(&self) -> CircuitState {
        self.state.state.load(Ordering::Acquire).into()
    }

    /// Check if request should be allowed (PERF-02 optimized - lock-free hot path)
    pub async fn should_allow_request(&self) -> bool {
        let state = self.state.state.load(Ordering::Acquire);

        match state {
            STATE_CLOSED => {
                // Fast path: lock-free check, always allow
                true
            }
            STATE_OPEN => {
                // Check if recovery timeout has elapsed
                let elapsed = self.state.elapsed_since_state_change();
                if elapsed >= self.config.recovery_timeout_secs {
                    // Try to transition to half-open using CAS
                    let result = self.state.state.compare_exchange(
                        STATE_OPEN,
                        STATE_HALF_OPEN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    
                    if result.is_ok() {
                        self.state.success_count.store(0, Ordering::Release);
                        self.state.last_state_change_ms.store(now_millis(), Ordering::Release);
                        info!(
                            "Circuit breaker '{}' transitioning to half-open after {} seconds",
                            self.name, elapsed
                        );
                    }
                    true
                } else {
                    false
                }
            }
            STATE_HALF_OPEN => true,
            _ => true, // Unknown state, allow
        }
    }

    /// Record a successful request
    pub async fn record_success(&self) {
        let state = self.state.state.load(Ordering::Acquire);

        match state {
            STATE_CLOSED => {
                // Reset failure count on success (atomic)
                self.state.failure_count.store(0, Ordering::Release);
                self.state.last_failure_time_ms.store(0, Ordering::Release);
            }
            STATE_HALF_OPEN => {
                let new_count = self.state.success_count.fetch_add(1, Ordering::AcqRel) + 1;
                if new_count >= self.config.success_threshold {
                    // Try to close the circuit using CAS
                    let result = self.state.state.compare_exchange(
                        STATE_HALF_OPEN,
                        STATE_CLOSED,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    
                    if result.is_ok() {
                        info!(
                            "Circuit breaker '{}' closing after {} successful requests",
                            self.name, new_count
                        );
                        self.state.failure_count.store(0, Ordering::Release);
                        self.state.success_count.store(0, Ordering::Release);
                        self.state.last_failure_time_ms.store(0, Ordering::Release);
                        self.state.last_state_change_ms.store(now_millis(), Ordering::Release);
                    }
                }
            }
            STATE_OPEN => {
                // Should not happen, but reset on success anyway
                self.state.failure_count.store(0, Ordering::Release);
                self.state.last_failure_time_ms.store(0, Ordering::Release);
            }
            _ => {}
        }
    }

    /// Record a failed request
    pub async fn record_failure(&self) {
        let state = self.state.state.load(Ordering::Acquire);
        let now = now_millis();

        match state {
            STATE_CLOSED => {
                // Check if we're still within the failure window
                let elapsed = self.state.elapsed_since_last_failure();
                
                let new_count = if elapsed > self.config.failure_window_secs {
                    // Reset counter if outside window
                    self.state.failure_count.store(1, Ordering::Release);
                    1
                } else {
                    self.state.failure_count.fetch_add(1, Ordering::AcqRel) + 1
                };

                self.state.last_failure_time_ms.store(now, Ordering::Release);

                // Check if we should open the circuit
                if new_count >= self.config.failure_threshold {
                    let result = self.state.state.compare_exchange(
                        STATE_CLOSED,
                        STATE_OPEN,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    
                    if result.is_ok() {
                        warn!(
                            "Circuit breaker '{}' opening after {} failures",
                            self.name, new_count
                        );
                        self.state.last_state_change_ms.store(now, Ordering::Release);
                        crate::metrics::circuit_breaker_state_change(&self.name, "open");
                    }
                }
            }
            STATE_HALF_OPEN => {
                // Any failure in half-open state reopens the circuit
                let result = self.state.state.compare_exchange(
                    STATE_HALF_OPEN,
                    STATE_OPEN,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                
                if result.is_ok() {
                    warn!(
                        "Circuit breaker '{}' reopening after failure in half-open state",
                        self.name
                    );
                    self.state.success_count.store(0, Ordering::Release);
                    self.state.failure_count.fetch_add(1, Ordering::AcqRel);
                    self.state.last_failure_time_ms.store(now, Ordering::Release);
                    self.state.last_state_change_ms.store(now, Ordering::Release);
                    crate::metrics::circuit_breaker_state_change(&self.name, "open");
                }
            }
            STATE_OPEN => {
                // Already open, just update failure time
                self.state.last_failure_time_ms.store(now, Ordering::Release);
            }
            _ => {}
        }
    }

    /// Get current circuit state
    #[allow(dead_code)]
    pub async fn get_state(&self) -> CircuitState {
        self.state.state.load(Ordering::Acquire).into()
    }

    /// Execute a function with circuit breaker protection
    #[allow(dead_code)]
    pub async fn execute<F, T, E>(&self, f: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        if !self.should_allow_request().await {
            return Err(CircuitBreakerError::CircuitOpen);
        }

        match f.await {
            Ok(result) => {
                self.record_success().await;
                Ok(result)
            }
            Err(e) => {
                self.record_failure().await;
                Err(CircuitBreakerError::OperationFailed(e))
            }
        }
    }
}

/// Circuit breaker error type
#[derive(Debug)]
#[allow(dead_code)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, request rejected
    CircuitOpen,
    /// Operation failed
    OperationFailed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::OperationFailed(e) => write!(f, "Operation failed: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::CircuitOpen => None,
            CircuitBreakerError::OperationFailed(e) => Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_allows_requests() {
        let cb = CircuitBreaker::new("test");
        assert!(cb.should_allow_request().await);
        assert_eq!(cb.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            failure_window_secs: 60,
            recovery_timeout_secs: 1,
            success_threshold: 2,
        };
        let cb = CircuitBreaker::with_config("test", config);

        // Record 3 failures
        for _ in 0..3 {
            cb.record_failure().await;
        }

        assert_eq!(cb.get_state().await, CircuitState::Open);
        assert!(!cb.should_allow_request().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new("test");
        
        // Record some failures
        cb.record_failure().await;
        cb.record_failure().await;
        
        // Record success - should reset
        cb.record_success().await;
        
        assert_eq!(cb.state.failure_count.load(Ordering::Relaxed), 0);
    }
}
