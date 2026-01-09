//! Circuit breaker implementation for fault tolerance.
//!
//! Protects against cascading failures by monitoring error rates and
//! temporarily halting requests when a service is failing.

use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{info, warn};

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

/// Circuit breaker implementation
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitBreakerState>>,
    config: CircuitBreakerConfig,
    name: String,
}

#[derive(Debug)]
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    last_state_change: Instant,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_config(name, CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration
    pub fn with_config(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
                last_state_change: Instant::now(),
            })),
            config,
            name: name.into(),
        }
    }

    /// Check if request should be allowed
    pub async fn should_allow_request(&self) -> bool {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if recovery timeout has elapsed
                let elapsed = state.last_state_change.elapsed();
                if elapsed.as_secs() >= self.config.recovery_timeout_secs {
                    info!(
                        "Circuit breaker '{}' transitioning to half-open after {} seconds",
                        self.name,
                        elapsed.as_secs()
                    );
                    state.state = CircuitState::HalfOpen;
                    state.success_count = 0;
                    state.last_state_change = Instant::now();
                    true
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful request
    pub async fn record_success(&self) {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => {
                // Reset failure count on success
                state.failure_count = 0;
                state.last_failure_time = None;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                if state.success_count >= self.config.success_threshold {
                    info!(
                        "Circuit breaker '{}' closing after {} successful requests",
                        self.name, state.success_count
                    );
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.last_failure_time = None;
                    state.last_state_change = Instant::now();
                }
            }
            CircuitState::Open => {
                // Should not happen, but reset on success anyway
                state.failure_count = 0;
                state.last_failure_time = None;
            }
        }
    }

    /// Record a failed request
    pub async fn record_failure(&self) {
        let mut state = self.state.write().await;
        let now = Instant::now();

        match state.state {
            CircuitState::Closed => {
                // Check if we're still within the failure window
                if let Some(last_failure) = state.last_failure_time {
                    let elapsed = now.duration_since(last_failure);
                    if elapsed.as_secs() > self.config.failure_window_secs {
                        // Reset counter if outside window
                        state.failure_count = 1;
                    } else {
                        state.failure_count += 1;
                    }
                } else {
                    state.failure_count = 1;
                }

                state.last_failure_time = Some(now);

                // Check if we should open the circuit
                if state.failure_count >= self.config.failure_threshold {
                    warn!(
                        "Circuit breaker '{}' opening after {} failures",
                        self.name, state.failure_count
                    );
                    state.state = CircuitState::Open;
                    state.last_state_change = now;
                    
                    // Record metric
                    crate::metrics::circuit_breaker_state_change(&self.name, "open");
                }
            }
            CircuitState::HalfOpen => {
                warn!(
                    "Circuit breaker '{}' reopening after failure in half-open state",
                    self.name
                );
                state.state = CircuitState::Open;
                state.success_count = 0;
                state.failure_count += 1;
                state.last_failure_time = Some(now);
                state.last_state_change = now;
                
                // Record metric
                crate::metrics::circuit_breaker_state_change(&self.name, "open");
            }
            CircuitState::Open => {
                // Already open, just update failure time
                state.last_failure_time = Some(now);
            }
        }
    }

    /// Get current circuit state
    #[allow(dead_code)]
    pub async fn get_state(&self) -> CircuitState {
        let state = self.state.read().await;
        state.state
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
