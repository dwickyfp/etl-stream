//! Health check module for monitoring system health.
//!
//! Provides HTTP endpoints for Kubernetes liveness and readiness probes.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};
use etl::store::state::StateStore;

use crate::store::redis_store::RedisStore;

/// Health status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub checks: HealthChecks,
}

/// Individual health checks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthChecks {
    pub database: CheckStatus,
    pub redis: CheckStatus,
    pub pipelines: PipelineHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckStatus {
    pub healthy: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineHealth {
    pub active: usize,
    pub healthy: bool,
}

/// Shared application state for health checks
#[derive(Clone)]
pub struct AppState {
    pub config_pool: PgPool,
    pub redis_store: RedisStore,
    pub active_pipeline_count: Arc<RwLock<usize>>,
}

impl AppState {
    pub fn new(
        config_pool: PgPool,
        redis_store: RedisStore,
        active_pipeline_count: Arc<RwLock<usize>>,
    ) -> Self {
        Self {
            config_pool,
            redis_store,
            active_pipeline_count,
        }
    }
}

/// Health check handler - comprehensive health check
async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db_check = check_database(&state.config_pool).await;
    let redis_check = check_redis(&state.redis_store).await;
    let pipeline_count = *state.active_pipeline_count.read().await;

    let pipeline_health = PipelineHealth {
        active: pipeline_count,
        healthy: true, // Can add more sophisticated checks here
    };

    let all_healthy = db_check.healthy && redis_check.healthy;

    let status = HealthStatus {
        status: if all_healthy {
            "healthy".to_string()
        } else {
            "unhealthy".to_string()
        },
        checks: HealthChecks {
            database: db_check,
            redis: redis_check,
            pipelines: pipeline_health,
        },
    };

    let status_code = if all_healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status_code, Json(status))
}

/// Readiness check - lightweight check for Kubernetes readiness probe
async fn ready_handler(State(state): State<AppState>) -> impl IntoResponse {
    // Quick check - just verify we can reach critical services
    let db_ok = sqlx::query("SELECT 1")
        .fetch_optional(&state.config_pool)
        .await
        .is_ok();

    if db_ok {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Liveness check - always returns OK unless process is completely dead
async fn liveness_handler() -> impl IntoResponse {
    (StatusCode::OK, "alive")
}

/// Check database connectivity
async fn check_database(pool: &PgPool) -> CheckStatus {
    match sqlx::query("SELECT 1").fetch_optional(pool).await {
        Ok(_) => CheckStatus {
            healthy: true,
            message: None,
        },
        Err(e) => CheckStatus {
            healthy: false,
            message: Some(format!("Database error: {}", e)),
        },
    }
}

/// Check Redis connectivity
async fn check_redis(store: &RedisStore) -> CheckStatus {
    // Try to get table mappings count as a health check
    match store.get_table_mappings().await {
        Ok(_) => CheckStatus {
            healthy: true,
            message: None,
        },
        Err(e) => CheckStatus {
            healthy: false,
            message: Some(format!("Redis error: {}", e)),
        },
    }
}

/// Create health check router
pub fn health_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route("/ready", get(ready_handler))
        .route("/liveness", get(liveness_handler))
        .with_state(state)
}

/// Start health check server on specified port
pub async fn start_health_server(state: AppState, port: u16) {
    let app = health_router(state);
    let addr = format!("[::]:{}", port);

    info!("Health check server starting on {}", addr);

    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to bind health server to {}: {}", addr, e);
            return;
        }
    };

    if let Err(e) = axum::serve(listener, app).await {
        error!("Health server error: {}", e);
    }
}
