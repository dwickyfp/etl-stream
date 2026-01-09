//! Redis transaction support for atomic state updates
//!
//! This module provides transaction support using Redis MULTI/EXEC
//! to ensure atomic updates of table state and mappings.

use deadpool_redis::{redis::cmd, Pool};
use etl::error::{EtlResult, ErrorKind};
use etl::etl_error;
use etl::types::TableId;
use tracing::{debug, error, info};

/// Execute multiple Redis operations in a transaction
#[allow(dead_code)]
pub async fn execute_transaction(
    pool: &Pool,
    operations: Vec<RedisOperation>,
) -> EtlResult<()> {
    let mut conn = pool.get().await.map_err(|e| {
        etl_error!(
            ErrorKind::Unknown,
            "Failed to get Redis connection",
            e.to_string()
        )
    })?;

    // Start transaction
    let _: () = cmd("MULTI")
        .query_async(&mut conn)
        .await
        .map_err(|e| {
            etl_error!(
                ErrorKind::Unknown,
                "Failed to start Redis transaction",
                e.to_string()
            )
        })?;

    // Queue all operations
    for op in &operations {
        match op {
            RedisOperation::Set { key, value } => {
                let _: () = cmd("SET")
                    .arg(key)
                    .arg(value)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| {
                        etl_error!(
                            ErrorKind::Unknown,
                            "Failed to queue SET operation",
                            e.to_string()
                        )
                    })?;
            }
            RedisOperation::Del { key } => {
                let _: () = cmd("DEL")
                    .arg(key)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| {
                        etl_error!(
                            ErrorKind::Unknown,
                            "Failed to queue DEL operation",
                            e.to_string()
                        )
                    })?;
            }
        }
    }

    // Execute transaction
    let result: Result<Vec<String>, _> = cmd("EXEC").query_async(&mut conn).await;

    match result {
        Ok(_) => {
            debug!("Redis transaction completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Redis transaction failed: {}", e);
            Err(etl_error!(
                ErrorKind::Unknown,
                "Redis transaction execution failed",
                e.to_string()
            ))
        }
    }
}

/// Redis operation to be executed in a transaction
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RedisOperation {
    Set { key: String, value: String },
    Del { key: String },
}

/// Atomic update of table state and mapping
#[allow(dead_code)]
pub async fn atomic_update_table_state(
    pool: &Pool,
    key_prefix: &str,
    table_id: TableId,
    state_value: String,
    mapping_value: Option<String>,
) -> EtlResult<()> {
    let mut operations = vec![RedisOperation::Set {
        key: format!("{}:states:{}", key_prefix, table_id.0),
        value: state_value,
    }];

    if let Some(mapping) = mapping_value {
        operations.push(RedisOperation::Set {
            key: format!("{}:mappings:{}", key_prefix, table_id.0),
            value: mapping,
        });
    }

    info!(
        "Executing atomic Redis transaction for table {} ({} operations)",
        table_id.0,
        operations.len()
    );

    execute_transaction(pool, operations).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_operation_creation() {
        let op = RedisOperation::Set {
            key: "test_key".to_string(),
            value: "test_value".to_string(),
        };
        assert!(matches!(op, RedisOperation::Set { .. }));

        let del_op = RedisOperation::Del {
            key: "test_key".to_string(),
        };
        assert!(matches!(del_op, RedisOperation::Del { .. }));
    }
}
