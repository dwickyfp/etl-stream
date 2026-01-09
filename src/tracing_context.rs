//! Distributed tracing context for correlation IDs across services.
//!
//! This module provides trace context propagation for following requests
//! through the ETL pipeline from Rust -> Python -> Snowflake.

use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Trace context for distributed tracing
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TraceContext {
    /// Unique trace ID for the entire operation
    pub trace_id: String,
    /// Span ID for the current operation
    pub span_id: String,
    /// Parent span ID (if any)
    pub parent_span_id: Option<String>,
    /// Operation name
    pub operation: String,
}

impl TraceContext {
    /// Create a new root trace context
    #[allow(dead_code)]
    pub fn new_root(operation: impl Into<String>) -> Self {
        Self {
            trace_id: Uuid::new_v4().to_string(),
            span_id: Uuid::new_v4().to_string(),
            parent_span_id: None,
            operation: operation.into(),
        }
    }

    /// Create a child span from this context
    #[allow(dead_code)]
    pub fn child(&self, operation: impl Into<String>) -> Self {
        Self {
            trace_id: self.trace_id.clone(),
            span_id: Uuid::new_v4().to_string(),
            parent_span_id: Some(self.span_id.clone()),
            operation: operation.into(),
        }
    }

    /// Get trace context as string for logging
    #[allow(dead_code)]
    pub fn to_log_string(&self) -> String {
        format!("[trace_id={} span_id={}]", self.trace_id, self.span_id)
    }
}

/// Thread-safe trace context storage
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TraceContextHolder {
    context: Arc<RwLock<Option<TraceContext>>>,
}

impl TraceContextHolder {
    /// Create a new empty context holder
    pub fn new() -> Self {
        Self {
            context: Arc::new(RwLock::new(None)),
        }
    }

    /// Set the current trace context
    #[allow(dead_code)]
    pub async fn set(&self, ctx: TraceContext) {
        let mut guard = self.context.write().await;
        *guard = Some(ctx);
    }

    /// Get the current trace context
    #[allow(dead_code)]
    pub async fn get(&self) -> Option<TraceContext> {
        let guard = self.context.read().await;
        guard.clone()
    }

    /// Clear the current trace context
    #[allow(dead_code)]
    pub async fn clear(&self) {
        let mut guard = self.context.write().await;
        *guard = None;
    }
}

impl Default for TraceContextHolder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trace_context_creation() {
        let ctx = TraceContext::new_root("test_operation");
        assert_eq!(ctx.operation, "test_operation");
        assert!(ctx.parent_span_id.is_none());
        assert!(!ctx.trace_id.is_empty());
        assert!(!ctx.span_id.is_empty());
    }

    #[test]
    fn test_child_span() {
        let parent = TraceContext::new_root("parent");
        let child = parent.child("child");
        
        assert_eq!(child.trace_id, parent.trace_id);
        assert_ne!(child.span_id, parent.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id));
        assert_eq!(child.operation, "child");
    }

    #[tokio::test]
    async fn test_context_holder() {
        let holder = TraceContextHolder::new();
        assert!(holder.get().await.is_none());

        let ctx = TraceContext::new_root("test");
        holder.set(ctx.clone()).await;
        
        let retrieved = holder.get().await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().trace_id, ctx.trace_id);

        holder.clear().await;
        assert!(holder.get().await.is_none());
    }
}
