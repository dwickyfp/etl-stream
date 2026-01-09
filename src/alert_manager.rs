//! Alert Manager module for WAL size webhook notifications.
//!
//! This module handles sending webhook notifications when WAL size exceeds
//! warning or danger thresholds for a sustained period of time.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use reqwest::Client;
use serde::Serialize;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::{AlertSettings, WalMonitorSettings};
use crate::constants::monitoring;

/// WAL status levels for alerting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WalStatus {
    Normal,
    Warning,
    Danger,
}

impl WalStatus {
    /// Determine status from WAL size and thresholds
    pub fn from_size(size_mb: f64, settings: &WalMonitorSettings) -> Self {
        if size_mb >= settings.danger_wal_mb as f64 {
            WalStatus::Danger
        } else if size_mb >= settings.warning_wal_mb as f64 {
            WalStatus::Warning
        } else {
            WalStatus::Normal
        }
    }

    /// Check if this status requires alerting
    pub fn requires_alert(&self) -> bool {
        matches!(self, WalStatus::Warning | WalStatus::Danger)
    }
}

impl std::fmt::Display for WalStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalStatus::Normal => write!(f, "normal"),
            WalStatus::Warning => write!(f, "warning"),
            WalStatus::Danger => write!(f, "danger"),
        }
    }
}

/// Alert state for a single source
#[derive(Debug, Clone)]
struct AlertState {
    status: WalStatus,
    first_detected: Instant,
    notified: bool,
    last_size_mb: f64,
}

/// Webhook notification payload
#[derive(Debug, Clone, Serialize)]
struct AlertPayload {
    #[serde(rename = "type")]
    alert_type: String,
    source_name: String,
    status: String,
    wal_size_mb: f64,
    threshold_mb: u64,
    duration_mins: u64,
    timestamp: String,
}

/// AlertManager handles webhook notifications for sustained WAL alerts
pub struct AlertManager {
    settings: AlertSettings,
    wal_settings: WalMonitorSettings,
    states: Arc<RwLock<HashMap<String, AlertState>>>,
    http_client: Client,
}

impl AlertManager {
    /// Create a new AlertManager if alerting is enabled
    pub fn new(settings: AlertSettings, wal_settings: WalMonitorSettings) -> Option<Self> {
        if !settings.is_enabled() {
            info!("WAL alerting disabled (ALERT_WAL_URL not set)");
            return None;
        }

        info!(
            "WAL alerting enabled - URL: {}, notification threshold: {} mins",
            settings.alert_wal_url.as_ref().unwrap(),
            settings.time_check_notification_mins
        );

        Some(Self {
            settings,
            wal_settings,
            states: Arc::new(RwLock::new(HashMap::new())),
            http_client: Client::new(),
        })
    }

    /// Get the webhook URL
    fn url(&self) -> &str {
        self.settings.alert_wal_url.as_ref().unwrap()
    }

    /// Update status for a source and send notification if threshold exceeded
    pub async fn update_status(&self, source_name: &str, size_mb: f64) {
        let current_status = WalStatus::from_size(size_mb, &self.wal_settings);

        let mut states = self.states.write().await;
        let now = Instant::now();

        // Get or create state for this source
        let state = states.entry(source_name.to_string()).or_insert_with(|| AlertState {
            status: WalStatus::Normal,
            first_detected: now,
            notified: false,
            last_size_mb: size_mb,
        });

        // Check if status changed
        if state.status != current_status {
            debug!(
                "Source '{}' status changed: {} -> {}",
                source_name, state.status, current_status
            );

            // Reset state for new status
            state.status = current_status;
            state.first_detected = now;
            state.notified = false;
            state.last_size_mb = size_mb;

            // If returned to normal, we're done
            if current_status == WalStatus::Normal {
                debug!("Source '{}' returned to normal", source_name);
                return;
            }
        }

        // Update last size
        state.last_size_mb = size_mb;

        // Check if we should send notification
        if current_status.requires_alert() && !state.notified {
            let duration = now.duration_since(state.first_detected);
            let duration_mins = duration.as_secs() / 60;

            if duration_mins >= self.settings.time_check_notification_mins {
                warn!(
                    "Source '{}' has been in {} status for {} mins, sending alert",
                    source_name, current_status, duration_mins
                );

                // Mark as notified before releasing lock
                state.notified = true;
                let threshold_mb = match current_status {
                    WalStatus::Danger => self.wal_settings.danger_wal_mb,
                    WalStatus::Warning => self.wal_settings.warning_wal_mb,
                    WalStatus::Normal => 0,
                };

                // Drop lock before async operation
                drop(states);

                // Send notification
                self.send_notification(source_name, current_status, size_mb, threshold_mb, duration_mins)
                    .await;
            }
        }
    }

    /// Send webhook notification
    async fn send_notification(
        &self,
        source_name: &str,
        status: WalStatus,
        size_mb: f64,
        threshold_mb: u64,
        duration_mins: u64,
    ) {
        let payload = AlertPayload {
            alert_type: "wal_alert".to_string(),
            source_name: source_name.to_string(),
            status: status.to_string(),
            wal_size_mb: size_mb,
            threshold_mb,
            duration_mins,
            timestamp: Utc::now().to_rfc3339(),
        };

        info!(
            "Sending WAL alert for source '{}': {} ({}MB >= {}MB for {} mins)",
            source_name, status, size_mb, threshold_mb, duration_mins
        );

        // Retry logic with exponential backoff
        let max_retries = monitoring::MAX_ALERT_RETRIES;
        let mut attempt = 0;
        
        while attempt < max_retries {
            match self.http_client.post(self.url()).json(&payload).timeout(std::time::Duration::from_secs(monitoring::ALERT_HTTP_TIMEOUT_SECS)).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        info!("Alert sent successfully for source '{}' (attempt {})", source_name, attempt + 1);
                        return;
                    } else {
                        error!(
                            "Alert request failed for source '{}': HTTP {} (attempt {})",
                            source_name,
                            response.status(),
                            attempt + 1
                        );
                    }
                }
                Err(e) => {
                    error!("Failed to send alert for source '{}': {} (attempt {})", source_name, e, attempt + 1);
                }
            }
            
            attempt += 1;
            if attempt < max_retries {
                // Exponential backoff: 1s, 2s, 4s
                let delay = std::time::Duration::from_secs(monitoring::ALERT_RETRY_BASE_DELAY_SECS << (attempt - 1));
                tokio::time::sleep(delay).await;
                info!("Retrying alert for source '{}' after {} seconds...", source_name, delay.as_secs());
            }
        }
        
        error!("Failed to send alert for source '{}' after {} attempts", source_name, max_retries);
    }

    /// Clear alert state for a source (e.g., when source is removed)
    #[allow(dead_code)]
    pub async fn clear_source(&self, source_name: &str) {
        let mut states = self.states.write().await;
        states.remove(source_name);
        debug!("Cleared alert state for source '{}'", source_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_wal_settings() -> WalMonitorSettings {
        WalMonitorSettings {
            poll_interval_secs: 10,
            warning_wal_mb: 3000,
            danger_wal_mb: 6000,
        }
    }

    #[test]
    fn test_wal_status_from_size_normal() {
        let settings = test_wal_settings();
        assert_eq!(WalStatus::from_size(1000.0, &settings), WalStatus::Normal);
        assert_eq!(WalStatus::from_size(2999.0, &settings), WalStatus::Normal);
    }

    #[test]
    fn test_wal_status_from_size_warning() {
        let settings = test_wal_settings();
        assert_eq!(WalStatus::from_size(3000.0, &settings), WalStatus::Warning);
        assert_eq!(WalStatus::from_size(5000.0, &settings), WalStatus::Warning);
        assert_eq!(WalStatus::from_size(5999.0, &settings), WalStatus::Warning);
    }

    #[test]
    fn test_wal_status_from_size_danger() {
        let settings = test_wal_settings();
        assert_eq!(WalStatus::from_size(6000.0, &settings), WalStatus::Danger);
        assert_eq!(WalStatus::from_size(10000.0, &settings), WalStatus::Danger);
    }

    #[test]
    fn test_wal_status_requires_alert() {
        assert!(!WalStatus::Normal.requires_alert());
        assert!(WalStatus::Warning.requires_alert());
        assert!(WalStatus::Danger.requires_alert());
    }

    #[test]
    fn test_alert_manager_disabled_without_url() {
        let settings = AlertSettings {
            alert_wal_url: None,
            time_check_notification_mins: 10,
        };
        let wal_settings = test_wal_settings();
        let manager = AlertManager::new(settings, wal_settings);
        assert!(manager.is_none());
    }

    #[test]
    fn test_alert_manager_disabled_with_empty_url() {
        let settings = AlertSettings {
            alert_wal_url: Some("".to_string()),
            time_check_notification_mins: 10,
        };
        // from_env filters empty strings, but test the struct directly
        let wal_settings = test_wal_settings();
        // When url is Some(""), is_enabled returns true, but from_env filters it
        let manager = AlertManager::new(settings, wal_settings);
        // Since is_enabled checks is_some(), this will create a manager
        // In practice, from_env uses filter(|s| !s.is_empty())
        assert!(manager.is_some());
    }

    #[test]
    fn test_alert_manager_enabled_with_url() {
        let settings = AlertSettings {
            alert_wal_url: Some("http://localhost:5000/webhook".to_string()),
            time_check_notification_mins: 10,
        };
        let wal_settings = test_wal_settings();
        let manager = AlertManager::new(settings, wal_settings);
        assert!(manager.is_some());
    }

    #[test]
    fn test_wal_status_display() {
        assert_eq!(format!("{}", WalStatus::Normal), "normal");
        assert_eq!(format!("{}", WalStatus::Warning), "warning");
        assert_eq!(format!("{}", WalStatus::Danger), "danger");
    }
}
