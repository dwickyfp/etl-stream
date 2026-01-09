//! Configuration validation module with comprehensive error messages
//!
//! Provides validation for all configuration parameters with specific,
//! actionable error messages.

use std::error::Error;
use std::path::Path;

/// Validate Snowflake account format
#[allow(dead_code)]
pub fn validate_snowflake_account(account: &str) -> Result<(), Box<dyn Error>> {
    if account.trim().is_empty() {
        return Err("Snowflake account cannot be empty".into());
    }
    
    // Account should be in format: <account_name> or <account_name>.<region> or <account_name>.<region>.<cloud>
    let parts: Vec<&str> = account.split('.').collect();
    
    if parts.is_empty() || parts[0].is_empty() {
        return Err("Invalid Snowflake account format. Expected: <account_name>[.<region>[.<cloud>]]".into());
    }
    
    // Check for invalid characters
    for part in parts {
        if !part.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            return Err(format!(
                "Invalid characters in Snowflake account '{}'. Only alphanumeric, hyphens, and underscores allowed",
                account
            ).into());
        }
    }
    
    Ok(())
}

/// Validate database name
pub fn validate_database_name(name: &str, name_type: &str) -> Result<(), Box<dyn Error>> {
    if name.trim().is_empty() {
        return Err(format!("{} name cannot be empty", name_type).into());
    }
    
    if name.len() > 255 {
        return Err(format!("{} name too long (max 255 characters): {}", name_type, name.len()).into());
    }
    
    // Check for SQL injection patterns
    let dangerous_patterns = ["--", "/*", "*/", ";", "DROP", "DELETE", "TRUNCATE"];
    for pattern in &dangerous_patterns {
        if name.to_uppercase().contains(pattern) {
            return Err(format!("{} name contains potentially dangerous pattern: {}", name_type, pattern).into());
        }
    }
    
    Ok(())
}

/// Validate file path exists and is readable
#[allow(dead_code)]
pub fn validate_file_path(path: &str, description: &str) -> Result<(), Box<dyn Error>> {
    let path_obj = Path::new(path);
    
    if !path_obj.exists() {
        return Err(format!("{} does not exist: {}", description, path).into());
    }
    
    if !path_obj.is_file() {
        return Err(format!("{} is not a file: {}", description, path).into());
    }
    
    // Check read permissions (basic check)
    if let Err(e) = std::fs::metadata(path) {
        return Err(format!("Cannot access {}: {}", description, e).into());
    }
    
    Ok(())
}

/// Validate port number with specific range
pub fn validate_port(port: u16, service: &str) -> Result<(), Box<dyn Error>> {
    if port == 0 {
        return Err(format!("{} port cannot be 0", service).into());
    }
    
    // Warn about privileged ports
    if port < 1024 {
        eprintln!("WARNING: {} port {} is in privileged range (< 1024), may require elevated permissions", service, port);
    }
    
    Ok(())
}

/// Validate hostname/IP format
pub fn validate_host(host: &str, service: &str) -> Result<(), Box<dyn Error>> {
    if host.trim().is_empty() {
        return Err(format!("{} hostname cannot be empty", service).into());
    }
    
    // Check for localhost variants that might be misconfigurations
    if host == "localhost" || host == "127.0.0.1" || host == "::1" {
        eprintln!("INFO: {} using localhost ({}), ensure this is correct for your deployment", service, host);
    }
    
    // Basic validation - contains valid characters
    if !host.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == ':' || c == '_') {
        return Err(format!("Invalid characters in {} hostname: {}", service, host).into());
    }
    
    Ok(())
}

/// Validate time interval (not zero, within reasonable range)
pub fn validate_interval(
    interval: u64,
    name: &str,
    min: u64,
    max: u64,
) -> Result<(), Box<dyn Error>> {
    if interval == 0 {
        return Err(format!("{} must be greater than 0", name).into());
    }
    
    if interval < min {
        return Err(format!(
            "{} value {} is too small (minimum: {})",
            name, interval, min
        ).into());
    }
    
    if interval > max {
        return Err(format!(
            "{} value {} is too large (maximum: {})",
            name, interval, max
        ).into());
    }
    
    // Warn if value seems unusual
    if interval < min * 2 {
        eprintln!(
            "WARNING: {} value {} is very aggressive (recommended minimum: {})",
            name, interval, min * 2
        );
    }
    
    Ok(())
}

/// Validate threshold values (warning < danger)
pub fn validate_thresholds(
    warning: u64,
    danger: u64,
    name: &str,
) -> Result<(), Box<dyn Error>> {
    if warning == 0 {
        return Err(format!("{} warning threshold must be greater than 0", name).into());
    }
    
    if danger == 0 {
        return Err(format!("{} danger threshold must be greater than 0", name).into());
    }
    
    if danger <= warning {
        return Err(format!(
            "{} danger threshold ({}) must be greater than warning threshold ({})",
            name, danger, warning
        ).into());
    }
    
    // Warn if thresholds are too close
    if danger < warning * 2 {
        eprintln!(
            "WARNING: {} danger threshold ({}) is less than 2x warning threshold ({}). Consider increasing the gap.",
            name, danger, warning
        );
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_snowflake_account() {
        assert!(validate_snowflake_account("myaccount").is_ok());
        assert!(validate_snowflake_account("myaccount.us-east-1").is_ok());
        assert!(validate_snowflake_account("myaccount.us-east-1.aws").is_ok());
        assert!(validate_snowflake_account("my-account").is_ok());
        
        assert!(validate_snowflake_account("").is_err());
        assert!(validate_snowflake_account("account@test").is_err());
    }

    #[test]
    fn test_validate_database_name() {
        assert!(validate_database_name("test_db", "Database").is_ok());
        assert!(validate_database_name("MyDatabase123", "Database").is_ok());
        
        assert!(validate_database_name("", "Database").is_err());
        assert!(validate_database_name("db--test", "Database").is_err());
        assert!(validate_database_name("db; DROP TABLE", "Database").is_err());
    }

    #[test]
    fn test_validate_port() {
        assert!(validate_port(5432, "PostgreSQL").is_ok());
        assert!(validate_port(8080, "HTTP").is_ok());
        
        assert!(validate_port(0, "Test").is_err());
    }

    #[test]
    fn test_validate_interval() {
        assert!(validate_interval(10, "Poll Interval", 1, 3600).is_ok());
        assert!(validate_interval(0, "Poll Interval", 1, 3600).is_err());
        assert!(validate_interval(5000, "Poll Interval", 1, 3600).is_err());
    }

    #[test]
    fn test_validate_thresholds() {
        assert!(validate_thresholds(100, 200, "WAL").is_ok());
        assert!(validate_thresholds(0, 100, "WAL").is_err());
        assert!(validate_thresholds(100, 100, "WAL").is_err());
        assert!(validate_thresholds(100, 50, "WAL").is_err());
    }
}
