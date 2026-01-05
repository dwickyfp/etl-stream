use reqwest::Client;
use serde_json::{json, Value as JsonValue};
use std::time::Duration;
use tracing::{info, warn};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::types::{Event, TableId, TableRow, TableSchema};
use etl::{bail, etl_error};

use crate::schema_cache::SchemaCache;

#[derive(Debug, Clone)]
pub struct HttpDestination {
    client: Client,
    base_url: String,
    schema_cache: SchemaCache,
}

impl HttpDestination {
    pub fn new(base_url: String, schema_cache: SchemaCache) -> EtlResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| etl_error!(ErrorKind::Unknown, "HTTP client error", source: e))?;
        Ok(Self {
            client,
            base_url,
            schema_cache,
        })
    }

    async fn post(&self, path: &str, body: serde_json::Value) -> EtlResult<()> {
        let url = format!("{}/{}", self.base_url.trim_end_matches('/'), path);

        for attempt in 1..=3 {
            match self.client.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) if resp.status().is_client_error() => {
                    bail!(ErrorKind::Unknown, "Client error", resp.status());
                }
                Ok(resp) => warn!("Attempt {}/3: status {}", attempt, resp.status()),
                Err(e) => warn!("Attempt {}/3: {}", attempt, e),
            }
            if attempt < 3 {
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
        bail!(ErrorKind::Unknown, "Request failed after retries");
    }

    /// Parse a debug-formatted Datum value and extract the raw value
    /// Handles PostgreSQL types including the rust_decimal::Decimal internal format
    fn parse_datum_value(debug_str: &str) -> JsonValue {
        if debug_str == "Null" {
            return JsonValue::Null;
        }

        if let Some(paren_start) = debug_str.find('(') {
            let type_name = &debug_str[..paren_start];
            let value_part = &debug_str[paren_start + 1..debug_str.len() - 1];

            match type_name {
                "Bool" => return json!(value_part == "true"),
                "I16" | "I32" | "I64" => {
                    if let Ok(n) = value_part.parse::<i64>() {
                        return json!(n);
                    }
                }
                "F32" | "F64" => {
                    if let Ok(n) = value_part.parse::<f64>() {
                        return json!(n);
                    }
                }
                "String" | "Uuid" => {
                    let unquoted = value_part.trim_matches('"');
                    return json!(unquoted);
                }
                "Timestamp" | "TimestampTz" | "Date" | "Time" => return json!(value_part),
                "Numeric" => {
                    // Handle rust_decimal::Decimal debug format:
                    // Value { sign: Positive, weight: 0, scale: 2, digits: [39, 7800] }
                    if value_part.starts_with("Value {") {
                        return Self::parse_rust_decimal(value_part);
                    }
                    // Fallback: simple numeric string
                    if let Ok(n) = value_part.parse::<f64>() {
                        return json!(n);
                    }
                    return json!(value_part);
                }
                "Json" | "JsonB" => {
                    if let Ok(parsed) = serde_json::from_str::<JsonValue>(value_part) {
                        return parsed;
                    }
                    return json!(value_part);
                }
                _ => return json!(value_part),
            }
        }

        json!(debug_str)
    }

    /// Parse rust_decimal::Decimal debug format and convert to JSON number
    /// Format: Value { sign: Positive, weight: 0, scale: 2, digits: [39, 7800] }
    fn parse_rust_decimal(value_str: &str) -> JsonValue {
        // Extract sign
        let is_negative = value_str.contains("sign: Negative");

        // Extract scale
        let scale: u32 = Self::extract_field(value_str, "scale:")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Extract digits array
        let digits: Vec<u64> = Self::extract_digits(value_str);

        if digits.is_empty() {
            return json!(0);
        }

        // Reconstruct the integer value from base-10000 digits
        // Each digit represents a value 0-9999 in base 10000
        let mut int_value: u128 = 0;
        for digit in &digits {
            int_value = int_value * 10000 + (*digit as u128);
        }

        // Apply scale (divide by 10^scale) and format as string for precision
        if scale == 0 {
            let result = if is_negative {
                -(int_value as i128)
            } else {
                int_value as i128
            };
            // Return as number if it fits in f64 safely
            if result.abs() <= (1i128 << 53) {
                return json!(result as f64);
            }
            return json!(result.to_string());
        }

        // Format with decimal point
        let int_str = int_value.to_string();
        let decimal_str = if int_str.len() <= scale as usize {
            // Need leading zeros after decimal point
            let zeros = scale as usize - int_str.len();
            format!("0.{}{}", "0".repeat(zeros), int_str)
        } else {
            let decimal_pos = int_str.len() - scale as usize;
            format!("{}.{}", &int_str[..decimal_pos], &int_str[decimal_pos..])
        };

        let final_str = if is_negative {
            format!("-{}", decimal_str)
        } else {
            decimal_str
        };

        // Parse as f64 and return as JSON number
        if let Ok(n) = final_str.parse::<f64>() {
            return json!(n);
        }

        // Fallback to string for very large numbers
        json!(final_str)
    }

    /// Extract a simple field value from the debug string
    fn extract_field(s: &str, field: &str) -> Option<String> {
        let start = s.find(field)?;
        let after = &s[start + field.len()..];
        let trimmed = after.trim_start();
        let end = trimmed.find(|c: char| c == ',' || c == ' ' || c == '}')?;
        Some(trimmed[..end].to_string())
    }

    /// Extract digits array from debug string
    fn extract_digits(s: &str) -> Vec<u64> {
        let start = match s.find("digits: [") {
            Some(pos) => pos + 9,
            None => return vec![],
        };
        let end = match s[start..].find(']') {
            Some(pos) => start + pos,
            None => return vec![],
        };
        
        s[start..end]
            .split(',')
            .filter_map(|d| d.trim().parse::<u64>().ok())
            .collect()
    }

    /// Convert a TableRow to a JSON object with column names as keys
    fn row_to_json(&self, schema: Option<&TableSchema>, row: &TableRow) -> JsonValue {
        match schema {
            Some(s) => {
                let mut obj = serde_json::Map::new();
                for (i, value) in row.values.iter().enumerate() {
                    let col_name = s.column_schemas.get(i)
                        .map(|c| c.name.clone())
                        .unwrap_or_else(|| format!("column_{}", i));
                    let debug_str = format!("{:?}", value);
                    obj.insert(col_name, Self::parse_datum_value(&debug_str));
                }
                JsonValue::Object(obj)
            }
            None => {
                warn!("No schema available, using column indices as keys");
                let mut obj = serde_json::Map::new();
                for (i, value) in row.values.iter().enumerate() {
                    let debug_str = format!("{:?}", value);
                    obj.insert(format!("column_{}", i), Self::parse_datum_value(&debug_str));
                }
                JsonValue::Object(obj)
            }
        }
    }
}

impl Destination for HttpDestination {
    fn name() -> &'static str {
        "http"
    }

    async fn truncate_table(&self, table_id: TableId) -> EtlResult<()> {
        info!("Truncating table {}", table_id.0);
        Ok(())
    }

    async fn write_table_rows(&self, table_id: TableId, rows: Vec<TableRow>) -> EtlResult<()> {
        if rows.is_empty() {
            return Ok(());
        }
        info!("Writing {} rows to table {}", rows.len(), table_id.0);
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        if events.is_empty() {
            return Ok(());
        }

        // First, process Relation events to update the SHARED schema cache
        for event in &events {
            if let Event::Relation(r) = event {
                self.schema_cache.store(r.table_schema.id, r.table_schema.clone()).await;
                info!("Stored schema for table {} ({}) in shared cache", r.table_schema.id.0, r.table_schema.name);
            }
        }

        let schemas = self.schema_cache.read().await;

        // Build row events with table names instead of OIDs
        let mut row_events: Vec<JsonValue> = Vec::new();

        for event in &events {
            match event {
                Event::Insert(i) => {
                    let schema = schemas.get(&i.table_id);
                    let table_name = self.schema_cache.get_table_name(i.table_id.0).await;
                    row_events.push(json!({
                        "type": "insert",
                        "table": table_name,
                        "row": self.row_to_json(schema.map(|s| s.as_ref()), &i.table_row)
                    }));
                }
                Event::Update(u) => {
                    let schema = schemas.get(&u.table_id);
                    let table_name = self.schema_cache.get_table_name(u.table_id.0).await;
                    row_events.push(json!({
                        "type": "update",
                        "table": table_name,
                        "old_row": u.old_table_row.as_ref().map(|r| self.row_to_json(schema.map(|s| s.as_ref()), &r.1)),
                        "new_row": self.row_to_json(schema.map(|s| s.as_ref()), &u.table_row)
                    }));
                }
                Event::Delete(d) => {
                    let schema = schemas.get(&d.table_id);
                    let table_name = self.schema_cache.get_table_name(d.table_id.0).await;
                    row_events.push(json!({
                        "type": "delete",
                        "table": table_name,
                        "old_row": d.old_table_row.as_ref().map(|r| self.row_to_json(schema.map(|s| s.as_ref()), &r.1))
                    }));
                }
                _ => {} // Skip Begin, Commit, Relation, Truncate, Unsupported
            }
        }

        drop(schemas);

        if row_events.is_empty() {
            return Ok(());
        }

        info!("Writing {} row events (insert/update/delete)", row_events.len());

        let payload = json!({
            "events": row_events
        });

        self.post("events", payload).await
    }
}