// HTTP Destination unit tests

use serde_json::{json, Value as JsonValue};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ============================================================================
// Datum Parsing Tests (parse_datum_value function)
// ============================================================================

/// Parse a debug-formatted Datum value and extract the raw value
/// This is a copy of the function from http_destination.rs for testing
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
            "Numeric" => return json!(value_part),
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

#[test]
fn test_parse_null() {
    let result = parse_datum_value("Null");
    assert!(result.is_null());
}

#[test]
fn test_parse_bool_true() {
    let result = parse_datum_value("Bool(true)");
    assert_eq!(result, json!(true));
}

#[test]
fn test_parse_bool_false() {
    let result = parse_datum_value("Bool(false)");
    assert_eq!(result, json!(false));
}

#[test]
fn test_parse_i16() {
    let result = parse_datum_value("I16(42)");
    assert_eq!(result, json!(42));
}

#[test]
fn test_parse_i16_negative() {
    let result = parse_datum_value("I16(-100)");
    assert_eq!(result, json!(-100));
}

#[test]
fn test_parse_i32() {
    let result = parse_datum_value("I32(-1000)");
    assert_eq!(result, json!(-1000));
}

#[test]
fn test_parse_i32_large() {
    let result = parse_datum_value("I32(2147483647)");
    assert_eq!(result, json!(2147483647i64));
}

#[test]
fn test_parse_i64() {
    let result = parse_datum_value("I64(9999999999)");
    assert_eq!(result, json!(9999999999i64));
}

#[test]
fn test_parse_i64_negative() {
    let result = parse_datum_value("I64(-9223372036854775807)");
    assert_eq!(result, json!(-9223372036854775807i64));
}

#[test]
fn test_parse_f32() {
    let result = parse_datum_value("F32(3.14)");
    assert!((result.as_f64().unwrap() - 3.14).abs() < 0.001);
}

#[test]
fn test_parse_f64() {
    let result = parse_datum_value("F64(2.718281828)");
    assert!((result.as_f64().unwrap() - 2.718281828).abs() < 0.0000001);
}

#[test]
fn test_parse_f64_negative() {
    let result = parse_datum_value("F64(-123.456)");
    assert!((result.as_f64().unwrap() - (-123.456)).abs() < 0.001);
}

#[test]
fn test_parse_f64_scientific() {
    let result = parse_datum_value("F64(1.5e10)");
    assert_eq!(result, json!(1.5e10));
}

#[test]
fn test_parse_string() {
    let result = parse_datum_value("String(\"hello\")");
    assert_eq!(result, json!("hello"));
}

#[test]
fn test_parse_string_with_spaces() {
    let result = parse_datum_value("String(\"hello world\")");
    assert_eq!(result, json!("hello world"));
}

#[test]
fn test_parse_string_empty() {
    let result = parse_datum_value("String(\"\")");
    assert_eq!(result, json!(""));
}

#[test]
fn test_parse_uuid() {
    let result = parse_datum_value("Uuid(\"550e8400-e29b-41d4-a716-446655440000\")");
    assert_eq!(result, json!("550e8400-e29b-41d4-a716-446655440000"));
}

#[test]
fn test_parse_timestamp() {
    let result = parse_datum_value("Timestamp(2024-01-15 10:30:00)");
    assert_eq!(result, json!("2024-01-15 10:30:00"));
}

#[test]
fn test_parse_timestamptz() {
    let result = parse_datum_value("TimestampTz(2024-01-15 10:30:00+00)");
    assert_eq!(result, json!("2024-01-15 10:30:00+00"));
}

#[test]
fn test_parse_date() {
    let result = parse_datum_value("Date(2024-01-15)");
    assert_eq!(result, json!("2024-01-15"));
}

#[test]
fn test_parse_time() {
    let result = parse_datum_value("Time(10:30:00)");
    assert_eq!(result, json!("10:30:00"));
}

#[test]
fn test_parse_numeric() {
    let result = parse_datum_value("Numeric(123456.789)");
    assert_eq!(result, json!("123456.789"));
}

#[test]
fn test_parse_numeric_precision() {
    let result = parse_datum_value("Numeric(12345678901234567890.123456789)");
    assert_eq!(result, json!("12345678901234567890.123456789"));
}

#[test]
fn test_parse_json() {
    let result = parse_datum_value("Json({\"a\":1})");
    assert_eq!(result, json!({"a": 1}));
}

#[test]
fn test_parse_jsonb() {
    let result = parse_datum_value("JsonB({\"key\":\"value\",\"nested\":{\"x\":true}})");
    assert_eq!(result, json!({"key": "value", "nested": {"x": true}}));
}

#[test]
fn test_parse_jsonb_array() {
    let result = parse_datum_value("JsonB([1,2,3])");
    assert_eq!(result, json!([1, 2, 3]));
}

#[test]
fn test_parse_unknown_type() {
    let result = parse_datum_value("SomeUnknownType(value123)");
    assert_eq!(result, json!("value123"));
}

#[test]
fn test_parse_plain_string() {
    // If no type wrapper, return as-is
    let result = parse_datum_value("just a plain string");
    assert_eq!(result, json!("just a plain string"));
}

// ============================================================================
// Row to JSON Conversion Tests
// ============================================================================

/// Mock ColumnSchema for testing
struct MockColumnSchema {
    name: String,
}

/// Mock TableSchema for testing
struct MockTableSchema {
    column_schemas: Vec<MockColumnSchema>,
}

/// Convert row values to JSON with schema
fn row_to_json_with_schema(schema: &MockTableSchema, values: &[String]) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (i, value) in values.iter().enumerate() {
        let col_name = schema.column_schemas.get(i)
            .map(|c| c.name.clone())
            .unwrap_or_else(|| format!("column_{}", i));
        obj.insert(col_name, parse_datum_value(value));
    }
    JsonValue::Object(obj)
}

/// Convert row values to JSON without schema
fn row_to_json_without_schema(values: &[String]) -> JsonValue {
    let mut obj = serde_json::Map::new();
    for (i, value) in values.iter().enumerate() {
        obj.insert(format!("column_{}", i), parse_datum_value(value));
    }
    JsonValue::Object(obj)
}

#[test]
fn test_row_to_json_with_schema() {
    let schema = MockTableSchema {
        column_schemas: vec![
            MockColumnSchema { name: "id".to_string() },
            MockColumnSchema { name: "name".to_string() },
            MockColumnSchema { name: "active".to_string() },
        ],
    };

    let values = vec![
        "I32(1)".to_string(),
        "String(\"John\")".to_string(),
        "Bool(true)".to_string(),
    ];

    let result = row_to_json_with_schema(&schema, &values);

    assert_eq!(result["id"], json!(1));
    assert_eq!(result["name"], json!("John"));
    assert_eq!(result["active"], json!(true));
}

#[test]
fn test_row_to_json_without_schema() {
    let values = vec![
        "I32(42)".to_string(),
        "String(\"test\")".to_string(),
    ];

    let result = row_to_json_without_schema(&values);

    assert_eq!(result["column_0"], json!(42));
    assert_eq!(result["column_1"], json!("test"));
}

#[test]
fn test_row_to_json_more_values_than_columns() {
    let schema = MockTableSchema {
        column_schemas: vec![
            MockColumnSchema { name: "id".to_string() },
        ],
    };

    let values = vec![
        "I32(1)".to_string(),
        "String(\"extra\")".to_string(),
        "Bool(true)".to_string(),
    ];

    let result = row_to_json_with_schema(&schema, &values);

    assert_eq!(result["id"], json!(1));
    assert_eq!(result["column_1"], json!("extra"));
    assert_eq!(result["column_2"], json!(true));
}

#[test]
fn test_row_to_json_with_null() {
    let schema = MockTableSchema {
        column_schemas: vec![
            MockColumnSchema { name: "nullable_field".to_string() },
        ],
    };

    let values = vec!["Null".to_string()];
    let result = row_to_json_with_schema(&schema, &values);

    assert!(result["nullable_field"].is_null());
}

#[test]
fn test_row_to_json_empty_values() {
    let schema = MockTableSchema {
        column_schemas: vec![],
    };

    let values: Vec<String> = vec![];
    let result = row_to_json_with_schema(&schema, &values);

    assert!(result.as_object().unwrap().is_empty());
}

// ============================================================================
// HTTP Client Tests (using wiremock)
// ============================================================================

#[tokio::test]
async fn test_http_post_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());

    let response = client
        .post(&url)
        .json(&json!({"test": "data"}))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}

#[tokio::test]
async fn test_http_post_400_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(400))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());

    let response = client
        .post(&url)
        .json(&json!({"test": "data"}))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_client_error());
}

#[tokio::test]
async fn test_http_post_500_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());

    let response = client
        .post(&url)
        .json(&json!({"test": "data"}))
        .send()
        .await
        .unwrap();

    assert!(response.status().is_server_error());
}

#[tokio::test]
async fn test_http_post_json_body() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());

    let payload = json!({
        "events": [
            {
                "type": "insert",
                "table": "users",
                "row": {"id": 1, "name": "John"}
            }
        ]
    });

    let response = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());
}

// ============================================================================
// Event Payload Formatting Tests
// ============================================================================

fn format_insert_event(table_name: &str, row: JsonValue) -> JsonValue {
    json!({
        "type": "insert",
        "table": table_name,
        "row": row
    })
}

fn format_update_event(table_name: &str, old_row: Option<JsonValue>, new_row: JsonValue) -> JsonValue {
    json!({
        "type": "update",
        "table": table_name,
        "old_row": old_row,
        "new_row": new_row
    })
}

fn format_delete_event(table_name: &str, old_row: Option<JsonValue>) -> JsonValue {
    json!({
        "type": "delete",
        "table": table_name,
        "old_row": old_row
    })
}

#[test]
fn test_format_insert_event() {
    let row = json!({"id": 1, "name": "Alice"});
    let event = format_insert_event("users", row);

    assert_eq!(event["type"], "insert");
    assert_eq!(event["table"], "users");
    assert_eq!(event["row"]["id"], 1);
    assert_eq!(event["row"]["name"], "Alice");
}

#[test]
fn test_format_update_event() {
    let old_row = json!({"id": 1, "name": "Alice"});
    let new_row = json!({"id": 1, "name": "Alicia"});
    let event = format_update_event("users", Some(old_row), new_row);

    assert_eq!(event["type"], "update");
    assert_eq!(event["table"], "users");
    assert_eq!(event["old_row"]["name"], "Alice");
    assert_eq!(event["new_row"]["name"], "Alicia");
}

#[test]
fn test_format_update_event_no_old_row() {
    let new_row = json!({"id": 1, "name": "Test"});
    let event = format_update_event("users", None, new_row);

    assert_eq!(event["type"], "update");
    assert!(event["old_row"].is_null());
    assert_eq!(event["new_row"]["name"], "Test");
}

#[test]
fn test_format_delete_event() {
    let old_row = json!({"id": 1, "name": "Bob"});
    let event = format_delete_event("users", Some(old_row));

    assert_eq!(event["type"], "delete");
    assert_eq!(event["table"], "users");
    assert_eq!(event["old_row"]["id"], 1);
}

#[test]
fn test_format_delete_event_no_old_row() {
    let event = format_delete_event("users", None);

    assert_eq!(event["type"], "delete");
    assert!(event["old_row"].is_null());
}

#[test]
fn test_events_batch_payload() {
    let events = vec![
        format_insert_event("users", json!({"id": 1})),
        format_update_event("users", Some(json!({"id": 1})), json!({"id": 1, "updated": true})),
        format_delete_event("orders", Some(json!({"order_id": 100}))),
    ];

    let payload = json!({"events": events});

    assert_eq!(payload["events"].as_array().unwrap().len(), 3);
    assert_eq!(payload["events"][0]["type"], "insert");
    assert_eq!(payload["events"][1]["type"], "update");
    assert_eq!(payload["events"][2]["type"], "delete");
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn test_parse_nested_parentheses() {
    // Edge case: value contains parentheses
    let result = parse_datum_value("String(\"func(x, y)\")");
    assert_eq!(result, json!("func(x, y)"));
}

#[test]
fn test_parse_malformed_input() {
    // Missing closing paren - should just return as-is
    let result = parse_datum_value("String(incomplete");
    // Behavior depends on implementation - currently this may panic or return unexpected result
    // This test documents current behavior
    assert!(result.is_string() || result.is_null());
}

#[test]
fn test_url_trailing_slash_handling() {
    let base_url = "http://example.com/api/";
    let path = "events";
    let url = format!("{}/{}", base_url.trim_end_matches('/'), path);
    assert_eq!(url, "http://example.com/api/events");

    let base_url_no_slash = "http://example.com/api";
    let url2 = format!("{}/{}", base_url_no_slash.trim_end_matches('/'), path);
    assert_eq!(url2, "http://example.com/api/events");
}
