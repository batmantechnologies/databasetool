use anyhow::Result;
use sqlx::postgres::PgRow;
use std::fs;
use std::io::Write;
use std::env;
use std::path::PathBuf;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};

pub async fn check_db_connection(db_url: &str) -> bool {
    match PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
    {
        Ok(_) => {
            println!("✅ Successfully connected to {}", db_url);
            true
        },
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", db_url, e);
            false
        }
    }
}

pub async fn get_row_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    let count: Option<i64> = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{}\"", table_name))
        .fetch_one(pool)
        .await?;
    
    match count {
        Some(c) => Ok(c),
        None => {
            eprintln!("⚠ Could not get row count for table {}", table_name);
            Ok(0)
        }
    }
}

/// Serializes database values for SQL output
pub fn serialize_value(row: &sqlx::postgres::PgRow, column: &str) -> Result<String> {
    // First try to get as text representation (works for most types)
    if let Ok(val) = row.try_get::<Option<String>, _>(column) {
        return Ok(val.map(|v| {
            if v.contains('\'') || v.contains('\\') {
                // Use dollar-quoting for strings with quotes
                format!("$${}$$", v)
            } else {
                format!("'{}'", v)
            }
        }).unwrap_or("NULL".to_string()));
    }

    // Special handling for array types
    if let Ok(val) = row.try_get::<Option<Vec<String>>, _>(column) {
        return Ok(val.map(|v| {
            let elements = v.iter()
                .map(|s| if s.contains('\'') { format!("$${}$$", s) } else { format!("'{}'", s) })
                .collect::<Vec<_>>()
                .join(",");
            format!("ARRAY[{}]", elements)
        }).unwrap_or("NULL".to_string()));
    }

    // Handle UUID types
    if let Ok(val) = row.try_get::<Option<uuid::Uuid>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }

    // Handle all integer types
    if let Ok(val) = row.try_get::<Option<i8>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i16>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // Handle all float types
    if let Ok(val) = row.try_get::<Option<f32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<f64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // Handle numeric/decimal types using BigDecimal
    if let Ok(val) = row.try_get::<Option<sqlx::types::BigDecimal>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // Handle boolean
    if let Ok(val) = row.try_get::<Option<bool>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // Handle JSON/JSONB
    if let Ok(val) = row.try_get::<Option<serde_json::Value>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v.to_string())).unwrap_or("NULL".to_string()));
    }

    // Handle timestamps with timezone
    if let Ok(val) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v.naive_utc())).unwrap_or("NULL".to_string()));
    }

    // Handle other date/time types
    if let Ok(val) = row.try_get::<Option<chrono::NaiveDate>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<chrono::NaiveTime>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<chrono::NaiveDateTime>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }

    // Fallback to text representation
    match row.try_get::<Option<String>, _>(column) {
        Ok(val) => Ok(val.map(|v| format!("'{}'", v.replace("'", "''"))).unwrap_or("NULL".to_string())),
        Err(_) => Err(anyhow::anyhow!("Unsupported data type for column {}", column)),
    }
}
