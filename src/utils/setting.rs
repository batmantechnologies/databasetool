use std::path::Path; // Keep Path, PathBuf might be unused if TempDir handles its own path types well.
// env, fs, process::Command removed as they appear unused.
use hex;
use anyhow::{anyhow, Context, Result};
use tempfile::{Builder as TempFileBuilder, TempDir};
use sqlx::{
    postgres::{PgPoolOptions, PgRow},
    PgPool, Row, ValueRef, TypeInfo,
};

#[allow(dead_code)]
pub async fn check_db_connection(db_url: &str) -> bool {
    match PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
    {
        Ok(_) => {
            println!("✅ Successfully connected to {}", db_url);
            true
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", db_url, e);
            false
        }
    }
}

#[allow(dead_code)]
pub async fn get_row_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    let count_query = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
    let count_row: (Option<i64>,) = sqlx::query_as(&count_query)
        .fetch_one(pool)
        .await
        .with_context(|| format!("Failed to get row count for table: {}", table_name))?;

    match count_row.0 {
        Some(c) => Ok(c),
        None => {
            eprintln!(
                "⚠ COUNT(*) for table {} returned None, which is unexpected. Assuming 0.",
                table_name
            );
            Ok(0)
        }
    }
}

/// Serializes database values for SQL output with support for all PostgreSQL data types
/// NOTE: This function is currently not used by the pg_dump based backup flow.
/// It's kept for potential future use in custom data handling or verification.
#[allow(dead_code)]
pub fn serialize_value(row: &PgRow, column: &str) -> Result<String> {
    // 1. First try to get as text representation (works for most types)
    if let Ok(val) = row.try_get::<Option<String>, _>(column) {
        return Ok(val
            .map(|v| {
                if v.contains('\'') || v.contains('\\') {
                    format!("$${}$$", v) // Dollar quoting for strings with quotes
                } else {
                    format!("'{}'", v) // Regular single quotes
                }
            })
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 2. Handle integer arrays
    if let Ok(val) = row.try_get::<Option<Vec<i32>>, _>(column) {
        return Ok(val
            .map(|v| {
                format!(
                    "ARRAY[{}]",
                    v.iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 3. Handle text arrays
    if let Ok(val) = row.try_get::<Option<Vec<String>>, _>(column) {
        return Ok(val
            .map(|v| {
                let elements = v
                    .iter()
                    .map(|s| {
                        if s.contains('\'') || s.contains('\\') {
                            format!("$${}$$", s)
                        } else {
                            format!("'{}'", s)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!("ARRAY[{}]", elements)
            })
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 4. Handle UUID types
    if let Ok(val) = row.try_get::<Option<uuid::Uuid>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 5. Handle UUID arrays
    if let Ok(val) = row.try_get::<Option<Vec<uuid::Uuid>>, _>(column) {
        return Ok(val
            .map(|v| {
                format!(
                    "ARRAY[{}]",
                    v.iter()
                        .map(|u| format!("'{}'", u))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            })
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 6. Handle all integer types
    if let Ok(val) = row.try_get::<Option<i16>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }

    // 7. Handle float types
    if let Ok(val) = row.try_get::<Option<f32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<f64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }

    // 8. Handle numeric/decimal
    if let Ok(val) = row.try_get::<Option<sqlx::types::BigDecimal>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }

    // 9. Handle boolean
    if let Ok(val) = row.try_get::<Option<bool>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or_else(|| "NULL".to_string()));
    }

    // 10. Handle JSON/JSONB
    if let Ok(val) = row.try_get::<Option<serde_json::Value>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v.to_string().replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 11. Handle timestamps
    if let Ok(val) = row.try_get::<Option<chrono::NaiveDateTime>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v))
            .unwrap_or_else(|| "NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v.naive_utc()))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // Handle date type
    if let Ok(val) = row.try_get::<Option<chrono::NaiveDate>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // Handle time type
    if let Ok(val) = row.try_get::<Option<chrono::NaiveTime>, _>(column) {
        return Ok(val
            .map(|v| format!("'{}'", v))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // Handle interval type
    if let Ok(val) = row.try_get::<Option<sqlx::postgres::types::PgInterval>, _>(column) {
        return Ok(val
            .map(|v| {
                format!(
                    "'{} seconds {}{} days'::interval",
                    v.microseconds as f64 / 1_000_000.0,
                    if v.months != 0 {
                        format!("{} months ", v.months)
                    } else {
                        "".to_string()
                    },
                    if v.days != 0 {
                        format!("{}", v.days)
                    } else {
                        "0".to_string()
                    }
                )
            })
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // 12. Handle binary data (bytea)
    if let Ok(val) = row.try_get::<Option<Vec<u8>>, _>(column) {
        return Ok(val
            .map(|v| format!("E'\\\\x{}'", hex::encode(v)))
            .unwrap_or_else(|| "NULL".to_string()));
    }

    // Fallback for types not explicitly handled above
    match row.try_get_raw(column) {
        Ok(raw_value) if !raw_value.is_null() => {
            if let Ok(str_val) = raw_value.as_str() {
                Ok(format!("'{}'", str_val.replace('\'', "''")))
            } else {
                eprintln!(
                    "Warning: Column '{}' has an unsupported type ('{}') for direct SQL serialization. Raw value could not be displayed as string.",
                    column,
                    raw_value.type_info().name()
                );
                Err(anyhow!(
                    "Unsupported data type ('{}') for column {} for direct SQL serialization",
                    raw_value.type_info().name(),
                    column
                ))
            }
        }
        Ok(_) => Ok("NULL".to_string()), // Handles SQL NULL explicitly
        Err(e) => Err(anyhow!("Failed to retrieve raw value for column {}: {}", column, e)),
    }
}


/// Prepares a backup archive for restore by extracting it to a new temporary directory.
///
/// This function is specifically for `.tar.gz` archives.
/// The caller is responsible for the lifetime of the returned `TempDir`.
///
/// # Arguments
/// * `archive_path` - Path to the `.tar.gz` archive file.
///
/// # Returns
/// A `Result` containing a `TempDir` where the archive has been extracted.
pub fn prepare_archive_for_restore(archive_path: &Path) -> Result<TempDir> {
    println!(
        "\n📦 Preparing archive for restore: {}",
        archive_path.display()
    );

    if !archive_path.is_file() {
        return Err(anyhow!(
            "Archive path for restore is not a file: {}",
            archive_path.display()
        ));
    }

    if !is_tar_gz(archive_path) {
        return Err(anyhow!(
            "Archive for restore is not a .tar.gz file: {}. Supported format is .tar.gz.",
            archive_path.display()
        ));
    }

    // Create a new temporary directory for extraction.
    let temp_dir = TempFileBuilder::new()
        .prefix("restore_extract_")
        .tempdir()
        .context("Failed to create temporary directory for archive extraction")?;

    println!(
        "Extracting archive {} to temporary directory {}",
        archive_path.display(),
        temp_dir.path().display()
    );

    // Use the robust archive extraction function.
    crate::backup::archive::extract_tar_gz_archive(archive_path, temp_dir.path())
        .with_context(|| {
            format!(
                "Failed to extract archive {} into temporary directory {}",
                archive_path.display(),
                temp_dir.path().display()
            )
        })?;

    println!(
        "✓ Archive successfully extracted to: {}",
        temp_dir.path().display()
    );
    Ok(temp_dir)
}

/// Checks if the given path likely points to a `.tar.gz` file based on its extension.
fn is_tar_gz(path: &Path) -> bool {
    path.extension()
        .map_or(false, |ext| ext.eq_ignore_ascii_case("gz"))
        && path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .map_or(false, |ext| ext.eq_ignore_ascii_case("tar"))
}