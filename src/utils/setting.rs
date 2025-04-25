use anyhow::anyhow;
use anyhow::Result;
use anyhow::Context;
use std::process::Command;
use std::path::{Path, PathBuf};
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

pub fn prepare_working_directory(archive_path: &Path) -> Result<PathBuf> {
    println!("\n📦 Preparing working directory...");
    println!("- Input path: {}", archive_path.display());
    println!("- Is tar.gz: {}", is_tar_gz(archive_path));
    println!("- Is directory: {}", archive_path.is_dir());

    if is_tar_gz(archive_path) {
        // Extract next to original archive
        let extract_to = archive_path.parent()
            .unwrap_or_else(|| Path::new("."));

        println!("🔍 Extracting {} to {}", 
            archive_path.display(), 
            extract_to.display());

            let status = Command::new("tar")
            .arg("-xzf")
            .arg(archive_path)
            .arg("-C")
            .arg(extract_to)
            .status()
            .context("❌ Failed to extract archive")?;

        if !status.success() {
            return Err(anyhow!("❌ tar command failed with exit code {}", status));
        }

        // Get the expected directory name (removes .tar.gz)
        let dir_name = archive_path.file_stem()
            .context("❌ Invalid archive name").unwrap()
            .to_str()
            .context("❌ Invalid UTF-8 in archive name").unwrap()
            .strip_suffix(".tar")
            .unwrap_or("backup")
            .strip_prefix("backup_")
            .unwrap_or("backup");
            
        let extracted_dir = extract_to.join(dir_name);
        println!("ℹ Using extracted files from: {}", extracted_dir.display());

        Ok(extracted_dir)

    } else if archive_path.is_dir() {
        println!("ℹ Using directory directly: {}", archive_path.display());
        Ok(archive_path.to_path_buf())
    } else {
        Err(anyhow::anyhow!(
            "Unsupported backup format. Must be .tar.gz file or directory (found: {})",
            archive_path.display()
        ))
    }
}

fn is_tar_gz(path: &Path) -> bool {
    path.extension().map_or(false, |ext| ext == "gz") &&
    path.file_stem().map_or(false, |stem| {
        stem.to_string_lossy().ends_with(".tar")
    })
}
