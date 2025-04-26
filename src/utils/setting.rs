use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};
use hex;
use anyhow::anyhow;
use anyhow::Result;
use anyhow::Context;
use tempfile::tempdir;
use sqlx::{postgres::{PgPoolOptions, PgRow}, PgPool, Row};

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

/// Serializes database values for SQL output with support for all PostgreSQL data types
pub fn serialize_value(row: &PgRow, column: &str) -> Result<String> {
    // 1. First try to get as text representation (works for most types)
    if let Ok(val) = row.try_get::<Option<String>, _>(column) {
        return Ok(val.map(|v| {
            if v.contains('\'') || v.contains('\\') {
                format!("$${}$$", v)  // Dollar quoting for strings with quotes
            } else {
                format!("'{}'", v)     // Regular single quotes
            }
        }).unwrap_or("NULL".to_string()));
    }

    // 2. Handle integer arrays (for user_ids, class_ids, notify_type)
    if let Ok(val) = row.try_get::<Option<Vec<i32>>, _>(column) {
        return Ok(val.map(|v| {
            format!("ARRAY[{}]", 
                v.iter()
                 .map(|n| n.to_string())
                 .collect::<Vec<_>>()
                 .join(","))
        }).unwrap_or("NULL".to_string()));
    }

    // 3. Handle text arrays
    if let Ok(val) = row.try_get::<Option<Vec<String>>, _>(column) {
        return Ok(val.map(|v| {
            let elements = v.iter()
                .map(|s| if s.contains('\'') || s.contains('\\') {
                    format!("$${}$$", s)
                } else {
                    format!("'{}'", s)
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("ARRAY[{}]", elements)
        }).unwrap_or("NULL".to_string()));
    }

    // 4. Handle UUID types
    if let Ok(val) = row.try_get::<Option<uuid::Uuid>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }

    // 5. Handle UUID arrays
    if let Ok(val) = row.try_get::<Option<Vec<uuid::Uuid>>, _>(column) {
        return Ok(val.map(|v| {
            format!("ARRAY[{}]", 
                v.iter()
                 .map(|u| format!("'{}'", u))
                 .collect::<Vec<_>>()
                 .join(","))
        }).unwrap_or("NULL".to_string()));
    }

    // 6. Handle all integer types
    if let Ok(val) = row.try_get::<Option<i16>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<i64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // 7. Handle float types
    if let Ok(val) = row.try_get::<Option<f32>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<f64>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // 8. Handle numeric/decimal
    if let Ok(val) = row.try_get::<Option<sqlx::types::BigDecimal>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // 9. Handle boolean
    if let Ok(val) = row.try_get::<Option<bool>, _>(column) {
        return Ok(val.map(|v| v.to_string()).unwrap_or("NULL".to_string()));
    }

    // 10. Handle JSON/JSONB
    if let Ok(val) = row.try_get::<Option<serde_json::Value>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v.to_string().replace("'", "''")))
                   .unwrap_or("NULL".to_string()));
    }

    // 11. Handle timestamps
    if let Ok(val) = row.try_get::<Option<chrono::NaiveDateTime>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v)).unwrap_or("NULL".to_string()));
    }
    if let Ok(val) = row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>(column) {
        return Ok(val.map(|v| format!("'{}'", v.naive_utc())).unwrap_or("NULL".to_string()));
    }

    // 12. Handle binary data (bytea)
    if let Ok(val) = row.try_get::<Option<Vec<u8>>, _>(column) {
        return Ok(val.map(|v| format!("E'\\\\x{}'", hex::encode(v)))
                   .unwrap_or("NULL".to_string()));
    }

    // Fallback to text representation
    match row.try_get::<Option<String>, _>(column) {
        Ok(val) => Ok(val.map(|v| format!("'{}'", v.replace("'", "''")))
                     .unwrap_or("NULL".to_string())),
        Err(_) => Err(anyhow!("Unsupported data type for column {}", column)),
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


pub fn setup_backup_dirs() -> Result<(PathBuf, PathBuf), anyhow::Error> {
    println!("ℹ Setting up backup directories...");

    // 1. Handle LOCAL_BACKUP_DIR (must be specified)
    let local_backup_dir = env::var("LOCAL_BACKUP_DIR")
        .map(PathBuf::from)
        .context("LOCAL_BACKUP_DIR environment variable must be set")?;

    // Create local backup dir if it doesn't exist
    if !local_backup_dir.exists() {
        println!("⚠ Local backup directory does not exist, creating: {}", local_backup_dir.display());
        fs::create_dir_all(&local_backup_dir)
            .context(format!("Failed to create local backup dir: {}", local_backup_dir.display()))?;
    }

    // 2. Handle TEMP_BACKUP_ROOT (either specified or use tempdir)
    let temp_backup_root = if let Ok(env_path) = env::var("TEMP_BACKUP_ROOT") {
        let path = PathBuf::from(env_path);
        if !path.exists() {
            println!("⚠ Temp backup directory does not exist, creating: {}", path.display());
            fs::create_dir_all(&path)
                .context(format!("Failed to create temp backup dir: {}", path.display()))?;
        }
        path
    } else {
        // Create a proper temp directory if no env var is set
        let temp_dir = tempdir()
            .context("Failed to create temporary directory")?;
        println!("ℹ Using temporary directory: {}", temp_dir.path().display());
        
        // Convert to PathBuf and leak the tempdir (so it's not deleted when temp_dir drops)
        temp_dir.into_path()
    };

    println!("✓ Local backup dir: {}", local_backup_dir.display());
    println!("✓ Temp working dir: {}", temp_backup_root.display());
    println!("✓ Backup directories setup complete");

    Ok((local_backup_dir, temp_backup_root))
}