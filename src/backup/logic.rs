use chrono::Local;
use std::{
    env, fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use anyhow::{Context, Result};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use url::Url;
use crate::utils::setting::check_db_connection;
use std::io::Seek;
use std::io::SeekFrom;

// Constants for configuration
const DEFAULT_BACKUP_DIR: &str = "./backups";
pub const TEMP_BACKUP_ROOT: &str = "./databasebackup"; // Changed from /tmp to local directory

/// Gets the list of databases to backup from environment variable
fn get_database_list() -> Result<Vec<String>> {
    env::var("DATABASE_LIST")
        .context("DATABASE_LIST must be set")?
        .split(',')
        .map(|s| Ok(s.trim().to_string()))
        .collect()
}

/// Extracts the base URL without database name
fn get_base_url_without_db(full_url: &str) -> Result<String> {
    let mut parsed = Url::parse(full_url).context("Invalid PostgreSQL URL")?;
    parsed.set_path("");
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

/// Creates backup directory structure
pub fn create_backup_dir() -> Result<PathBuf> {
    // Create root temp directory first with debug output
    println!("🛠 Attempting to create backup root: {}", TEMP_BACKUP_ROOT);
    match fs::create_dir_all(TEMP_BACKUP_ROOT) {
        Ok(_) => println!("✅ Created backup root directory"),
        Err(e) => {
            eprintln!("❌ Failed to create backup root: {}", e);
            eprintln!("⚠️ Current /tmp permissions: {:?}",
                fs::metadata("/tmp").map(|m| m.permissions()));
            return Err(e).context(format!(
                "Failed to create backup root directory: {}. \n\
                Try running with elevated permissions or specify a different \
                TEMP_BACKUP_ROOT in settings.rs",
                TEMP_BACKUP_ROOT
            ));
        }
    }
        
    let timestamp = Local::now().format("%Y-%m-%d_%H_%M_%S").to_string();
    let backup_path = format!("{}/{}", TEMP_BACKUP_ROOT, timestamp);
    let local_path = env::var("LOCAL_BACKUP_DIR").unwrap_or(DEFAULT_BACKUP_DIR.to_string());

    fs::create_dir_all(&backup_path)
        .context("Failed to create temporary backup directory")?;
    fs::create_dir_all(&local_path)
        .context("Failed to create local backup directory")?;

    // Also create extract directory for restore operations
    let extract_path = format!("{}/extract", TEMP_BACKUP_ROOT);
    fs::create_dir_all(&extract_path)
        .context("Failed to create extract directory")?;

    println!("📂 Backup directory created at: {}", backup_path);
    Ok(PathBuf::from(backup_path))
}

/// Dumps database schema and data to files
pub async fn dump_databases(backup_dir: &Path) -> Result<()> {
    // Ensure backup directory exists
    fs::create_dir_all(backup_dir)
        .context(format!("Failed to create backup directory: {}", backup_dir.display()))?;
    
    let source_url = env::var("SOURCE_DATABASE_URL")
        .context("SOURCE_DATABASE_URL must be set")?;
    let databases = get_database_list()?;
    let base_url = get_base_url_without_db(&source_url)?;
    let timestamp = backup_dir.file_name()
        .and_then(|n| n.to_str())
        .context("Invalid backup directory name")?;

    for db in databases {
        println!("🔍 Backing up database: {}", db);
        
        // Create schema file
        let schema_filename = format!("{}/{}_{}_schema.sql", backup_dir.display(), db, timestamp);
        let schema_path = Path::new(&schema_filename);
        let mut schema_file = File::create(schema_path)
            .context(format!("Failed to create schema file: {}", schema_path.display()))?;
        
        // Create data file
        let data_filename = format!("{}/{}_{}_data.sql", backup_dir.display(), db, timestamp);
        let data_path = Path::new(&data_filename);
        let mut data_file = File::create(data_path)
            .context(format!("Failed to create data file: {}", data_path.display()))?;

        // Verify files are writable
        schema_file.write_all(b"-- Test write\n")
            .context(format!("Schema file not writable: {}", schema_path.display()))?;
        data_file.write_all(b"-- Test write\n")
            .context(format!("Data file not writable: {}", data_path.display()))?;
        schema_file.seek(SeekFrom::Start(0))?;
        data_file.seek(SeekFrom::Start(0))?;

        // Write headers
        writeln!(schema_file, "-- PostgreSQL schema backup")?;
        writeln!(schema_file, "-- Database: {}", db)?;
        writeln!(schema_file, "-- Backup time: {}", Local::now())?;
        writeln!(schema_file, "BEGIN;\n")?;

        writeln!(data_file, "-- PostgreSQL data backup")?;
        writeln!(data_file, "-- Database: {}", db)?;
        writeln!(data_file, "-- Backup time: {}", Local::now())?;
        writeln!(data_file, "BEGIN;\n")?;

        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&format!("{}/{}", base_url, db))
            .await
            .context(format!("Failed to connect to database {}", db))?;
    
        // Check and create pg_get_tabledef function with robust error handling
        println!("🔍 Checking for pg_get_tabledef function...");
        match sqlx::query("SELECT 1 FROM pg_proc WHERE proname = 'pg_get_tabledef'")
            .fetch_optional(&pool)
            .await {
            Ok(Some(_)) => println!("✅ pg_get_tabledef function exists"),
            Ok(None) => {
                println!("⚠ pg_get_tabledef not found, checking privileges...");
                
                // Check if we can create functions
                match sqlx::query("SELECT has_function_privilege(current_user, 'CREATE')")
                    .fetch_one(&pool)
                    .await {
                    Ok(row) => {
                        let has_privilege: bool = row.get(0);
                        if !has_privilege {
                            println!("❌ No CREATE FUNCTION privilege, using fallback method");
                        } else {
                            println!("ℹ User has CREATE FUNCTION privilege, attempting to create...");
                            match sqlx::query(
                                r#"
                                CREATE OR REPLACE FUNCTION public.pg_get_tabledef(table_name text)
                                RETURNS text AS $$
                                DECLARE
                                    ddl text;
                                BEGIN
                                    SELECT 'CREATE TABLE ' || table_name || ' (' ||
                                    string_agg(column_name || ' ' || data_type ||
                                    CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
                                    CASE WHEN column_default IS NOT NULL THEN ' DEFAULT ' || column_default ELSE '' END,
                                    ', ') || ');'
                                    INTO ddl
                                    FROM information_schema.columns
                                    WHERE table_schema = 'public' AND table_name = $1
                                    GROUP BY table_name;
                                    
                                    RETURN ddl;
                                EXCEPTION WHEN others THEN
                                    RETURN NULL;
                                END;
                                $$ LANGUAGE plpgsql;
                                "#
                            )
                            .execute(&pool)
                            .await {
                                Ok(_) => println!("✅ Successfully created pg_get_tabledef function"),
                                Err(e) => {
                                    println!("❌ Failed to create pg_get_tabledef: {}", e);
                                    println!("⚠ Falling back to manual table definitions");
                                },
                            }
                        }
                    },
                    Err(e) => {
                        println!("❌ Failed to check privileges: {}", e);
                        println!("⚠ Falling back to manual table definitions");
                    },
                }
            },
            Err(e) => {
                println!("❌ Error checking for pg_get_tabledef: {}", e);
                println!("⚠ Falling back to manual table definitions");
            }
        }
    
        // Backup schema objects to schema file
        backup_schema(&pool, &mut schema_file).await
            .context(format!("Failed to backup schema for {}", db))?;
        writeln!(schema_file, "\nCOMMIT;")?;


        backup_table_data(&pool, &mut data_file).await
            .context(format!("Failed to backup data for {}", db))?;
        writeln!(data_file, "\nCOMMIT;")?;
        println!("-----------------------✅ Successfully backed up database: {}-------------------", db);
    }

    Ok(())
}

async fn backup_schema(pool: &PgPool, file: &mut File) -> Result<()> {
    // Phase 1: Basic table structure (no constraints)
    let table_rows = sqlx::query(
        "SELECT table_name FROM information_schema.tables 
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE' 
         ORDER BY table_name"
    )
    .fetch_all(pool)
    .await
    .context("Failed to fetch Tables")?;

    writeln!(file, "-- PHASE 1: BASIC TABLE STRUCTURES")?;
    
    for row in &table_rows {
        let table_name: String = row.get("table_name");

        // Get just column definitions without constraints
        let columns = sqlx::query(
            "SELECT column_name, data_type, is_nullable, column_default
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1
             ORDER BY ordinal_position"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        // Build minimal CREATE TABLE statement
        let mut ddl = format!("CREATE TABLE \"{}\" (\n", table_name);

        for (i, row) in columns.iter().enumerate() {
            let col_name: String = row.get("column_name");
            let data_type: String = row.get("data_type");
            let is_nullable: String = row.get("is_nullable");
            let default: Option<String> = row.get("column_default");

            if i > 0 {
                ddl.push_str(",\n");
            }
            ddl.push_str(&format!("  \"{}\" {}", col_name, data_type));
            if is_nullable == "NO" {
                ddl.push_str(" NOT NULL");
            }
            if let Some(def) = default {
                // Handle timestamp defaults specially
                if data_type == "timestamp with time zone" && def == "now()" {
                    ddl.push_str(" DEFAULT CURRENT_TIMESTAMP");
                } else {
                    ddl.push_str(&format!(" DEFAULT {}", def));
                }
            }
        }

        ddl.push_str("\n);\n");
        writeln!(file, "{}", ddl)?;
    }

    // Phase 2: Sequences (needed before data insertion)
    writeln!(file, "\n-- PHASE 2: SEQUENCES")?;
    let sequences = sqlx::query(
        "SELECT sequence_name FROM information_schema.sequences 
         WHERE sequence_schema = 'public'"
    )
    .fetch_all(pool)
    .await?;

    for seq in sequences {
        let seq_name: String = seq.get("sequence_name");
        writeln!(file, "CREATE SEQUENCE IF NOT EXISTS \"{}\";", seq_name)?;
    }

    // Phase 3: Data insertion will be handled in backup_table_data

    // Phase 4: Constraints and indexes (after data is loaded)
    writeln!(file, "\n-- PHASE 4: CONSTRAINTS AND INDEXES")?;
    
    for row in &table_rows {
        let table_name: String = row.get("table_name");

        // Primary keys
        let pks = sqlx::query(
            "SELECT pg_get_constraintdef(oid) as def
             FROM pg_constraint
             WHERE conrelid = $1::regclass AND contype = 'p'"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        for pk in pks {
            let def: String = pk.get("def");
            writeln!(file, "ALTER TABLE \"{}\" ADD {};", table_name, def)?;
        }

        // Foreign keys
        let fks = sqlx::query(
            "SELECT pg_get_constraintdef(oid) as def
             FROM pg_constraint
             WHERE conrelid = $1::regclass AND contype = 'f'"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        for fk in fks {
            let def: String = fk.get("def");
            writeln!(file, "ALTER TABLE \"{}\" ADD {};", table_name, def)?;
        }

        // Unique constraints
        let uniques = sqlx::query(
            "SELECT pg_get_constraintdef(oid) as def
             FROM pg_constraint
             WHERE conrelid = $1::regclass AND contype = 'u'"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        for unique in uniques {
            let def: String = unique.get("def");
            writeln!(file, "ALTER TABLE \"{}\" ADD {};", table_name, def)?;
        }

        // Indexes (non-constraint)
        let indexes = sqlx::query(
            "SELECT pg_get_indexdef(i.indexrelid) as def
             FROM pg_index i
             JOIN pg_class t ON t.oid = i.indrelid
             WHERE t.relname = $1 AND NOT i.indisprimary AND NOT i.indisunique"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await?;

        for idx in indexes {
            let def: String = idx.get("def");
            writeln!(file, "{};", def)?;
        }
    }

    // Phase 5: Other schema objects
    writeln!(file, "\n-- PHASE 5: OTHER SCHEMA OBJECTS")?;
    
    let other_objects = [
        // Skip functions and triggers - only backup views
        ("Views", "SELECT definition AS def FROM pg_views WHERE schemaname = 'public'")
    ];

    for (obj_type, query) in other_objects {
        writeln!(file, "\n-- {} definitions", obj_type)?;
        
        let rows = sqlx::query(query)
            .fetch_all(pool)
            .await
            .context(format!("Failed to fetch {}", obj_type))?;

        for row in rows {
            let def: String = row.get("def");
            writeln!(file, "{};", def)?;
        }
    }

    Ok(())
}
async fn backup_table_data(pool: &PgPool, file: &mut File) -> Result<()> {
    writeln!(file, "\n-- PHASE 3: TABLE DATA")?;
    let tables = sqlx::query(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name"
    )
    .fetch_all(pool)
    .await
    .context("Failed to fetch table list")?;


    for table in tables {
        let table_name: String = table.get("table_name");
        
        let columns = sqlx::query(
            "SELECT column_name, data_type, udt_name
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1
             ORDER BY ordinal_position"
        )
        .bind(&table_name)
        .fetch_all(pool)
        .await
        .context(format!("Failed to fetch columns for table {}", table_name))?;

        let column_names: Vec<String> = columns.iter()
            .map(|row| row.get::<String, _>("column_name"))
            .collect();

        let row_count = get_row_count(pool, &table_name).await?;
        writeln!(file, "\n-- Data for table: {}", table_name)?;
        writeln!(file, "-- Total rows: {}", row_count);

        if row_count == 0 {
            continue;
        }

        // Fetch data in batches
        let mut offset = 0;
        const BATCH_SIZE: i64 = 500;
        let mut total_rows = 0;
        
        loop {
            let query = format!(
                "SELECT * FROM \"{}\" ORDER BY 1 LIMIT {} OFFSET {}",
                table_name, BATCH_SIZE, offset
            );
            
            let rows = match sqlx::query(&query).fetch_all(pool).await {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("    ❌ Failed to fetch batch from table {}: {}", table_name, e);
                    break;
                }
            };

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let values_result: Result<Vec<String>> = column_names.iter()
                    .map(|col| serialize_value(row, col))
                    .collect();

                match values_result {
                    Ok(values) => {
                        writeln!(file, "INSERT INTO \"{}\" ({}) VALUES ({});",
                            table_name,
                            column_names.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(", "),
                            values.join(", "))?;
                    }
                    Err(e) => {
                        eprintln!("    ❌ Failed to serialize row: {}", e);
                        continue;
                    }
                }
            }

            total_rows += rows.len();
            offset += BATCH_SIZE;
        }

    }

    Ok(())
}

async fn get_row_count(pool: &PgPool, table_name: &str) -> Result<i64> {
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
fn serialize_value(row: &sqlx::postgres::PgRow, column: &str) -> Result<String> {
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

/// Compresses the backup directory into a tar.gz archive
pub fn compress_backup(backup_dir: &Path) -> Result<()> {
    let timestamp = backup_dir.file_name()
        .and_then(|n| n.to_str())
        .context("Invalid backup directory name")?;
    
    let archive_name = format!("database_backup_{}.tar.gz", timestamp);
    let local_backup_dir = env::var("LOCAL_BACKUP_DIR")
        .unwrap_or(DEFAULT_BACKUP_DIR.to_string());

    println!("🗜 Compressing backup to {}/{}", local_backup_dir, archive_name);

    let status = Command::new("tar")
        .args(["-czf", 
              &format!("{}/{}", local_backup_dir, archive_name), 
              "-C", TEMP_BACKUP_ROOT, 
              timestamp])
        .status()
        .context("Failed to execute tar command")?;

    if !status.success() {
        return Err(anyhow::anyhow!("Failed to compress backup (tar exit code: {})", status));
    }

    println!("✅ Backup compressed successfully");
    Ok(())
}

/// Main backup flow
pub async fn run_backup_flow() -> Result<()> {
    println!("🚀 Starting database backup process");
    println!("🛠 Using backup root: {}", TEMP_BACKUP_ROOT);
    println!("🛠 Current directory: {:?}", std::env::current_dir()?);
    println!("🛠 Backup root exists: {}", Path::new(TEMP_BACKUP_ROOT).exists());

    let source_url = env::var("SOURCE_DATABASE_URL")
        .context("SOURCE_DATABASE_URL must be set")?;
    
    if !check_db_connection(&source_url).await {
        anyhow::bail!("❌ Cannot proceed with backup - database connection failed");
    }

    let backup_dir = create_backup_dir()?;
    dump_databases(&backup_dir).await?;
    compress_backup(&backup_dir)?;

    println!("\n🎉 Backup completed successfully");
    Ok(())
}