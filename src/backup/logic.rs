use std::{
    env, fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};
use aws_config::{
    meta::region::RegionProviderChain,
    retry::RetryConfig,
    timeout::TimeoutConfig,
    BehaviorVersion,
};
use url::Url;
use chrono::Local;
use std::io::Seek;
use std::io::SeekFrom;
use aws_config::{Region};
use tokio::io::AsyncReadExt;
use anyhow::{Context, Result};
use aws_sdk_s3::{Client, Config};
use aws_sdk_s3::config::Credentials;
use tokio::time::Duration;
use aws_sdk_s3::primitives::ByteStream;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use crate::utils::setting::{self,check_db_connection,get_row_count,serialize_value};

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

/// Creates timestamped backup directory inside the temp working dir
pub fn create_timestamped_backup_dir(temp_root: &Path) -> Result<PathBuf, anyhow::Error> {
    let timestamp = chrono::Local::now().format("%Y-%m-%d_%H_%M_%S").to_string();
    let backup_dir = temp_root.join(&timestamp);

    fs::create_dir_all(&backup_dir)
        .context(format!("Failed to create backup dir: {}", backup_dir.display()))?;

    println!("✓ Created backup dir: {}", backup_dir.display());
    Ok(backup_dir)
}

/// Stores backup in all locations
pub fn store_backup_in_all_locations(
    backup_dir: &Path,
    local_dir: &Path,
    temp_dir: &Path,
) -> Result<PathBuf, anyhow::Error> {
    let archive_name = format!(
        "backup_{}.tar.gz",
        backup_dir.file_name().unwrap().to_str().unwrap()
    );

    // 1. Create in local backup dir (primary location)
    let primary_path = if local_dir.is_file() {
        local_dir.parent()
            .unwrap_or_else(|| Path::new("/tmp"))
            .join(&archive_name)
    } else {
        local_dir.join(&archive_name)
    };
    create_tar_archive(&backup_dir, &primary_path)?;

    // // 2. Copy to temp working dir
    let temp_path = temp_dir.join(&archive_name);
    if primary_path != temp_path {
        fs::copy(&primary_path, &temp_path)
            .context(format!("Failed to copy to temp dir: {}", temp_path.display()))?;
    }

    Ok(primary_path)
}

fn create_tar_archive(source_dir: &Path, dest_path: &Path) -> Result<(), anyhow::Error> {
    println!("ℹ Creating tar archive from {} to {}", source_dir.display(), dest_path.display());
    let status = Command::new("tar")
        .args([
            "-czf",
            dest_path.to_str().unwrap(),
            "-C",
            source_dir.parent().unwrap().to_str().unwrap(),
            source_dir.file_name().unwrap().to_str().unwrap(),
        ])
        .status()
        .context("Failed to execute tar command")?;

    if !status.success() {
        return Err(anyhow::anyhow!("Tar failed with exit code {}", status));
    }
    println!("✓ Tar archive created successfully at {}", dest_path.display());
    Ok(())
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

            offset += BATCH_SIZE;
        }

    }

    Ok(())
}

/// Uploads backup archive to configured object storage (S3/Spaces compatible).
/// Requires STORAGE_BUCKET_NAME, ACCESS_KEY_ID and SECRET_ACCESS_KEY env vars.
/// Returns Ok(()) on success or Err with failure details.
pub async fn upload_to_object_storage(archive_path: &Path) -> Result<()> {
    // 1. Validate environment variables
    let bucket = env::var("STORAGE_BUCKET_NAME").context("Missing STORAGE_BUCKET_NAME")?;
    let access_key = env::var("STORAGE_ACCESS_KEY_ID").context("Missing STORAGE_ACCESS_KEY_ID")?;
    let secret_key = env::var("STORAGE_SECRET_ACCESS_KEY").context("Missing STORAGE_SECRET_ACCESS_KEY")?;

    // 2. Validate file using async version
    if !tokio::fs::metadata(archive_path).await.is_ok() {
        return Err(anyhow::anyhow!("File not found at {}", archive_path.display()));
    }

    let file_name = archive_path.file_name()
        .and_then(|n| n.to_str())
        .context("Invalid filename")?;
    
    let metadata = tokio::fs::metadata(archive_path).await?;
    let file_size = metadata.len();
    println!("📦 Preparing to upload {} ({} bytes)", file_name, file_size);

    // 3. Configure AWS client
    let region = env::var("STORAGE_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let endpoint_override = env::var("STORAGE_ENDPOINT_URL").ok();
    
    let credentials = Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "manual-credentials",
    );

    let timeout_config = TimeoutConfig::builder()
        .connect_timeout(Duration::from_secs(60))
        .read_timeout(Duration::from_secs(300))
        .build();

    let retry_config = RetryConfig::standard()
        .with_max_attempts(5)
        .with_initial_backoff(Duration::from_secs(2));

    let mut config_builder = Config::builder()
        .behavior_version(BehaviorVersion::v2023_11_09())
        .region(Region::new(region))
        .credentials_provider(credentials)
        .retry_config(retry_config)
        .timeout_config(timeout_config);

    if let Some(endpoint) = endpoint_override {
        if !endpoint.trim().is_empty() {
            config_builder = config_builder.endpoint_url(endpoint);
        }
    }

    let config = config_builder.build();
    let client = Client::from_conf(config);

    // 4. Execute upload with proper async file handling
    println!("🚀 Beginning upload...");
    
    // Open file with tokio's async File
    let mut file = tokio::fs::File::open(archive_path).await?;
    let mut buffer = tokio_util::bytes::BytesMut::with_capacity(1024 * 1024); // 1MB buffer
    
    // Verify we can read the file
    let mut total_read = 0;
    loop {
        let read = file.read_buf(&mut buffer).await?;
        if read == 0 {
            break;
        }
        total_read += read;
        println!("🔹 Verified {} bytes (total: {})", read, total_read);
        buffer.clear();
    }

    // Reset file position to beginning
    file = tokio::fs::File::open(archive_path).await?;

    // Perform the actual upload
    let body = ByteStream::from_path(archive_path).await
        .context("Failed to create ByteStream from file")?;

    // Execute upload
    match client
        .put_object()
        .bucket(&bucket)
        .key(file_name)
        .body(body)
        .send()
        .await
    {
        Ok(_) => {
            println!("✅ Successfully uploaded {} to object storage", file_name);
            Ok(())
        }
        Err(e) => {
            let err_msg = format!("❌ Upload failed: {:?}", e);
            eprintln!("{}", err_msg);
            Err(anyhow::anyhow!(err_msg))
        }
    }
}

/// Verifies required AWS credentials (access key and secret) are present in environment variables.
pub async fn check_aws_cred() -> Result<()> {

    println!("🔍 Checking AWS credential configuration...");

    let access_key = env::var("STORAGE_ACCESS_KEY_ID")?;
    let secret_key = env::var("STORAGE_SECRET_ACCESS_KEY")?;
    let region = env::var("STORAGE_REGION").unwrap_or_else(|_| "us-east-1".into());

    let region_provider = RegionProviderChain::first_try(Region::new(region.clone()));

    let creds = Credentials::new(
        access_key,
        secret_key,
        None,
        None,
        "manual-credentials",
    );

    let config_loader = aws_config::from_env()
        .region(region_provider)
        .credentials_provider(creds);

    let sdk_config = config_loader.load().await;

    // Set the behavior version explicitly
    let config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
        .build();

    let client = Client::from_conf(config);

    // Try listing buckets
    match client.list_buckets().send().await {
        Ok(_) => {
            println!("✅ Connected successfully! Buckets:");
        }
        Err(err) => {
            println!("❌ Failed to list buckets: {:?}", err);
        }
    }
    Ok(())
}

/// Main backup flow
pub async fn run_backup_flow() -> Result<()> {

    println!("🚀 Starting database backup process");
    println!("🛠 Current directory: {:?}", std::env::current_dir()?);

    let source_url = env::var("SOURCE_DATABASE_URL").context("SOURCE_DATABASE_URL must be set")?;

    if !check_db_connection(&source_url).await {
        anyhow::bail!("❌ Cannot proceed with backup - database connection failed");
    }

    let (local_dir, temp_dir) =setting::setup_backup_dirs()?;
    let backup_dir = create_timestamped_backup_dir(&temp_dir)?;

    dump_databases(&backup_dir).await?;

    let archive_path = store_backup_in_all_locations(&backup_dir, &local_dir, &temp_dir)?;
    // Only attempt upload if AWS_BUCKET_NAME is set
    if let Ok(bucket_name) = std::env::var("STORAGE_BUCKET_NAME") {

        if !bucket_name.is_empty() {
        let _ = check_aws_cred().await;
        let upload_success = upload_to_object_storage(&archive_path).await;

        if !upload_success.is_err() {
            println!("🎉 Backup completed and uploaded successfully");
        } else {
            println!("⚠ Backup completed but upload failed - check logs for details");
        }
        } else{
            println!("⚠ Backup process completed");
        }
        
    } else {
        println!("\nℹ Backup process completed (no upload attempted - STORAGE_BUCKET_NAME not set or empty)");
    }
    
    Ok(())
}

