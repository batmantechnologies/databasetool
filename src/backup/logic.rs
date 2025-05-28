// databasetool/src/backup/logic.rs
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::{Builder as TempFileBuilder, TempDir};

use crate::config::{AppConfig, BackupConfig};
use crate::backup::{archive, db_dump, s3_upload};


/// Orchestrates the entire database backup process.
///
/// 1. Sets up a temporary directory for SQL dumps.
/// 2. Dumps databases to this temporary directory.
/// 3. Creates a tar.gz archive of the dumped files.
/// 4. Optionally uploads the archive to S3-compatible storage.
/// 5. Cleans up the temporary dump directory.
pub async fn perform_backup_orchestration(
    app_config: &AppConfig,
    backup_config: &BackupConfig,
) -> Result<()> {
    println!("🚀 Starting backup orchestration...");
    println!("Current working directory: {:?}", std::env::current_dir().unwrap_or_default());
    println!("Backup configuration: {:?}", backup_config);

    // 1. Prepare temporary directory for SQL dumps
    // This will be a directory like /configured_temp_root/timestamp/ or /system_temp/timestamp/
    // The `_temp_dump_dir_guard` ensures cleanup if `temp_dump_root` was None.
    // If `temp_dump_root` was Some(path), then `current_operation_dump_dir` is a child of it,
    // and we only clean this child.
    let (_temp_dump_dir_guard, current_operation_dump_dir) =
        setup_temporary_dump_directory(backup_config.temp_dump_root.as_deref())?;

    println!("Temporary dump directory for this operation: {}", current_operation_dump_dir.display());


    // 2. Dump databases
    let dumped_db_names = db_dump::dump_databases(backup_config, &current_operation_dump_dir)
        .await
        .context("Failed to dump databases")?;

    if dumped_db_names.is_empty() {
        println!("No databases were dumped. Backup process might be incomplete or no databases were targeted.");
        // Depending on requirements, this could be an error or just a warning.
        // For now, continue to allow archiving an empty directory if that's the (unlikely) outcome.
    } else {
        println!("Successfully dumped databases: {:?}", dumped_db_names);
    }

    // 3. Create tar.gz archive
    // The archive name will be based on the timestamp used for the current_operation_dump_dir name.
    let archive_file_name_stem = current_operation_dump_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_else(|| "backup_unknown_ts"); // Fallback, should not happen with current setup

    let archive_file_name = format!("{}.tar.gz", archive_file_name_stem);
    
    // Ensure the local_backup_path (e.g., /mnt/backups) exists
    if !backup_config.local_backup_path.exists() {
        fs::create_dir_all(&backup_config.local_backup_path).with_context(|| {
            format!(
                "Failed to create local backup directory: {}",
                backup_config.local_backup_path.display()
            )
        })?;
        println!("Created local backup directory: {}", backup_config.local_backup_path.display());
    } else if !backup_config.local_backup_path.is_dir() {
        return Err(anyhow::anyhow!(
            "LOCAL_BACKUP_DIR '{}' exists but is not a directory.",
            backup_config.local_backup_path.display()
        ));
    }


    let final_archive_path = backup_config.local_backup_path.join(&archive_file_name);

    archive::create_tar_gz_archive(&current_operation_dump_dir, &final_archive_path)
        .context("Failed to create tar.gz archive")?;
    println!("Archive created at: {}", final_archive_path.display());

    // 4. Upload to S3/Spaces (if configured)
    if backup_config.upload_to_spaces {
        if let Some(spaces_conf) = &app_config.spaces_config {
            println!("Uploading archive to DigitalOcean Spaces...");
            // Optional: Perform a connection check. Could be made configurable.
            // s3_upload::check_s3_connection(spaces_conf).await.context("S3 connection check failed")?;

            let s3_key = format!("database_backups/{}", archive_file_name); // Example S3 key structure

            s3_upload::upload_file_to_s3(spaces_conf, &final_archive_path, &s3_key)
                .await
                .context("Failed to upload archive to S3/Spaces")?;
            println!("Successfully uploaded archive to S3/Spaces bucket: {}, key: {}", spaces_conf.bucket_name, s3_key);
        } else {
            println!("Upload to Spaces requested, but Spaces is not configured. Skipping upload.");
        }
    } else {
        println!("Upload to Spaces not requested. Skipping upload.");
    }

    // 5. Cleanup
    // If `_temp_dump_dir_guard` was created from `TempDir::new()`, it will be cleaned up when it goes out of scope.
    // If `current_operation_dump_dir` was created inside a user-specified `temp_dump_root`,
    // we should explicitly remove `current_operation_dump_dir`.
    // The current `setup_temporary_dump_directory` returns the guard which handles the system temp case.
    // For user-specified temp root, we need to manually clean the timestamped subdir.

    if backup_config.temp_dump_root.is_some() {
        println!("Cleaning up user-specified temporary dump sub-directory: {}", current_operation_dump_dir.display());
        fs::remove_dir_all(&current_operation_dump_dir).with_context(|| {
            format!(
                "Failed to clean up temporary dump sub-directory: {}",
                current_operation_dump_dir.display()
            )
        })?;
    } else {
        // The TempDir guard (`_temp_dump_dir_guard`) will handle cleanup automatically on drop.
        println!("System temporary dump directory {} will be cleaned up automatically.", current_operation_dump_dir.display());
    }
    
    println!("✅ Backup orchestration completed.");
    Ok(())
}

/// Sets up the temporary directory for storing SQL dumps before archiving.
///
/// If `configured_temp_root` is `Some`, a timestamped subdirectory is created within it.
/// If `configured_temp_root` is `None`, a new system temporary directory is created,
/// and a timestamped subdirectory is created within that.
///
/// Returns a tuple:
/// 1. An optional `TempDir` guard. This is `Some` if a new system temp dir was created,
///    ensuring it's cleaned up on drop. It's `None` if a user-provided path was used.
/// 2. The `PathBuf` to the actual timestamped directory where dumps should be placed.
fn setup_temporary_dump_directory(
    configured_temp_root: Option<&Path>,
) -> Result<(Option<TempDir>, PathBuf)> {
    let timestamp = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();

    match configured_temp_root {
        Some(root_path) => {
            // User specified a root temporary directory.
            // Ensure it exists.
            if !root_path.exists() {
                fs::create_dir_all(root_path).with_context(|| {
                    format!("Failed to create configured temporary root directory: {}", root_path.display())
                })?;
                println!("Created configured temporary root: {}", root_path.display());
            } else if !root_path.is_dir() {
                return Err(anyhow::anyhow!(
                    "Configured temporary root path '{}' exists but is not a directory.",
                    root_path.display()
                ));
            }
            let specific_dump_dir = root_path.join(&timestamp);
            fs::create_dir_all(&specific_dump_dir).with_context(|| {
                format!("Failed to create timestamped dump directory in configured root: {}", specific_dump_dir.display())
            })?;
            Ok((None, specific_dump_dir))
        }
        None => {
            // No specific temporary root, use system's temp directory.
            let system_temp_parent = TempFileBuilder::new()
                .prefix("db_backup_parent_")
                .tempdir()
                .context("Failed to create system temporary parent directory")?;
            
            let specific_dump_dir = system_temp_parent.path().join(&timestamp);
            fs::create_dir_all(&specific_dump_dir).with_context(|| {
                format!("Failed to create timestamped dump directory in system temp: {}", specific_dump_dir.display())
            })?;
            // The system_temp_parent guard will clean itself and its contents (including specific_dump_dir)
            // when it goes out of scope. We return the path to the specific dir for use.
            Ok((Some(system_temp_parent), specific_dump_dir))
        }
    }
}

// --- Old functions from the original logic.rs ---
// These are largely replaced by the new modular approach or moved.
// Kept here temporarily for reference during refactoring, will be removed.

/*
fn get_base_url_without_db(full_url: &str) -> Result<String> { ... MOVED to db_dump.rs ... }
pub fn create_timestamped_backup_dir(temp_root: &Path) -> Result<PathBuf> { ... REPLACED by setup_temporary_dump_directory ... }
pub fn store_backup_in_all_locations(...) -> Result<PathBuf> { ... REPLACED by archive.rs and direct path handling ... }
fn create_tar_archive(source_dir: &Path, dest_path: &Path) -> Result<()> { ... MOVED/REPLACED by archive.rs ... }
pub async fn dump_databases(backup_dir: &Path) -> Result<()> { ... REPLACED by db_dump.rs ... }
async fn backup_schema(pool: &PgPool, schema_path: &Path, db_name: &str) -> Result<()> { ... REPLACED by pg_dump in db_dump.rs ... }
async fn backup_table_data(pool: &PgPool, data_path: &Path, db_name: &str, _table_name: &str) -> Result<()> { ... REPLACED by pg_dump in db_dump.rs ... }
pub async fn upload_to_object_storage(archive_path: &Path) -> Result<()> { ... MOVED to s3_upload.rs ... }
pub async fn check_aws_cred() -> Result<()> { ... MOVED/REPLACED by s3_upload.rs check ... }
async fn get_database_list(conn: &mut PgConnection) -> Result<Vec<String>> { ... MOVED to db_dump.rs ... }
*/

// Utility function for database connection check (can be moved to a shared utils module if needed elsewhere)
// For now, individual components (like db_dump) handle their own connection needs and sqlx errors provide feedback.
/*
use sqlx::postgres::PgPoolOptions;
async fn check_db_connection(db_url: &str) -> bool {
    println!(\"Verifying database connection to: {}\", db_url); // Potentially hide sensitive parts of URL
    match PgPoolOptions::new().max_connections(1).connect(db_url).await {
        Ok(_pool) => {
            println!(\"✓ Database connection successful.\");
            true
        }
        Err(e) => {
            eprintln!(\"❌ Database connection failed: {}\", e);
            false
        }
    }
}
*/