// databasetool/src/sync/logic.rs
use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;
use tempfile::Builder as TempFileBuilder;
use url::Url;
use which::{which};

use crate::config::{AppConfig, SyncConfig};
use crate::restore::db_restore; // For manage_target_database and psql execution

/// Finds the pg_dump executable in the system PATH.
fn find_pg_dump_executable() -> Result<PathBuf> {
    which("pg_dump").context("pg_dump executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.")
}

/// Finds the psql executable in the system PATH.
fn find_psql_executable() -> Result<PathBuf> {
    which("psql").context("psql executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.")
}

/// Finds the pg_restore executable in the system PATH.
fn find_pg_restore_executable() -> Result<PathBuf> {
    which("pg_restore").context("pg_restore executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.")
}

/// Orchestrates the database synchronization process.
///
/// For each database specified in the sync configuration:
/// 1. Creates a temporary directory for the dump.
/// 2. Dumps the schema from the source database.
/// 3. Dumps the data from the source database.
/// 4. Manages the target database (drops if exists, then creates).
/// 5. Restores the schema to the target database.
/// 6. Restores the data to the target database.
/// 7. Cleans up the temporary dump directory.
pub async fn perform_sync_orchestration(
    _app_config: &AppConfig, // _app_config might be used later for S3 credentials if direct S3->S3 sync is added
    sync_config: &SyncConfig,
) -> Result<()> {
    println!("⚙️ Starting database synchronization orchestration...");
    println!("Sync configuration: {:?}", sync_config);

    let pg_dump_path = find_pg_dump_executable()?;
    let psql_path = find_psql_executable()?; // psql is needed for schema restore
    let pg_restore_path = find_pg_restore_executable()?; // pg_restore is needed for data restore

    let databases_to_sync = match &sync_config.databases_to_sync {
        Some(dbs) if !dbs.is_empty() => dbs.clone(),
        _ => {
            println!("No specific databases listed or list is empty in sync_config.databases_to_sync. Nothing to sync.");
            return Ok(());
        }
    };

    let source_base_url_str = get_base_url_without_db(&sync_config.source_db_url)?;
    let target_base_url_str = get_base_url_without_db(&sync_config.target_db_url)?;


    for db_name in &databases_to_sync {
        println!("\n🔄 Synchronizing database: {}", db_name);

        // 1. Create a temporary directory for this database's dump
        let temp_dump_dir = TempFileBuilder::new()
            .prefix(&format!("sync_dump_{}_", db_name))
            .tempdir()
            .with_context(|| format!("Failed to create temporary dump directory for database {}", db_name))?;
        let temp_dump_path = temp_dump_dir.path();
        println!("Temporary dump directory for {}: {}", db_name, temp_dump_path.display());

        let source_db_specific_url = format!("{}/{}", source_base_url_str, db_name);
        let target_db_specific_url = format!("{}/{}", target_base_url_str, db_name);


        // --- 2. Dump Schema from Source ---
        let schema_file_path = temp_dump_path.join(format!("{}_schema.sql", db_name));
        println!("Dumping schema for {} from {} to {}...", db_name, source_db_specific_url, schema_file_path.display());
        let schema_dump_cmd_output = Command::new(&pg_dump_path)
            .arg("--schema-only")
            .arg("-f")
            .arg(&schema_file_path)
            .arg(&source_db_specific_url)
            .output()
            .with_context(|| format!("Failed to execute pg_dump for schema of source database: {}", db_name))?;

        if !schema_dump_cmd_output.status.success() {
            return Err(anyhow::anyhow!(
                "pg_dump (schema) for source database {} failed with status: {}\\nStdout: {}\\nStderr: {}",
                db_name,
                schema_dump_cmd_output.status,
                String::from_utf8_lossy(&schema_dump_cmd_output.stdout),
                String::from_utf8_lossy(&schema_dump_cmd_output.stderr)
            ));
        }
        println!("✓ Schema for source {} dumped successfully.", db_name);

        // --- 3. Dump Data from Source ---
        let data_file_path = temp_dump_path.join(format!("{}_data.sql", db_name));
        println!("Dumping data for {} from {} to {}...", db_name, source_db_specific_url, data_file_path.display());
        let data_dump_cmd_output = Command::new(&pg_dump_path)
            .arg("--data-only")
            .arg("--format=custom") // Use custom format for pg_restore compatibility
            .arg("-f")
            .arg(&data_file_path)
            .arg(&source_db_specific_url)
            .output()
            .with_context(|| format!("Failed to execute pg_dump for data of source database: {}", db_name))?;

        if !data_dump_cmd_output.status.success() {
            return Err(anyhow::anyhow!(
                "pg_dump (data) for source database {} failed with status: {}\\nStdout: {}\\nStderr: {}",
                db_name,
                data_dump_cmd_output.status,
                String::from_utf8_lossy(&data_dump_cmd_output.stdout),
                String::from_utf8_lossy(&data_dump_cmd_output.stderr)
            ));
        }
        println!("✓ Data for source {} dumped successfully.", db_name);

        // --- 4. Manage Target Database (Drop if exists, then Create) ---
        // For sync, we always drop and create.
        // We use a dummy RestoreConfig here as manage_target_database expects it.
        // The important parts are the target_db_url and the drop/create flags.
        let temp_restore_config_for_manage = crate::config::RestoreConfig {
            target_db_url: sync_config.target_db_url.clone(), // The main URL for connecting to 'postgres' db
            archive_source_path: String::new(), // Not used by manage_target_database
            databases_to_restore: None, // Not used
            download_from_spaces: false, // Not used
            drop_target_database_if_exists: true, // Key for sync: always drop
            create_target_database_if_not_exists: true, // Key for sync: always create
        };
        db_restore::manage_target_database(&temp_restore_config_for_manage, db_name)
            .await
            .with_context(|| format!("Failed to manage target database (drop/create): {}", db_name))?;


        // --- 5. Restore Schema to Target ---
        println!("Restoring schema for {} to target database {}...", db_name, target_db_specific_url);
        let psql_schema_restore_output = Command::new(&psql_path)
            .arg("-X")
            .arg("-q")
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-d")
            .arg(&target_db_specific_url)
            .arg("-f")
            .arg(&schema_file_path)
            .output()
            .with_context(|| format!("Failed to execute psql for schema restore to target database: {}", db_name))?;

        if !psql_schema_restore_output.status.success() {
            return Err(anyhow::anyhow!(
                "psql (schema restore) for target database {} failed with status: {}\\nStdout: {}\\nStderr: {}",
                db_name,
                psql_schema_restore_output.status,
                String::from_utf8_lossy(&psql_schema_restore_output.stdout),
                String::from_utf8_lossy(&psql_schema_restore_output.stderr)
            ));
        }
        println!("✓ Schema for target {} restored successfully.", db_name);

        // --- 6. Restore Data to Target ---
        println!("Restoring data for {} to target database {}...", db_name, target_db_specific_url);
        
        // Use pg_restore with disable-triggers option to handle foreign key constraints
        let pg_restore_data_output = Command::new(&pg_restore_path)
            .arg("--data-only")
            .arg("--disable-triggers") // Disable triggers during data restore to avoid FK violations
            .arg("--no-owner")
            .arg("--no-acl")
            .arg("--exit-on-error")
            .arg("--dbname")
            .arg(&target_db_specific_url)
            .arg(&data_file_path)
            .output()
            .with_context(|| format!("Failed to execute pg_restore for data restore to target database: {}", db_name))?;

        if !pg_restore_data_output.status.success() {
            return Err(anyhow::anyhow!(
                "pg_restore (data restore) for target database {} failed with status: {}\\nStdout: {}\\nStderr: {}",
                db_name,
                pg_restore_data_output.status,
                String::from_utf8_lossy(&pg_restore_data_output.stdout),
                String::from_utf8_lossy(&pg_restore_data_output.stderr)
            ));
        }
        println!("✓ Data for target {} restored successfully.", db_name);

        // 7. Cleanup for this database is handled by TempDir going out of scope.
        println!("✓ Successfully synchronized database: {}", db_name);
    }

    println!("✅ Database synchronization orchestration completed.");
    Ok(())
}

/// Gets the base URL string (e.g., "postgres://user:pass@host:port") without the database path.
fn get_base_url_without_db(full_url: &str) -> Result<String> {
    let mut parsed_url = Url::parse(full_url)
        .with_context(|| format!("Invalid database URL format: {}", full_url))?;
    parsed_url.set_path(""); // Remove the database part of the path
    Ok(parsed_url.to_string())
}