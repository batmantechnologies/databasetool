// databasetool/src/restore/logic.rs
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use url::Url;

use crate::config::{AppConfig, RestoreConfig};
use crate::restore::{db_restore, s3_download, verification};
use crate::utils::setting::prepare_archive_for_restore; // Corrected import


/// Orchestrates the entire database restore process.
pub async fn perform_restore_orchestration(
    app_config: &AppConfig,
    restore_config: &RestoreConfig,
) -> Result<()> {
    println!("🔄 Starting restore orchestration...");
    println!("Restore configuration: {:?}", restore_config);

    // 1. Determine archive path: Download from S3 or use local path
    let local_archive_path: PathBuf;
    let _s3_download_temp_dir: Option<TempDir> = None; // To hold temp dir if downloaded

    if restore_config.download_from_spaces {
        let spaces_conf = app_config.spaces_config.as_ref().context(
            "S3 download requested, but S3/Spaces configuration is missing.",
        )?;
        let (bucket, key) = s3_download::parse_s3_uri(&restore_config.archive_source_path)
            .context("Failed to parse S3 URI for archive download")?;

        // Create a temporary directory to download the archive
        let temp_s3_download_dir = tempfile::Builder::new()
            .prefix("s3_download_")
            .tempdir()
            .context("Failed to create temporary directory for S3 download")?;
        
        let archive_filename = Path::new(&key)
            .file_name()
            .context("Could not determine filename from S3 key")?
            .to_string_lossy()
            .into_owned();
            
        let downloaded_path = temp_s3_download_dir.path().join(archive_filename);

        s3_download::download_file_from_s3(
            spaces_conf,
            &bucket,
            &key,
            &downloaded_path,
        )
        .await
        .context("Failed to download archive from S3/Spaces")?;
        
        local_archive_path = downloaded_path;
        // _s3_download_temp_dir = Some(temp_s3_download_dir); 
        // Guard will clean up. We just need the path for now.
        // Actually, we DO need to keep the guard, otherwise the archive is deleted before extraction.
        // So, the archive will live in this temp dir, then be extracted to another temp dir.
        // This is acceptable.
        // To avoid local_archive_path being dropped, we can move the temp_s3_download_dir
        // to a variable that lives through the function scope.
        // For simplicity now, let's assume `download_file_from_s3` returns the path
        // and we need to ensure this path stays valid.
        // The current structure: downloaded to temp_s3_download_dir; this dir guard needs to live.
        // We will pass local_archive_path (which is inside _s3_download_temp_dir) to extraction.
        // Best to keep _s3_download_temp_dir itself.
        // Let's re-think. The archive is downloaded. Then prepare_archive_for_restore will extract it.
        // So, the _s3_download_temp_dir must live until extraction is complete.
        // The `local_archive_path` is what we need.
        // The _s3_download_temp_dir will be dropped at end of this function.
        // If prepare_archive_for_restore reads from local_archive_path while _s3_download_temp_dir
        // is still in scope, it's fine.
    } else {
        local_archive_path = PathBuf::from(&restore_config.archive_source_path);
        if !local_archive_path.exists() {
            return Err(anyhow::anyhow!("Local archive path does not exist: {}", local_archive_path.display()));
        }
    }
    println!("Using archive for restore: {}", local_archive_path.display());

    // 2. Prepare working directory by extracting the archive
    // `extraction_temp_dir` guard ensures cleanup of extracted files.
    let extraction_temp_dir = prepare_archive_for_restore(&local_archive_path)
        .context("Failed to prepare archive and extract to temporary directory")?;
    let extracted_files_path = extraction_temp_dir.path();
    println!("Archive extracted to temporary directory: {}", extracted_files_path.display());

    // List contents of extracted directory for debugging
    println!("Contents of extracted directory ({}):", extracted_files_path.display());
    for entry in fs::read_dir(extracted_files_path)? {
        let entry = entry?;
        println!("  - {}", entry.path().display());
    }

    // 3. Determine which databases to restore
    //    If `restore_config.databases_to_restore` is Some, use that list.
    //    If None, discover databases from the extracted files (e.g., by looking for *_schema.sql patterns).
    let databases_to_process: Vec<String>;
    if let Some(dbs_from_config) = &restore_config.databases_to_restore {
        if dbs_from_config.is_empty() {
             println!("DATABASE_LIST is empty in config. Attempting to discover databases from archive.");
             databases_to_process = discover_databases_from_archive(extracted_files_path)?;
        } else {
            databases_to_process = dbs_from_config.clone();
        }
    } else {
        println!("No DATABASE_LIST in config. Attempting to discover databases from archive.");
        databases_to_process = discover_databases_from_archive(extracted_files_path)?;
    }

    if databases_to_process.is_empty() {
        anyhow::bail!("No databases found in archive or specified in config to restore.");
    }
    println!("Databases to be restored: {:?}", databases_to_process);


    // 4. For each database:
    for db_name_from_archive in &databases_to_process {
        println!("\nProcessing restore for database from archive: {}", db_name_from_archive);

        // Determine the actual target database name.
        // Current TARGET_DATABASE_URL specifies the connection, and its path component is the DB name.
        // If multiple databases are in the archive, we need a strategy:
        //  a) Restore all into the single DB specified by TARGET_DATABASE_URL (potentially messy if schemas clash).
        //  b) TARGET_DATABASE_URL's path is a template, and we append/replace with db_name_from_archive.
        //  c) The config `databases_to_restore` should map archive DB names to target DB names if they differ.
        // For now, assume TARGET_DATABASE_URL's path IS the target database name,
        // and if multiple dbs are in archive, we restore them sequentially into this ONE target db.
        // This is simplistic and might need refinement based on user intent.
        // A better approach: if `databases_to_restore` is set, it means these specific DBs from the
        // archive should be restored. If `TARGET_DATABASE_URL` points to `db_A`, and archive contains `db_X`, `db_Y`,
        // and `databases_to_restore = ["db_X"]`, then `db_X` content goes into `db_A`.
        // If `TARGET_DATABASE_URL` structure is `postgres://user:pass@host:port/`, and we want to restore `db_X` as `db_X_restored`,
        // the target URL needs to be dynamically constructed.

        // Let's use the db name from TARGET_DATABASE_URL as the *target* database for restoration.
        // If multiple databases are listed in `databases_to_process`, they will all be restored into this one target.
        // This might not be ideal if the archive contains distinct databases.
        // The current config `RestoreConfig` has `target_db_url`. The database name is part of this URL.
        
        let target_db_name_from_url = db_restore::get_db_name_from_url(&restore_config.target_db_url)?;
        println!("Target database for restore operations: {}", target_db_name_from_url);

        // Manage the target database (drop/create if configured)
        // This function uses the `target_db_name_from_url` to manage the DB on the server.
        let _db_was_created_or_modified = db_restore::manage_target_database(restore_config, &target_db_name_from_url)
            .await
            .with_context(|| format!("Failed to manage target database: {}", target_db_name_from_url))?;

        // Construct the specific URL for connecting to the now-managed target database.
        // The host, port, user, pass come from restore_config.target_db_url.
        // The path is the target_db_name_from_url.
        let mut actual_target_db_conn_url = Url::parse(&restore_config.target_db_url)?;
        actual_target_db_conn_url.set_path(&target_db_name_from_url);
        let actual_target_db_conn_url_str = actual_target_db_conn_url.to_string();

        // Create a connection pool to the target database for schema/data restore and verification
        println!("Connecting to target database \'{}\' for restore operations...", target_db_name_from_url);
        let target_db_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5) // Adjust as needed
            .connect(&actual_target_db_conn_url_str)
            .await
            .with_context(|| format!("Failed to connect to target database \'{}\' at {} for restore operations", target_db_name_from_url, actual_target_db_conn_url_str))?;


        // Find schema and data files for `db_name_from_archive`
        // The archive files are named like `dbname_YYYY-MM-DD_HH_MM_SS_schema.sql` or `dbname_schema.sql`
        // We need to find the correct schema/data files within `extracted_files_path`
        // that correspond to `db_name_from_archive`.
        // The `db_dump` module created files like `DBNAME_schema.sql` and `DBNAME_data.sql`.
        
        let schema_file_name = format!("{}_schema.sql", db_name_from_archive);
        let schema_file_path = extracted_files_path.join(&schema_file_name);

        let data_file_name = format!("{}_data.sql", db_name_from_archive);
        let data_file_path = extracted_files_path.join(&data_file_name);

        if !schema_file_path.exists() {
            return Err(anyhow::anyhow!(
                "Schema file not found for database '{}' in extracted archive: {}. Expected pattern: {}_schema.sql",
                db_name_from_archive, schema_file_path.display(), db_name_from_archive
            ));
        }
         if !data_file_path.exists() {
            // Data file might be optional for some backup types (e.g. schema-only)
            // However, our current backup process creates both.
            println!(
                "Warning: Data file not found for database '{}' in extracted archive: {}. Expected pattern: {}_data.sql. Proceeding with schema restore only.",
                db_name_from_archive, data_file_path.display(), db_name_from_archive
            );
        }

        // 4a. Restore schema
        println!("Restoring schema for {} from {}...", db_name_from_archive, schema_file_path.display());
        db_restore::restore_database_schema(&actual_target_db_conn_url_str, &schema_file_path)
            .await
            .with_context(|| format!("Failed to restore schema for database \'{}\' from file {}", db_name_from_archive, schema_file_path.display()))?;
        println!("✓ Schema restoration completed for {}.", db_name_from_archive);

        // 4b. Restore data (if data file exists)
        if data_file_path.exists() {
            println!("Restoring data for {} from {}...", db_name_from_archive, data_file_path.display());
            db_restore::restore_database_data(&actual_target_db_conn_url_str, &data_file_path)
                .await
                .with_context(|| format!("Failed to restore data for database \'{}\' from file {}", db_name_from_archive, data_file_path.display()))?;
            println!("✓ Data restoration completed for {}.", db_name_from_archive);
        } else {
             println!("Skipping data restoration for {} as data file was not found.", db_name_from_archive);
        }

        // 4c. Verify restore for this database
        verification::verify_restore(&target_db_pool, restore_config, &target_db_name_from_url, extracted_files_path)
            .await
            .with_context(|| format!("Failed to verify_restore for database \'{}\'", target_db_name_from_url))?;
        
        // Close the pool for the current database being restored
        target_db_pool.close().await;
    }

    // 5. Cleanup: extraction_temp_dir and _s3_download_temp_dir (if any) will be cleaned up when they go out of scope.
    println!("✓ Restore orchestration completed.");
    Ok(())
}


/// Discovers database names from the files in the extracted archive directory.
/// Looks for files matching `*_schema.sql`.
fn discover_databases_from_archive(extracted_path: &Path) -> Result<Vec<String>> {
    let mut db_names = Vec::new();
    for entry in fs::read_dir(extracted_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(file_name_os) = path.file_name() {
                let file_name = file_name_os.to_string_lossy();
                if file_name.ends_with("_schema.sql") {
                    if let Some(db_name) = file_name.strip_suffix("_schema.sql") {
                        if !db_name.is_empty() {
                            db_names.push(db_name.to_string());
                        }
                    }
                }
            }
        }
    }
    db_names.sort();
    db_names.dedup();
    if db_names.is_empty() {
         println!("Warning: Could not discover any database schema files (*_schema.sql) in the archive at {}", extracted_path.display());
    } else {
        println!("Discovered databases from archive: {:?}", db_names);
    }
    Ok(db_names)
}


// Old functions from the original logic.rs, to be removed after full refactor.
/*
Original run_restore_flow and its helpers like restore_schema, restore_from_sql_file,
split_sql_with_dollar_quotes, check_db_connection (within restore),
extract_table_name_from_create, extract_table_name, create_table_from_insert.
These are being replaced by the new modular approach with perform_restore_orchestration,
db_restore module, etc.
*/