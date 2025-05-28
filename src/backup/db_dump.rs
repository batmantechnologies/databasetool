// databasetool/src/backup/db_dump.rs
use anyhow::{Context, Result};
use sqlx::{Connection, PgConnection, Row};
use std::path::{Path, PathBuf};
use std::process::Command;
use url::Url;
use which::which;

use crate::config::BackupConfig;

// Helper function to find pg_dump executable
fn find_pg_dump_executable() -> Result<PathBuf> {
    which("pg_dump")
        .context("pg_dump executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.")
}

/// Dumps all specified databases or all non-template databases from the source using pg_dump.
pub async fn dump_databases(
    backup_config: &BackupConfig,
    target_dump_dir: &Path,
) -> Result<Vec<String>> {
    println!(
        "Starting pg_dump based database dump process. Target directory: {}",
        target_dump_dir.display()
    );

    let pg_dump_path = find_pg_dump_executable()?;
    println!("Found pg_dump executable at: {}", pg_dump_path.display());

    let base_url_str = get_base_url_without_db(&backup_config.source_db_url)?;
    // Admin connection is still needed if the list of databases isn't explicitly provided.
    let mut admin_conn_opt = if backup_config.databases_to_backup.is_none() {
        Some(PgConnection::connect(&format!("{}/postgres", base_url_str))
            .await
            .with_context(|| {
                format!(
                    "Failed to connect to 'postgres' database on {} for listing databases",
                    base_url_str
                )
            })?)
    } else {
        None
    };

    let databases_to_backup = match &backup_config.databases_to_backup {
        Some(dbs) => {
            if dbs.iter().any(|name| name.trim().is_empty() || name.contains(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')) {
                return Err(anyhow::anyhow!("Invalid character in database name list from config: {:?}. Check DATABASE_LIST env var.", dbs));
            }
            dbs.clone()
        }
        None => {
            println!("No specific databases listed in config, fetching all non-template databases...");
            if let Some(conn) = &mut admin_conn_opt {
                get_database_list(conn).await?
            } else {
                // This case should ideally not be reached if logic is correct,
                // as admin_conn_opt is Some when databases_to_backup is None.
                return Err(anyhow::anyhow!("Admin connection not available to fetch database list."));
            }
        }
    };

    if databases_to_backup.is_empty() {
        anyhow::bail!("No databases found or specified to back up.");
    }

    println!("Databases to be backed up: {:?}", databases_to_backup);
    let mut successfully_dumped_dbs = Vec::new();

    for db_name in &databases_to_backup {
        if db_name.trim().is_empty() || db_name.contains(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
            eprintln!("Skipping invalid database name: {}", db_name);
            continue;
        }

        if db_name.starts_with("template")
            || (db_name == "postgres"
                && backup_config
                    .databases_to_backup
                    .as_ref()
                    .map_or(true, |dbs| !dbs.contains(db_name)))
        {
            println!("Skipping system/template database: {}", db_name);
            continue;
        }

        println!("Processing database with pg_dump: {}", db_name);
        let db_specific_url_for_pg_dump = format!("{}/{}", base_url_str, db_name);

        let schema_file_path = target_dump_dir.join(format!("{}_schema.sql", db_name));
        let data_file_path = target_dump_dir.join(format!("{}_data.sql", db_name));

        // Dump schema using pg_dump
        println!("Dumping schema for {} to {} using pg_dump...", db_name, schema_file_path.display());
        let schema_dump_cmd_output = Command::new(&pg_dump_path)
            .arg("--schema-only")
            .arg("-f")
            .arg(&schema_file_path)
            .arg(&db_specific_url_for_pg_dump) // pg_dump accepts the full URL
            .output()
            .with_context(|| format!("Failed to execute pg_dump for schema of database: {}", db_name))?;

        if !schema_dump_cmd_output.status.success() {
            return Err(anyhow::anyhow!(
                "pg_dump (schema) for database {} failed with status: {}\nStdout: {}\nStderr: {}",
                db_name,
                schema_dump_cmd_output.status,
                String::from_utf8_lossy(&schema_dump_cmd_output.stdout),
                String::from_utf8_lossy(&schema_dump_cmd_output.stderr)
            ));
        }
        println!("✓ Schema for {} dumped successfully via pg_dump.", db_name);

        // Dump data using pg_dump
        println!("Dumping data for {} to {} using pg_dump...", db_name, data_file_path.display());
        let data_dump_cmd_output = Command::new(&pg_dump_path)
            .arg("--data-only")
            .arg("--column-inserts") // Produces INSERT statements; good for compatibility if restore uses psql or similar
            // .arg("--inserts") // Alternative: might be faster, one large INSERT per table
            .arg("-f")
            .arg(&data_file_path)
            .arg(&db_specific_url_for_pg_dump)
            .output()
            .with_context(|| format!("Failed to execute pg_dump for data of database: {}", db_name))?;

        if !data_dump_cmd_output.status.success() {
            return Err(anyhow::anyhow!(
                "pg_dump (data) for database {} failed with status: {}\nStdout: {}\nStderr: {}",
                db_name,
                data_dump_cmd_output.status,
                String::from_utf8_lossy(&data_dump_cmd_output.stdout),
                String::from_utf8_lossy(&data_dump_cmd_output.stderr)
            ));
        }
        println!("✓ Data for {} dumped successfully via pg_dump.", db_name);
        
        successfully_dumped_dbs.push(db_name.clone());
        println!("✓ Successfully dumped schema and data for {} using pg_dump", db_name);
    }

    Ok(successfully_dumped_dbs)
}

async fn get_database_list(conn: &mut PgConnection) -> Result<Vec<String>> {
    println!("Fetching list of databases...");
    let rows = sqlx::query(
        "SELECT datname FROM pg_database WHERE datistemplate = false AND datallowconn = true;",
    )
    .fetch_all(conn)
    .await
    .context("Failed to fetch database list from pg_database")?;

    let db_names: Vec<String> = rows
        .iter()
        .map(|row| row.try_get("datname"))
        .collect::<Result<_, _>>()
        .context("Failed to get 'datname' from row when fetching database list")?;
    
    println!("Found databases: {:?}", db_names);
    Ok(db_names)
}

fn get_base_url_without_db(full_url: &str) -> Result<String> {
    let mut parsed_url = Url::parse(full_url)
        .with_context(|| format!("Invalid database URL format: {}", full_url))?;
    parsed_url.set_path("");
    Ok(parsed_url.to_string())
}
// The pure Rust schema and data dumping functions (dump_schema_pure_rust, dump_data_pure_rust)
// have been removed as we are now using pg_dump.