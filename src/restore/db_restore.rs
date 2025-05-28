// databasetool/src/restore/db_restore.rs
use anyhow::{Context, Result};
use sqlx::{Pool, Postgres};
use std::path::{Path, PathBuf};
use std::process::Command;
use url::Url;
use which::which;

use crate::config::RestoreConfig;

/// Finds the psql executable in the system PATH.
fn find_psql_executable() -> Result<PathBuf> {
    which("psql").context(
        "psql executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.",
    )
}

/// Executes a SQL file against the specified database using the `psql` command-line tool.
///
/// # Arguments
/// * `target_db_url` - The connection URL string for the target database.
/// * `sql_file_path` - Path to the .sql file to execute.
/// * `log_context` - A string for logging context (e.g., "schema", "data").
async fn execute_sql_file_with_psql(
    target_db_url: &str,
    sql_file_path: &Path,
    log_context: &str,
) -> Result<()> {
    if !sql_file_path.exists() {
        return Err(anyhow::anyhow!(
            "SQL file for {} restoration not found: {}",
            log_context,
            sql_file_path.display()
        ));
    }

    let psql_path = find_psql_executable()?;
    println!(
        "Executing {} SQL file with psql: {} on database {}...",
        log_context,
        sql_file_path.display(),
        target_db_url // Be mindful of logging full URLs with credentials in production
    );

    let output = Command::new(psql_path)
        .arg("-X") // Do not read psqlrc
        .arg("-q") // Quiet mode
        .arg("-v")
        .arg("ON_ERROR_STOP=1") // Exit on first error
        .arg("-d")
        .arg(target_db_url)
        .arg("-f")
        .arg(sql_file_path)
        .output()
        .with_context(|| {
            format!(
                "Failed to execute psql for {} restoration of file: {}",
                log_context,
                sql_file_path.display()
            )
        })?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "psql execution for {} restoration failed for file: {}.\nStatus: {}\nStdout: {}\nStderr: {}",
            log_context,
            sql_file_path.display(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    println!(
        "✓ Successfully executed {} SQL file with psql: {}",
        log_context,
        sql_file_path.display()
    );
    Ok(())
}


/// Manages the target database based on restore configuration.
/// This includes potentially dropping and/or creating the database.
pub async fn manage_target_database(
    restore_config: &RestoreConfig,
    db_name_to_manage: &str,
) -> Result<bool> {
    println!("Managing target database: {}", db_name_to_manage);

    let mut admin_url = Url::parse(&restore_config.target_db_url)
        .context("Invalid TARGET_DATABASE_URL format for admin connection")?;
    
    let original_db_path = admin_url.path().trim_start_matches('/').to_string();

    admin_url.set_path("/postgres"); 

    let admin_pool = Pool::<Postgres>::connect(&admin_url.to_string())
        .await
        .with_context(|| format!("Failed to connect to 'postgres' database on target server: {}", admin_url.host_str().unwrap_or("unknown_host")))?;

    let db_exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(db_name_to_manage)
        .fetch_one(&admin_pool)
        .await
        .with_context(|| format!("Failed to check existence of database '{}'", db_name_to_manage))?;

    if db_exists {
        println!("Database '{}' already exists on the target server.", db_name_to_manage);
        if restore_config.drop_target_database_if_exists {
            if db_name_to_manage.eq_ignore_ascii_case("postgres") || 
               (original_db_path.eq_ignore_ascii_case("postgres") && db_name_to_manage.eq_ignore_ascii_case(&original_db_path)) {
                 return Err(anyhow::anyhow!("Configuration indicates dropping database '{}', but it is a critical system database. This is not allowed.", db_name_to_manage));
            }

            println!("Dropping database '{}' as per configuration...", db_name_to_manage);
            
            let terminate_sql = format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid();"
            );
            sqlx::query(&terminate_sql)
                .bind(db_name_to_manage)
                .execute(&admin_pool)
                .await
                .with_context(|| format!("Failed to terminate connections to database '{}'. This might require superuser privileges.", db_name_to_manage))?;
            
            sqlx::query(&format!(r#"DROP DATABASE "{}" WITH (FORCE)"#, db_name_to_manage.replace('\"', "\"\"")))
                .execute(&admin_pool)
                .await
                .with_context(|| format!("Failed to drop database '{}'", db_name_to_manage))?;
            println!("✓ Database '{}' dropped.", db_name_to_manage);
            
            create_database_if_not_exists(&admin_pool, db_name_to_manage, &restore_config.target_db_url).await?;
            return Ok(true); 
        } else {
            println!("Database '{}' exists and 'DROP_TARGET_DATABASE_IF_EXISTS' is false. No action taken on database structure. Tables within might be affected by restore.", db_name_to_manage);
            return Ok(false); 
        }
    } else {
        println!("Database '{}' does not exist on the target server.", db_name_to_manage);
        if restore_config.create_target_database_if_not_exists {
            create_database_if_not_exists(&admin_pool, db_name_to_manage, &restore_config.target_db_url).await?;
            return Ok(true);
        } else {
            return Err(anyhow::anyhow!(
                "Database '{}' does not exist and 'CREATE_TARGET_DATABASE_IF_NOT_EXISTS' is false. Cannot proceed with restore for this database.",
                db_name_to_manage
            ));
        }
    }
}

async fn create_database_if_not_exists(
    admin_pool: &Pool<Postgres>,
    db_name: &str,
    original_target_db_url: &str,
) -> Result<()> {
    println!("Creating database '{}'...", db_name);
    
    let parsed_original_url = Url::parse(original_target_db_url)?;
    let owner = parsed_original_url.username();

    let mut create_sql = format!(r#"CREATE DATABASE "{}" "#, db_name.replace('\"', "\"\""));
    if !owner.is_empty() {
        create_sql.push_str(&format!(r#" OWNER "{}" "#, owner.replace('\"', "\"\"")));
    }

    sqlx::query(&create_sql)
        .execute(admin_pool)
        .await
        .with_context(|| format!("Failed to create database '{}'", db_name))?;
    println!("✓ Database '{}' created.", db_name);
    Ok(())
}

/// Restores schema for a single database from its SQL file using psql.
pub async fn restore_database_schema(
    target_db_url: &str,
    schema_sql_path: &Path,
) -> Result<()> {
    println!(
        "Restoring schema from {} into target database (using psql)",
        schema_sql_path.display()
    );
    execute_sql_file_with_psql(target_db_url, schema_sql_path, "schema").await
}

/// Restores data for a single database from its SQL file using psql.
pub async fn restore_database_data(
    target_db_url: &str,
    data_sql_path: &Path,
) -> Result<()> {
    println!(
        "Restoring data from {} into target database (using psql)",
        data_sql_path.display()
    );
    execute_sql_file_with_psql(target_db_url, data_sql_path, "data").await
}

/// Extracts the database name from a PostgreSQL connection URL.
pub fn get_db_name_from_url(db_url: &str) -> Result<String> {
    let parsed_url = Url::parse(db_url)
        .with_context(|| format!("Invalid database URL format: {}", db_url))?;
    let path = parsed_url.path().trim_start_matches('/');
    if path.is_empty() {
        Err(anyhow::anyhow!("Database name not found in URL path: {}", db_url))
    } else {
        Ok(path.to_string())
    }
}