// databasetool/src/restore/db_restore.rs
use anyhow::{Context, Result};
use sqlx::{Pool, Postgres};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::NamedTempFile;
use url::Url;
use crate::utils::find_psql_executable;

use crate::config::RestoreConfig;




/// Executes a SQL file against the specified database using the `psql` command-line tool.
///
/// # Arguments
/// * `target_db_url` - The connection URL string for the target database.
/// * `sql_file_path` - Path to the .sql file to execute.
/// * `log_context` - A string for logging context (e.g., "schema", "data").
/// * `source_db_name` - Optional source database name for renaming (if provided, replaces occurrences in SQL).
/// * `target_db_name` - Optional target database name for renaming.
async fn execute_sql_file_with_psql(
    target_db_url: &str,
    sql_file_path: &Path,
    log_context: &str,
    source_db_name: Option<&str>,
    target_db_name: Option<&str>,
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

    // If database renaming is requested, create a temporary file with replaced content
    let (sql_file_to_execute, _temp_file_guard) = if let (Some(source), Some(target)) = (source_db_name, target_db_name) {
        if source != target {
            println!("Renaming database references from '{}' to '{}' in {} file", source, target, log_context);
            let sql_content = fs::read_to_string(sql_file_path)
                .with_context(|| format!("Failed to read {} SQL file: {}", log_context, sql_file_path.display()))?;
            
            // Replace database name references intelligently
            let mut modified_content = replace_database_references(&sql_content, source, target);
            
            // Add constraint handling for data files
            if log_context == "data" {
                modified_content = format!("SET session_replication_role = 'replica';\n{}\nSET session_replication_role = 'origin';", modified_content);
            }
            
            let temp_file = NamedTempFile::new()?;
            fs::write(&temp_file, modified_content)
                .with_context(|| format!("Failed to write modified {} SQL content", log_context))?;
            let temp_path = temp_file.into_temp_path();
            (temp_path.to_path_buf(), Some(temp_path))
        } else {
            (PathBuf::from(sql_file_path), None)
        }
    } else {
        (PathBuf::from(sql_file_path), None)
    };

    let output = Command::new(psql_path)
        .arg("-X") // Do not read psqlrc
        .arg("-q") // Quiet mode
        .arg("-v")
        .arg("ON_ERROR_STOP=1") // Exit on first error
        .arg("-d")
        .arg(target_db_url)
        .arg("-f")
        .arg(&sql_file_to_execute)
        .output()
        .with_context(|| {
            format!(
                "Failed to execute psql for {} restoration of file: {}",
                log_context,
                sql_file_to_execute.display()
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_database_renaming_in_sql_content() -> Result<()> {
        // Create a temporary directory and SQL file
        let temp_dir = tempdir()?;
        let sql_file_path = temp_dir.path().join("test_schema.sql");
        
        // SQL content with original database name
        let sql_content = r#"
CREATE DATABASE hotelrule_prod;
\c hotelrule_prod

CREATE SCHEMA IF NOT EXISTS hotelrule_prod;
CREATE TABLE hotelrule_prod.users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

ALTER TABLE hotelrule_prod.users OWNER TO hotelrule_prod_admin;
"#;
        
        fs::write(&sql_file_path, sql_content)?;

        // Test renaming functionality using the new robust function
        let modified_content = replace_database_references(sql_content, "hotelrule_prod", "hotelrule_prod_dev");

        // Debug: print the modified content to see what actually happened
        println!("Original content:\n{}", sql_content);
        println!("Modified content:\n{}", modified_content);
        
        // Verify the replacements worked
        assert!(modified_content.contains("CREATE DATABASE hotelrule_prod_dev"));
        assert!(modified_content.contains("\\c hotelrule_prod_dev"));
        assert!(modified_content.contains("hotelrule_prod_dev.users"));
        assert!(!modified_content.contains("CREATE DATABASE hotelrule_prod;"));
        assert!(!modified_content.contains("\\c hotelrule_prod;"));
        assert!(!modified_content.contains("hotelrule_prod.users"));

        Ok(())
    }
}

/// Intelligently replaces database name references in SQL content
fn replace_database_references(sql_content: &str, source_db: &str, target_db: &str) -> String {
    if source_db == target_db {
        return sql_content.to_string();
    }
    
    // Use a more robust approach that doesn't hardcode specific patterns
    // Focus on replacing the database name as a standalone identifier
    let mut result = sql_content.to_string();
    
    // Replace database name in common contexts where it appears as an identifier
    let patterns = vec![
        format!(" {} ", source_db),
        format!("\"{}\" ", source_db),
        format!(" {}.", source_db),
        format!("\"{}\".", source_db),
        format!(" {};", source_db),
        format!("\"{}\";", source_db),
        format!("\\c {}", source_db),
        format!("\\c \"{}\"", source_db),
    ];
    
    for pattern in patterns {
        let replacement = pattern.replace(source_db, target_db);
        result = result.replace(&pattern, &replacement);
    }
    
    result
}

/// Restores schema for a single database from its SQL file using psql.
pub async fn restore_database_schema(
    target_db_url: &str,
    schema_sql_path: &Path,
    source_db_name: Option<&str>,
    target_db_name: Option<&str>,
) -> Result<()> {
    println!(
        "Restoring schema from {} into target database (using psql)",
        schema_sql_path.display()
    );
    execute_sql_file_with_psql(target_db_url, schema_sql_path, "schema", source_db_name, target_db_name).await
}

/// Restores data for a single database from its SQL file using psql.
pub async fn restore_database_data(
    target_db_url: &str,
    data_sql_path: &Path,
    source_db_name: Option<&str>,
    target_db_name: Option<&str>,
) -> Result<()> {
    println!(
        "Restoring data from {} into target database (using psql)",
        data_sql_path.display()
    );
    
    // Execute the data restoration (constraint handling is now embedded in the SQL file)
    execute_sql_file_with_psql(target_db_url, data_sql_path, "data", source_db_name, target_db_name).await
}

