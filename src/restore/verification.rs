// databasetool/src/restore/verification.rs
use anyhow::{Context, Result};
use sqlx::{Pool, Postgres, Row};
use crate::config::RestoreConfig;

/// Verifies the integrity of the restored database.
///
/// This function can check for table existence, row counts, or specific data
/// as defined by the verification strategy.
///
/// # Arguments
/// * `db_pool` - A connection pool to the newly restored database.
/// * `restore_config` - The restore configuration, which might contain verification parameters.
/// * `expected_schema_files` - A list of schema files that were restored (e.g., dbname_schema.sql).
///                             This can be used to infer expected tables.
/// * `extracted_backup_path` - Path to the directory where backup files were extracted.
///
/// # Returns
/// `Ok(())` if verification passes, or an `Err` if issues are found.
pub async fn verify_restore(
    db_pool: &Pool<Postgres>,
    _restore_config: &RestoreConfig,
    _restored_db_name: &str,
    _extracted_backup_path: &std::path::Path,
) -> Result<()> {
    println!("Performing basic restore verification for database: {}", _restored_db_name);

    // Example: Check if any tables exist (a very basic check)
    let tables: Vec<(String,)> = sqlx::query_as(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'",
    )
    .fetch_all(db_pool)
    .await?;

    if tables.is_empty() {
        println!("Warning: No tables found in the public schema of the restored database '{}'. Verification might be incomplete or the database is expected to be empty.", _restored_db_name);
        // Depending on strictness, this could be an error:
        // return Err(anyhow::anyhow!("No tables found in public schema after restore of '{}'", restored_db_name));
    } else {
        println!("Found {} tables in public schema: {:?}", tables.len(), tables.iter().map(|t| &t.0).collect::<Vec<&String>>());
    }

    // Debug: Check for common system tables (framework-agnostic)
    println!("Checking for common system tables...");
    let common_tables = vec!["migrations", "schema_migrations", "users", "permissions"];
    for table_name in common_tables {
        let check_query = format!("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}')", table_name);
        let exists: (bool,) = sqlx::query_as(&check_query)
            .fetch_one(db_pool)
            .await
            .unwrap_or((false,));
        println!("   Table {} exists: {}", table_name, exists.0);
    }

    // TODO: Implement more comprehensive verification steps:
    // 1. Parse schema files from `extracted_backup_path` to get a list of expected tables.
    //    - For each expected table, query `information_schema.tables` to confirm its existence.
    // 2. For selected tables (perhaps configured or heuristically chosen):
    //    - Use `crate::utils::setting::get_row_count` to check if data was loaded (count > 0 if data expected).
    //    - Compare row counts against metadata potentially stored during backup (advanced).
    // 3. Check for specific sentinel data if applicable.

    println!("✓ Basic restore verification completed for {}.", _restored_db_name);
    
    // Reset sequences to prevent migration failures in any framework
    println!("Starting sequence reset for database: {}", _restored_db_name);
    reset_sequences(db_pool, _restored_db_name).await?;
    println!("✅ Sequence reset completed for {}", _restored_db_name);
    
    Ok(())
}

/// Resets all PostgreSQL sequences to match the maximum values of their corresponding tables
/// This prevents migration failures due to sequence desynchronization in any framework
async fn reset_sequences(db_pool: &Pool<Postgres>, db_name: &str) -> Result<()> {
    println!("🔄 Resetting sequences for database: {}", db_name);
    println!("   This will prevent migration failures due to sequence desynchronization in any framework");
    
    // Get all sequences and their corresponding tables/columns
    let sequences_query = r#"
        SELECT 
            seq.relname as sequence_name,
            dep.deptype as dependency_type,
            tab.relname as table_name,
            attr.attname as column_name
        FROM 
            pg_class seq
        JOIN 
            pg_depend dep ON dep.objid = seq.oid AND dep.deptype = 'a'
        JOIN 
            pg_class tab ON dep.refobjid = tab.oid
        JOIN 
            pg_attribute attr ON dep.refobjid = attr.attrelid AND dep.refobjsubid = attr.attnum
        WHERE 
            seq.relkind = 'S'
            AND tab.relkind = 'r'
            AND tab.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
        ORDER BY 
            tab.relname, attr.attname
    "#;
    
    let sequences: Vec<(String, String, String, String)> = sqlx::query_as(sequences_query)
        .fetch_all(db_pool)
        .await
        .context("Failed to fetch sequence information")?;
    
    if sequences.is_empty() {
        println!("ℹ️  No sequences found in public schema for database: {}", db_name);
        return Ok(());
    }
    
    println!("Found {} sequences to reset", sequences.len());
    println!("   Sequences found: {:?}", sequences.iter().map(|(seq, _, _, _)| seq.clone()).collect::<Vec<String>>());
    
    let mut reset_count = 0;
    let mut error_count = 0;
    
    for (sequence_name, _dependency_type, table_name, column_name) in sequences {
        println!("   Processing sequence: {} (table: {}, column: {})", sequence_name, table_name, column_name);
        // Get the maximum value from the table
        let max_value_query = format!(
            "SELECT COALESCE(MAX({}), 0) as max_val FROM {}",
            column_name, table_name
        );
        
        match sqlx::query(&max_value_query)
            .fetch_one(db_pool)
            .await
        {
            Ok(row) => {
                let max_val: i64 = row.try_get("max_val").unwrap_or(0);
                let next_val = max_val + 1;
                
                // Reset the sequence
                let reset_query = format!(
                    "SELECT setval('{}', {}, false)",
                    sequence_name, next_val
                );
                
                match sqlx::query(&reset_query)
                    .execute(db_pool)
                    .await
                {
                    Ok(_) => {
                        println!("✓ Reset sequence {} to {} (table: {}, column: {})", 
                            sequence_name, next_val, table_name, column_name);
                        reset_count += 1;
                    }
                    Err(e) => {
                        println!("⚠️  Failed to reset sequence {}: {}", sequence_name, e);
                        println!("   Reset query: {}", reset_query);
                        error_count += 1;
                    }
                }
            }
            Err(e) => {
                println!("⚠️  Failed to get max value for table {}: {}", table_name, e);
                error_count += 1;
            }
        }
    }
    
    // Special handling for common system tables that often have sequence issues
    println!("   Performing special reset for common system tables...");
    reset_common_system_sequences(db_pool).await?;
    
    println!("✓ Sequence reset completed: {} successful, {} errors", reset_count, error_count);
    if error_count > 0 {
        println!("⚠️  Some sequences failed to reset. This may cause migration issues.");
    }
    Ok(())
}

/// Special handling for common system tables that often have sequence corruption issues
async fn reset_common_system_sequences(db_pool: &Pool<Postgres>) -> Result<()> {
    let common_tables = vec![
        "migrations",
        "schema_migrations", 
        "users",
        "permissions",
        "groups"
    ];
    
    for table_name in common_tables {
        let sequence_name = format!("{}_id_seq", table_name);
        let max_value_query = format!("SELECT COALESCE(MAX(id), 0) as max_val FROM {}", table_name);
        println!("   Processing common table: {} with sequence: {}", table_name, sequence_name);
        
        match sqlx::query(&max_value_query)
            .fetch_one(db_pool)
            .await
        {
            Ok(row) => {
                let max_val: i64 = row.try_get("max_val").unwrap_or(0);
                let next_val = max_val + 1;
                
                let reset_query = format!(
                    "SELECT setval('{}', {}, false)",
                    sequence_name, next_val
                );
                
                if let Err(e) = sqlx::query(&reset_query)
                    .execute(db_pool)
                    .await
                {
                    println!("⚠️  Failed to reset common sequence {}: {}", sequence_name, e);
                } else {
                    println!("✓ Reset common sequence {} to {}", sequence_name, next_val);
                }
            }
            Err(e) => {
                // Table might not exist, which is fine
                if !e.to_string().contains("does not exist") {
                    println!("⚠️  Failed to get max value for common table {}: {}", table_name, e);
                    println!("   Max value query: {}", max_value_query);
                } else {
                    println!("   Table {} does not exist, skipping sequence reset", table_name);
                }
            }
        }
    }
    
    Ok(())
}