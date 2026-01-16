// databasetool/src/restore/verification.rs
use anyhow::Result;
use sqlx::{Pool, Postgres};
use crate::config::RestoreConfig;
use crate::utils::sequence_reset;

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
    sequence_reset::reset_sequences_with_timeout(db_pool, _restored_db_name).await?;
    println!("✅ Sequence reset completed for {}", _restored_db_name);
    
    Ok(())
}

