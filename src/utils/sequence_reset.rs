// databasetool/src/utils/sequence_reset.rs
use anyhow::{Context, Result};
use sqlx::{Pool, Postgres, Row};
use std::time::Duration;
use tokio::time::timeout;

/// Resets all PostgreSQL sequences to match the maximum values of their corresponding tables
/// This prevents migration failures due to sequence desynchronization
pub async fn reset_all_sequences(db_pool: &Pool<Postgres>, db_name: &str) -> Result<()> {
    println!("🔄 Resetting all sequences for database: {}", db_name);
    
    // Query to get all sequences and their corresponding tables/columns
    let sequences_query = r#"
        SELECT 
            seq.relname as sequence_name,
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
        JOIN
            pg_namespace nsp ON seq.relnamespace = nsp.oid
        WHERE 
            seq.relkind = 'S'
            AND tab.relkind = 'r'
            AND nsp.nspname = 'public'
        ORDER BY 
            tab.relname, attr.attname
    "#;
    
    let sequences = sqlx::query_as::<_, (String, String, String)>(sequences_query)
        .fetch_all(db_pool)
        .await
        .context("Failed to fetch sequence information")?;
    
    if sequences.is_empty() {
        println!("ℹ️  No sequences found in public schema for database: {}", db_name);
        return Ok(());
    }
    
    println!("Found {} sequences to reset", sequences.len());
    
    let mut reset_count = 0;
    let mut error_count = 0;
    
    // Reset each sequence
    for (sequence_name, table_name, column_name) in sequences {
        println!("   Processing sequence: {} (table: {}, column: {})", sequence_name, table_name, column_name);
        
        // Get the maximum value from the table
        let max_value_query = format!(
            "SELECT COALESCE(MAX({}), 0) as max_val FROM {}",
            column_name, table_name
        );
        
        // Use a more flexible approach to handle different integer types
        match sqlx::query(&max_value_query)
            .fetch_one(db_pool)
            .await
        {
            Ok(row) => {
                // Try different integer types to handle INT4 (i32) and INT8 (i64)
                let max_val = if let Ok(val) = row.try_get::<i64, _>("max_val") {
                    val
                } else if let Ok(val) = row.try_get::<i32, _>("max_val") {
                    val as i64
                } else {
                    println!("⚠️  Failed to parse max value for table {} - unsupported type", table_name);
                    error_count += 1;
                    continue;
                };
                
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
    
    // Handle common system tables that might not be caught by the above query
    reset_common_system_sequences(db_pool).await?;
    
    println!("✅ Sequence reset completed: {} successful, {} errors", reset_count, error_count);
    Ok(())
}

/// Special handling for common system tables that often have sequence issues
async fn reset_common_system_sequences(db_pool: &Pool<Postgres>) -> Result<()> {
    let common_tables = vec![
        ("migrations", "id"),
        ("schema_migrations", "id"), 
        ("users", "id"),
        ("permissions", "id"),
        ("groups", "id"),
        ("otp", "id"),  // Specifically handle the otp table mentioned in the issue
    ];
    
    println!("   Processing common system tables...");
    
    for (table_name, column_name) in common_tables {
        let sequence_name = format!("{}_{}_seq", table_name, column_name);
        let max_value_query = format!("SELECT COALESCE(MAX({}), 0) as max_val FROM {}", column_name, table_name);
        
        match sqlx::query(&max_value_query)
            .fetch_one(db_pool)
            .await
        {
            Ok(row) => {
                // Try different integer types to handle INT4 (i32) and INT8 (i64)
                let max_val = if let Ok(val) = row.try_get::<i64, _>("max_val") {
                    val
                } else if let Ok(val) = row.try_get::<i32, _>("max_val") {
                    val as i64
                } else {
                    println!("   Note: Could not parse max value for table {} - unsupported type", table_name);
                    continue;
                };
                
                let next_val = max_val + 1;
                let reset_query = format!("SELECT setval('{}', {}, false)", sequence_name, next_val);
                
                match sqlx::query(&reset_query)
                    .execute(db_pool)
                    .await
                {
                    Ok(_) => {
                        println!("✓ Reset common sequence {} to {}", sequence_name, next_val);
                    }
                    Err(e) => {
                        // It's okay if the sequence doesn't exist for some tables
                        println!("   Note: Could not reset sequence {} (might not exist): {}", sequence_name, e);
                    }
                }
            }
            Err(e) => {
                // Table might not exist, which is fine
                if !e.to_string().contains("does not exist") {
                    println!("⚠️  Failed to get max value for common table {}: {}", table_name, e);
                } else {
                    println!("   Table {} does not exist, skipping sequence reset", table_name);
                }
            }
        }
    }
    
    Ok(())
}

/// Ensures sequences are properly reset with a timeout
pub async fn reset_sequences_with_timeout(db_pool: &Pool<Postgres>, db_name: &str) -> Result<()> {
    let timeout_duration = Duration::from_secs(300); // 5 minutes timeout
    
    match timeout(timeout_duration, reset_all_sequences(db_pool, db_name)).await {
        Ok(result) => result,
        Err(_) => {
            Err(anyhow::anyhow!(
                "Sequence reset operation timed out after {} seconds for database: {}",
                timeout_duration.as_secs(),
                db_name
            ))
        }
    }
}