use std::fs;
use std::env;
use url::Url;
use sqlx::PgPool;
use anyhow::{Result, Context};
use std::path::{Path, PathBuf};
use sqlx::postgres::PgPoolOptions;
use crate::utils::setting::{prepare_working_directory};

pub async fn restore_schema(pool: &PgPool, schema_path: &str) -> Result<()> {
    let schema_content = fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {}", schema_path))?;

    // Split into individual statements
    let statements = split_sql_with_dollar_quotes(&schema_content);

    // Separate statements into sequences, tables, and others
    let (sequences, rest): (Vec<_>, Vec<_>) = statements.into_iter()
        .partition(|stmt| stmt.contains("CREATE SEQUENCE"));
    
    let (tables, others): (Vec<_>, Vec<_>) = rest.into_iter()
        .partition(|stmt| stmt.contains("CREATE TABLE"));

    // Phase 1: Create sequences first
    for stmt in sequences {
        let mut transaction = pool.begin().await?;
        match sqlx::query(&stmt).execute(&mut *transaction).await {
            Ok(_) => {
                transaction.commit().await?;
                println!("✅ Created sequence");
            }
            Err(e) if e.to_string().contains("already exists") => {
                transaction.rollback().await?;
                println!("ℹ Sequence already exists");
            }
            Err(e) => {
                transaction.rollback().await?;
                eprintln!("⚠️ Failed to create sequence: {}", e);
                return Err(e.into());
            }
        }
    }

    // Phase 2: Create tables with retry logic
    let mut remaining_tables = tables;
    let mut attempts = 0;
    const MAX_ATTEMPTS: usize = 3;

    while !remaining_tables.is_empty() && attempts < MAX_ATTEMPTS {
        attempts += 1;
        let mut next_remaining = Vec::new();

        for stmt in remaining_tables {
            let mut transaction = pool.begin().await?;
            match sqlx::query(&stmt).execute(&mut *transaction).await {
                Ok(_) => {
                    transaction.commit().await?;
                    if let Some(table_name) = extract_table_name_from_create(&stmt) {
                        println!("✅ Created table: {}", table_name);
                    }
                }
                Err(e) if e.to_string().contains("already exists") => {
                    transaction.rollback().await?;
                    println!("ℹ Table already exists");
                }
                Err(e) => {
                    transaction.rollback().await?;
                    eprintln!("⚠️ Failed to create table (attempt {}): {}", attempts, e);
                    next_remaining.push(stmt);
                }
            }
        }

        remaining_tables = next_remaining;
    }

    if !remaining_tables.is_empty() {
        return Err(anyhow::anyhow!(
            "Failed to create {} tables after {} attempts",
            remaining_tables.len(),
            MAX_ATTEMPTS
        ));
    }

    // Phase 3: Process other statements
    for stmt in others {
        let mut transaction = pool.begin().await?;
        match sqlx::query(&stmt).execute(&mut *transaction).await {
            Ok(_) => transaction.commit().await?,
            Err(e) if e.to_string().contains("already exists") => {
                transaction.rollback().await?;
                println!("ℹ Object already exists");
            }
            Err(e) => {
                transaction.rollback().await?;
                eprintln!("⚠️ Failed to execute statement (skipping): {}", e);
            }
        }
    }

    Ok(())
}

pub async fn restore_from_sql_file(pool: &PgPool, sql_path: &str) -> Result<()> {
    let sql = fs::read_to_string(sql_path)
        .with_context(|| format!("Failed to read SQL file: {}", sql_path))?;

    let statements = split_sql_with_dollar_quotes(&sql);
    let batch_size = 100; // Process in batches to avoid memory issues
    let mut total_processed = 0;

    for chunk in statements.chunks(batch_size) {
        println!("Processing statements {}-{} of {}",
            total_processed + 1,
            total_processed + chunk.len(),
            statements.len());

        // Execute statements individually to avoid prepared statement issues
        for stmt in chunk {
            total_processed += 1;
            // Split on semicolons and process each command separately
            let commands: Vec<&str> = stmt.split(';')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            
            for cmd in commands {
                if let Err(e) = sqlx::query(cmd).execute(pool).await {
                    // Handle missing tables for INSERT statements
                    if e.to_string().contains("relation") && e.to_string().contains("does not exist") {
                        if let Some(table_name) = extract_table_name(stmt) {
                            eprintln!("Table {} missing, attempting to create", table_name);
                            if create_table_from_insert(stmt, pool).await.is_ok() {
                                // Retry the statement after creating table
                                if let Err(e) = sqlx::query(stmt).execute(pool).await {
                                    eprintln!("⚠️ Failed to execute statement {} after creating table (skipping): {}\n{}",
                                        total_processed, e, stmt);
                                }
                                continue;
                            }
                        }
                    }
                    eprintln!("⚠️ Failed to execute statement {} (skipping): {}\n{}",
                        total_processed, e, stmt);
                }
            }
        }
    }

    println!("✅ Data restore completed ({} statements processed)", total_processed);
    Ok(())
}

fn split_sql_with_dollar_quotes(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_dollar_quote = false;
    let mut quote_tag = String::new();
    let mut in_single_quote = false;
    let mut in_comment = false;

    for line in sql.lines() {
        let mut chars = line.chars().peekable();
        
        while let Some(c) = chars.next() {
            // Handle line comments
            if !in_dollar_quote && !in_single_quote && c == '-' && chars.peek() == Some(&'-') {
                chars.next(); // Skip second '-'
                in_comment = true;
                break;
            }

            match c {
                '\'' if !in_dollar_quote && !in_comment => in_single_quote = !in_single_quote,
                '$' if !in_single_quote && !in_comment => {
                    if chars.peek() == Some(&'$') {
                        chars.next();
                        if !in_dollar_quote {
                            // Start of dollar quote
                            let mut tag = String::new();
                            while let Some(&tc) = chars.peek() {
                                if tc == '$' { break; }
                                tag.push(tc);
                                chars.next();
                            }
                            quote_tag = tag;
                            in_dollar_quote = true;
                        } else {
                            // Check for end tag
                            let mut potential_end = String::new();
                            while let Some(&tc) = chars.peek() {
                                if tc == '$' { break; }
                                potential_end.push(tc);
                                chars.next();
                            }
                            if potential_end == quote_tag {
                                in_dollar_quote = false;
                                quote_tag.clear();
                            }
                        }
                    }
                }
                ';' if !in_dollar_quote && !in_single_quote && !in_comment => {
                    current.push(c);
                    if !current.trim().is_empty() {
                        statements.push(current.trim().to_string());
                    }
                    current = String::new();
                    continue;
                }
                _ => {}
            }
            current.push(c);
        }
        
        if !in_comment && !in_dollar_quote {
            current.push('\n');
        }
        in_comment = false;
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    // Handle function definitions as single statements
    let mut final_statements = Vec::new();
    let mut current_func = String::new();
    let mut in_function = false;

    for stmt in statements {
        if stmt.contains("CREATE OR REPLACE FUNCTION") ||
           stmt.contains("CREATE FUNCTION") {
            in_function = true;
            current_func = stmt;
            continue;
        }

        if in_function {
            current_func.push_str(&stmt);
            if stmt.contains("LANGUAGE") && stmt.contains("$$") {
                final_statements.push(current_func.trim().to_string());
                current_func.clear();
                in_function = false;
            }
        } else {
            final_statements.push(stmt);
        }
    }

    if !current_func.is_empty() {
        final_statements.push(current_func);
    }

    final_statements
}


pub async fn run_restore_flow() -> Result<(), anyhow::Error> {

    let original_url = env::var("TARGET_DATABASE_URL").context("TARGET_DATABASE_URL must be set")?;
    let original_url = if original_url.starts_with("postgres://") || original_url.starts_with("postgresql://") {
        original_url
    } else {
        format!("postgres://{}", original_url)
    };
    let db_names = env::var("DATABASE_LIST").context("DATABASE_LIST must be set")?;
    let archive_path = env::var("ARCHIVE_FILE_PATH").context("ARCHIVE_FILE_PATH must be set")?;
    
    let path = Path::new(&archive_path);
    let working_path: PathBuf;

    // Handle tar.gz files
    if path.extension().map_or(false, |ext| ext == "gz") &&
       path.file_stem().map_or(false, |stem| {
                   let stem = stem.to_string_lossy();
                   stem.ends_with(".tar") || stem.ends_with("_tar")
               }) {
               println!("🔍 Detected tar archive, extracting...");
               
               working_path = prepare_working_directory(&path)?;
    } else {
        working_path = path.to_path_buf();
    }

    println!("ℹ Using backup path: {}", working_path.display());
    // Process each database in the comma-separated list
    for db_name in db_names.split(',').map(|s| s.trim()) {
        let restored_db_name = format!("{}_restored", db_name);
        
        // Create admin connection to postgres database
        let mut admin_url = Url::parse(&original_url).context("Invalid database URL - must be in format postgres://user:password@host:port/database")?;
        admin_url.set_path("/postgres");
        
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url.to_string())
            .await
            .expect("Failed to create admin database pool");

        // Check if restored database already exists
        let db_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)"
        )
        .bind(&restored_db_name)
        .fetch_one(&admin_pool)
        .await
        .expect("Failed to check database existence");

        if !db_exists {
            println!("Creating empty restored database '{}'...", restored_db_name);
            sqlx::query(&format!(r#"CREATE DATABASE "{}""#, restored_db_name))
                .execute(&admin_pool)
                .await
                .expect("Failed to create empty restored database");
        } else {
           println!("Restored database '{}' already exists", restored_db_name);
           return Ok(());
        }

        // Create target URL for restored database
        let mut target_url = Url::parse(&original_url).expect("Invalid database URL");
        target_url.set_path(&format!("/{}", restored_db_name));
        let target_url = target_url.to_string();

        if !check_db_connection(&target_url).await {
            println!("❌ Cannot connect to database. Exiting.");
            return Ok(());
        }

        // Show target database info
        let target_db = Url::parse(&target_url)
            .expect("Invalid database URL")
            .path()
            .trim_start_matches('/')
            .to_string();
        println!("🔌 Connecting to target database: {}", target_db);
        
        // Create pool connection for our target DB
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&target_url)
            .await
            .expect("Failed to create database pool");

        let path = &working_path;
        if path.is_dir() {
            println!("📁 Processing directory backup format");
            println!("ℹ Directory contents:");
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                println!("- {}", entry.path().display());
            }

            let timestamp = path.file_name()
                .and_then(|n| n.to_str())
                .context("Invalid backup directory name - should be in format YYYYMMDD_HHMMSS")?;
            
            println!("ℹ Extracted timestamp from directory name: {}", timestamp);

            println!("🔍 Looking for backup files in: {}", path.display());
            let schema_path = path.join(format!("{}_{}_schema.sql", db_name, timestamp));
            let data_path = path.join(format!("{}_{}_data.sql", db_name, timestamp));

            println!("ℹ Expected schema file: {}", schema_path.display());
            println!("ℹ Expected data file: {}", data_path.display());

            if !schema_path.exists() {
                return Err(anyhow::anyhow!(
                    "Schema file not found: {}",
                    schema_path.display()
                ));
            }
            if !data_path.exists() {
                return Err(anyhow::anyhow!(
                    "Data file not found: {}",
                    data_path.display()
                ));
            }

            println!("✅ Found both schema and data files");

            // Restore schema first
            println!("🔄 Starting schema restoration...");
            match restore_schema(&pool, &schema_path.to_string_lossy()).await {
                Ok(_) => println!("✅ Schema restoration completed"),
                Err(e) => {
                    eprintln!("❌ Schema restoration failed: {}", e);
                    return Err(e);
                }
            }

            // Then restore data
            println!("🔄 Starting data restoration...");
            match restore_from_sql_file(&pool, &data_path.to_string_lossy()).await {
                Ok(_) => println!("✅ Data restoration completed"),
                Err(e) => {
                    eprintln!("❌ Data restoration failed: {}", e);
                    return Err(e);
                }
            }
        } else {
            // Handle single file case (legacy)
            restore_from_sql_file(&pool, &archive_path).await
                .context("Restore failed")?;
        }
    }

    println!("\n✅ Restore completed.");
    Ok(())
}

async fn check_db_connection(db_url: &str) -> bool {
    PgPoolOptions::new().max_connections(1).connect(db_url).await.is_ok()
}

fn extract_table_name_from_create(query: &str) -> Option<String> {
    if query.starts_with("CREATE TABLE") {
        let parts: Vec<&str> = query.split_whitespace().collect();
        if parts.len() >= 3 {
            Some(parts[2].trim_matches('"').to_string())
        } else {
            None
        }
    } else {
        None
    }
}

fn extract_table_name(query: &str) -> Option<String> {
    if query.starts_with("INSERT INTO") {
        let start = query.find("INSERT INTO")? + "INSERT INTO".len();
        let end = query[start..].find(' ').map(|i| start + i).unwrap_or(query.len());
        Some(query[start..end].trim().trim_matches('"').to_string())
    } else {
        None
    }
}

async fn create_table_from_insert(query: &str, pool: &PgPool) -> Result<(), sqlx::Error> {
    eprintln!("Creating table from INSERT statement");
    let table_name = extract_table_name(query)
        .ok_or(sqlx::Error::Protocol("Could not extract table name".into()))?;
    
    // Extract column names from INSERT statement
    let cols_start = query.find('(')
        .ok_or(sqlx::Error::Protocol("Could not find column list".into()))? + 1;
    let cols_end = query[cols_start..].find(')')
        .ok_or(sqlx::Error::Protocol("Could not find column list end".into()))?;
    let cols_str = &query[cols_start..cols_start + cols_end];
    
    let columns: Vec<&str> = cols_str.split(',')
        .map(|s| s.trim().trim_matches('"'))
        .collect();
    
    // Create table with TEXT columns by default (simplest approach)
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS \"{}\" ({})",
        table_name,
        columns.iter().map(|c| format!("\"{}\" TEXT", c)).collect::<Vec<_>>().join(", ")
    );
    
    sqlx::query(&create_sql).execute(pool).await?;
    Ok(())
}