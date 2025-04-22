use sqlx::postgres::PgRow;
use std::fs;
use std::io::Write;
use std::env;
use std::path::PathBuf;
use sqlx::postgres::PgPoolOptions;

pub async fn check_db_connection(db_url: &str) -> bool {
    match PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
    {
        Ok(_) => {
            println!("✅ Successfully connected to {}", db_url);
            true
        },
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", db_url, e);
            false
        }
    }
}