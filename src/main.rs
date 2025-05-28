//! Database Backup/Restore Tool
//!
//! Provides CLI interface for database backup and restore operations

// databasetool/src/main.rs
mod utils;
mod backup;
mod restore;
mod sync; // Added sync module
mod config; // Added config module

use anyhow::{Context, Result};
use config::{
    AppConfig, OperationConfig, load_backup_config_from_json, load_restore_config_from_json,
    load_sync_config_from_json,
};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

/// Main entry point for the backup/restore tool
#[tokio::main]
async fn main() -> ExitCode {
    match run_app().await {
        Ok(_) => {
            println!("✅ Operation completed successfully.");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("❌ Error: {:?}", e);
            ExitCode::FAILURE
        }
    }
}

async fn run_app() -> Result<()> {
    // Define the path to config.json. Expects it in the same directory as the executable
    // or the project root if running with `cargo run`.
    let config_path = PathBuf::from("config.json");
    let mut app_config = AppConfig::load_from_json(&config_path)
        .context(format!("Failed to load application configuration from {}", config_path.display()))?;

    let args: Vec<String> = env::args().collect();
    let choice = if args.len() > 1 {
        args[1].trim().to_string()
    } else {
        prompt_choice()?
    };

    let spaces_is_configured = app_config.spaces_config.is_some();

    match choice.as_str() {
        "1" | "backup" => {
            println!("🚀 Starting Backup Process...");
            let backup_config = load_backup_config_from_json(&app_config.raw_json_config, spaces_is_configured)
                .context("Failed to load backup configuration from JSON")?;
            app_config.operation = Some(OperationConfig::Backup(backup_config));
            backup::run_backup_flow(&app_config).await
                .context("Backup process failed")?;
        }
        "2" | "restore" => {
            println!("🔄 Starting Restore Process...");
            let restore_config = load_restore_config_from_json(&app_config.raw_json_config, spaces_is_configured)
                .context("Failed to load restore configuration from JSON")?;
            app_config.operation = Some(OperationConfig::Restore(restore_config.clone()));
            
            println!("Restore target: {}, Archive: {}", restore_config.target_db_url, restore_config.archive_source_path);
            restore::run_restore_flow(&app_config).await.context("Restore process failed")?;

        }
        "3" | "sync" => {
            println!("⚙️ Starting Sync Process...");
            let sync_config = load_sync_config_from_json(&app_config.raw_json_config)
                .context("Failed to load sync configuration from JSON")?;
            app_config.operation = Some(OperationConfig::Sync(sync_config));
            sync::run_sync_flow(&app_config).await
                .context("Sync process failed")?;
        }
        _ => {
            println!("❌ Invalid choice. Please enter '1' (backup), '2' (restore), or '3' (sync).");
            anyhow::bail!("Invalid operation choice");
        }
    }
    Ok(())
}

/// Prompts user to select backup or restore operation
///
/// Returns the user's choice as String
fn prompt_choice() -> Result<String> {
    use std::io::{stdin, stdout, Write};

    println!("Select an operation:");
    println!("1. Take Backup (or type 'backup')");
    println!("2. Restore Backup (or type 'restore')");
    println!("3. Sync Databases (Source to Target) (or type 'sync')");
    print!("Enter your choice: ");
    let _ = stdout().flush().context("Failed to flush stdout")?;

    let mut input = String::new();
    stdin().read_line(&mut input).context("Failed to read user input")?;
    Ok(input.trim().to_string())
}
