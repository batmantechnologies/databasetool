//! Database Backup/Restore Tool
//!
//! Provides CLI interface for database backup and restore operations

mod utils;
mod backup;
mod restore;
use dotenv::dotenv;
use std::env;

/// Main entry point for the backup/restore tool
#[tokio::main]
async fn main() {
    dotenv().ok();

    // Check for CLI argument first
    let args: Vec<String> = env::args().collect();
    let choice = if args.len() > 1 {
        args[1].trim().to_string()
    } else {
        prompt_choice()
    };

    match choice.as_str() {
        "1" => run_backup().await,
        "2" => run_restore().await,
        _ => println!("❌ Invalid choice. Exiting."),
    }
}

/// Prompts user to select backup or restore operation
///
/// Returns the user's choice as String ("1" or "2")
fn prompt_choice() -> String {
    use std::io::{stdin, stdout, Write};

    println!("Select an operation:");
    println!("1. Take Backup");
    println!("2. Restore Backup");
    print!("Enter your choice: ");
    let _ = stdout().flush();

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}


/// Runs the backup workflow
async fn run_backup() {
    println!("\n🚀 Starting Backup Process...");

    let backup = backup::logic::run_backup_flow().await;
    backup.unwrap();
}

/// Runs the restore workflow
async fn run_restore() {
    println!("\n🔄 Starting Restore Process...");
    
    // Call the restore flow from logic module
    let restore = restore::logic::run_restore_flow().await;
    restore.unwrap()
}
