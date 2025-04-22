// src/main.rs
use std::path::Path;
mod utils;
mod backup;
mod restore;
use dotenv::dotenv;
use std::env;
use backup::logic::run_backup_flow;
use restore::logic::run_restore_flow;

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


async fn run_backup() {
    println!("\nStarting Backup...");

    run_backup_flow().await;
}

async fn run_restore() {
    println!("\nStarting Restore...");
    
    // Get archive path from environment variable
    let archive_path = env::var("ARCHIVE_FILE_PATH").expect("ARCHIVE_FILE_PATH must be set");

    // Validate archive exists
    if !Path::new(&archive_path).exists() {
        println!("❌ Archive file not found at: {}", archive_path);
        return;
    }

    // Call the restore flow from logic module
    restore::logic::run_restore_flow(&archive_path).await;
}
