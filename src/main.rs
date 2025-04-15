// src/main.rs

mod utils;
mod backup;
mod restore;

use backup::logic::run_backup_flow;
use restore::logic::run_restore_flow;

fn main() {
    dotenv::dotenv().ok();
    let choice = prompt_choice();

    match choice.as_str() {
        "1" => run_backup(),
        "2" => run_restore(),
        _ => println!("Invalid choice. Exiting."),
    }
}

fn prompt_choice() -> String {
    use std::io::{stdin, stdout, Write};
    let mut input = String::new();
    println!("Select an operation:");
    println!("1. Take Backup");
    println!("2. Restore Backup");
    print!("Enter your choice: ");
    let _ = stdout().flush();
    stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn run_backup() {
    println!("\nStarting Backup...");

    run_backup_flow();

    println!("\n✅ Backup completed.");
}

fn run_restore() {
    println!("\nStarting Restore...");
    
    run_restore_flow();

    println!("\n✅ Restore completed.");
}
