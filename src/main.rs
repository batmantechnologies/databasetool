// src/main.rs

mod utils;
mod backup;
mod restore;
use dotenv::dotenv;
use std::env;
use backup::logic::run_backup_flow;
use restore::logic::run_restore_flow;

fn main() {
    dotenv().ok();

    // Check for CLI argument first
    let args: Vec<String> = env::args().collect();
    let choice = if args.len() > 1 {
        args[1].trim().to_string()
    } else {
        prompt_choice()
    };

    match choice.as_str() {
        "1" => run_backup(),
        "2" => run_restore(),
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


fn run_backup() {
    println!("\nStarting Backup...");

    run_backup_flow();
}

fn run_restore() {
    println!("\nStarting Restore...");
    
    run_restore_flow();
}
