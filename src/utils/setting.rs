// Run shell commands
use postgres::{Client, NoTls};

pub fn check_db_connection(db_url: &str) -> bool {
    match Client::connect(db_url, NoTls) {
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