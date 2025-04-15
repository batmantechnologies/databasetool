use std::path::PathBuf;
use chrono::Local;
use std::fs;
use std::process::Command;
use std::env;
use crate::utils::databases::DATABASES;
use crate::utils::setting::check_db_connection;
use std::path::Path;
use url::Url;

pub fn extract_archive() -> String {

    let archive_file = env::var("ARCHIVE_FILE_PATH").expect("ARCHIVE_FILE_PATH must be set");

    let file_name = Path::new(&archive_file)
    .file_name()
    .unwrap()
    .to_string_lossy()
    .to_string();

    let timestamp = file_name
        .trim_start_matches("database_backup_")
        .trim_end_matches(".tar.gz")
        .to_string();

    let backup_dir = "/tmp/databasebackup";

    // ✅ Just extract — no need to create the subdir
    let _ = Command::new("tar")
        .args(["-xzvf", &archive_file, "-C", &format!("{}/extract", backup_dir)])
        .status();

    timestamp
}


pub fn verify_files(timestamp: &str) {
    for db in DATABASES {
        let dump_file = format!("/tmp/databasebackup/extract/{}/{}_{}.dump", timestamp, db, timestamp);
        if !Path::new(&dump_file).exists() {
            println!("Missing file for {}", db);
        }
    }
}

pub fn restore_databases(timestamp: &str) {
    let target_url = env::var("TARGET_DATABASE_URL").expect("TARGET_DATABASE_URL must be set");

    // Parse the URL to extract components
    let parsed = Url::parse(&target_url).expect("Invalid TARGET_DATABASE_URL");
    let host = parsed.host_str().unwrap_or("localhost");
    let user = parsed.username();
    let port = parsed.port().unwrap_or(5432);
    let password = parsed.password().unwrap_or("");

    for db in DATABASES {
        let restored_db_name = format!("{}{}", db, "_restored");
        let dump_path = format!("/tmp/databasebackup/extract/{}/{}_{}.dump", timestamp, db, timestamp);

        // Create DB
        let status = Command::new("createdb")
                            .env("PGPASSWORD", password) // ✅ also here
                            .args(["-U", user, "-h", host, "-p", &port.to_string(), &restored_db_name])
                            .status()
                            .expect("Failed to create database");

        // Restore DB
        Command::new("pg_restore")
                .env("PGPASSWORD", password)
                .args([
                    "--clean", "--if-exists", "--no-owner",
                    "-U", user,
                    "-h", host,
                    "-p", &port.to_string(),
                    "-d", &restored_db_name,
                    &dump_path
                ])
                .status()
                .expect("Failed to execute pg_restore");
                }
}


pub fn run_restore_flow() {
    let source_url = env::var("TARGET_DATABASE_URL").expect("TARGET_DATABASE_URL must be set");
    if !check_db_connection(&source_url) {
        println!("❌ Cannot proceed with restore. Exiting.");
        return;
    }
    let timestamp = extract_archive();
    verify_files(&timestamp);
    restore_databases(&timestamp);
}
