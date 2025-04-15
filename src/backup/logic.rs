// src/backup/logic.rs
use std::path::PathBuf;
use chrono::Local;
use std::fs;
use std::process::Command;
use std::env;
use crate::utils::setting::check_db_connection;

fn get_database_list() -> Vec<String> {
    // Read the DATABASE_LIST from the environment variable
    env::var("DATABASE_LIST")
        .expect("DATABASE_LIST must be set")
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

pub fn create_backup_dir() -> PathBuf {
    let timestamp = Local::now().format("%Y-%m-%d_%H_%M_%S").to_string();
    let backup_path = format!("/tmp/databasebackup/{}", timestamp);
    let local_path = env::var("LOCAL_BACKUP_DIR").unwrap_or("./backups".to_string());

    fs::create_dir_all(&backup_path).unwrap();
    fs::create_dir_all(&local_path).unwrap();

    println!("Backup is stored at: {}", backup_path);

    PathBuf::from(backup_path)
}

pub fn dump_databases(backup_dir: &PathBuf) {
    let source_url = env::var("SOURCE_DATABASE_URL").expect("SOURCE_DATABASE_URL must be set");
    let databases = get_database_list();

    for db in databases {
        let timestamp = backup_dir.file_name().unwrap().to_string_lossy();
        let filename = format!("{}_{}.dump", db, timestamp);
        println!("Backing up {}", db);

        let _ = Command::new("pg_dump")
            .args(["--no-owner", "-c", "-F", "c", "-f", &format!("{}/{}", backup_dir.display(), filename), "-d", &format!("{}/{}", source_url, db)])
            .status();
    }
}

pub fn compress_backup(backup_dir: &PathBuf) {
    let timestamp = backup_dir.file_name().unwrap().to_string_lossy();
    let archive_name = format!("database_backup_{}.tar.gz", timestamp);

    let _ = Command::new("tar")
        .args(["-czvf", &format!("./backups/{}", archive_name), "-C", "/tmp/databasebackup", &timestamp])
        .status();
}

pub fn run_backup_flow() {
    let source_url = env::var("SOURCE_DATABASE_URL").expect("SOURCE_DATABASE_URL must be set");
    if !check_db_connection(&source_url) {
        println!("❌ Cannot proceed with restore. Exiting.");
        return;
    }

    let backup_dir = create_backup_dir();
    dump_databases(&backup_dir);
    compress_backup(&backup_dir);
}
