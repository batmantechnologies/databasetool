mod logic; // Keep existing logic, will be refactored internally
pub(crate) mod s3_upload; // New module for S3 interactions
pub(crate) mod archive;   // New module for tarball creation
pub(crate) mod db_dump;    // New module for database dumping logic

use anyhow::Result;
use crate::config::AppConfig;

/// Public entry point for the backup process.
/// This function will orchestrate the backup flow using the provided configuration.
pub async fn run_backup_flow(app_config: &AppConfig) -> Result<()> {
    let backup_config = match &app_config.operation {
        Some(crate::config::OperationConfig::Backup(cfg)) => cfg,
        _ => anyhow::bail!("Backup operation selected but no backup configuration found."),
    };

    // Delegate to the internal logic function, which will be refactored
    // to use the new modular components (s3_upload, archive, db_dump).
    logic::perform_backup_orchestration(app_config, backup_config).await
}