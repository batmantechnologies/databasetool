// databasetool/src/sync/mod.rs
pub(crate) mod logic;

use anyhow::Result;
use crate::config::AppConfig;

/// Public entry point for the sync process.
/// This function will orchestrate the sync flow using the provided configuration.
pub async fn run_sync_flow(app_config: &AppConfig) -> Result<()> {
    let sync_config = match &app_config.operation {
        Some(crate::config::OperationConfig::Sync(cfg)) => cfg,
        _ => anyhow::bail!("Sync operation selected but no sync configuration found."),
    };

    logic::perform_sync_orchestration(app_config, sync_config).await
}