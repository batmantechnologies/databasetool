// databasetool/src/config/mod.rs
use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

// Structs for deserializing config.json
#[derive(Debug, Clone, Deserialize)]
pub struct JsonS3StorageConfig {
    pub bucket_name: Option<String>,
    pub region: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub endpoint_url: Option<String>,
    pub folder_prefix: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRestoreOptions {
    pub drop_target_database_if_exists: bool,
    pub create_target_database_if_not_exists: bool,
}

#[derive(Debug, Clone, Deserialize)] // Added Deserialize here
pub struct RawJsonConfig {
    pub source_database_url: Option<String>,
    pub target_database_url: Option<String>,
    pub local_backup_dir: Option<PathBuf>,
    pub temp_dump_root: Option<PathBuf>,
    pub archive_file_path_for_restore: Option<String>,
    pub database_list: Option<Vec<String>>,
    pub restore_options: Option<JsonRestoreOptions>,
    pub s3_storage: Option<JsonS3StorageConfig>,
}

// Application's internal configuration structs
#[derive(Debug, Clone)]
pub struct SpacesConfig {
    pub endpoint_url: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub bucket_name: String,
    pub folder_prefix: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BackupConfig {
    pub source_db_url: String,
    pub databases_to_backup: Option<Vec<String>>,
    pub local_backup_path: PathBuf,
    pub temp_dump_root: Option<PathBuf>,
    pub upload_to_spaces: bool,
}

#[derive(Debug, Clone)]
pub struct RestoreConfig {
    pub target_db_url: String,
    pub archive_source_path: String,
    pub databases_to_restore: Option<Vec<String>>,
    pub download_from_spaces: bool,
    pub drop_target_database_if_exists: bool,
    pub create_target_database_if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct SyncConfig {
    pub source_db_url: String,
    pub target_db_url: String,
    pub databases_to_sync: Option<Vec<String>>, // If None, sync all eligible from source based on its list.
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub operation: Option<OperationConfig>,
    pub spaces_config: Option<SpacesConfig>,
    pub raw_json_config: RawJsonConfig, // Store the parsed raw config
}

#[derive(Debug, Clone)]
pub enum OperationConfig {
    Backup(BackupConfig),
    Restore(RestoreConfig),
    Sync(SyncConfig),
}

impl AppConfig {
    pub fn load_from_json(config_path: &Path) -> Result<Self> {
        let config_content = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file at {}", config_path.display()))?;
        let raw_json_config: RawJsonConfig = serde_json::from_str(&config_content)
            .with_context(|| {
                format!(
                    "Failed to parse JSON from config file at {}",
                    config_path.display()
                )
            })?;

        let spaces_config = raw_json_config.s3_storage.as_ref().and_then(|s3_raw| {
            if let (
                Some(bucket),
                Some(region),
                Some(key_id),
                Some(secret),
                Some(endpoint),
            ) = (
                s3_raw.bucket_name.as_ref().filter(|s| !s.is_empty()), // Ensure not empty
                s3_raw.region.as_ref().filter(|s| !s.is_empty()),
                s3_raw.access_key_id.as_ref().filter(|s| !s.is_empty()),
                s3_raw.secret_access_key.as_ref().filter(|s| !s.is_empty()),
                s3_raw.endpoint_url.as_ref().filter(|s| !s.is_empty()),
            ) {
                Some(SpacesConfig {
                    bucket_name: bucket.clone(),
                    region: region.clone(),
                    access_key_id: key_id.clone(),
                    secret_access_key: secret.clone(),
                    endpoint_url: endpoint.clone(),
                    folder_prefix: s3_raw.folder_prefix.clone().filter(|s| !s.is_empty()),
                })
            } else {
                if s3_raw.bucket_name.is_some()
                    || s3_raw.region.is_some()
                    || s3_raw.access_key_id.is_some()
                    || s3_raw.secret_access_key.is_some()
                    || s3_raw.endpoint_url.is_some()
                {
                    // Only print warning if some S3 fields were provided but were incomplete/empty
                    println!("S3 configuration is present in config.json but some required fields (bucket_name, region, access_key_id, secret_access_key, endpoint_url) are missing or empty. S3 operations will be disabled.");
                }
                None
            }
        });

        Ok(AppConfig {
            operation: None, // To be filled by main after parsing CLI args
            spaces_config,
            raw_json_config,
        })
    }
}

pub fn load_backup_config_from_json(
    raw_config: &RawJsonConfig,
    spaces_is_configured: bool,
) -> Result<BackupConfig> {
    let source_db_url = raw_config
        .source_database_url
        .as_ref()
        .context("source_database_url must be set in config.json for backup")?
        .clone();
    let local_backup_path = raw_config
        .local_backup_dir
        .as_ref()
        .context("local_backup_dir must be set in config.json for backup")?
        .clone();

    if local_backup_path.to_string_lossy().is_empty() {
        return Err(anyhow::anyhow!(
            "local_backup_dir cannot be empty in config.json."
        ));
    }

    Ok(BackupConfig {
        source_db_url,
        databases_to_backup: raw_config.database_list.clone(),
        local_backup_path,
        temp_dump_root: raw_config.temp_dump_root.clone(),
        upload_to_spaces: spaces_is_configured, // Enable upload if S3 is generally configured
    })
}

pub fn load_restore_config_from_json(
    raw_config: &RawJsonConfig,
    spaces_is_configured: bool,
) -> Result<RestoreConfig> {
    let target_db_url = raw_config
        .target_database_url
        .as_ref()
        .context("target_database_url must be set in config.json for restore")?
        .clone();
    let archive_source_path = raw_config
        .archive_file_path_for_restore
        .as_ref()
        .context("archive_file_path_for_restore must be set in config.json for restore")?
        .clone();

    if archive_source_path.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "archive_file_path_for_restore cannot be empty in config.json."
        ));
    }

    let restore_opts = raw_config
        .restore_options
        .as_ref()
        .context("restore_options must be defined in config.json for restore")?;

    let download_from_spaces = archive_source_path.starts_with("s3://");
    if download_from_spaces && !spaces_is_configured {
        return Err(anyhow::anyhow!(
            "archive_file_path_for_restore in config.json is an S3 URI, but S3 storage (s3_storage) is not fully configured or is missing required fields."
        ));
    }

    Ok(RestoreConfig {
        target_db_url,
        archive_source_path,
        databases_to_restore: raw_config.database_list.clone(),
        download_from_spaces,
        drop_target_database_if_exists: restore_opts.drop_target_database_if_exists,
        create_target_database_if_not_exists: restore_opts.create_target_database_if_not_exists,
    })
}

pub fn load_sync_config_from_json(
    raw_config: &RawJsonConfig,
) -> Result<SyncConfig> {
    let source_db_url = raw_config
        .source_database_url
        .as_ref()
        .context("source_database_url must be set in config.json for sync operation")?
        .clone();

    let target_db_url = raw_config
        .target_database_url
        .as_ref()
        .context("target_database_url must be set in config.json for sync operation")?
        .clone();

    let databases_to_sync = raw_config.database_list.clone();
    if databases_to_sync.as_ref().map_or(true, |dbs| dbs.is_empty()) {
         println!("Warning: 'database_list' in config.json is empty or not provided for sync operation. This means no databases will be synced unless discovered (if that feature is added). Currently, it likely means nothing will happen.");
        // For sync, an empty or None list usually means no operation.
        // Unlike backup where None might mean "all". For sync, explicit is better.
        // Consider making database_list non-optional in RawJsonConfig for sync if it's always required.
    }


    Ok(SyncConfig {
        source_db_url,
        target_db_url,
        databases_to_sync,
    })
}