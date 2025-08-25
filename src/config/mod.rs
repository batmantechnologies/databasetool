// databasetool/src/config/mod.rs
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
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
    pub database_list: Option<serde_json::Value>,
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
    pub databases_to_restore: Option<HashMap<String, String>>,
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
        databases_to_backup: parse_database_list_for_backup_sync(&raw_config.database_list)?,
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
        databases_to_restore: parse_database_list_for_restore(&raw_config.database_list)?,
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

    let databases_to_sync = parse_database_list_for_backup_sync(&raw_config.database_list)?;
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

/// Parses the database_list configuration for backup and sync operations
/// Returns a vector of source database names
fn parse_database_list_for_backup_sync(database_list: &Option<serde_json::Value>) -> Result<Option<Vec<String>>> {
    match database_list {
        Some(value) => {
            if value.is_array() {
                // Old format: ["db1", "db2"]
                let databases: Vec<String> = serde_json::from_value(value.clone())
                    .context("Failed to parse database_list as array")?;
                Ok(Some(databases))
            } else if value.is_object() {
                // New format: {"source_db": "target_db"}
                // For backup/sync, we only need the source database names
                let mapping: HashMap<String, String> = serde_json::from_value(value.clone())
                    .context("Failed to parse database_list as mapping object")?;
                let source_databases: Vec<String> = mapping.keys().cloned().collect();
                Ok(Some(source_databases))
            } else {
                Err(anyhow::anyhow!("database_list must be either an array of database names or a mapping object"))
            }
        }
        None => Ok(None),
    }
}

/// Parses the database_list configuration for restore operations
/// Returns a mapping of source database names to target database names
fn parse_database_list_for_restore(database_list: &Option<serde_json::Value>) -> Result<Option<HashMap<String, String>>> {
    match database_list {
        Some(value) => {
            if value.is_array() {
                // Old format: ["db1", "db2"] - map each database to itself
                let databases: Vec<String> = serde_json::from_value(value.clone())
                    .context("Failed to parse database_list as array")?;
                let mapping: HashMap<String, String> = databases.into_iter().map(|db| (db.clone(), db)).collect();
                Ok(Some(mapping))
            } else if value.is_object() {
                // New format: {"source_db": "target_db"}
                let mapping: HashMap<String, String> = serde_json::from_value(value.clone())
                    .context("Failed to parse database_list as mapping object")?;
                Ok(Some(mapping))
            } else {
                Err(anyhow::anyhow!("database_list must be either an array of database names or a mapping object"))
            }
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_database_list_for_backup_sync_array() -> anyhow::Result<()> {
        let value = Some(json!(["db1", "db2", "db3"]));
        let result = parse_database_list_for_backup_sync(&value)?;
        
        assert_eq!(result, Some(vec!["db1".to_string(), "db2".to_string(), "db3".to_string()]));
        Ok(())
    }

    #[test]
    fn test_parse_database_list_for_backup_sync_mapping() -> anyhow::Result<()> {
        let value = Some(json!({
            "hotelrule_prod": "hotelrule_prod_dev",
            "analytics_db": "analytics_staging"
        }));
        let result = parse_database_list_for_backup_sync(&value)?;
        
        let mut expected = vec!["hotelrule_prod".to_string(), "analytics_db".to_string()];
        expected.sort();
        
        let mut result_vec = result.unwrap();
        result_vec.sort();
        
        assert_eq!(result_vec, expected);
        Ok(())
    }

    #[test]
    fn test_parse_database_list_for_backup_sync_none() -> anyhow::Result<()> {
        let result = parse_database_list_for_backup_sync(&None)?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_parse_database_list_for_restore_array() -> anyhow::Result<()> {
        let value = Some(json!(["db1", "db2"]));
        let result = parse_database_list_for_restore(&value)?;
        
        let expected = vec![
            ("db1".to_string(), "db1".to_string()),
            ("db2".to_string(), "db2".to_string())
        ].into_iter().collect();
        
        assert_eq!(result, Some(expected));
        Ok(())
    }

    #[test]
    fn test_parse_database_list_for_restore_mapping() -> anyhow::Result<()> {
        let value = Some(json!({
            "hotelrule_prod": "hotelrule_prod_dev",
            "analytics_db": "analytics_staging"
        }));
        let result = parse_database_list_for_restore(&value)?;
        
        let expected = vec![
            ("hotelrule_prod".to_string(), "hotelrule_prod_dev".to_string()),
            ("analytics_db".to_string(), "analytics_staging".to_string())
        ].into_iter().collect();
        
        assert_eq!(result, Some(expected));
        Ok(())
    }

    #[test]
    fn test_parse_database_list_for_restore_none() -> anyhow::Result<()> {
        let result = parse_database_list_for_restore(&None)?;
        assert_eq!(result, None);
        Ok(())
    }

    #[test]
    fn test_parse_database_list_invalid_format() {
        let value = Some(json!("invalid_string"));
        let result = parse_database_list_for_backup_sync(&value);
        assert!(result.is_err());
        
        let result = parse_database_list_for_restore(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_complete_database_renaming_workflow() -> anyhow::Result<()> {
        // Test the complete workflow from configuration to restore mapping
        let config_json = json!({
            "hotelrule_prod": "hotelrule_prod_dev",
            "analytics_db": "analytics_staging"
        });

        // Test backup/sync parsing (should extract source names only)
        let backup_result = parse_database_list_for_backup_sync(&Some(config_json.clone()))?;
        let backup_dbs = backup_result.unwrap();
        assert!(backup_dbs.contains(&"hotelrule_prod".to_string()));
        assert!(backup_dbs.contains(&"analytics_db".to_string()));
        assert_eq!(backup_dbs.len(), 2);

        // Test restore parsing (should extract full mapping)
        let restore_result = parse_database_list_for_restore(&Some(config_json))?;
        let restore_mapping = restore_result.unwrap();
        assert_eq!(restore_mapping.get("hotelrule_prod"), Some(&"hotelrule_prod_dev".to_string()));
        assert_eq!(restore_mapping.get("analytics_db"), Some(&"analytics_staging".to_string()));
        assert_eq!(restore_mapping.len(), 2);

        // Test backward compatibility with array format
        let array_config = json!(["hotelrule_prod", "analytics_db"]);
        
        let backup_array = parse_database_list_for_backup_sync(&Some(array_config.clone()))?;
        let backup_array_dbs = backup_array.unwrap();
        assert_eq!(backup_array_dbs, vec!["hotelrule_prod", "analytics_db"]);

        let restore_array = parse_database_list_for_restore(&Some(array_config))?;
        let restore_array_mapping = restore_array.unwrap();
        assert_eq!(restore_array_mapping.get("hotelrule_prod"), Some(&"hotelrule_prod".to_string()));
        assert_eq!(restore_array_mapping.get("analytics_db"), Some(&"analytics_db".to_string()));

        Ok(())
    }
}