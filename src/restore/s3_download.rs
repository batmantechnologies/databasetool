// databasetool/src/restore/s3_download.rs
use anyhow::{Context, Result};
use aws_sdk_s3 as s3;
use s3::config::Region;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::AsyncWriteExt; // For write_all

use crate::config::SpacesConfig;

/// Parses an S3 URI (s3://bucket/key) into bucket and key.
pub fn parse_s3_uri(s3_uri: &str) -> Result<(String, String)> {
    let uri = url::Url::parse(s3_uri)
        .with_context(|| format!("Invalid S3 URI format: {}", s3_uri))?;
    if uri.scheme() != "s3" {
        return Err(anyhow::anyhow!("S3 URI must start with s3://"));
    }
    let bucket = uri.host_str().context("S3 URI missing bucket name")?.to_string();
    let key = uri.path().trim_start_matches('/').to_string();
    if key.is_empty() {
        return Err(anyhow::anyhow!("S3 URI missing key (object path)"));
    }
    Ok((bucket, key))
}

/// Downloads a file from an S3-compatible object storage service.
///
/// # Arguments
/// * `spaces_config` - Configuration for the S3-compatible service.
/// * `s3_bucket` - The name of the S3 bucket.
/// * `s3_key` - The key (path) of the object in the S3 bucket.
/// * `destination_path` - The local path where the downloaded file will be saved.
///
/// # Returns
/// Path to the downloaded file.
pub async fn download_file_from_s3(
    spaces_config: &SpacesConfig,
    s3_bucket: &str, 
    s3_key: &str,
    destination_path: &Path,
) -> Result<PathBuf> {
    println!(
        "Attempting to download s3://{}/{} to {}",
        s3_bucket,
        s3_key,
        destination_path.display()
    );

    if let Some(parent_dir) = destination_path.parent() {
        if !parent_dir.exists() {
            tokio::fs::create_dir_all(parent_dir)
                .await
                .with_context(|| format!("Failed to create directory for download: {}", parent_dir.display()))?;
        }
    }

    let sdk_config = aws_config::defaults(s3::config::BehaviorVersion::latest())
        .endpoint_url(&spaces_config.endpoint_url)
        .region(Region::new(spaces_config.region.clone()))
        .credentials_provider(s3::config::Credentials::new(
            &spaces_config.access_key_id,
            &spaces_config.secret_access_key,
            None, // session_token
            None, // expiry
            "Static", // provider_name
        ))
        .load()
        .await;

    let client = s3::Client::new(&sdk_config);

    let mut output_file = File::create(destination_path)
        .await
        .with_context(|| format!("Failed to create destination file: {}", destination_path.display()))?;

    let mut object = client
        .get_object()
        .bucket(s3_bucket)
        .key(s3_key)
        .send()
        .await
        .with_context(|| format!("Failed to get object s3://{}/{}", s3_bucket, s3_key))?;

    let mut total_bytes_downloaded = 0;
    // Corrected loop pattern here:
    while let Ok(Some(bytes_chunk)) = object.body.try_next().await {
        output_file.write_all(&bytes_chunk).await // Use write_all, which takes &[u8]
            .with_context(|| format!("Failed to write to destination file: {}", destination_path.display()))?;
        total_bytes_downloaded += bytes_chunk.len();
    }
    
    println!(
        "✅ Successfully downloaded {} bytes from s3://{}/{} to {}",
        total_bytes_downloaded,
        s3_bucket,
        s3_key,
        destination_path.display()
    );
    Ok(destination_path.to_path_buf())
}