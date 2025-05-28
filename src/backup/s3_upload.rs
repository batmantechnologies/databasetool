// databasetool/src/backup/s3_upload.rs
use anyhow::{Context, Result};
use aws_sdk_s3 as s3;
use s3::primitives::ByteStream;
use s3::config::Region;
use std::path::Path;
// Removed: use tokio::fs::File;
use crate::config::SpacesConfig;

/// Uploads a file to an S3-compatible object storage service (like DigitalOcean Spaces).
pub async fn upload_file_to_s3(
    spaces_config: &SpacesConfig,
    file_path: &Path,
    s3_key: &str,
) -> Result<()> {
    println!(
        "Attempting to upload {} to S3 bucket {} with key {}",
        file_path.display(),
        spaces_config.bucket_name,
        s3_key
    );

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

    let body = ByteStream::from_path(file_path)
        .await
        .with_context(|| format!("Failed to create ByteStream from file: {}", file_path.display()))?;

    client
        .put_object()
        .bucket(&spaces_config.bucket_name)
        .key(s3_key)
        .body(body)
        .send()
        .await
        .with_context(|| {
            format!(
                "Failed to upload file {} to S3 bucket {} with key {}",
                file_path.display(),
                spaces_config.bucket_name,
                s3_key
            )
        })?;

    println!(
        "✅ Successfully uploaded {} to S3 bucket {} with key {}",
        file_path.display(),
        spaces_config.bucket_name,
        s3_key
    );
    Ok(())
}

// Basic check for S3 credentials and connectivity (optional, can be expanded)
#[allow(dead_code)]
pub async fn check_s3_connection(spaces_config: &SpacesConfig) -> Result<()> {
    println!("Checking S3 connection to endpoint: {}", spaces_config.endpoint_url);
    let sdk_config = aws_config::defaults(s3::config::BehaviorVersion::latest())
        .endpoint_url(&spaces_config.endpoint_url)
        .region(Region::new(spaces_config.region.clone()))
        .credentials_provider(s3::config::Credentials::new(
            &spaces_config.access_key_id,
            &spaces_config.secret_access_key,
            None, None, "Static",
        ))
        .load()
        .await;

    let client = s3::Client::new(&sdk_config);

    // Attempt to list buckets as a simple connection check
    // This requires `s3:ListBuckets` permission, which might not always be granted.
    // A more robust check might be to try a HEAD request on the target bucket.
    match client.list_buckets().send().await {
        Ok(_) => {
            println!("✓ S3 connection successful (ListBuckets).");
            // Further check: HEAD request on the specific bucket to ensure it exists and is accessible.
            match client.head_bucket().bucket(&spaces_config.bucket_name).send().await {
                Ok(_) => println!("✓ Target bucket {} is accessible.", spaces_config.bucket_name),
                Err(e) => {
                    eprintln!("⚠️ Could not verify target bucket {} with HEAD request: {}. Please ensure it exists and you have permissions.", spaces_config.bucket_name, e);
                    // Depending on strictness, you might not want to bail here,
                    // as PutObject might still work if the bucket exists but HeadBucket is denied.
                    // For now, we'll just warn.
                }
            }
        }
        Err(e) => {
            // ListBuckets failed, but this might be due to permissions.
            // Try a HEAD request on the bucket as an alternative check.
            eprintln!("⚠️ S3 ListBuckets failed (this might be due to restricted permissions): {}. Trying HeadBucket as an alternative check...", e);
            match client.head_bucket().bucket(&spaces_config.bucket_name).send().await {
                Ok(_) => {
                     println!("✓ S3 connection successful (HeadBucket on target bucket {}).", spaces_config.bucket_name);
                }
                Err(head_err) => {
                    return Err(anyhow::anyhow!(
                        "S3 connection failed. Could not list buckets or access target bucket \'{}\' with HEAD request. Endpoint: {}, Error: {}",
                        spaces_config.bucket_name, spaces_config.endpoint_url, head_err
                    ).context(e)); // Chain the original ListBuckets error as context
                }
            }
        }
    }
    Ok(())
}