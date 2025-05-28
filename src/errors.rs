use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Environment variable error: {0}")]
    EnvVar(#[from] std::env::VarError),

    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("HTTP request error: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("AWS SDK S3 error: {0}")]
    S3Sdk(String), // General S3 SDK errors

    #[error("Backup operation failed: {0}")]
    Backup(String),

    #[error("Restore operation failed: {0}")]
    Restore(String),

    #[error("Storage operation failed: {0}")]
    Storage(String),

    #[error("Command execution failed: {stderr}")]
    Command { stdout: String, stderr: String },

    #[error("Serde JSON error: {0}")]
    SerdeJson(#[from] serde_json::Error),

    #[error("UTF-8 conversion error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Operation cancelled: {0}")]
    Cancelled(String),

    #[error("Verification failed: {0}")]
    Verification(String),

    #[error("Generic error: {0}")]
    Generic(String),

    #[error(transparent)]
    Anyhow(#[from] anyhow::Error), // To ease transition from existing code
}

// Specific S3 error conversion if using aws-sdk-s3 directly
// This is a placeholder, you'd implement `From` for specific S3 error types
// For example:
// impl<E> From<aws_sdk_s3::error::SdkError<E>> for AppError
// where
//     E: std::error::Error + Send + Sync + 'static,
// {
//     fn from(err: aws_sdk_s3::error::SdkError<E>) -> Self {
//         AppError::S3Sdk(format!("S3 SDK error: {}", err))
//     }
// }

pub type Result<T> = std::result::Result<T, AppError>;