pub mod setting;
pub mod sequence_reset;

use anyhow::{Context, Result};
use std::path::PathBuf;
use which::which;

/// Finds the psql executable in the system PATH.
pub fn find_psql_executable() -> Result<PathBuf> {
    which("psql").context("psql executable not found in PATH. Please ensure PostgreSQL client tools are installed and in your PATH.")
}
