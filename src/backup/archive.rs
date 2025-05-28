// databasetool/src/backup/archive.rs
use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::path::{Path, PathBuf};
use tar::Builder;
use walkdir::WalkDir;

/// Creates a GZipped TAR archive from a source directory.
///
/// The archive will contain all files and directories within `source_dir`.
/// The paths inside the archive will be relative to `source_dir`.
///
/// # Arguments
/// * `source_dir` - The directory whose contents will be archived.
/// * `archive_dest_path` - The full path where the `.tar.gz` archive will be created.
///
/// # Returns
/// Path to the created archive file.
pub fn create_tar_gz_archive(
    source_dir: &Path,
    archive_dest_path: &Path,
) -> Result<PathBuf> {
    if !source_dir.is_dir() {
        return Err(anyhow::anyhow!(
            "Source for archival is not a directory: {}",
            source_dir.display()
        ));
    }
    if let Some(parent) = archive_dest_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!(
                    "Failed to create parent directory for archive: {}",
                    parent.display()
                )
            })?;
        }
    }


    println!(
        "Creating tar.gz archive from {} to {}",
        source_dir.display(),
        archive_dest_path.display()
    );

    let archive_file = File::create(archive_dest_path).with_context(|| {
        format!(
            "Failed to create archive file: {}",
            archive_dest_path.display()
        )
    })?;
    let enc = GzEncoder::new(archive_file, Compression::default());
    let mut tar_builder = Builder::new(enc);

    // Add files from the source directory recursively.
    // The paths in the archive will be relative to source_dir.
    for entry in WalkDir::new(source_dir) {
        let entry = entry.with_context(|| format!("Failed to walk directory: {}", source_dir.display()))?;
        let path = entry.path();
        let name = path
            .strip_prefix(source_dir)
            .with_context(|| {
                format!(
                    "Failed to strip prefix {} from {}",
                    source_dir.display(),
                    path.display()
                )
            })?;

        if name.as_os_str().is_empty() { // Skip the root directory itself
            continue;
        }

        if path.is_dir() {
            tar_builder.append_dir_all(name, path).with_context(|| {
                format!("Failed to append directory {} to archive", path.display())
            })?;
        } else if path.is_file() {
            tar_builder.append_path_with_name(path, name).with_context(|| {
                 format!("Failed to append file {} as {} to archive", path.display(), name.display())
            })?;
        }
    }

    let encoder = tar_builder.into_inner().with_context(|| {
        format!(
            "Failed to get inner encoder from tar builder for archive: {}",
            archive_dest_path.display()
        )
    })?;
    
    encoder.finish().with_context(|| {
        format!(
            "Failed to finish Gzip encoding for archive: {}",
            archive_dest_path.display()
        )
    })?;

    println!(
        "✓ Tar.gz archive created successfully at {}",
        archive_dest_path.display()
    );
    Ok(archive_dest_path.to_path_buf())
}

/// Extracts a GZipped TAR archive to a destination directory.
///
/// # Arguments
/// * `archive_path` - Path to the `.tar.gz` archive file.
/// * `extract_to_dir` - The directory where the contents will be extracted.
///
/// # Returns
/// Path to the directory where files were extracted.
pub fn extract_tar_gz_archive(
    archive_path: &Path,
    extract_to_dir: &Path,
) -> Result<PathBuf> {
    if !archive_path.is_file() {
        return Err(anyhow::anyhow!(
            "Archive for extraction is not a file: {}",
            archive_path.display()
        ));
    }

    if !extract_to_dir.exists() {
        std::fs::create_dir_all(extract_to_dir).with_context(|| {
            format!(
                "Failed to create extraction directory: {}",
                extract_to_dir.display()
            )
        })?;
    } else if !extract_to_dir.is_dir() {
         return Err(anyhow::anyhow!(
            "Extraction path exists but is not a directory: {}",
            extract_to_dir.display()
        ));
    }

    println!(
        "Extracting tar.gz archive from {} to {}",
        archive_path.display(),
        extract_to_dir.display()
    );

    let archive_file = File::open(archive_path).with_context(|| {
        format!("Failed to open archive file: {}", archive_path.display())
    })?;
    let gz_decoder = flate2::read::GzDecoder::new(archive_file);
    let mut archive = tar::Archive::new(gz_decoder);

    archive.unpack(extract_to_dir).with_context(|| {
        format!(
            "Failed to unpack archive {} to {}",
            archive_path.display(),
            extract_to_dir.display()
        )
    })?;

    println!(
        "✓ Tar.gz archive extracted successfully to {}",
        extract_to_dir.display()
    );
    Ok(extract_to_dir.to_path_buf())
}