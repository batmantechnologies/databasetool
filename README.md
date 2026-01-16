# DatabaseTool: Supercharge Your PostgreSQL Workflow 🚀

[![Rust](https://img.shields.io/badge/Made%20with-Rust-orange?style=for-the-badge&logo=rust)](https://www.rust-lang.org/)

**Unlock seamless PostgreSQL management with `DatabaseTool` – your ultimate command-line companion for effortless backups, high-fidelity restores, and intelligent database synchronization. Built with the power and safety of Rust, `DatabaseTool` is designed for developers and DBAs who demand reliability and speed.**

## Table of Contents

- [Why DatabaseTool?](#why-databasetool-)
- [Core Features](#core-features-)
  - [🛡️ Robust Backups](#️-robust-backups)
  - [🔄 Flawless Restores](#-flawless-restores)
  - [⚙️ Intelligent Sync](#️-intelligent-sync)
- [🔧 Sequence Reset Solution](#-sequence-reset-solution)
- [Prerequisites](#prerequisites)
- [Quick Start: Installation](#quick-start-installation)
- [Configuration: Your Central Command](#configuration-your-central-command)
  - [The `config.json` File](#the-configjson-file)
  - [Environment Variables (Optional overrides)](#environment-variables-optional-overrides)
- [Usage: Take Control](#usage-take-control)
  - [Interactive Mode](#interactive-mode)
  - [Direct Commands](#direct-commands)
- [Built With Power](#built-with-power)
- [Development & Contribution](#development--contribution)
- [License](#license)

## Why DatabaseTool? ✨

Tired of juggling complex scripts and manual steps for your PostgreSQL databases? `DatabaseTool` is here to revolutionize your workflow:

*   **Simplicity at its Core:** Manage complex database operations with simple commands and a straightforward JSON configuration.
*   **Speed & Efficiency:** Leveraging Rust's performance and Tokio's asynchronous capabilities for swift operations.
*   **Reliability You Can Trust:** Built with Rust's emphasis on safety and correctness, ensuring your data is handled with care.
*   **Flexible Configuration:** Tailor every aspect of your operations through a central `config.json` file.
*   **Cloud-Ready:** Seamlessly integrate with S3-compatible object storage (like AWS S3, DigitalOcean Spaces) for remote backups and restores.
*   **The Power of Sync:** Go beyond backup/restore with intelligent database synchronization, perfect for staging, testing, or data migration.

## Core Features 🚀

### 🛡️ Robust Backups

Secure your valuable data with versatile and reliable backup capabilities.

*   **Comprehensive Dumps:** Creates full logical backups of your PostgreSQL databases.
*   **Compression:** Automatic GZip compression to save storage space.
*   **Archiving:** (Assumed, often `tar` under the hood with `pg_dump`) Neatly packages backup files.
*   **Cloud Upload:** Directly upload your backups to configured S3-compatible storage.
*   **Customizable:** Define output directories, filenames, and more via `config.json`.

### 🔄 Flawless Restores

Get your systems back online quickly or set up new environments with ease.

*   **From Local or Cloud:** Restore from local backup files or directly download from S3-compatible storage.
*   **Targeted Restoration:** Precisely restore data to your specified target database.
*   **Handles Complexity:** Manages the intricacies of the restore process, ensuring data integrity.

### ⚙️ Intelligent Sync

## 🔧 Sequence Reset Solution

When restoring PostgreSQL databases, you may encounter errors like:

```
duplicate key value violates unique constraint "otp_pkey"
Key (id)=(1) already exists.
```

This occurs because PostgreSQL sequences (which control auto-incrementing primary keys) are not properly synchronized with the existing data after a restore operation.

### Automatic Fix

DatabaseTool now automatically resets all sequences after data restoration to prevent these errors. This happens in two places:

1. **During Restore**: Sequences are reset immediately after data is restored
2. **During Verification**: Additional sequence reset is performed as part of verification

### Manual Solutions

If you need to manually reset sequences, DatabaseTool provides two options:

#### Standalone Shell Script
```bash
./reset_sequences.sh postgresql://username:password@host:port/database_name
```

#### SQL Script
Execute `reset_sequences.sql` directly in your database:
```bash
psql -d your_database -f reset_sequences.sql
```

For more details about the sequence reset solution, see [SEQUENCE_RESET_README.md](SEQUENCE_RESET_README.md).

### ⚙️ Intelligent Sync

**This is where `DatabaseTool` truly shines!** Synchronize data between two PostgreSQL databases effortlessly.

*   **Source to Target:** Define a source and a target database, and let `DatabaseTool` handle the synchronization.
*   **Use Cases:**
    *   Populating staging or development environments with production data (anonymize if needed!).
    *   Migrating data between database servers.
    *   Keeping analytical databases up-to-date.
*   **Configurable:** Fine-tune sync parameters within your `config.json`.

## Prerequisites

Before you unleash the power of `DatabaseTool`:

*   **Rust Toolchain:** Ensure you have Rust and Cargo installed. Visit [rustup.rs](https://rustup.rs/) for easy installation.
*   **PostgreSQL Client Tools:** `pg_dump`, `pg_restore`, and `psql` are often expected to be in your system's `PATH`, as many tools leverage them. (Verify if `DatabaseTool` uses them directly or implements natively via `sqlx`/`postgres` crates).
*   **OpenSSL:** Often required for PostgreSQL drivers.
    *   Debian/Ubuntu: `sudo apt-get install libssl-dev pkg-config`
    *   Fedora/RHEL: `sudo dnf install openssl-devel pkgconfig`
*   **(Optional) AWS CLI:** If interacting with AWS S3, having the AWS CLI configured can be helpful, though `DatabaseTool` should also support direct credential configuration.

## Quick Start: Installation

1.  **Clone the Repository (if not already done):**
    ```bash
    git clone <your-repository-url> # Replace with actual URL
    cd databasetool
    ```

2.  **Build for Release:**
    ```bash
    cargo build --release
    ```
    The optimized executable will be available at `target/release/databasetool`.

3.  **Install (Optional but Recommended):**
    Copy the executable to a directory in your system's `PATH`:
    ```bash
    sudo cp target/release/databasetool /usr/local/bin/databasetool
    ```

## Configuration: Your Central Command

`DatabaseTool` is driven by a `config.json` file located in the same directory as the executable (or project root when using `cargo run`).

### The `config.json` File

This file centralizes all your settings for database connections, backup, restore, and sync operations.

**Example `config.json` Structure:**

```json
{
  "app_name": "MyApplicationDBTool", // Optional: just an identifier
  "database_url": "postgresql://user:password@host:port/default_database", // General default, can be overridden per operation
  "spaces_config": { // For S3-compatible object storage (e.g., AWS S3, DigitalOcean Spaces)
    "access_key_id": "YOUR_ACCESS_KEY",
    "secret_access_key": "YOUR_SECRET_KEY",
    "region": "your-region-1",
    "bucket_name": "your-backup-bucket-name",
    "endpoint": "https://your-region-1.digitaloceanspaces.com", // Or AWS S3 endpoint
    "path_prefix": "database_backups/" // Optional prefix for all objects
  },
  "backup_options": {
    "source_db_url": "postgresql://user:password@host:port/source_prod_db", // Specific for backup
    "local_backup_directory": "./db_backups",
    "filename_template": "backup_{timestamp}_{dbname}.sql.gz",
    "upload_to_spaces": true, // Set to true to use 'spaces_config'
    "pg_dump_options": ["--schema-only"] // Example: array of additional pg_dump flags
  },
  "restore_options": {
    "target_db_url": "postgresql://user:password@host:port/restore_target_db",
    "archive_source_type": "local", // "local" or "spaces"
    "local_archive_path": "./db_backups/latest.sql.gz", // If type is "local"
    "spaces_archive_key": "database_backups/backup_YYYYMMDD_HHMMSS_dbname.sql.gz", // If type is "spaces"
    "pg_restore_options": ["--clean", "--if-exists"] // Example: array of additional pg_restore flags
  },
  "sync_options": {
    "source_db_url": "postgresql://user:password@host:port/source_sync_db",
    "target_db_url": "postgresql://user:password@host:port/target_sync_db",
    "sync_mode": "full_overwrite", // "full_overwrite", "append_only", "selective_tables" (hypothetical)
    "tables_to_sync": ["users", "products"], // If mode is "selective_tables"
    "pre_sync_scripts": ["./scripts/truncate_target.sql"], // SQL scripts to run before sync
    "post_sync_scripts": [] // SQL scripts to run after sync
  }
}
```
**Note:** The exact structure and available options within `backup_options`, `restore_options`, and `sync_options` will depend on the implementation in `src/config.rs` and the respective modules. Please refer to the source code or more detailed documentation for precise fields.

### Environment Variables (Optional overrides)

While `config.json` is primary, you might support overriding certain sensitive values (like passwords or API keys) via environment variables for enhanced security in CI/CD pipelines. (This needs to be explicitly implemented in `src/config.rs`).

Example environment variables (if implemented):
*   `DB_TOOL_SOURCE_DB_URL`
*   `DB_TOOL_TARGET_DB_URL`
*   `DB_TOOL_SPACES_ACCESS_KEY_ID`
*   `DB_TOOL_SPACES_SECRET_ACCESS_KEY`

## Database Renaming Feature 🔄

`DatabaseTool` now supports database renaming during restore operations. You can specify different target database names for each source database in your backup.

### Configuration Options

**Traditional Format (Backward Compatible):**
```json
"database_list": ["hotelrule_prod", "analytics_db"]
```

**New Mapping Format (For Renaming):**
```json
"database_list": {
  "hotelrule_prod": "hotelrule_prod_dev",
  "analytics_db": "analytics_staging", 
  "users_db": "users_test"
}
```

### How It Works

- **Backup Operations:** Uses only the source database names (keys from the mapping)
- **Restore Operations:** Restores each source database to its corresponding target database name
- **Sync Operations:** Uses only the source database names (keys from the mapping)

This feature is perfect for:
- Creating development/staging environments from production backups
- Testing database migrations with renamed databases
- Maintaining multiple environment copies with different naming conventions

## Usage: Take Control

Execute `DatabaseTool` from your terminal.

### Interactive Mode

Simply run the tool without arguments to get an interactive prompt:

```bash
databasetool
```

You'll be guided to choose an operation:
```
Select an operation:
1. Take Backup (or type 'backup')
2. Restore Backup (or type 'restore')
3. Sync Databases (Source to Target) (or type 'sync')
Enter your choice:
```

### Direct Commands

You can also specify the operation directly as a command-line argument:

*   **Perform a Backup:**
    ```bash
    databasetool backup
    ```
    This will use the `backup_options` from your `config.json`.

*   **Perform a Restore:**
    ```bash
    databasetool restore
    ```
    This will use the `restore_options` from your `config.json`.

*   **Perform a Database Sync:**
    ```bash
    databasetool sync
    ```
    This will use the `sync_options` from your `config.json`.

For detailed help on commands and their specific options (if any are added beyond the config file):
```bash
databasetool --help # Or specific subcommands if using a CLI parser like Clap
```

## Built With Power

`DatabaseTool` leverages a robust ecosystem of Rust crates:

*   **Tokio:** For lightning-fast asynchronous operations.
*   **SQLx / Postgres:** For native, type-safe PostgreSQL interaction.
*   **Serde:** For seamless JSON configuration parsing.
*   **Anyhow:** For flexible and user-friendly error handling.
*   **Chrono:** For date and time manipulations (e.g., timestamped backups).
*   **Flate2:** For GZip compression/decompression.
*   **Tar:** For archiving backup files.
*   **AWS SDK for S3 (`aws-sdk-s3`):** For powerful and reliable S3 integration.
*   **Url:** For parsing database connection strings.
*   **Dotenv:** For loading environment variables (if used for configuration).
*   And more! Check `Cargo.toml` for the full list.

## Development & Contribution

Interested in making `DatabaseTool` even better? Contributions are welcome!

1.  **Fork the repository.**
2.  **Create your feature branch:** `git checkout -b feature/AmazingNewFeature`
3.  **Implement your changes.**
    *   Ensure your code is well-formatted: `cargo fmt`
    *   Check for linter issues: `cargo clippy --all-targets --all-features`
    *   Write tests and ensure they pass: `cargo test --all-targets --all-features`
4.  **Commit your changes:** `git commit -m 'Add some AmazingNewFeature'`
5.  **Push to the branch:** `git push origin feature/AmazingNewFeature`
6.  **Open a Pull Request.**

## License

This project is licensed under the terms of the [LICENSE](LICENSE) file. (Please ensure you have a `LICENSE` file, e.g., MIT or Apache 2.0).

---

We hope `DatabaseTool` streamlines your PostgreSQL workflows and empowers you to manage your data with confidence and ease!