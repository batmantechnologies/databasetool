# PostgreSQL Sequence Reset Solution

## Problem Description

When restoring PostgreSQL databases, you may encounter errors like:

```
duplicate key value violates unique constraint "otp_pkey"
Key (id)=(1) already exists.
```

This occurs because PostgreSQL sequences (which control auto-incrementing primary keys) are not properly synchronized with the existing data after a restore operation.

## Root Cause

During database backup and restore:
1. The sequence values are not properly preserved
2. Sequences retain their original starting values (often 1)
3. But the restored data may already contain records with those IDs
4. When inserting new records, PostgreSQL tries to use existing IDs, causing constraint violations

## Solution Overview

This repository includes multiple approaches to fix sequence synchronization issues:

### 1. Automatic Sequence Reset (Built into Restore Process)

The restore process now automatically resets all sequences after data restoration:

- **During Restore**: Sequences are reset immediately after data is restored
- **During Verification**: Additional sequence reset is performed as part of verification

### 2. Manual Sequence Reset Scripts

#### Standalone Shell Script
```bash
./reset_sequences.sh postgresql://username:password@host:port/database_name
```

#### SQL Script
Execute `reset_sequences.sql` directly in your database:
```bash
psql -d your_database -f reset_sequences.sql
```

## How It Works

### Technical Implementation

1. **Sequence Discovery**: Queries `pg_class`, `pg_depend`, and related system tables to identify all sequences
2. **Maximum Value Detection**: For each sequence, finds the maximum value in the associated table column
3. **Sequence Reset**: Sets each sequence to `MAX(column_value) + 1`
4. **Error Handling**: Gracefully handles missing tables or sequences

### Code Structure

- `src/utils/sequence_reset.rs`: Core sequence reset logic
- `src/restore/verification.rs`: Integration with restore verification
- `reset_sequences.sql`: Pure SQL implementation
- `reset_sequences.sh`: Standalone shell script

## Usage Instructions

### For New Restores

Simply run the restore process as usual. Sequence reset is now automatic:

```bash
cargo run restore
```

### For Existing Databases with Sequence Issues

1. **Using the shell script**:
   ```bash
   ./reset_sequences.sh postgresql://username:password@host:port/database_name
   ```

2. **Using psql directly**:
   ```bash
   psql -d your_database -f reset_sequences.sql
   ```

3. **Programmatically**:
   Call `crate::utils::sequence_reset::reset_sequences_with_timeout()` in your Rust code.

## Common Tables Handled

The solution specifically handles common system tables:
- migrations
- schema_migrations
- users
- permissions
- groups
- otp (mentioned in your error)

## Troubleshooting

### If You Still See Sequence Errors

1. Check that the database user has sufficient privileges:
   ```sql
   GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_user;
   ```

2. Manually reset a specific sequence:
   ```sql
   SELECT setval('table_name_id_seq', (SELECT MAX(id) FROM table_name) + 1);
   ```

### Performance Considerations

For very large databases, sequence reset might take time. The process includes:
- 5-minute timeout protection
- Progress notifications
- Individual sequence error handling

## Prevention for Future Backups

To prevent this issue in future backups:
1. Ensure your backup process preserves sequence information
2. Consider using `pg_dump` with custom format (`--format=custom`)
3. Always test restores in a staging environment

## Support

If you continue to experience issues:
1. Check the logs for specific error messages
2. Verify database connectivity and permissions
3. Ensure PostgreSQL client tools are installed (`psql`, `pg_dump`)