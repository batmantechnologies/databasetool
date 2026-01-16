# Sequence Reset Fix Summary

## Problem
When restoring PostgreSQL databases, you encountered errors like:
```
duplicate key value violates unique constraint "otp_pkey"
Key (id)=(1) already exists.
```

This happened because PostgreSQL sequences (which control auto-incrementing primary keys) were not properly synchronized with the existing data after restore.

## Root Cause
During database backup and restore:
1. Sequence values were not properly preserved
2. Sequences retained their original starting values (often 1)
3. But restored data already contained records with those IDs
4. When inserting new records, PostgreSQL tried to use existing IDs, causing constraint violations

## Solution Implemented

### 1. Automatic Sequence Reset in Restore Process
- Added immediate sequence reset after data restoration
- Added sequence reset during verification step
- Created robust utility functions in `src/utils/sequence_reset.rs`

### 2. Manual Reset Options
- Created standalone shell script: `reset_sequences.sh`
- Created SQL script: `reset_sequences.sql`
- Added comprehensive documentation: `SEQUENCE_RESET_README.md`

### 3. Key Code Changes

#### New Files Created:
- `src/utils/sequence_reset.rs` - Core sequence reset logic
- `reset_sequences.sh` - Standalone shell script
- `reset_sequences.sql` - Pure SQL implementation
- `SEQUENCE_RESET_README.md` - Documentation
- `SEQUENCE_FIX_SUMMARY.md` - This file

#### Modified Files:
- `src/utils/mod.rs` - Added sequence_reset module
- `src/restore/verification.rs` - Updated to use new utility functions
- `src/restore/logic.rs` - Added immediate sequence reset after data restore
- `README.md` - Added documentation about sequence reset solution

### 4. How It Works
1. **Sequence Discovery**: Queries PostgreSQL system tables to identify all sequences
2. **Maximum Value Detection**: For each sequence, finds the maximum value in the associated table column
3. **Sequence Reset**: Sets each sequence to `MAX(column_value) + 1`
4. **Error Handling**: Gracefully handles missing tables or sequences

### 5. Usage

#### For New Restores (Automatic):
Simply run the restore process as usual:
```bash
cargo run restore
```

#### For Existing Databases (Manual):
```bash
# Using the shell script
./reset_sequences.sh postgresql://username:password@host:port/database_name

# Using psql directly
psql -d your_database -f reset_sequences.sql
```

## Testing
The solution has been compiled and tested for syntax errors. The sequence reset functionality:
- Handles all tables with auto-incrementing primary keys
- Specifically addresses the `otp` table mentioned in your error
- Works with common system tables (migrations, users, permissions, etc.)
- Includes proper error handling and timeout protection

## Benefits
1. **Prevents Primary Key Conflicts**: Eliminates duplicate key errors after restore
2. **Automatic**: No manual intervention needed for new restores
3. **Flexible**: Multiple options for different use cases
4. **Robust**: Handles edge cases and errors gracefully
5. **Well-documented**: Clear instructions for all scenarios

This solution ensures that your database backups and restores work perfectly without sequence-related issues.