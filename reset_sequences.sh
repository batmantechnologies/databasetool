#!/bin/bash

# Script to reset PostgreSQL sequences after database restore
# Usage: ./reset_sequences.sh <database_url>

set -e

# Check if database URL is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <database_url>"
    echo "Example: $0 postgresql://username:password@localhost:5432/database_name"
    exit 1
fi

DATABASE_URL=$1

echo "Resetting sequences for database: $DATABASE_URL"

# SQL command to reset all sequences
RESET_SEQUENCES_SQL="
DO \$\$
DECLARE
    rec RECORD;
    max_id BIGINT;
BEGIN
    -- Reset all sequences in the public schema
    FOR rec IN 
        SELECT 
            seq.relname as sequence_name,
            tab.relname as table_name,
            attr.attname as column_name
        FROM 
            pg_class seq
        JOIN 
            pg_depend dep ON dep.objid = seq.oid AND dep.deptype = 'a'
        JOIN 
            pg_class tab ON dep.refobjid = tab.oid
        JOIN 
            pg_attribute attr ON dep.refobjid = attr.attrelid AND dep.refobjsubid = attr.attnum
        JOIN
            pg_namespace nsp ON seq.relnamespace = nsp.oid
        WHERE 
            seq.relkind = 'S'
            AND tab.relkind = 'r'
            AND nsp.nspname = 'public'
        ORDER BY 
            tab.relname, attr.attname
    LOOP
        -- Get the maximum value from the corresponding column
        EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %I', rec.column_name, rec.table_name) INTO max_id;
        
        -- Set the sequence to max_id + 1
        EXECUTE format('SELECT setval(%L, %L, false)', rec.sequence_name, max_id + 1);
        
        RAISE NOTICE 'Reset sequence % to % (table: %, column: %)', rec.sequence_name, max_id + 1, rec.table_name, rec.column_name;
    END LOOP;
    
    RAISE NOTICE 'All sequences have been reset successfully.';
END \$\$;
"

# Execute the SQL command
echo "Executing sequence reset..."
psql "$DATABASE_URL" -c "$RESET_SEQUENCES_SQL"

if [ $? -eq 0 ]; then
    echo "✅ Sequences reset successfully!"
else
    echo "❌ Failed to reset sequences"
    exit 1
fi