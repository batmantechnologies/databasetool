-- Script to reset all PostgreSQL sequences after database restore
-- This should be run after restoring a database to ensure sequences are properly synchronized

DO $$
DECLARE
    rec RECORD;
    max_id BIGINT;
    seq_name TEXT;
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
    
    -- Handle common system tables that might not be caught by the above query
    -- These are tables that often have sequence issues
    FOR rec IN
        SELECT 'migrations' as table_name, 'id' as column_name
        UNION ALL
        SELECT 'schema_migrations', 'id'
        UNION ALL
        SELECT 'users', 'id'
        UNION ALL
        SELECT 'permissions', 'id'
        UNION ALL
        SELECT 'groups', 'id'
    LOOP
        seq_name := rec.table_name || '_' || rec.column_name || '_seq';
        
        BEGIN
            -- Get the maximum value from the corresponding column
            EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %I', rec.column_name, rec.table_name) INTO max_id;
            
            -- Set the sequence to max_id + 1
            EXECUTE format('SELECT setval(%L, %L, false)', seq_name, max_id + 1);
            
            RAISE NOTICE 'Reset common sequence % to % (table: %, column: %)', seq_name, max_id + 1, rec.table_name, rec.column_name;
        EXCEPTION
            WHEN undefined_table THEN
                -- Table doesn't exist, which is fine
                RAISE NOTICE 'Table % does not exist, skipping sequence reset', rec.table_name;
            WHEN undefined_column THEN
                -- Column doesn't exist, which is fine
                RAISE NOTICE 'Column % does not exist in table %, skipping sequence reset', rec.column_name, rec.table_name;
            WHEN others THEN
                -- Other error, log it but continue
                RAISE NOTICE 'Error resetting sequence for table %: %', rec.table_name, SQLERRM;
        END;
    END LOOP;
    
    RAISE NOTICE 'All sequences have been reset successfully.';
END $$;