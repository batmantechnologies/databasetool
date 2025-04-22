use sqlx::postgres::PgRow;
use std::fs;
use std::io::Write;
use std::env;
use std::path::PathBuf;
use sqlx::postgres::PgPoolOptions;

pub async fn check_db_connection(db_url: &str) -> bool {
    match PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
    {
        Ok(_) => {
            println!("✅ Successfully connected to {}", db_url);
            true
        },
        Err(e) => {
            eprintln!("❌ Failed to connect to {}: {}", db_url, e);
            false
        }
    }
}

// async fn dump_database_schema_and_data(
//     pool: &sqlx::Pool<sqlx::Postgres>,
//     file: &mut fs::File,
// ) -> Result<(), sqlx::Error> {
//     // Helper function to write schema to the file
//     fn write_definitions<T>(
//         definitions: Vec<T>,
//         definition_name: &str,
//         file: &mut fs::File,
//     ) -> Result<(), std::io::Error>
//     where
//         T: sqlx::Row,
//     {
//         for row in definitions {
//             if let Some(def) = row.get::<Option<String>, _>(definition_name) {
//                 file.write_all(def.as_bytes())?;
//                 file.write_all(b";\n")?;
//             }
//         }
//         Ok(())
//     }

//     // Fetch schema definitions (functions, views, triggers, etc.)
//     let function_defs = sqlx::query("SELECT pg_get_functiondef(oid) as def FROM pg_proc")
//         .fetch_all(pool)
//         .await?;
//     write_definitions(function_defs, "def", file)?;

//     let view_defs = sqlx::query("SELECT pg_get_viewdef(oid) as def FROM pg_class WHERE relkind = 'v'")
//         .fetch_all(pool)
//         .await?;
//     write_definitions(view_defs, "def", file)?;

//     let trigger_defs = sqlx::query("SELECT pg_get_triggerdef(oid) as def FROM pg_trigger")
//         .fetch_all(pool)
//         .await?;
//     write_definitions(trigger_defs, "def", file)?;

//     let constraint_defs = sqlx::query("SELECT pg_get_constraintdef(oid) as def FROM pg_constraint")
//         .fetch_all(pool)
//         .await?;
//     write_definitions(constraint_defs, "def", file)?;

//     let index_defs = sqlx::query("SELECT pg_get_indexdef(indexrelid) as def FROM pg_index")
//         .fetch_all(pool)
//         .await?;
//     write_definitions(index_defs, "def", file)?;

//     Ok(())
// }

// async fn dump_table_data(
//     pool: &sqlx::Pool<sqlx::Postgres>,
//     file: &mut fs::File,
// ) -> Result<(), sqlx::Error> {
//     let tables = sqlx::query(
//         "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
//     )
//     .fetch_all(pool)
//     .await?;

//     for table in tables {
//         let table_name = table.get::<String, _>("table_name");

//         // Fetch table data
//         let data = sqlx::query(&format!("SELECT * FROM {}", table_name))
//             .fetch_all(pool)
//             .await?;

//         // Write data to the file
//         file.write_all(format!("\n-- Data for table {}\n", table_name).as_bytes())?;

//         for row in data {
//             let mut values = Vec::new();
//             for i in 0..row.len() {
//                 // Try to get each value as a string
//                 let value: Result<String, _> = row.try_get(i);
//                 match value {
//                     Ok(val) => values.push(val),
//                     Err(_) => values.push("NULL".to_string()),
//                 }
//             }
//             let line = values.join(", ") + "\n";
//             file.write_all(line.as_bytes())?;
//         }
//     }
//     Ok(())
// }
