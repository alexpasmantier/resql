use crate::cli;
use crate::database;
use anyhow;

pub fn process_command(command: cli::Command) -> anyhow::Result<()> {
    match command {
        cli::Command::DatabaseInformation { filename } => database_information(filename),
        cli::Command::ListTables { filename } => list_tables(filename),
        cli::Command::Query { filename, query } => process_query(filename, query),
    }
}

fn database_information(filename: String) -> anyhow::Result<()> {
    let database = database::Database::init_from_file(&filename)?;
    // println!("database page size: {}", database.header.page_size);
    println!("database header: {:?}", database.header);
    Ok(())
}

// TODO:
fn list_tables(filename: String) -> anyhow::Result<()> {
    let database = database::Database::init_from_file(&filename)?;

    Ok(())
}

// TODO:
fn process_query(filename: String, query: String) -> anyhow::Result<()> {
    let database = database::Database::init_from_file(&filename)?;
    todo!()
}
