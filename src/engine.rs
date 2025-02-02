use crate::cli;
use crate::database::schema::{ObjectInformation, TableInformation};
use crate::database::TABLE_SCHEMA_ROOT_PAGE_NUMBER;
use crate::database::{self, schema};
use crate::sql::{self, sql_query};
use anyhow;

pub fn process_command(command: cli::Command) -> anyhow::Result<()> {
    match command {
        cli::Command::DatabaseInformation { filename } => database_information(filename),
        cli::Command::ListTables { filename } => list_tables(filename),
        cli::Command::Query { filename, query } => process_query(filename, query),
    }
}

/// Prints out general database information by reading the database header.
fn database_information(filename: String) -> anyhow::Result<()> {
    let database = database::Database::init_from_file(&filename)?;
    // println!("database page size: {}", database.header.page_size);
    println!("database header: {:?}", database.header);
    Ok(())
}

/// Lists all schema objects in the database.
/// This is done by reading the schema table and parsing its (potentially) multiple pages.
fn list_schema_objects(filename: String) -> anyhow::Result<Vec<ObjectInformation>> {
    let mut database = database::Database::init_from_file(&filename)?;
    let schema_rows = database
        .traverse_btree_table(
            TABLE_SCHEMA_ROOT_PAGE_NUMBER,
            true,
            &schema::SCHEMA_TABLE_INFORMATION,
        )
        .expect("Failed to parse schema table");

    schema_rows.iter().map(|row| ObjectInformation::from(row))
}

/// Lists all tables in the database.
fn list_tables(filename: String) -> anyhow::Result<Vec<TableInformation>> {
    let schema_objects = list_schema_objects(filename)?;
    schema_objects
        .iter()
        .map(|o| TableInformation::try_from(o))
        .collect()
}

// TODO:
fn process_query(filename: String, query: String) -> anyhow::Result<()> {
    let statement = sql_query::select_statement(&query)?;
    let database = database::Database::init_from_file(&filename)?;
    match statement.from_target {
        sql::Targetable::TableOrView(name) => {
            // check that targetable exists in database
        }
        sql::Targetable::Other(_) => {
            anyhow::bail!("Unsupported target");
        }
    }
    // we need to parse the table info first and pass that into validate_statement instead of
    // passing the entire database object
    validate_statement(&statement, &database)?;

    todo!()
}

fn validate_statement(
    statement: &sql::Statement,
    database: &database::Database,
) -> anyhow::Result<bool> {
    match statement {
        sql::Statement::SelectStatement(select) => validate_select_statement(select, database),
        sql::Statement::CreateTableStatement(create) => validate_create_statement(create, database),
    }
    Ok(true)
}

// TODO: finish this
fn validate_select_statement(
    statement: &sql::SelectStatement,
    database: &database::Database,
) -> anyhow::Result<bool> {
    if statement.selectables.is_empty() {
        anyhow::bail!("No columns selected");
    } else {
        statement.selectables.iter().for_each(|s| match s {
            sql::Selectable::Column(name) => {
                todo!()
            }
            sql::Selectable::CountStar => {}
        });
    }
    if statement.from_target.is_empty() {
        anyhow::bail!("No table selected");
    }
    Ok(true)
}

// TODO: finish this
fn validate_create_statement(
    statement: &sql::CreateTableStatement,
    database: &database::Database,
) -> anyhow::Result<bool> {
    if statement.table_name.is_empty() {
        anyhow::bail!("No table name provided");
    }
    if statement.columns.is_empty() {
        anyhow::bail!("No columns provided");
    }
    Ok(true)
}
