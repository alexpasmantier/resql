use anyhow::{bail, Result};

pub enum Command {
    DatabaseInformation { filename: String },
    ListTables { filename: String },
    Query { filename: String, query: String },
}

const DBINFO_COMMAND: &str = ".dbinfo";
const TABLES_COMMAND: &str = ".tables";

pub fn parse_command() -> Result<Command> {
    let args: Vec<String> = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database file path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {
            let filename = args[1];
            let command: &str = &args[2];
            match command {
                DBINFO_COMMAND => Ok(Command::DatabaseInformation { filename }),
                TABLES_COMMAND => Ok(Command::ListTables { filename }),
                query => Ok(Command::Query {
                    filename,
                    query: query.to_string(),
                }),
            }
        }
    }
}
