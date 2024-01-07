use anyhow::{anyhow, Result};

pub mod dbinfo;
pub mod query;
pub mod tables;

pub enum Command {
    DBInfo,
    Tables,
    Query { table: String },
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            ".dbinfo" => Ok(Command::DBInfo),
            ".tables" => Ok(Command::Tables),
            c => {
                let command_parts: Vec<&str> = c.split(" ").collect();
                if command_parts[0] == "SELECT" {
                    let table_name = command_parts.last().unwrap();
                    return Ok(Command::Query {
                        table: table_name.to_string(),
                    });
                } else {
                    return Err(anyhow!("Unrecognized command {}", c));
                }
            }
        }
    }
}
