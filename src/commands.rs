use anyhow::{anyhow, Result};
use regex::Regex;

pub mod dbinfo;
pub mod query;
pub mod tables;

pub enum Command {
    DBInfo,
    Tables,
    Query {
        expression: String,
        relation: String,
    },
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            ".dbinfo" => Ok(Command::DBInfo),
            ".tables" => Ok(Command::Tables),
            c => {
                if let Ok(command) = parse_query(c) {
                    return Ok(command);
                } else {
                    return Err(anyhow!("Unrecognized command {}", c));
                }
            }
        }
    }
}

fn parse_query(str_query: &str) -> Result<Command> {
    let re = Regex::new(r"(?i)select (?P<expression>[\w\(\)\*]+) from (?P<relation>\w+)").unwrap();
    if let Some(captures) = re.captures(str_query) {
        return Ok(Command::Query {
            expression: captures["expression"].to_string(),
            relation: captures["relation"].to_string(),
        });
    }
    return Err(anyhow!("Unable to parse input query"));
}
