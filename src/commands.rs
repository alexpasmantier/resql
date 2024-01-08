use anyhow::{anyhow, Result};
use regex::Regex;

pub mod dbinfo;
pub mod query;
pub mod tables;

pub enum Command {
    DBInfo,
    Tables,
    Query {
        expressions: Vec<String>,
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
    let re =
        Regex::new(r"(?i)select\s+(?P<expressions>[\w,\s]+)\s+from\s+(?P<relation>\w+)").unwrap();
    let captures = re.captures(str_query).unwrap();

    let cols_re = Regex::new(r"(?i)([\w\(\*\)]+)").unwrap();
    let expressions: Vec<String> = cols_re
        .captures_iter(&captures["expressions"])
        .map(|c| c[1].to_string())
        .collect();

    return Ok(Command::Query {
        expressions,
        relation: captures["relation"].to_string(),
    });
}
