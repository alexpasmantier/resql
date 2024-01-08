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
        filter: Option<(String, String)>,
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
    let re = Regex::new(r"(?i)select\s+(?P<expressions>[\w,\(\)\*\s]+)\s+from\s+(?P<relation>\w+)")
        .unwrap();
    let main_captures = re.captures(str_query).unwrap();

    let cols_re = Regex::new(r"(?i)([\w\(\*\)]+)").unwrap();
    let expressions: Vec<String> = cols_re
        .captures_iter(&main_captures["expressions"])
        .map(|c| c[1].to_string())
        .collect();

    let filter_re =
        Regex::new(r"(?)\s+where\s+(?P<filtercol>\w+)\s+=\s+'(?P<filtervalue>\w+)'").unwrap();
    let mut filter: Option<(String, String)> = None;
    if let Some(filter_captures) = re.captures(str_query) {
        let filtercol = filter_captures["filtercol"].to_string();
        let filtervalue = filter_captures["filtervalue"].to_string();
        filter = Some((filtercol, filtervalue));
    }

    return Ok(Command::Query {
        expressions,
        relation: main_captures["relation"].to_string(),
        filter,
    });
}
