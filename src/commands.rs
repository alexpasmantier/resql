use anyhow::{anyhow, Result};
use itertools::any;
use regex::Regex;

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
    let query_re: Regex =
        Regex::new(r"(?i)select\s+(?P<expressions>[\w,\(\)\*\s]+)\s+from\s+(?P<relation>\w+)")
            .unwrap();
    let query_cols_re: Regex = Regex::new(r"(?i)([\w\(\*\)]+)").unwrap();
    // this is over-simplified for now and can be extended later
    let query_filter_re: Regex =
        Regex::new(r"(?i)\s+where\s+(?P<filtercol>\w+)\s*=\s*'(?P<filtervalue>\w+)'").unwrap();

    let main_captures = query_re.captures(str_query).unwrap();

    let expressions: Vec<String> = query_cols_re
        .captures_iter(&main_captures["expressions"])
        .map(|c| c[1].to_string())
        .collect();

    let mut filter: Option<(String, String)> = None;
    if let Some(filter_captures) = query_filter_re.captures(str_query) {
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

pub fn expressions_contain_count(expressions: &Vec<String>) -> bool {
    let count_re: Regex = Regex::new(r"(?i)count\([\w*]+\)").unwrap();
    any(expressions.iter(), |e| count_re.is_match(e))
}
