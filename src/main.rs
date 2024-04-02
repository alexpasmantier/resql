use anyhow::{bail, Result};
use commands::query::process_query;
use commands::tables::parse_schema_table;
use std::fs::File;

use crate::commands::{expressions_contain_count, Command};
use itertools::Itertools;

pub mod cli;
pub mod commands;
pub mod database;
pub mod engine;
pub mod parsing;
pub mod sql;

fn main() -> Result<()> {
    let command = cli::parse_command()?;
    engine::process_command(command)
}

fn old_main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let mut file = File::open(&args[1])?;

    match Command::try_from(command.as_str()) {
        Ok(Command::DBInfo) => {
            // dbinfo(&mut file)?;
        }
        Ok(Command::Tables) => {
            let relations = parse_schema_table(&mut file)?;
            println!("{}", relations.iter().map(|r| &r.name).join(" "));
        }
        Ok(Command::Query {
            expressions,
            relation,
            filter,
        }) => {
            let result = process_query(&mut file, &expressions, &relation, filter)?;
            if expressions_contain_count(&expressions) {
                println!("{}", result.count)
            } else if result.count > 0 {
                for r in result.results.iter() {
                    println!(
                        "{}",
                        r.iter()
                            .map(|s| {
                                if let SerialType::String { length: _, content } = s {
                                    content
                                } else {
                                    ""
                                }
                            })
                            .join("|")
                    )
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
