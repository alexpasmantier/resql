use anyhow::{bail, Result};
use commands::dbinfo::dbinfo;
use commands::query::query;
use commands::tables::tables;
use std::fs::File;

use crate::commands::Command;
use crate::parsing::records::SerialType;
use itertools::Itertools;

pub mod commands;
pub mod parsing;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match Command::try_from(command.as_str()) {
        Ok(Command::DBInfo) => {
            let mut file = File::open(&args[1])?;
            dbinfo(&mut file)?;
        }
        Ok(Command::Tables) => {
            let mut file = File::open(&args[1])?;
            let records = tables(&mut file)?;
            println!(
                "{}",
                records
                    .iter()
                    .map(|r| {
                        if let SerialType::String { length: _, content } = &r.data[1] {
                            content
                        } else {
                            ""
                        }
                    })
                    .join(" ")
            );
        }
        Ok(Command::Query { table }) => {
            let mut file = File::open(&args[1])?;
            let result = query(&mut file, &table)?;
            println!("{}", result);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
