use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

use crate::commands::Command;
use crate::parsing::database_header::DatabaseHeader;
use crate::parsing::page_header::parse_btree_page_header;

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
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let database_header = DatabaseHeader::try_from(header)?;
            let schema_page_header = parse_btree_page_header(&mut file)?;

            // Uncomment this block to pass the first stage
            println!("database page size: {}", database_header.page_size);
            println!("number of tables: {}", schema_page_header.number_of_cells);
        }
        Ok(Command::Tables) => {
            let mut file = File::open(&args[1])?;
            file.seek(std::io::SeekFrom::Start(100))?;
            let schema_page_header = parse_btree_page_header(&mut file)?;

            println!("schema page header {:?}", schema_page_header);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
