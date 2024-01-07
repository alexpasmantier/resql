use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

use crate::parsing::database_header::DatabaseHeader;

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
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let database_header = DatabaseHeader::try_from(header)?;

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", database_header.page_size);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
