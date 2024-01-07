use std::fs::File;
use std::io::prelude::*;

use anyhow::Result;

use crate::parsing::database_header::DatabaseHeader;
use crate::parsing::page_header::parse_btree_page_header;

pub fn dbinfo(file: &mut File) -> Result<()> {
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let database_header = DatabaseHeader::try_from(header)?;
    let schema_page_header = parse_btree_page_header(file)?;

    // Uncomment this block to pass the first stage
    println!("database page size: {}", database_header.page_size);
    println!("number of tables: {}", schema_page_header.number_of_cells);

    Ok(())
}
