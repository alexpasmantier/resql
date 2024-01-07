use std::fs::File;
use std::io::prelude::*;

use anyhow::Result;
use itertools::Itertools;

use crate::parsing::page_header::parse_btree_page_header;
use crate::parsing::records::{parse_record, Record, SerialType};

pub fn tables(file: &mut File) -> Result<Vec<Record>> {
    file.seek(std::io::SeekFrom::Start(100))?;
    let schema_page_header = parse_btree_page_header(file)?;
    // println!("schema page header {:?}", schema_page_header);

    let _table_names: Vec<String> = Vec::new();
    let mut records: Vec<Record> = Vec::new();

    for _ in 0..schema_page_header.number_of_cells {
        let mut buffer = [0; 2];
        file.read_exact(&mut buffer)?;
        // save current position
        let current_position = file.stream_position()?;
        let cell_content_offset = u16::from_be_bytes(buffer);
        let record = parse_record(file, cell_content_offset as u64)?;
        records.push(record);
        file.seek(std::io::SeekFrom::Start(current_position))?;
    }

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

    Ok(records)
}
