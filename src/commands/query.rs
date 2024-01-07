use std::io::prelude::*;
use std::{
    fs::File,
    io::{Read, SeekFrom},
};

use anyhow::Result;

use crate::parsing::page_header::parse_btree_page_header;
use crate::parsing::records::parse_record;
use crate::parsing::{
    database_header::DatabaseHeader,
    records::{Record, SerialType},
};

use super::tables::tables;

pub fn query(file: &mut File, table_name: &str) -> Result<usize> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;
    dbg!(&database_header);

    // read master table to find root_page_number
    let table_records = tables(file)?;
    for rec in table_records.iter() {
        if let SerialType::String { length: _, content } = &rec.data[1] {
            if content == table_name {
                let record = rec.clone();
                if let SerialType::Int8(root_page_number) = record.data[3] {
                    let page_offset = root_page_number as u64 * database_header.page_size as u64;
                    file.seek(SeekFrom::Start(page_offset))?;

                    // parse page header
                    let page_header = parse_btree_page_header(file)?;

                    let mut records: Vec<Record> = Vec::new();

                    for _ in 0..page_header.number_of_cells {
                        let mut buffer = [0; 2];
                        file.read_exact(&mut buffer)?;
                        // save current position
                        let current_position = file.stream_position()?;
                        let cell_content_offset = u16::from_be_bytes(buffer);
                        let record = parse_record(file, cell_content_offset as u64)?;
                        records.push(record);
                        file.seek(std::io::SeekFrom::Start(current_position))?;
                    }

                    return Ok(records.len());
                }
            }
        }
    }
    Ok(0)
}
