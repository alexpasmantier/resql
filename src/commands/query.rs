use itertools::Itertools;
use regex::Regex;
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

// TODO: refactor all this
pub fn query_count(file: &mut File, relation: &str) -> Result<usize> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;

    // read master table to find root_page_number
    let table_records = tables(file)?;
    for rec in table_records.iter() {
        if let SerialType::String { length: _, content } = &rec.data[1] {
            if content == relation {
                let record = rec.clone();
                dbg!(&record);
                if let SerialType::Int8(root_page_number) = record.data[3] {
                    // page numbers start at 1
                    let page_offset =
                        (root_page_number - 1) as u64 * database_header.page_size as u64;
                    file.seek(SeekFrom::Start(page_offset))?;

                    // parse page header
                    let page_header = parse_btree_page_header(file)?;

                    let mut records: Vec<Record> = Vec::new();

                    for _ in 0..page_header.number_of_cells {
                        let mut buffer = [0; 2];
                        file.read_exact(&mut buffer)?;
                        let cell_content_offset = u16::from_be_bytes(buffer);
                        // save current position
                        let current_position = file.stream_position()?;

                        let record = parse_record(
                            file,
                            SeekFrom::Start(page_offset + cell_content_offset as u64),
                        )?;
                        records.push(record);
                        file.seek(std::io::SeekFrom::Start(current_position))?;
                    }

                    // decide what to do with records (this is crappy and will need refactoring)
                    return Ok(records.len());
                }
            }
        }
    }
    Ok(0)
}

pub fn query_expression(
    file: &mut File,
    relation: &str,
    expressions: Vec<String>,
) -> Result<Vec<Vec<SerialType>>> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;

    // read master table to find root_page_number
    let table_records = tables(file)?;
    for rec in table_records.iter() {
        if let SerialType::String { length: _, content } = &rec.data[1] {
            if content == relation {
                let record = rec.clone();
                if let SerialType::String { length: _, content } = &record.data[4] {
                    let col_indexes = get_col_indexes(&content, expressions.clone());

                    if let SerialType::Int8(root_page_number) = record.data[3] {
                        // page numbers start at 1
                        let page_offset =
                            (root_page_number - 1) as u64 * database_header.page_size as u64;
                        file.seek(SeekFrom::Start(page_offset))?;

                        // parse page header
                        let page_header = parse_btree_page_header(file)?;

                        let mut records: Vec<Record> = Vec::new();

                        for _ in 0..page_header.number_of_cells {
                            let mut buffer = [0; 2];
                            file.read_exact(&mut buffer)?;
                            let cell_content_offset = u16::from_be_bytes(buffer);
                            // save current position
                            let current_position = file.stream_position()?;

                            let record = parse_record(
                                file,
                                SeekFrom::Start(page_offset + cell_content_offset as u64),
                            )?;
                            records.push(record);
                            file.seek(std::io::SeekFrom::Start(current_position))?;
                        }

                        let mut result: Vec<Vec<SerialType>> = Vec::new();
                        for record in records.iter() {
                            let mut cols: Vec<SerialType> = Vec::new();
                            for (i, col) in record.data.iter().enumerate() {
                                if col_indexes.contains(&i) {
                                    cols.push(col.clone());
                                }
                            }
                            result.push(cols);
                        }
                        return Ok(result);
                    }
                }
            }
        }
    }
    Ok(vec![])
}

fn get_col_indexes(ddl: &str, columns: Vec<String>) -> Vec<usize> {
    // "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)\
    let clean_ddl = ddl.replace("\n", " ").replace("\t", " ");
    let col_declaration_segment_re = Regex::new(r"\((.*)\)").unwrap();
    let col_name_re = Regex::new(r"\s*(\w+)\s+[\w\s]*").unwrap();

    let col_segment: &str = col_declaration_segment_re
        .captures(&clean_ddl)
        .unwrap()
        .get(1)
        .unwrap()
        .as_str();
    let mut col_names: Vec<String> = Vec::new();
    for cap in col_name_re.captures_iter(col_segment) {
        col_names.push(cap[1].to_string());
    }
    // dbg!(&col_names);
    // dbg!(&columns);
    let col_indexes = columns
        .iter()
        .map(|c| {
            let (index, _) = col_names.iter().find_position(|cn| *cn == c).unwrap();
            index
        })
        .collect();
    col_indexes
}
