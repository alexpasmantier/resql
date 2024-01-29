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

use super::tables::{load_relation_metadata, parse_schema_table, Relation};
use super::Command;

pub struct Row {
    pub values: Vec<SerialType>,
}

pub struct QueryResult {
    pub count: usize,
    pub rows: Vec<Row>,
}

/// note this is kept really simple for now and only handles single expression count queries and
/// regular sets of single-column expressions
pub fn process_query(
    file: &mut File,
    expressions: &Vec<String>,
    relation_name: &str,
    filter: Option<(String, String)>,
) -> Result<QueryResult> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;

    // read schema table to load the relevant relation metadata and go to the correct page
    let relation = load_relation_metadata(file, relation_name)?;
    let page_offset = (relation.root_page_number - 1) as u64 * database_header.page_size as u64;
    file.seek(SeekFrom::Start(page_offset))?;

    // parse page
    let page_header = parse_btree_page_header(file)?;
    let mut cell_offsets: Vec<u16> = Vec::new();
    // gather cell offsets
    for _ in 0..page_header.number_of_cells {
        let mut buffer = [0; 2];
        file.read_exact(&mut buffer)?;
        cell_offsets.push(u16::from_be_bytes(buffer));
    }
    // parse each individual cell into a record
    let records: Vec<Record> = cell_offsets
        .iter()
        // NOTE: why can't we collect this into a Vec<Record> ? (check out anyhow's documentation)
        .map(|o| parse_record(file, SeekFrom::Start(page_offset + *o as u64)).unwrap())
        .collect();

    // TODO: finish this (filtering, count)

    // placeholder
    Ok(QueryResult {
        count: 0,
        rows: vec![],
    })
}

pub fn query_count(
    file: &mut File,
    relation: &str,
    filter: Option<(String, String)>,
) -> Result<usize> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;

    // read master table to find root_page_number
    let table_records = parse_schema_table(file)?;
    for rec in table_records.iter() {
        if let SerialType::String { length: _, content } = &rec.data[1] {
            if content == relation {
                let record = rec.clone();
                if let SerialType::String { length: _, content } = &record.data[4] {
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

                        if let Some((filtercol, filtervalue)) = filter {
                            let filtercol_index = get_col_indexes(&content, vec![filtercol])[0];
                            records = records
                                .iter()
                                .filter(|r| {
                                    if let SerialType::String {
                                        length: _,
                                        content: value,
                                    } = &r.data[filtercol_index]
                                    {
                                        return *value == filtervalue;
                                    } else {
                                        return true;
                                    }
                                })
                                .map(|r| r.clone())
                                .collect_vec();
                        }

                        // decide what to do with records (this is crappy and will need refactoring)
                        return Ok(records.len());
                    }
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
    filter: Option<(String, String)>,
) -> Result<Vec<Vec<SerialType>>> {
    // read header to find out page size
    let mut buf = [0; 100];
    file.read_exact(&mut buf)?;
    let database_header = DatabaseHeader::try_from(buf)?;

    // read master table to find root_page_number
    let table_records = parse_schema_table(file)?;
    for rec in table_records.iter() {
        if let SerialType::String { length: _, content } = &rec.data[1] {
            if content == relation {
                let record = rec.clone();
                if let SerialType::String { length: _, content } = &record.data[4] {
                    let select_col_indexes = get_col_indexes(&content, expressions.clone());

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
                        if let Some((filtercol, filtervalue)) = filter {
                            let filtercol_index = get_col_indexes(&content, vec![filtercol])[0];
                            records = records
                                .iter()
                                .filter(|r| {
                                    if let SerialType::String {
                                        length: _,
                                        content: value,
                                    } = &r.data[filtercol_index]
                                    {
                                        return *value == filtervalue;
                                    } else {
                                        return true;
                                    }
                                })
                                .map(|r| r.clone())
                                .collect_vec();
                        }

                        let mut result: Vec<Vec<SerialType>> = Vec::new();
                        for record in records.iter() {
                            let mut cols: Vec<SerialType> = Vec::new();

                            for i in select_col_indexes.iter() {
                                cols.push(record.data[*i].clone());
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
    dbg!(&col_names);
    dbg!(&columns);
    let col_indexes = columns
        .iter()
        .map(|c| {
            let (index, _) = col_names.iter().find_position(|cn| *cn == c).unwrap();
            index
        })
        .collect();
    col_indexes
}
