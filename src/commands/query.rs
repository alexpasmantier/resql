use itertools::any;
use std::collections::HashMap;
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

use super::expressions_contain_count;
use super::tables::{load_relation, Relation};

#[derive(Debug, Clone)]
pub struct Row {
    pub values: HashMap<String, SerialType>,
}

#[derive(Debug)]
pub struct QueryResult {
    pub count: usize,
    pub results: Vec<Vec<SerialType>>,
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
    let relation = load_relation(file, relation_name)?;
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
    // parse each individual cell into a row
    let records: Vec<Record> = cell_offsets
        .iter()
        .map(|o| parse_record(file, SeekFrom::Start(page_offset + *o as u64)).unwrap())
        .collect();
    let rows: Vec<Row> = records
        .iter()
        .map(|r| convert_record_to_row(r, &relation))
        .collect();

    // filter rows based on where clause
    let filtered_rows = filter_rows_based_on_where_clause(&rows, filter);

    // filter expressions
    let results =
        filter_row_expressions_based_on_select_clause(&filtered_rows, expressions, &relation);

    // placeholder
    Ok(QueryResult {
        count: rows.len(),
        results,
    })
}

fn convert_record_to_row(record: &Record, relation: &Relation) -> Row {
    let mut values: HashMap<String, SerialType> = HashMap::new();
    for (i, col_name) in relation.columns.iter().enumerate() {
        let col_value: SerialType;
        if col_name == "id" {
            col_value = SerialType::Int64(record.row_id as i64);
        } else {
            col_value = record.data[i].to_owned();
        }
        values.insert(col_name.to_string(), col_value);
    }
    Row { values }
}

fn filter_rows_based_on_where_clause(
    rows: &Vec<Row>,
    filter: Option<(String, String)>,
) -> Vec<Row> {
    if let Some((col, value)) = filter {
        rows.iter()
            .filter(|r| {
                // this is overly simplified and will only work for strings
                r.values.get(&col).unwrap()
                    == &SerialType::String {
                        length: value.len() as u64,
                        content: value.clone(),
                    }
            })
            .map(|r| r.clone())
            .collect()
    } else {
        rows.clone()
    }
}

fn filter_row_expressions_based_on_select_clause(
    rows: &Vec<Row>,
    expressions: &Vec<String>,
    relation: &Relation,
) -> Vec<Vec<SerialType>> {
    // treat count expressions separately
    if expressions_contain_count(expressions) {
        rows.iter().map(|_| vec![]).collect()
    } else {
        rows.iter()
            .map(|r| {
                if any(expressions.iter(), |e| e == "*") {
                    relation
                        .columns
                        .iter()
                        .map(|c| r.values.get(c).unwrap().clone())
                        .collect()
                } else {
                    expressions
                        .iter()
                        .map(|e| r.values.get(e).unwrap().clone())
                        .collect::<Vec<SerialType>>()
                }
            })
            .collect()
    }
}
