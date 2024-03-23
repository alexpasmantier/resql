use std::fs::File;
use std::io::prelude::*;

use anyhow::{anyhow, Result};

use crate::database::page::btree::btree_page_header::parse_btree_page_header;
use crate::parsing::ddl::parse_column_names_from_ddl;
use crate::parsing::record::{parse_record, Record, SerialType};

#[derive(Debug, Clone)]
pub enum RelationType {
    Table,
    Index,
    View,
    // we'll ignore triggers for now
    // Trigger,
}

impl TryFrom<&SerialType> for RelationType {
    type Error = anyhow::Error;

    fn try_from(value: &SerialType) -> Result<Self> {
        match value {
            SerialType::String { length: _, content } => {
                if content == "table" {
                    return Ok(RelationType::Table);
                } else if content == "index" {
                    return Ok(RelationType::Index);
                } else if content == "view" {
                    return Ok(RelationType::View);
                } else {
                    return Err(anyhow!("Unable to parse RelationType from {:?}", content));
                }
            }
            v => return Err(anyhow!("Unable to parse RelationType from {:?}", v)),
        }
    }
}

/// This represents a table, an index, a view, ...
#[derive(Debug, Clone)]
pub struct Relation {
    pub relation_type: RelationType,
    pub name: String,
    pub table_name: String,
    pub root_page_number: i8,
    // this could also hold the type but we're sticking to just the names for now
    pub columns: Vec<String>,
}
impl Relation {
    pub fn root_page_offset(&self, page_size: u16) -> u64 {
        (self.root_page_number - 1) as u64 * page_size as u64
    }
}
impl TryFrom<Record> for Relation {
    type Error = anyhow::Error;

    fn try_from(value: Record) -> Result<Self> {
        let data = value.data;
        let relation_type = RelationType::try_from(&data[0])?;
        let name = match &data[1] {
            SerialType::String { length: _, content } => Ok(content.to_string()),
            _ => Err(anyhow!("Error parsing name")),
        }?;
        let table_name = match &data[2] {
            SerialType::String { length: _, content } => Ok(content.to_string()),
            _ => Err(anyhow!("Error parsing table name")),
        }?;
        let root_page_number = match data[3] {
            SerialType::Int8(number) => Ok(number),
            _ => Err(anyhow!("Error parsing root page number")),
        }?;
        let columns = match &data[4] {
            SerialType::String { length: _, content } => parse_column_names_from_ddl(&content),
            _ => Err(anyhow!("Error parsing ddl")),
        }?;
        return Ok(Relation {
            relation_type,
            name,
            table_name,
            root_page_number,
            columns,
        });
    }
}

/// Parses the main schema table in order to get every relation the database currently holds
pub fn parse_schema_table(file: &mut File) -> Result<Vec<Relation>> {
    file.seek(std::io::SeekFrom::Start(100))?;
    todo!();
    // let schema_page_header = parse_btree_page_header(file)?;
    // // println!("schema page header {:?}", schema_page_header);
    //
    // let _table_names: Vec<String> = Vec::new();
    // let mut records: Vec<Record> = Vec::new();
    //
    // for _ in 0..schema_page_header.number_of_cells {
    //     let mut buffer = [0; 2];
    //     file.read_exact(&mut buffer)?;
    //     // save current position
    //     let current_position = file.stream_position()?;
    //     let cell_content_offset = u16::from_be_bytes(buffer);
    //     let record = parse_record(file, std::io::SeekFrom::Start(cell_content_offset as u64))?;
    //     records.push(record);
    //     file.seek(std::io::SeekFrom::Start(current_position))?;
    // }
    //
    // let relations: Vec<Relation> = records
    //     .iter()
    //     .map(move |r| Relation::try_from(r.clone()).unwrap())
    //     .collect();
    //
    // Ok(relations)
}

/// Parses the main schema table and tries to extract the relevant relation from it
pub fn load_relation(file: &mut File, relation_name: &str) -> Result<Relation> {
    let relations = parse_schema_table(file)?;
    match relations.iter().find(|r| &r.name == relation_name) {
        Some(&ref relation) => Ok(relation.clone()),
        None => Err(anyhow!("relation {} does not exist", relation_name)),
    }
}
