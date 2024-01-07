use std::fs::File;
use std::io::prelude::*;

use anyhow::Result;
use itertools::Itertools;

use crate::parsing::page_header::parse_btree_page_header;
use crate::parsing::records::SerialType;
use crate::parsing::utils::parse_varint;

pub fn tables(file: &mut File) -> Result<()> {
    file.seek(std::io::SeekFrom::Start(100))?;
    let schema_page_header = parse_btree_page_header(file)?;
    // println!("schema page header {:?}", schema_page_header);

    let _table_names: Vec<String> = Vec::new();
    let mut records: Vec<Vec<SerialType>> = Vec::new();

    for _ in 0..schema_page_header.number_of_cells {
        let mut buffer = [0; 2];
        file.read_exact(&mut buffer)?;
        // save current position
        let current_position = file.stream_position()?;
        let cell_content_offset = u16::from_be_bytes(buffer);
        // content is at cell_content_offset
        file.seek(std::io::SeekFrom::Start(cell_content_offset as u64))?;
        // parse payload_size, row_id, cell header
        let (_payload_size, _) = parse_varint(file)?;
        let (_row_id, _) = parse_varint(file)?;
        let (header_size, varint_size) = parse_varint(file)?;

        let mut serial_types: Vec<SerialType> = Vec::new();
        let mut offset = 0;
        loop {
            let (varint, size) = parse_varint(file)?;
            serial_types.push(SerialType::try_from(varint)?);
            offset += size;
            if offset as u64 == header_size - varint_size as u64 {
                break;
            }
        }
        // parse cell data
        let mut cell_data: Vec<SerialType> = Vec::new();
        for serial_type in serial_types.iter() {
            match serial_type {
                SerialType::Null => cell_data.push(SerialType::Null),
                SerialType::Int8(_i) => {
                    let mut buf = [0; 1];
                    file.read_exact(&mut buf)?;
                    cell_data.push(SerialType::Int8(i8::from_be_bytes(buf)))
                }
                SerialType::Int16(_i) => {
                    let mut buf = [0; 2];
                    file.read_exact(&mut buf)?;
                    cell_data.push(SerialType::Int16(i16::from_be_bytes(buf)))
                }
                SerialType::Int24(_i) => {
                    let mut buf = [0; 3];
                    file.read_exact(&mut buf)?;
                    let bufnew = [vec![0], buf.to_vec()].concat();
                    cell_data.push(SerialType::Int32(i32::from_be_bytes(
                        bufnew.try_into().unwrap(),
                    )))
                }
                SerialType::Int32(_i) => {
                    let mut buf = [0; 4];
                    file.read_exact(&mut buf)?;
                    cell_data.push(SerialType::Int32(i32::from_be_bytes(buf)))
                }
                SerialType::Int48(_i) => {
                    let mut buf = [0; 6];
                    file.read_exact(&mut buf)?;
                    let bufnew = [vec![0, 0], buf.to_vec()].concat();
                    cell_data.push(SerialType::Int48(i64::from_be_bytes(
                        bufnew.try_into().unwrap(),
                    )))
                }
                SerialType::Int64(_i) => {
                    let mut buf = [0; 8];
                    file.read_exact(&mut buf)?;
                    cell_data.push(SerialType::Int64(i64::from_be_bytes(buf)))
                }
                SerialType::Float64(_i) => {
                    let mut buf = [0; 8];
                    file.read_exact(&mut buf)?;
                    cell_data.push(SerialType::Float64(f64::from_be_bytes(buf)))
                }
                SerialType::IntZero => cell_data.push(SerialType::IntZero),
                SerialType::IntOne => cell_data.push(SerialType::IntOne),
                SerialType::Blob { length, content: _ } => {
                    let mut buf = [0; 1];
                    let mut blob_contents: Vec<u8> = Vec::new();
                    for _ in 0..*length {
                        file.read_exact(&mut buf)?;
                        blob_contents.push(buf[0]);
                    }
                    cell_data.push(SerialType::Blob {
                        length: *length,
                        content: blob_contents,
                    })
                }
                SerialType::String { length, content: _ } => {
                    let mut buf = [0; 1];
                    let mut string_characters: Vec<char> = Vec::new();
                    for _ in 0..*length {
                        file.read_exact(&mut buf)?;
                        string_characters.push(buf[0] as char);
                    }
                    cell_data.push(SerialType::String {
                        length: *length,
                        content: string_characters.iter().collect(),
                    })
                }
                SerialType::Reserved => {}
            }
        }

        records.push(cell_data);
        file.seek(std::io::SeekFrom::Start(current_position))?;
    }

    // println!("records {:?}", records);

    println!(
        "{}",
        records
            .iter()
            .map(|r| {
                if let SerialType::String { length: _, content } = &r[1] {
                    content
                } else {
                    ""
                }
            })
            .join(" ")
    );

    Ok(())
}
