use std::fs::File;
use std::io::prelude::*;

use anyhow::{anyhow, Result};

use super::utils::parse_varint;

#[derive(Debug, Clone)]
pub enum SerialType {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    IntZero,
    IntOne,
    Reserved,
    Blob { length: u64, content: Vec<u8> },
    String { length: u64, content: String },
}

impl TryFrom<u64> for SerialType {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SerialType::Null),
            1 => Ok(SerialType::Int8(0)),
            2 => Ok(SerialType::Int16(0)),
            3 => Ok(SerialType::Int24(0)),
            4 => Ok(SerialType::Int32(0)),
            5 => Ok(SerialType::Int48(0)),
            6 => Ok(SerialType::Int64(0)),
            7 => Ok(SerialType::Float64(0.0)),
            8 => Ok(SerialType::IntZero),
            9 => Ok(SerialType::IntOne),
            10..=11 => Ok(SerialType::Reserved),
            v if v % 2 == 0 => Ok(SerialType::Blob {
                length: (v - 12) / 2,
                content: Vec::new(),
            }),
            v if v % 2 == 1 => Ok(SerialType::String {
                length: (v - 13) / 2,
                content: String::new(),
            }),
            v => Err(anyhow!("Unrecognized serial type value {}", v)),
        }
    }
}

#[derive(Clone)]
pub struct Record {
    pub payload_size: u64,
    pub row_id: u64,
    pub data: Vec<SerialType>,
}

pub fn parse_record(file: &mut File, record_offset: u64) -> Result<Record> {
    // content is at cell_content_offset
    file.seek(std::io::SeekFrom::Start(record_offset))?;
    // parse payload_size, row_id, cell header
    let (payload_size, _) = parse_varint(file)?;
    let (row_id, _) = parse_varint(file)?;
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
    // parse record data
    let mut record_data: Vec<SerialType> = Vec::new();
    for serial_type in serial_types.iter() {
        match serial_type {
            SerialType::Null => record_data.push(SerialType::Null),
            SerialType::Int8(_i) => {
                let mut buf = [0; 1];
                file.read_exact(&mut buf)?;
                record_data.push(SerialType::Int8(i8::from_be_bytes(buf)))
            }
            SerialType::Int16(_i) => {
                let mut buf = [0; 2];
                file.read_exact(&mut buf)?;
                record_data.push(SerialType::Int16(i16::from_be_bytes(buf)))
            }
            SerialType::Int24(_i) => {
                let mut buf = [0; 3];
                file.read_exact(&mut buf)?;
                let bufnew = [vec![0], buf.to_vec()].concat();
                record_data.push(SerialType::Int32(i32::from_be_bytes(
                    bufnew.try_into().unwrap(),
                )))
            }
            SerialType::Int32(_i) => {
                let mut buf = [0; 4];
                file.read_exact(&mut buf)?;
                record_data.push(SerialType::Int32(i32::from_be_bytes(buf)))
            }
            SerialType::Int48(_i) => {
                let mut buf = [0; 6];
                file.read_exact(&mut buf)?;
                let bufnew = [vec![0, 0], buf.to_vec()].concat();
                record_data.push(SerialType::Int48(i64::from_be_bytes(
                    bufnew.try_into().unwrap(),
                )))
            }
            SerialType::Int64(_i) => {
                let mut buf = [0; 8];
                file.read_exact(&mut buf)?;
                record_data.push(SerialType::Int64(i64::from_be_bytes(buf)))
            }
            SerialType::Float64(_i) => {
                let mut buf = [0; 8];
                file.read_exact(&mut buf)?;
                record_data.push(SerialType::Float64(f64::from_be_bytes(buf)))
            }
            SerialType::IntZero => record_data.push(SerialType::IntZero),
            SerialType::IntOne => record_data.push(SerialType::IntOne),
            SerialType::Blob { length, content: _ } => {
                let mut buf = [0; 1];
                let mut blob_contents: Vec<u8> = Vec::new();
                for _ in 0..*length {
                    file.read_exact(&mut buf)?;
                    blob_contents.push(buf[0]);
                }
                record_data.push(SerialType::Blob {
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
                record_data.push(SerialType::String {
                    length: *length,
                    content: string_characters.iter().collect(),
                })
            }
            SerialType::Reserved => {}
        }
    }

    Ok(Record {
        payload_size,
        row_id,
        data: record_data,
    })
}
