use std::cmp::Ordering;
use std::io::prelude::*;
use std::{fs::File, io::SeekFrom};

use anyhow::{anyhow, Result};

use super::utils::take_varint;

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

impl PartialEq for SerialType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (Self::Reserved, _) | (_, Self::Null) | (_, Self::Reserved) => true,
            // numbers
            (SerialType::Int8(a), SerialType::Int8(b)) => a == b,
            (SerialType::Int16(a), SerialType::Int16(b)) => a == b,
            (SerialType::Int24(a), SerialType::Int24(b)) => a == b,
            (SerialType::Int32(a), SerialType::Int32(b)) => a == b,
            (SerialType::Int48(a), SerialType::Int48(b)) => a == b,
            (SerialType::Int64(a), SerialType::Int64(b)) => a == b,
            (SerialType::Float64(a), SerialType::Float64(b)) => a == b,
            // boolean comparisons
            (SerialType::IntZero, SerialType::IntZero) => true,
            (SerialType::IntOne, SerialType::IntOne) => true,
            // blobs and strings
            (
                SerialType::Blob {
                    length: _,
                    content: c1,
                },
                SerialType::Blob {
                    length: _,
                    content: c2,
                },
            ) => c1 == c2,
            (
                SerialType::String {
                    length: _,
                    content: c1,
                },
                SerialType::String {
                    length: _,
                    content: c2,
                },
            ) => c1 == c2,
            _ => false,
        }
    }
}
impl PartialOrd for SerialType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, _) | (Self::Reserved, _) | (_, Self::Null) | (_, Self::Reserved) => None,
            // numbers
            (SerialType::Int8(a), SerialType::Int8(b)) => a.partial_cmp(b),
            (SerialType::Int16(a), SerialType::Int16(b)) => a.partial_cmp(b),
            (SerialType::Int24(a), SerialType::Int24(b)) => a.partial_cmp(b),
            (SerialType::Int32(a), SerialType::Int32(b)) => a.partial_cmp(b),
            (SerialType::Int48(a), SerialType::Int48(b)) => a.partial_cmp(b),
            (SerialType::Int64(a), SerialType::Int64(b)) => a.partial_cmp(b),
            (SerialType::Float64(a), SerialType::Float64(b)) => a.partial_cmp(b),
            // boolean comparisons
            (SerialType::IntZero, SerialType::IntZero) => Some(Ordering::Equal),
            (SerialType::IntOne, SerialType::IntOne) => Some(Ordering::Equal),
            // blobs and strings
            (
                SerialType::Blob {
                    length: _,
                    content: c1,
                },
                SerialType::Blob {
                    length: _,
                    content: c2,
                },
            ) => c1.partial_cmp(c2),
            (
                SerialType::String {
                    length: _,
                    content: c1,
                },
                SerialType::String {
                    length: _,
                    content: c2,
                },
            ) => c1.partial_cmp(c2),
            _ => None,
        }
    }
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

#[derive(Clone, Debug)]
pub struct Record {
    pub payload_size: u64,
    pub row_id: u64,
    // TODO: refactor this so that serial types and the actual contained value class
    // are different, and make the record hold a vector of that different class.
    pub data: Vec<SerialType>,
}

/// Attempts to parse a record (content cell) at the given file offset.
pub fn parse_record(file: &mut File, record_offset: SeekFrom) -> Result<Record> {
    // content cell is at record_offset
    file.seek(record_offset)?;
    // parse payload_size, row_id, cell header
    let (payload_size, _) = take_varint(file)?;
    let (row_id, _) = take_varint(file)?;
    let (header_size, varint_size) = take_varint(file)?;

    let mut serial_types: Vec<SerialType> = Vec::new();
    let mut offset = 0;
    loop {
        let (varint, size) = take_varint(file)?;
        serial_types.push(SerialType::try_from(varint)?);
        offset += size;
        if offset as u64 == header_size - varint_size as u64 {
            break;
        }
    }
    // parse record data
    match serial_types
        .iter()
        .map(|s| parse_serial_type(file, s))
        .collect()
    {
        Ok(record_data) => {
            return Ok(Record {
                payload_size,
                row_id,
                data: record_data,
            })
        }
        Err(e) => Err(anyhow!("unable to parse record: {}", e)),
    }
}

/// Attempts to parse a value of type `SerialType` at the current file offset.
fn parse_serial_type(file: &mut File, serial_type: &SerialType) -> Result<SerialType> {
    match serial_type {
        SerialType::Null => Ok(SerialType::Null),
        SerialType::Int8(_i) => {
            let mut buf = [0; 1];
            file.read_exact(&mut buf)?;
            Ok(SerialType::Int8(i8::from_be_bytes(buf)))
        }
        SerialType::Int16(_i) => {
            let mut buf = [0; 2];
            file.read_exact(&mut buf)?;
            Ok(SerialType::Int16(i16::from_be_bytes(buf)))
        }
        SerialType::Int24(_i) => {
            let mut buf = [0; 3];
            file.read_exact(&mut buf)?;
            let bufnew = [vec![0], buf.to_vec()].concat();
            Ok(SerialType::Int32(i32::from_be_bytes(
                bufnew.try_into().unwrap(),
            )))
        }
        SerialType::Int32(_i) => {
            let mut buf = [0; 4];
            file.read_exact(&mut buf)?;
            Ok(SerialType::Int32(i32::from_be_bytes(buf)))
        }
        SerialType::Int48(_i) => {
            let mut buf = [0; 6];
            file.read_exact(&mut buf)?;
            let bufnew = [vec![0, 0], buf.to_vec()].concat();
            Ok(SerialType::Int48(i64::from_be_bytes(
                bufnew.try_into().unwrap(),
            )))
        }
        SerialType::Int64(_i) => {
            let mut buf = [0; 8];
            file.read_exact(&mut buf)?;
            Ok(SerialType::Int64(i64::from_be_bytes(buf)))
        }
        SerialType::Float64(_i) => {
            let mut buf = [0; 8];
            file.read_exact(&mut buf)?;
            Ok(SerialType::Float64(f64::from_be_bytes(buf)))
        }
        SerialType::IntZero => Ok(SerialType::IntZero),
        SerialType::IntOne => Ok(SerialType::IntOne),
        SerialType::Blob { length, content: _ } => {
            let mut buf = [0; 1];
            let mut blob_contents: Vec<u8> = Vec::new();
            for _ in 0..*length {
                file.read_exact(&mut buf)?;
                blob_contents.push(buf[0]);
            }
            Ok(SerialType::Blob {
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
            Ok(SerialType::String {
                length: *length,
                content: string_characters.iter().collect(),
            })
        }
        SerialType::Reserved => Ok(SerialType::Reserved),
    }
}
