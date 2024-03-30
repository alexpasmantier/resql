use std::cmp::Ordering;

use anyhow::{anyhow, Result};
use nom::error::ErrorKind;
use nom::number::complete::{be_f64, be_i64};
use nom::{
    bytes::complete::take,
    number::complete::{be_i16, be_i24, be_i32, be_i8},
};
use nom::{Err, IResult};

#[derive(Debug, Clone)]
pub enum SerialType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float64,
    IntZero,
    IntOne,
    Reserved,
    Blob { length: u64 },
    String { length: u64 },
}

impl TryFrom<u64> for SerialType {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SerialType::Null),
            1 => Ok(SerialType::Int8),
            2 => Ok(SerialType::Int16),
            3 => Ok(SerialType::Int24),
            4 => Ok(SerialType::Int32),
            5 => Ok(SerialType::Int48),
            6 => Ok(SerialType::Int64),
            7 => Ok(SerialType::Float64),
            8 => Ok(SerialType::IntZero),
            9 => Ok(SerialType::IntOne),
            10..=11 => Ok(SerialType::Reserved),
            v if v % 2 == 0 => Ok(SerialType::Blob {
                length: (v - 12) / 2,
            }),
            v if v % 2 == 1 => Ok(SerialType::String {
                length: (v - 13) / 2,
            }),
            v => Err(anyhow!("Unrecognized serial type value {}", v)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Blob(Vec<u8>),
    String(String),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => true,
            // numbers
            (Value::Int8(a), Value::Int8(b)) => a == b,
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a == b,
            // boolean comparisons
            (Value::Bool(a), Value::Bool(b)) => a == b,
            // blobs and strings
            (Value::Blob(a), Value::Blob(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }
}
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => None,
            // numbers
            (Value::Int8(a), Value::Int8(b)) => a.partial_cmp(b),
            (Value::Int16(a), Value::Int16(b)) => a.partial_cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            // boolean comparisons
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            // blobs and strings
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

fn parse_value<'a>(data: &'a [u8], serial_type: &SerialType) -> IResult<&'a [u8], Value> {
    match serial_type {
        SerialType::Null => Ok((data, Value::Null)),
        SerialType::Int8 => {
            let (rest, result) = be_i8(data)?;
            Ok((rest, Value::Int8(result)))
        }
        SerialType::Int16 => {
            let (rest, result) = be_i16(data)?;
            Ok((rest, Value::Int16(result)))
        }
        SerialType::Int24 => {
            let (rest, result) = be_i24(data)?;
            Ok((rest, Value::Int32(result as i32)))
        }
        SerialType::Int32 => {
            let (rest, result) = be_i32(data)?;
            Ok((rest, Value::Int32(result)))
        }
        SerialType::Int48 => {
            let (rest, result) = take(6usize)(data)?;
            let mut buf = [0; 8];
            buf[2..].clone_from_slice(result);
            Ok((rest, Value::Int64(i64::from_be_bytes(buf))))
        }
        SerialType::Int64 => {
            let (rest, result) = be_i64(data)?;
            Ok((rest, Value::Int64(result)))
        }
        SerialType::Float64 => {
            let (rest, result) = be_f64(data)?;
            Ok((rest, Value::Float64(result)))
        }
        SerialType::IntZero => Ok((data, Value::Bool(false))),
        SerialType::IntOne => Ok((data, Value::Bool(true))),
        SerialType::Blob { length } => {
            let (rest, result) = take(*length)(data)?;
            Ok((rest, Value::Blob(result.to_vec())))
        }
        SerialType::String { length } => {
            let (rest, result) = take(*length)(data)?;
            Ok((
                rest,
                Value::String(String::from_utf8_lossy(result).to_string()),
            ))
        }
        SerialType::Reserved => Err(Err::Error(nom::error::Error {
            input: data,
            code: ErrorKind::Fail,
        })),
    }
}
