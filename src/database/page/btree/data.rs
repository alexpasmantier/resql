use anyhow::{anyhow, Result};
use nom::{number::complete::be_u32, IResult};

use crate::parsing::utils::take_varint;

pub enum CellType {
    LeafTable(LeafTableCell),
    InteriorTable(InteriorTableCell),
    LeafIndex(LeafIndexCell),
    InteriorIndex(InteriorIndexCell),
}
pub mod record;
pub mod serial_types;

/// Table B-Tree Leaf Cell (header 0x0d)
pub struct LeafTableCell {
    /// A varint which is the total number of bytes of payload, including any overflow
    payload_size: u64,
    /// A varint which is the integer key, a.k.a. "rowid"
    key: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    payload: Payload,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: Option<u32>,
}

/// Table B-Tree Interior Cell (header 0x05):
pub struct InteriorTableCell {
    /// A 4-byte big-endian page number which is the left child pointer.
    left_child_pointer: u32,
    /// A varint which is the integer key, a.k.a. "rowid"
    key: u64,
}

/// Index B-Tree Leaf Cell (header 0x0a):
pub struct LeafIndexCell {
    /// A varint which is the total number of bytes of key payload, including any overflow
    payload_size: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    payload: Payload,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: Option<u32>,
}

/// Index B-Tree Interior Cell (header 0x02):
pub struct InteriorIndexCell {
    /// A 4-byte big-endian page number which is the left child pointer.
    left_child_pointer: u32,
    /// A varint which is the total number of bytes of key payload, including any overflow
    payload_size: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    payload: Payload,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: Option<u32>,
}

// varint
fn parse_payload_size(input: &[u8]) -> IResult<&[u8], u64> {
    take_varint(input)
}

// varint
fn parse_rowid(input: &[u8]) -> IResult<&[u8], u64> {
    take_varint(input)
}

// u32
fn parse_left_child_pointer(input: &[u8]) -> IResult<&[u8], u32> {
    be_u32(input)
}

// u32
fn parse_first_overflow_page_number(input: &[u8]) -> IResult<&[u8], u32> {
    be_u32(input)
}

pub fn parse_leaf_table_cell(input: &[u8]) -> Result<CellType> {
    let (input, payload_size) = parse_payload_size(input)?;
    let (input, key) = parse_rowid(input)?;
    // overflow is not handled for the moment
    if let Some(payload_content) = input.get(..payload_size as usize) {
        Ok(CellType::LeafTable(LeafTableCell {
            payload_size,
            key,
            payload: Payload {
                content: payload_content.to_vec(),
            },
            first_overflow_page_number: None,
        }))
    } else {
        Err(anyhow!(
            "Couldn't read enough bytes from page to extract the entire payload.\
            This might be a case of payload overflow, for which the logic isn't yet implemented."
        ))
    }
}

pub fn parse_interior_table_cell(input: &[u8]) -> Result<CellType> {
    let (input, left_child_pointer) = parse_left_child_pointer(input)?;
    let (input, key) = parse_rowid(input)?;
    Ok(CellType::InteriorTable(InteriorTableCell {
        left_child_pointer,
        key,
    }))
}

pub fn parse_leaf_index_cell(input: &[u8]) -> Result<CellType> {
    let (input, payload_size) = parse_payload_size(input)?;
    // overflow is not handled for the moment
    if let Some(payload_content) = input.get(..payload_size as usize) {
        Ok(CellType::LeafIndex(LeafIndexCell {
            payload_size,
            payload: Payload {
                content: payload_content.to_vec(),
            },
            first_overflow_page_number: None,
        }))
    } else {
        Err(anyhow!(
            "Couldn't read enough bytes from page to extract the entire payload.\
            This might be a case of payload overflow, for which the logic isn't yet implemented."
        ))
    }
}

pub fn parse_interior_index_cell(input: &[u8]) -> Result<CellType> {
    let (input, left_child_pointer) = parse_left_child_pointer(input)?;
    let (input, payload_size) = parse_payload_size(input)?;
    // overflow is not handled for the moment
    if let Some(payload_content) = input.get(..payload_size as usize) {
        Ok(CellType::InteriorIndex(InteriorIndexCell {
            left_child_pointer,
            payload_size,
            payload: Payload {
                content: payload_content.to_vec(),
            },
            first_overflow_page_number: None,
        }))
    } else {
        Err(anyhow!(
            "Couldn't read enough bytes from page to extract the entire payload.\
            This might be a case of payload overflow, for which the logic isn't yet implemented."
        ))
    }
}

/// a cell's payload section
pub struct Payload {
    pub content: Vec<u8>,
}
