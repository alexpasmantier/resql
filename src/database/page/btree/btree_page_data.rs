use nom::{number::complete::be_u32, IResult};

use crate::parsing::utils::take_varint;

/// Table B-Tree Leaf Cell (header 0x0d)
pub struct TableBTreeLeafCell {
    /// A varint which is the total number of bytes of payload, including any overflow
    payload_size: u64,
    /// A varint which is the integer key, a.k.a. "rowid"
    key: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    local_payload: Vec<u8>,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: u32,
}

/// Table B-Tree Interior Cell (header 0x05):
pub struct TableBTreeInteriorCell {
    /// A 4-byte big-endian page number which is the left child pointer.
    left_child_pointer: u32,
    /// A varint which is the integer key, a.k.a. "rowid"
    key: u64,
}

/// Index B-Tree Leaf Cell (header 0x0a):
pub struct IndexBTreeLeafCell {
    /// A varint which is the total number of bytes of key payload, including any overflow
    payload_size: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    local_payload: Vec<u8>,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: u32,
}

/// Index B-Tree Interior Cell (header 0x02):
pub struct IndexBTreeInteriorCell {
    /// A 4-byte big-endian page number which is the left child pointer.
    left_child_pointer: u32,
    /// A varint which is the total number of bytes of key payload, including any overflow
    payload_size: u64,
    /// The initial portion of the payload that does not spill to overflow pages.
    payload: Vec<u8>,
    /// A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
    first_overflow_page_number: u32,
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


// NOTE: is this really a good idea? does it bring anything? how is FromStr implemented?
pub trait ParseFrom {
    fn parse_from(data: &[u8]) -> IResult<&[u8], Self>
    where Self: ;
}


impl TableBTreeLeafCell {
    pub fn parse_from()
}
