use std::{fs::File, io::Read};

use anyhow::{anyhow, Error, Result};

#[derive(Debug)]
pub enum BtreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
impl TryFrom<u8> for BtreePageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(BtreePageType::InteriorIndex),
            5 => Ok(BtreePageType::InteriorTable),
            10 => Ok(BtreePageType::LeafIndex),
            13 => Ok(BtreePageType::LeafTable),
            i => Err(anyhow!(
                "Value {} does not correspond to any valid page type",
                i
            )),
        }
    }
}

#[derive(Debug)]
pub struct BTreePageHeader {
    /// The one-byte flag at offset 0 indicating the b-tree page type.
    ///      A value of 2 (0x02) means the page is an interior index b-tree page.
    ///      A value of 5 (0x05) means the page is an interior table b-tree page.
    ///      A value of 10 (0x0a) means the page is a leaf index b-tree page.
    ///      A value of 13 (0x0d) means the page is a leaf table b-tree page.
    /// Any other value for the b-tree page type is an error.
    pub page_type: BtreePageType,
    /// or zero if there are none
    pub first_freeblock_offset: u16,
    pub number_of_cells: u16,
    /// A zero value for this integer is interpreted as 65536
    pub cell_content_area_offset: u16,
    /// the number of fragmented free bytes within the cell content area
    pub number_of_fragmented_free_bytes: u8,
    /// This value appears in the header of interior b-tree pages only and is omitted from all other pages.
    pub right_most_pointer: Option<u32>,
}

// TODO: this will now need to take a Vec<u8> instead of a file handle
pub fn parse_btree_page_header(file: &mut File) -> Result<BTreePageHeader> {
    // read the first 8 bytes
    let mut buffer = [0; 8];
    file.read_exact(&mut buffer)?;
    let page_type = BtreePageType::try_from(u8::from_be_bytes([buffer[0]]))?;
    let first_freeblock_offset = u16::from_be_bytes(buffer[1..3].try_into().unwrap());
    let number_of_cells = u16::from_be_bytes(buffer[3..5].try_into().unwrap());
    let cell_content_area_offset = u16::from_be_bytes(buffer[5..7].try_into().unwrap());
    let number_of_fragmented_free_bytes = u8::from_be_bytes([buffer[7]]);
    let right_most_pointer: Option<u32>;

    match page_type {
        BtreePageType::InteriorIndex | BtreePageType::InteriorTable => {
            // read the 4 extra bytes and produce value
            let mut buffer = [0; 4];
            file.read_exact(&mut buffer)?;
            right_most_pointer = Some(u32::from_be_bytes(buffer));
        }
        _ => {
            // set value to None
            right_most_pointer = None;
        }
    }

    let page_header = BTreePageHeader {
        page_type,
        first_freeblock_offset,
        number_of_cells,
        cell_content_area_offset,
        number_of_fragmented_free_bytes,
        right_most_pointer,
    };
    return Ok(page_header);
}
