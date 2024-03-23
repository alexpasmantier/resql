use anyhow::Result;

use super::btree_page::BtreePageType;

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

pub fn parse_btree_page_header(hypothetical_header: &[u8; 12]) -> Result<BTreePageHeader> {
    // read the first 8 bytes
    let page_type = BtreePageType::try_from(u8::from_be_bytes([hypothetical_header[0]]))?;
    let first_freeblock_offset = u16::from_be_bytes(hypothetical_header[1..3].try_into().unwrap());
    let number_of_cells = u16::from_be_bytes(hypothetical_header[3..5].try_into().unwrap());
    let cell_content_area_offset =
        u16::from_be_bytes(hypothetical_header[5..7].try_into().unwrap());
    let number_of_fragmented_free_bytes = u8::from_be_bytes([hypothetical_header[7]]);
    let right_most_pointer: Option<u32>;

    match page_type {
        BtreePageType::InteriorIndex | BtreePageType::InteriorTable => {
            // read the 4 extra bytes and produce value
            right_most_pointer = Some(u32::from_be_bytes(
                hypothetical_header[8..12].try_into().unwrap(),
            ));
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