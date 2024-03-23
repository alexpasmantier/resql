use std::ops::Range;

use anyhow::{anyhow, Context, Error, Result};

use super::btree_page_header::{parse_btree_page_header, BTreePageHeader};

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

pub struct BTreePage {
    btree_type: BtreePageType,
    page_header: BTreePageHeader,
    // page_contents: BtreePageData,
    // TODO: add content-related fields
    // and maybe move btree-related parsing into here?
}

impl TryFrom<Vec<u8>> for BTreePage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        // parse page header
        let hypothetical_header: [u8; 12] = value[..12].try_into().unwrap();
        let header = parse_btree_page_header(&hypothetical_header)
            .context("Unable to parse btree page header")?;

        // parse cell pointer array
        let cell_pointer_array_bytes: &[u8];
        let cell_pointer_array_offset: usize;
        match header.page_type {
            // cell pointer array @12..
            BtreePageType::InteriorIndex | BtreePageType::InteriorTable => {
                cell_pointer_array_offset = 12;
            }
            // cell pointer array @8..
            BtreePageType::LeafIndex | BtreePageType::LeafTable => {
                cell_pointer_array_offset = 8;
            }
        }
        cell_pointer_array_bytes = &value[cell_pointer_array_offset
            ..cell_pointer_array_offset + 2 * header.number_of_cells as usize];
        let cell_pointer_array: Vec<u16> = Range {
            start: 0,
            end: header.number_of_cells as usize,
        }
        .map(|i| u16::from_be_bytes(cell_pointer_array_bytes[i..i + 2].try_into().unwrap()))
        .collect();

        todo!()
    }
}

// TODO: we will then need some query handling logic (maybe not in this module) that
// iterates across pages and indexes
