use std::ops::Range;

use anyhow::{anyhow, Context, Error, Result};
use itertools::Itertools;

use crate::database::page::btree::btree_page_data::{
    parse_interior_index_cell, parse_interior_table_cell, parse_leaf_index_cell,
    parse_leaf_table_cell,
};

use super::btree_page_data::CellType;
use super::btree_page_header::{parse_btree_page_header, BTreePageHeader};

#[derive(Debug)]
pub enum BTreePageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}
impl TryFrom<u8> for BTreePageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(BTreePageType::InteriorIndex),
            5 => Ok(BTreePageType::InteriorTable),
            10 => Ok(BTreePageType::LeafIndex),
            13 => Ok(BTreePageType::LeafTable),
            i => Err(anyhow!(
                "Value {} does not correspond to any valid page type",
                i
            )),
        }
    }
}

// FIXME: this looks pretty shitty and should probably be refactored into an enum and several
// variants
pub struct BTreePage {
    btree_type: BTreePageType,
    page_header: BTreePageHeader,
    cells: Vec<CellType>,
}

impl TryFrom<Vec<u8>> for BTreePage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        // parse page header
        let rest: &[u8];
        let (rest, header) =
            parse_btree_page_header(&value).context("Unable to parse btree page header")?;

        // parse cell pointer array
        let (_, mut cell_pointer_array) =
            parse_cell_pointer_array(rest, header.number_of_cells as usize)?;
        // sort the pointer array so we can then parse cells working with smaller bounded
        // contiguous chunks of memory safely
        cell_pointer_array.sort();
        cell_pointer_array.push(value.len() as u16);

        // parse cells
        let cell_parser: fn(&[u8]) -> Result<CellType>;
        match header.page_type {
            BTreePageType::InteriorTable => cell_parser = parse_interior_table_cell,
            BTreePageType::LeafTable => cell_parser = parse_leaf_table_cell,
            BTreePageType::InteriorIndex => cell_parser = parse_interior_index_cell,
            BTreePageType::LeafIndex => cell_parser = parse_leaf_index_cell,
        }
        let cells = parse_cells(&value, cell_pointer_array, cell_parser)?;

        Ok(BTreePage {
            btree_type: header.page_type,
            page_header: header,
            cells,
        })
    }
}

fn parse_cell_pointer_array(data: &[u8], number_of_cells: usize) -> Result<(&[u8], Vec<u16>)> {
    let cell_pointer_array: Vec<u16> = Range {
        start: 0,
        end: number_of_cells,
    }
    .map(|i| u16::from_be_bytes(data[i..i + 2].try_into().unwrap()))
    .collect();
    Ok((&data[number_of_cells + 2..], cell_pointer_array))
}

fn parse_cells(
    page_data: &[u8],
    cell_pointer_array: Vec<u16>,
    cell_parser: fn(&[u8]) -> Result<CellType>,
) -> Result<Vec<CellType>> {
    Ok(cell_pointer_array
        .windows(2)
        .map(|[ps, pe]| {
            cell_parser(&page_data[*ps as usize..*pe as usize]).expect("error parsing cell")
        })
        .collect_vec())
}

// TODO: we will then need some query handling logic (maybe not in this module) that
// iterates across pages and indexes
