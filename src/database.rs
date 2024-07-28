use anyhow::{anyhow, bail, Result};
use page::btree::data::record::Record;
use page::btree::data::serial_types::Value;
use std::collections::HashMap;
use std::io::SeekFrom;
use std::ops::Index;

use crate::sql;

use self::header::{DatabaseHeader, DATABASE_HEADER_SIZE};
use self::io::SQLiteFile;
use self::page::btree::data::{CellType, TableLeafCell};
use self::page::{
    btree::page::BTreePage, freelist_page::FreeListPage, lockbyte_page::LockBytePage,
    payload_overflow_page::PayloadOverflowPage, pointer_map_page::PointerMapPage, Page, PageType,
};
use self::schema::{ObjectInformation, TableInformation};

pub mod header;
mod io;
pub mod page;
pub mod schema;

pub const TABLE_SCHEMA_ROOT_PAGE_NUMBER: u32 = 0;

pub struct Database {
    db_file: SQLiteFile,
    pub header: DatabaseHeader,
}

pub trait Filter {
    fn evaluate(&self, row: &Row) -> bool;
}

impl Filter for sql::SelectStatement {
    fn evaluate(&self, row: &Row) -> bool {
        self.conditions.iter().all(|condition| {
            // it's fine to unwrap here because the statement has been validated
            // at this point so we know the column exists
            let value = &row[&condition.column].unwrap();
            // this only accounts for string literals which is fine for now
            value == condition.value
        })
    }
}

impl Filter for bool {
    fn evaluate(&self, _row: &Row) -> bool {
        *self
    }
}

pub struct Row {
    pub rowid: u64,
    pub columns: Vec<Column>,
    pub hmap: HashMap<String, Value>,
}

fn cell_to_row(cell: CellType, table_information: &TableInformation) -> Result<Row> {
    match cell {
        CellType::TableLeaf(TableLeafCell { key, payload, .. }) => {
            let record = Record::try_from(payload)?;
            let columns: Vec<Column> = table_information
                .column_names
                .iter()
                .zip(record.values)
                .map(|(name, value)| Column {
                    name: name.to_string(),
                    value,
                })
                .collect();
            Ok(Row::new(key, columns))
        }
        _ => bail!("Expected TableLeafCell, got {:?}", cell),
    }
}

impl Row {
    pub fn new(rowid: u64, columns: Vec<Column>) -> Row {
        let mut hmap = HashMap::new();
        hmap.insert("rowid".to_string(), Value::Int64(rowid as i64));
        for column in &columns {
            hmap.insert(column.name, column.value);
        }
        Row {
            rowid,
            columns,
            hmap,
        }
    }
}

impl<K> Index<K> for Row
where
    K: std::hash::Hash + Eq,
{
    type Output = Option<Value>;

    fn index(&self, index: K) -> &Self::Output {
        self.hmap.get(&index)
    }
}

pub struct Column {
    pub name: String,
    pub value: page::btree::data::serial_types::Value,
}

fn page_number_to_offset(page_number: u64, page_size: u32) -> SeekFrom {
    if page_number == 1 {
        SeekFrom::Start(DATABASE_HEADER_SIZE as u64)
    } else {
        SeekFrom::Start((page_number - 1) * page_size as u64)
    }
}

impl Database {
    pub fn init_from_file(path: &str) -> Result<Database> {
        let mut db_file = SQLiteFile::new(path)?;
        let header_bytes = db_file.read_exact_at(DATABASE_HEADER_SIZE, SeekFrom::Start(0))?;
        let header = DatabaseHeader::try_from(header_bytes)?;
        Ok(Database { db_file, header })
    }

    fn read_page(&mut self, page_number: u64, page_type: PageType) -> Result<Page> {
        let offset: SeekFrom = page_number_to_offset(page_number, self.header.page_size);
        let page_contents: Vec<u8> = self
            .db_file
            .read_exact_at(self.header.page_size as usize, offset)?;
        match page_type {
            PageType::LockByte => {
                let page = LockBytePage::try_from(page_contents)?;
                Ok(Page::LockByte(page))
            }
            PageType::FreeList => {
                let page = FreeListPage::try_from(page_contents)?;
                Ok(Page::FreeList(page))
            }
            PageType::PayloadOverflow => {
                let page = PayloadOverflowPage::try_from(page_contents)?;
                Ok(Page::PayloadOverflow(page))
            }
            PageType::PointerMap => {
                let page = PointerMapPage::try_from(page_contents)?;
                Ok(Page::PointerMap(page))
            }
            PageType::BTree => {
                let page = BTreePage::try_from(page_contents)?;
                Ok(Page::BTree(page))
            }
        }
    }

    fn write_page(&mut self, page_contents: Vec<u8>, page_number: u64) -> Result<usize> {
        let offset: SeekFrom = SeekFrom::Start((page_number - 1) * self.header.page_size as u64);
        // TODO: write a copy of that page into the rollback journal before doing any modifications
        // (unless page is a freelist leaf page)
        self.db_file.write_at(page_contents, offset)
    }

    fn read_btree_page(&mut self, page_number: u64) -> Result<BTreePage> {
        let offset: SeekFrom = page_number_to_offset(page_number, self.header.page_size);
        let page_contents: Vec<u8> = self
            .db_file
            .read_exact_at(self.header.page_size as usize, offset)?;
        BTreePage::try_from(page_contents)
    }

    pub fn list_objects(&mut self) -> Result<Vec<ObjectInformation>> {
        if let Ok(page::Page::BTree(table_schema_page)) = self.read_page(0, PageType::BTree) {
            todo!()
        } else {
            todo!()
        }
    }

    pub fn list_tables(&mut self) -> Result<Vec<TableInformation>> {
        todo!()
    }

    fn traverse_btree_index(&mut self, root_page_number: u32, condition: dyn Filter) -> Result<()> {
        //let mut page_pointer_stack: Vec<u32> = vec![root_page_number];
        //while let Some(page_number) = page_pointer_stack.pop() {
        //    if let Ok(page::Page::BTree(btree_page)) =
        //        self.read_page(page_number as u64, page::PageType::BTree)
        //    {
        //        match btree_page {
        //            page::btree::page::BTreePage::IndexLeaf(header, cells) => {
        //                // do we really have the correct data structures for this?
        //                todo!("finish this");
        //            }
        //            page::btree::page::BTreePage::IndexInterior(header, cells) => {
        //                todo!("traverse interior indexes, pushing pointers to page numbers onto a stack etc")
        //            }
        //            _ => bail!(
        //                "Wrong BTreePageType; expected LeafIndex or InteriorIndex, got {:?}",
        //                btree_page.page_header.page_type
        //            ),
        //        }
        //    } else {
        //        bail!("Couldn't read page number {}", page_number);
        //    }
        //}
        todo!()
    }

    /// Traverse a BTree table and return all rows that satisfy the given condition.
    /// This function traverses the Btree in a depth-first manner, starting from the root page.
    pub fn traverse_btree_table(
        &mut self,
        root_page_number: u32,
        condition: dyn Filter,
        table_information: &TableInformation,
    ) -> Result<Vec<Row>> {
        let mut page_pointer_stack: Vec<u32> = vec![root_page_number];
        let mut rows: Vec<Row> = Vec::new();
        while let Some(page_number) = page_pointer_stack.pop() {
            let btree_page = self.read_btree_page(page_number)?;
            match btree_page {
                page::btree::page::BTreePage::TableLeaf(header, cells) => {
                    for cell in cells {
                        let row = cell_to_row(cell, table_information)?;
                        if condition.evaluate(&row) {
                            rows.push(row);
                        }
                    }
                }
                page::btree::page::BTreePage::TableInterior(header, cells) => {
                    for cell in cells {
                        page_pointer_stack.push(cell.left_child_pointer);
                    }
                }
                _ => bail!(
                    "Wrong BTreePageType; expected LeafTable or InteriorTable, got {:?}",
                    btree_page
                ),
            }
        }
    }
}
