use anyhow::Result;
use std::io::SeekFrom;

use self::header::{DatabaseHeader, DATABASE_HEADER_SIZE};
use self::io::SQLiteFile;
use self::page::{
    btree::page::BTreePage, freelist_page::FreeListPage, lockbyte_page::LockBytePage,
    payload_overflow_page::PayloadOverflowPage, pointer_map_page::PointerMapPage, Page, PageType,
};

pub mod header;
mod io;
pub mod page;

pub struct Database {
    db_file: SQLiteFile,
    header: DatabaseHeader,
}

impl Database {
    pub fn init_from_file(path: &str) -> Result<Database> {
        let mut db_file = SQLiteFile::new(path)?;
        let header_bytes = db_file.read_exact_at(DATABASE_HEADER_SIZE, SeekFrom::Start(0))?;
        let header = DatabaseHeader::try_from(header_bytes)?;
        Ok(Database { db_file, header })
    }

    fn read_page(&mut self, page_number: usize, page_type: PageType) -> Result<Page> {
        let offset: SeekFrom;
        if page_number == 1 {
            offset = SeekFrom::Start(DATABASE_HEADER_SIZE as u64);
        } else {
            offset = SeekFrom::Start((page_number as u64 - 1) * self.header.page_size as u64);
        }
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

    fn write_page(&mut self, page_contents: Vec<u8>, page_number: usize) -> Result<usize> {
        let offset: SeekFrom =
            SeekFrom::Start((page_number as u64 - 1) * self.header.page_size as u64);
        // TODO: write a copy of that page into the rollback journal before doing any modifications
        // (unless page is a freelist leaf page)
        self.db_file.write_at(page_contents, offset)
    }
}
