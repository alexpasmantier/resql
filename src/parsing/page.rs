use std::fs::File;
use std::io::Read;

// Page types:
// At any point in time, every page in the main database has a single use which is one of the following:
//
// The lock-byte page
// A freelist page
//     A freelist trunk page
//     A freelist leaf page
// A b-tree page
//     A table b-tree interior page
//     A table b-tree leaf page
//     An index b-tree interior page
//     An index b-tree leaf page
// A payload overflow page
// A pointer map page

pub struct Page {}

pub fn parse_page(file: &mut File, page_offset: u64) -> Page {
    todo!()
}
