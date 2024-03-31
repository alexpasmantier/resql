use self::{
    btree::page::BTreePage, freelist_page::FreeListPage, lockbyte_page::LockBytePage,
    payload_overflow_page::PayloadOverflowPage, pointer_map_page::PointerMapPage,
};

pub mod btree;
pub mod freelist_page;
pub mod lockbyte_page;
pub mod payload_overflow_page;
pub mod pointer_map_page;

/// Page types:
/// At any point in time, every page in the main database has a single use which is one of the following:
///
/// The lock-byte page
/// A freelist page
///     A freelist trunk page
///     A freelist leaf page
/// A b-tree page
///     A table b-tree interior page
///     A table b-tree leaf page
///     An index b-tree interior page
///     An index b-tree leaf page
/// A payload overflow page
/// A pointer map page
pub enum PageType {
    LockByte,
    FreeList,
    BTree,
    PayloadOverflow,
    PointerMap,
}

pub enum Page {
    LockByte(LockBytePage),
    FreeList(FreeListPage),
    BTree(BTreePage),
    PayloadOverflow(PayloadOverflowPage),
    PointerMap(PointerMapPage),
}
