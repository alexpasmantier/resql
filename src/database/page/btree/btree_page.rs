use anyhow::Result;

use super::btree_page_header::{BTreePageHeader, BtreePageType};

pub struct BTreePage {
    btree_type: BtreePageType,
    page_header: BTreePageHeader,
    // TODO: add content-related fields
    // and maybe move btree-related parsing into here?
}

impl TryFrom<Vec<u8>> for BTreePage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        todo!()
    }
}
