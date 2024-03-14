use anyhow::Result;

pub struct FreeListPage;

impl TryFrom<Vec<u8>> for FreeListPage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        todo!()
    }
}
