use anyhow::Result;

pub struct PayloadOverflowPage;

impl TryFrom<Vec<u8>> for PayloadOverflowPage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        todo!()
    }
}
