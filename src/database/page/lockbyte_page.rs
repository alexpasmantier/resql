use anyhow::Result;

pub struct LockBytePage;

impl TryFrom<Vec<u8>> for LockBytePage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        todo!()
    }
}
