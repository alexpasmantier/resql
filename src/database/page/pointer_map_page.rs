use anyhow::Result;

pub struct PointerMapPage;

impl TryFrom<Vec<u8>> for PointerMapPage {
    type Error = anyhow::Error;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        todo!()
    }
}
