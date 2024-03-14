use anyhow::Result;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

pub struct SQLiteFile {
    file: File,
}

impl SQLiteFile {
    pub fn new(path: &str) -> Result<SQLiteFile> {
        let file = File::open(path)?;
        Ok(SQLiteFile { file })
    }

    pub fn read_exact_at(&mut self, n_bytes: usize, offset: SeekFrom) -> Result<Vec<u8>> {
        let mut buf = vec![0; n_bytes];
        self.file.seek(offset)?;
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn write_at(&mut self, buf: Vec<u8>, offset: SeekFrom) -> Result<usize> {
        self.file.seek(offset)?;
        Ok(self.file.write(&buf)?)
    }
}
