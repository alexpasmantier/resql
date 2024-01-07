use std::{fs::File, io::Read};

use anyhow::Result;

pub fn parse_varint(file: &mut File) -> Result<(u64, usize)> {
    let mut varint_bytes: Vec<u8> = Vec::new();
    loop {
        let mut buffer = [0; 1];
        file.read_exact(&mut buffer)?;
        varint_bytes.push(buffer[0]);
        if buffer[0] < 128 {
            break;
        }
    }
    let varint_size = varint_bytes.len();
    varint_bytes = varint_bytes.iter().map(|b| *b & 0b01111111).collect();

    varint_bytes.reverse();
    varint_bytes.resize(8, 0);
    varint_bytes.reverse();

    Ok((
        u64::from_be_bytes(varint_bytes.try_into().unwrap()),
        varint_size,
    ))
}
