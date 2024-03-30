use anyhow::{bail, Result};
use nom::{
    error::Error,
    multi::{fold_many1, many1},
};

use crate::parsing::utils::take_varint;

use super::serial_types::{SerialType, Value};

struct Record {
    serial_types: Vec<SerialType>,
    values: Vec<Value>,
}

fn parse_record(payload: &[u8]) -> Result<Record> {
    // parse header size
    let (rest, header_size) = take_varint::<Error<&[u8]>>(payload)?;
    let varint_size = payload.len() - rest.len();
    // parse serial types
    let remaining_header_size = header_size as usize - varint_size;
    let (remaining_header_bytes, coll_maybe_serial_types) = fold_many1(
        take_varint::<Error<&[u8]>>,
        Vec::new,
        |mut acc: Vec<Result<SerialType>>, varint| {
            acc.push(SerialType::try_from(varint));
            acc
        },
    )(&rest[..remaining_header_size])?;
    let maybe_serial_types: Result<Vec<SerialType>> = coll_maybe_serial_types.into_iter().collect();
    // check the entire header was consumed
    if remaining_header_bytes.len() > 0 {
        bail!("Malformed record header")
    }
    // if serial types parsing succeeded, parse corresponding values
    match maybe_serial_types {
        Ok(serial_types) => {
            // TODO: maybe the tuple combinator here, built with partial applications of
            // `parse_value(x, st)` for st in serial_types
            todo!()
            // let (rest, values) =
        }
        Err(E) => Err(E),
    }
}
