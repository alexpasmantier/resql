use anyhow::{bail, Result};
use nom::{error::Error, multi::fold_many1, IResult};

use crate::{database::page::btree::data::Payload, parsing::utils::take_varint};

use super::serial_types::{parse_value, SerialType, Value};

pub struct Record {
    serial_types: Vec<SerialType>,
    pub values: Vec<Value>,
}

impl TryFrom<Payload> for Record {
    type Error = anyhow::Error;

    fn try_from(value: Payload) -> Result<Self> {
        parse_record(&value.content)
    }
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
            let parsers: Vec<_> = serial_types
                .iter()
                .map(|st| curried_parse_value(st))
                .collect();
            let (rest, values) =
                dynamic_parser_sequence(&parsers)(&payload[header_size as usize..])?;
            Ok(Record {
                serial_types,
                values,
            })
        }
        Err(E) => Err(E),
    }
}

fn curried_parse_value<'a>(
    serial_type: &SerialType,
) -> impl Fn(&'a [u8]) -> IResult<&'a [u8], Value> + '_ {
    |data| parse_value(data, serial_type)
}

fn dynamic_parser_sequence<'a, O, P>(
    parsers: &'a [P],
) -> impl Fn(&'a [u8]) -> IResult<&'a [u8], Vec<O>> + 'a
where
    P: Fn(&'a [u8]) -> IResult<&'a [u8], O> + 'a,
{
    move |input: &'a [u8]| {
        let mut results = Vec::new();
        let mut current_input = input;

        for parser in parsers {
            match parser(current_input) {
                Ok((next_input, result)) => {
                    current_input = next_input;
                    results.push(result);
                }
                Err(e) => return Err(e),
            }
        }

        Ok((current_input, results))
    }
}
