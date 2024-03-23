use nom::{bytes::complete::take, error::ParseError, Err::*, Needed::Unknown};

/// A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement
/// integers that uses less space for small positive values.
///
/// A varint is between 1 and 9 bytes in length.
///
/// The varint consists of either zero or more bytes which have the high-order bit set followed by
/// a single byte with the high-order bit clear, or nine bytes, whichever is shorter.
///
/// The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used
/// to reconstruct the 64-bit twos-complement integer.
///
/// Varints are big-endian: bits taken from the earlier byte of the varint are more significant
/// than bits taken from the later bytes.
pub fn take_varint<'a, E>(input: &'a [u8]) -> nom::IResult<&'a [u8], u64, E>
where
    E: ParseError<&'a [u8]>,
{
    let mut result: u64 = 0;
    let mut count: usize = 0;
    let mut remainder = input;
    // take while most significant bit is 1
    loop {
        let byte = match take::<usize, &[u8], ()>(1)(remainder) {
            Ok((rest, bytes)) => {
                remainder = rest;
                bytes[0]
            }
            Err(_) => return Err(Incomplete(Unknown)),
        };
        // mask byte to only take the 7 least significant bits and add to result after
        // shifting left by `(8 - count - 1)*7 + 8` (the idea here being we assume the varint is of its full 8
        // bytes length (8*7bits + 8bits) which we'll re-shift right afterwards if it's shorter)
        if count < 8 {
            result += ((byte & 127) as u64) << (8 - count - 1) * 7 + 8;
        } else {
            // (the 9th varint byte uses all of its bits for value)
            result += byte as u64;
        }
        count += 1;
        // if the varint reached its full length
        if count == 9 {
            return Ok((remainder, result));
        } else if (byte >> 7) == 0 {
            // e.g. if most significant bit is 0 and varint is not of full length, re-shift
            // accordingly
            return Ok((remainder, result >> (8 - count) * 7 + 8));
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn parse_varint_simple() {
        let result = super::take_varint::<()>(&[0x0b, 0x01, 0x02, 0x03]);
        assert_eq!(
            result,
            Ok((b"\x01\x02\x03" as &[u8], 11)),
            "Not equal: expected {}, got {:?}",
            11,
            result,
        );
    }

    #[test]
    fn parse_varint_twobyte() {
        let result = super::take_varint::<()>(&[0x84, 0x02, 0x04, 0x05, 0x06]);
        assert_eq!(
            result,
            Ok((b"\x04\x05\x06" as &[u8], 514)),
            "Not equal: expected {}, got {:?}",
            514,
            result,
        );
    }

    #[test]
    fn parse_varint_full() {
        let result =
            super::take_varint::<()>(&[0x84, 0x97, 0x8a, 0xa2, 0xc0, 0x89, 0x85, 0xdd, 0xff]);
        assert_eq!(
            result,
            Ok((b"" as &[u8], 602446781950909951)),
            "Not equal: expected {}, got {:?}",
            602446781950909951,
            result,
        );
    }
}
