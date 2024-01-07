use anyhow::anyhow;

// Serial Type	Content Size	Meaning
// 0	        0	Value is a NULL.
// 1	        1	Value is an 8-bit twos-complement integer.
// 2	        2	Value is a big-endian 16-bit twos-complement integer.
// 3	        3	Value is a big-endian 24-bit twos-complement integer.
// 4	        4	Value is a big-endian 32-bit twos-complement integer.
// 5	        6	Value is a big-endian 48-bit twos-complement integer.
// 6	        8	Value is a big-endian 64-bit twos-complement integer.
// 7	        8	Value is a big-endian IEEE 754-2008 64-bit floating point number.
// 8	        0	Value is the integer 0. (Only available for schema format 4 and higher.)
// 9	        0	Value is the integer 1. (Only available for schema format 4 and higher.)
// 10,11	    variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
// N≥12 and even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
// N≥13 and odd	    (N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
#[derive(Debug)]
pub enum SerialType {
    Null,
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    IntZero,
    IntOne,
    Reserved,
    Blob { length: u64, content: Vec<u8> },
    String { length: u64, content: String },
}

impl TryFrom<u64> for SerialType {
    type Error = anyhow::Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(SerialType::Null),
            1 => Ok(SerialType::Int8(0)),
            2 => Ok(SerialType::Int16(0)),
            3 => Ok(SerialType::Int24(0)),
            4 => Ok(SerialType::Int32(0)),
            5 => Ok(SerialType::Int48(0)),
            6 => Ok(SerialType::Int64(0)),
            7 => Ok(SerialType::Float64(0.0)),
            8 => Ok(SerialType::IntZero),
            9 => Ok(SerialType::IntOne),
            10..=11 => Ok(SerialType::Reserved),
            v if v % 2 == 0 => Ok(SerialType::Blob {
                length: (v - 12) / 2,
                content: Vec::new(),
            }),
            v if v % 2 == 1 => Ok(SerialType::String {
                length: (v - 13) / 2,
                content: String::new(),
            }),
            v => Err(anyhow!("Unrecognized serial type value {}", v)),
        }
    }
}
