use anyhow::anyhow;
use nom::AsChar;

const MAGIC_STRING: &str = "SQLite format 3\0";

#[derive(Debug)]
pub struct DatabaseHeader {
    // 16 bytes "SQLite format 3\000"
    pub magic_bytes: String,
    pub page_size: u16,
    pub file_format_write_version: u8,
    pub file_format_read_version: u8,
    // in bytes
    pub page_reserved_space: u8,
    // must be 64
    pub max_embedded_payload_fraction: u8,
    // must be 32
    pub min_embedded_payload_fraction: u8,
    // must be 32
    pub leaf_payload_fraction: u8,
    pub file_change_counter: u32,
    pub db_size_in_pages: u32,
    pub first_freelist_trunk_page_number: u32,
    pub number_of_freelist_pages: u32,
    pub schema_cookie: u32,
    pub schema_format_number: u32,
    pub default_page_cache_size: u32,
    // The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    pub largest_root_btree_page_number: u32,
    // A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    pub text_encoding: u32,
    pub user_version: u32,
    // True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    pub incremental_vacuum_mode: u32,
    pub application_id: u32,
    // Reserved for expansion. Must be zero.
    pub reserved_for_expansion: [u8; 20],
    pub version_valid_for: u32,
    pub sqlite_version_number: u32,
}

impl TryFrom<[u8; 100]> for DatabaseHeader {
    type Error = anyhow::Error;

    fn try_from(value: [u8; 100]) -> Result<Self, Self::Error> {
        let magic_bytes: String = value[..16].iter().map(|i| i.as_char()).collect();
        let page_size = u16::from_be_bytes(value[16..18].try_into().unwrap());
        let file_format_write_version = u8::from_be_bytes(value[18..19].try_into().unwrap());
        let file_format_read_version = u8::from_be_bytes(value[19..20].try_into().unwrap());
        let page_reserved_space = u8::from_be_bytes(value[20..21].try_into().unwrap());
        let max_embedded_payload_fraction = u8::from_be_bytes(value[21..22].try_into().unwrap());
        let min_embedded_payload_fraction = u8::from_be_bytes(value[22..23].try_into().unwrap());
        let leaf_payload_fraction = u8::from_be_bytes(value[23..24].try_into().unwrap());
        let file_change_counter = u32::from_be_bytes(value[24..28].try_into().unwrap());
        let db_size_in_pages = u32::from_be_bytes(value[28..32].try_into().unwrap());
        let first_freelist_trunk_page_number =
            u32::from_be_bytes(value[32..36].try_into().unwrap());
        let number_of_freelist_pages = u32::from_be_bytes(value[36..40].try_into().unwrap());
        let schema_cookie = u32::from_be_bytes(value[40..44].try_into().unwrap());
        let schema_format_number = u32::from_be_bytes(value[44..48].try_into().unwrap());
        let default_page_cache_size = u32::from_be_bytes(value[48..52].try_into().unwrap());
        let largest_root_btree_page_number = u32::from_be_bytes(value[52..56].try_into().unwrap());
        let text_encoding = u32::from_be_bytes(value[56..60].try_into().unwrap());
        let user_version = u32::from_be_bytes(value[60..64].try_into().unwrap());
        let incremental_vacuum_mode = u32::from_be_bytes(value[64..68].try_into().unwrap());
        let application_id = u32::from_be_bytes(value[68..72].try_into().unwrap());
        let reserved_for_expansion: [u8; 20] = value[72..92].try_into()?;
        let version_valid_for = u32::from_be_bytes(value[92..96].try_into().unwrap());
        let sqlite_version_number = u32::from_be_bytes(value[96..].try_into().unwrap());

        let header = DatabaseHeader {
            magic_bytes,
            page_size,
            file_format_write_version,
            file_format_read_version,
            page_reserved_space,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            db_size_in_pages,
            first_freelist_trunk_page_number,
            number_of_freelist_pages,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            largest_root_btree_page_number,
            text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            reserved_for_expansion,
            version_valid_for,
            sqlite_version_number,
        };

        if !validate_header(&header) {
            return Err(anyhow!("header is invalid"));
        } else {
            return Ok(header);
        }
    }
}

fn validate_header(header: &DatabaseHeader) -> bool {
    if header.magic_bytes != MAGIC_STRING
        || header.max_embedded_payload_fraction != 64
        || header.min_embedded_payload_fraction != 32
        || header.leaf_payload_fraction != 32
        || header.text_encoding > 3
        || header.reserved_for_expansion != [0; 20]
    {
        return false;
    }
    return true;
}
