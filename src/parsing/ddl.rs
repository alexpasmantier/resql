use anyhow::Result;
use regex::Regex;

pub fn parse_column_names_from_ddl(ddl: &str) -> Result<Vec<String>> {
    // CREATE TABLE oranges\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tdescription text\n)
    // CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)\r\
    let clean_ddl = ddl.replace("\n", " ").replace("\t", " ");
    let col_declaration_segment_re = Regex::new(r"\((.*)\)").unwrap();
    let col_name_re = Regex::new(r"\s*(\w+)\s+[\w\s]*").unwrap();
    let col_segment_captures = col_declaration_segment_re.captures(&clean_ddl).unwrap();
    let col_segment = col_segment_captures.get(1).unwrap().as_str();
    Ok(col_name_re
        .captures_iter(col_segment)
        .map(|c| c[1].to_string())
        .collect())
}
