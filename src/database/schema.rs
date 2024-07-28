use super::Row;
use anyhow::{bail, Result};

pub struct TableInformation {
    pub table_name: String,
    pub root_page: u64,
    pub ddl: Option<String>,
    pub column_names: Vec<String>,
}

impl TryFrom<ObjectInformation> for TableInformation {
    type Error = anyhow::Error;

    fn try_from(object_information: ObjectInformation) -> Result<Self> {
        match object_information.object_type {
            // TODO: parse column names from DDL
            ObjectType::Table => Ok(TableInformation {
                table_name: object_information.table_name.unwrap(),
                root_page: object_information.root_page,
                ddl: Some(object_information.object_ddl),
                column_names: vec![],
            }),
            _ => bail!("Object is not a table"),
        }
    }
}

pub struct ObjectInformation {
    pub object_type: ObjectType,
    pub object_name: String,
    pub table_name: Option<String>,
    pub root_page: u64,
    pub object_ddl: String,
}

impl From<Row> for ObjectInformation {
    fn from(row: Row) -> Self {
        let object_type = match row["type"] {
            "table" => ObjectType::Table,
            "index" => ObjectType::Index,
            "view" => ObjectType::View,
            "trigger" => ObjectType::Trigger,
            _ => panic!("Unknown object type"),
        };
        let object_name = row["name"];
        let table_name = row["tbl_name"];
        let root_page = row["rootpage"];
        let object_ddl = row["sql"];
        ObjectInformation {
            object_type,
            object_name,
            table_name,
            root_page,
            object_ddl,
        }
    }
}

pub enum ObjectType {
    Table,
    Index,
    View,
    Trigger,
}

pub const SCHEMA_TABLE_INFORMATION: TableInformation = TableInformation {
    table_name: String::from("schema"),
    root_page: 1,
    ddl: None,
    column_names: vec![
        String::from("type"),
        String::from("name"),
        String::from("tbl_name"),
        String::from("rootpage"),
        String::from("sql"),
    ],
};
