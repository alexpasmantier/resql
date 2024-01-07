use anyhow::{anyhow, Result};

pub mod dbinfo;
pub mod tables;

pub enum Command {
    DBInfo,
    Tables,
}

impl TryFrom<&str> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            ".dbinfo" => Ok(Command::DBInfo),
            ".tables" => Ok(Command::Tables),
            c => Err(anyhow!("Unrecognized command {}", c)),
        }
    }
}
