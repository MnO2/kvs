use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
// TODO: add CRC32
pub(crate) struct Record {
    pub(crate) timestamp: u64,
    pub(crate) key: String,
    pub(crate) value: String,
}

impl Record {
    pub(crate) fn new() -> Self {
        Record {
            timestamp: 0,
            key: "".to_string(),
            value: "".to_string(),
        }
    }
}