#[macro_use] extern crate failure;

use std::collections::hash_map::HashMap;
use std::path::Path;
use std::result;
use std::io;

pub type Result<T> = result::Result<T, KvsError>;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "unknown error")]
    Unknown,
}

pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore { map: HashMap::new() }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = KvStore { map: HashMap::new() };
        Ok(store)
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(value) = self.map.get(&key).map(|x| x.to_owned()) {
            Ok(Some(value))
        } else {
            Err(KvsError::Unknown)
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        Ok(())
    }
}
