#[macro_use] extern crate failure;

use std::collections::hash_map::HashMap;
use std::path::Path;
use std::result;
use std::io;
use std::fs::File;

pub type KvsResult<T> = result::Result<T, KvsError>;

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

    pub fn open<P: AsRef<Path>>(path: P) -> KvsResult<Self> {
        let file = File::open(path);
        let store = KvStore { map: HashMap::new() };
        Ok(store)
    }

    pub fn get(&self, key: &str) -> KvsResult<Option<String>> {
        if let Some(value) = self.map.get(key).map(|x| x.to_owned()) {
            Ok(Some(value))
        } else {
            Err(KvsError::Unknown)
        }
    }

    pub fn set(&mut self, key: String, value: String) -> KvsResult<()> {
        self.map.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> KvsResult<()> {
        self.map.remove(key);
        Ok(())
    }
}
