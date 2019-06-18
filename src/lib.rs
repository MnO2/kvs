use std::collections::hash_map::HashMap;
use std::path::Path;
use std::result;
use std::io;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    Seek
}

pub fn new_error(kind: ErrorKind) -> Error {
    Error(Box::new(kind))
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        new_error(ErrorKind::Io(err))
    }
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
            Err(new_error(ErrorKind::Seek))
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
