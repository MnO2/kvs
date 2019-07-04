#[macro_use] extern crate failure;

use std::collections::hash_map::HashMap;
use std::path::Path;
use std::result;
use std::io;
use std::fs;
use serde::{Serialize, Deserialize};
use rmp_serde::{Deserializer, Serializer};
use std::io::prelude::*;

pub type KvsResult<T> = result::Result<T, KvsError>;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "data folder not found")]
    DataFolderNotFound,
    #[fail(display = "unknown error")]
    Unknown,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

type KeyDir = HashMap<Key, KeyInfo>;
type Key = String;

struct KeyInfo {
    file_id: u64,
    record_size: u64,
    record_pos: u64,
    timestamp: u64
}

#[derive(Serialize, Deserialize, Debug)]
// TODO: add CRC32
struct Record {
    timestamp: u64,
    key: String,
    value: String,
}

pub struct KvStore {
    counter: u64,
    keydir: KeyDir,
    file_handles: Vec<fs::File>,
}

impl KvStore {
    pub fn open(path: &Path) -> KvsResult<Self> {
        if !path.is_dir() {
            return Err(KvsError::DataFolderNotFound);
        }

        let mut active_file_handle: Option<fs::File> = None;
        let active_file_name = path.join("active.data");
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_name() == "active.data" {
                active_file_handle = Some(fs::File::open(&active_file_name)?);
            }
        }

        if active_file_handle.is_none() {
            active_file_handle = Some(fs::File::create(&active_file_name)?);
        }

        let mut file_handles = Vec::new();
        file_handles.push(active_file_handle.unwrap());

        let store = KvStore { 
            counter: 0,  //FIXME: read from file
            keydir: HashMap::new(),
            file_handles: file_handles,
        };

        Ok(store)
    }

    pub fn get(&mut self, key: &str) -> KvsResult<Option<String>> {
        if let Some(keyinfo) = self.keydir.get(key) {
            let mut buf: Vec<u8> = Vec::with_capacity(keyinfo.record_size as usize);

            self.file_handles[keyinfo.file_id as usize].seek(io::SeekFrom::Start(keyinfo.record_pos))?;
            self.file_handles[keyinfo.file_id as usize].read(&mut buf);

            let mut de = Deserializer::new(&buf[..]);
            let record: Record = Deserialize::deserialize(&mut de).unwrap();
            Ok(Some(record.value.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn set(&mut self, key: String, value: String) -> KvsResult<()> {
        let mut buf = Vec::new();

        let new_record = Record {
            timestamp: self.counter,
            key: key.clone(),
            value: value,
        };

        new_record.serialize(&mut Serializer::new(&mut buf)).unwrap();

        let file_offset = self.file_handles[0].seek(io::SeekFrom::End(0))?;
        self.file_handles[0].write(&buf)?;

        let record_size: u64 = buf.len() as u64;

        let keyinfo = KeyInfo {
            file_id: 0,
            record_size: record_size,
            record_pos: file_offset,
            timestamp: self.counter 
        };

        self.keydir.insert(key, keyinfo);

        self.counter += 1;
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> KvsResult<()> {
        unimplemented!();
    }
}
