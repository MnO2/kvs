#[macro_use]
extern crate failure;

mod reader;
mod record;

use crate::record::Record;
use byteorder::{BigEndian, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::HashMap;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::result;

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

#[derive(Debug)]
struct KeyInfo {
    file_id: u64,
    record_pos: u64,
    timestamp: u64,
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

        let mut keydir: KeyDir = HashMap::new();
        let mut file_handles = Vec::new();
        if active_file_handle.is_none() {
            active_file_handle = Some(fs::File::create(&active_file_name)?);
            file_handles.push(active_file_handle.unwrap());
        } else {
            //restore the keydir
            file_handles.push(active_file_handle.unwrap());

            let buf_reader = io::BufReader::with_capacity(1024, &file_handles[0]);
            let mut reader = reader::Reader::new(buf_reader);
            let mut record = Record::new();

            let mut curr_offset = 0;
            let mut next_offset = 0;
            while reader.read_record(io::SeekFrom::Current(0), &mut record, &mut next_offset)? != false {
                let keyinfo = KeyInfo {
                    file_id: 0,
                    record_pos: curr_offset,
                    timestamp: record.timestamp,
                };

                dbg!(&keyinfo);
                keydir.insert(record.key.clone(), keyinfo);
                curr_offset = next_offset;
            }
        }

        let store = KvStore {
            counter: 0, //FIXME: read from file
            keydir: keydir,
            file_handles: file_handles,
        };

        Ok(store)
    }

    pub fn get(&mut self, key: &str) -> KvsResult<Option<String>> {
        if let Some(keyinfo) = self.keydir.get(key) {
            let buf_reader = io::BufReader::with_capacity(1024, &self.file_handles[keyinfo.file_id as usize]);
            let mut reader = reader::Reader::new(buf_reader);
            let mut record = Record::new();
            let mut next_offset = 0;

            reader.read_record(io::SeekFrom::Start(keyinfo.record_pos), &mut record, &mut next_offset);

            Ok(Some(record.value.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn set(&mut self, key: String, value: String) -> KvsResult<()> {
        let file_offset = self.file_handles[0].seek(io::SeekFrom::End(0))?;

        let mut buf_record = Vec::new();
        let new_record = Record {
            timestamp: self.counter,
            key: key.clone(),
            value: value,
        };
        new_record.serialize(&mut Serializer::new(&mut buf_record)).unwrap();

        let record_len: u64 = buf_record.len() as u64;
        let mut buf = Vec::new();
        buf.write_u64::<BigEndian>(record_len).unwrap();

        self.file_handles[0].write(&buf)?;
        self.file_handles[0].write(&buf_record)?;

        let keyinfo = KeyInfo {
            file_id: 0,
            record_pos: file_offset,
            timestamp: self.counter,
        };

        self.keydir.insert(key, keyinfo);

        self.counter += 1;
        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> KvsResult<()> {
        unimplemented!();
    }
}
