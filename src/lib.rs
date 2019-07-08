#[macro_use]
extern crate failure;

mod reader;
mod record;
mod writer;

use crate::record::Record;
use byteorder::{BigEndian, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::collections::hash_map::HashMap;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::result;
use tempfile::tempfile;

pub type Result<T> = result::Result<T, KvsError>;

#[derive(Fail, Debug)]
pub enum KvsError {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
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
    path: PathBuf,
    largest_segment_seq: u64,
}

impl KvStore {
    pub fn open(path: &Path) -> Result<Self> {
        if !path.exists() {
            fs::create_dir(path)?;
        }

        let mut list_of_files: Vec<(String, fs::File)> = Vec::new();
        let mut largest_segment_seq: u64 = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;

            if entry.path().as_path().extension() == Some(std::ffi::OsStr::new("bcd")) {
                let file_name = entry.file_name().into_string().unwrap();

                let file_path = path.join(&file_name);
                let file_path = file_path.as_path();

                let segment_seq: u64 = file_path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
                largest_segment_seq = std::cmp::max(largest_segment_seq, segment_seq);

                let f = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .append(true)
                    .open(&file_path)?;

                list_of_files.push((file_name, f));
            }
        }

        list_of_files.sort_by(|f1, f2| f1.0.cmp(&f2.0));

        let mut keydir: KeyDir = HashMap::new();
        let mut file_handles = Vec::new();
        let mut largest_timestamp: u64 = 0;

        if list_of_files.is_empty() {
            let file_name = format!("{:08}.bcd", 0);
            let file_path = path.join(&file_name);
            let file_path = file_path.as_path();

            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)?;
            file_handles.push(f);
        } else {
            //restore the keydir
            for (file_id, (file_name, file_to_read)) in list_of_files.into_iter().enumerate() {
                let buf_reader = io::BufReader::with_capacity(1024, &file_to_read);
                let mut reader = reader::Reader::new(buf_reader);
                let mut record = Record::new();

                let mut curr_offset = 0;
                let mut next_offset = 0;
                while reader.read_record(io::SeekFrom::Current(0), &mut record, &mut next_offset)? != false {
                    largest_timestamp = std::cmp::max(largest_timestamp, record.timestamp);

                    let keyinfo = KeyInfo {
                        file_id: file_id as u64,
                        record_pos: curr_offset,
                        timestamp: record.timestamp,
                    };

                    if record.tombstone == 1 {
                        keydir.remove(&record.key);
                    } else {
                        keydir.insert(record.key.clone(), keyinfo);
                    }

                    curr_offset = next_offset;
                }

                file_handles.push(file_to_read);
            }
        }

        let store = KvStore {
            counter: largest_timestamp,
            keydir: keydir,
            file_handles: file_handles,
            path: PathBuf::from(path),
            largest_segment_seq: largest_segment_seq,
        };

        Ok(store)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(keyinfo) = self.keydir.get(&key) {
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

    fn should_write_to_new_file(&self, f: &fs::File) -> io::Result<bool> {
        let metadata = f.metadata()?;
        Ok(metadata.len() > 1000)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut file_to_write = if self.should_write_to_new_file(self.file_handles.last().unwrap())? {
            let file_name = format!("{:08}.bcd", self.file_handles.len());
            let file_path = self.path.join(file_name);
            let file_path = file_path.as_path();

            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)?;
            self.file_handles.push(f);
            self.file_handles.last().unwrap()
        } else {
            self.file_handles.last().unwrap()
        };

        let file_offset = file_to_write.seek(io::SeekFrom::End(0))?;

        let new_record = Record {
            timestamp: self.counter,
            tombstone: 0,
            key: key.clone(),
            value: value,
        };

        let mut writer = writer::Writer::new(file_to_write);
        writer.write_record(&new_record);

        let file_id = (self.file_handles.len() - 1) as u64;

        let keyinfo = KeyInfo {
            file_id: file_id,
            record_pos: file_offset,
            timestamp: self.counter,
        };

        self.keydir.insert(key, keyinfo);

        self.counter += 1;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.keydir.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let mut file_to_write = if self.should_write_to_new_file(self.file_handles.last().unwrap())? {
            let active_file_name = format!("{}.bcd", self.file_handles.len());
            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(&active_file_name)?;
            self.file_handles.push(f);
            self.file_handles.last().unwrap()
        } else {
            self.file_handles.last().unwrap()
        };

        let file_offset = file_to_write.seek(io::SeekFrom::End(0))?;

        let new_record = Record {
            timestamp: self.counter,
            tombstone: 1,
            key: key.clone(),
            value: "".to_string(),
        };

        let mut writer = writer::Writer::new(file_to_write);
        writer.write_record(&new_record);

        self.keydir.remove(&key);

        self.counter += 1;
        Ok(())
    }

    fn compaction(&mut self, to_be_compacted: &[fs::File]) -> Result<fs::File> {
        let mut dest_file = tempfile()?;
        let file_id = self.file_handles.len() as u64;

        for source_file in to_be_compacted {
            let buf_reader = io::BufReader::with_capacity(1024, source_file);
            let mut reader = reader::Reader::new(buf_reader);
            let mut record = Record::new();

            let mut curr_offset = 0;
            let mut next_offset = 0;
            while reader.read_record(io::SeekFrom::Current(0), &mut record, &mut next_offset)? != false {
                if let Some(keyinfo) = self.keydir.get(&record.key) {
                    if keyinfo.timestamp == record.timestamp {
                        let file_offset = dest_file.seek(io::SeekFrom::End(0))?;
                        let mut writer = writer::Writer::new(&dest_file);
                        writer.write_record(&record)?;

                        let new_key_info = KeyInfo {
                            file_id: file_id,
                            record_pos: file_offset,
                            timestamp: keyinfo.timestamp,
                        };

                        self.keydir.insert(record.key.clone(), new_key_info);
                    }
                }

                curr_offset = next_offset;
            }
        }

        Ok(dest_file)
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        for f in self.file_handles.iter() {
            f.sync_data();
        }
    }
}
