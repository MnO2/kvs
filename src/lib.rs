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
    file_id: String,
    record_pos: u64,
    timestamp: u64,
}

pub struct KvStore {
    counter: u64,
    keydir: KeyDir,
    file_handles: HashMap<String, fs::File>,
    file_names: Vec<String>,
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
        let mut file_handles = HashMap::new();
        let mut file_names = Vec::new();
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
            file_names.push(file_name.clone());
            file_handles.insert(file_name, f);
        } else {
            //restore the keydir
            for (file_name, file_to_read) in list_of_files.into_iter() {
                let buf_reader = io::BufReader::with_capacity(1024, &file_to_read);
                let mut reader = reader::Reader::new(buf_reader);
                let mut record = Record::new();

                let mut curr_offset = 0;
                let mut next_offset = 0;
                while reader.read_record(io::SeekFrom::Current(0), &mut record, &mut next_offset)? != false {
                    largest_timestamp = std::cmp::max(largest_timestamp, record.timestamp);

                    let keyinfo = KeyInfo {
                        file_id: file_name.clone(),
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

                file_names.push(file_name.clone());
                file_handles.insert(file_name, file_to_read);
            }
        }

        let store = KvStore {
            counter: largest_timestamp + 1,
            keydir: keydir,
            file_handles: file_handles,
            file_names: file_names,
            path: PathBuf::from(path),
            largest_segment_seq: largest_segment_seq,
        };

        Ok(store)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(keyinfo) = self.keydir.get(&key) {
            let buf_reader = io::BufReader::with_capacity(1024, self.file_handles.get(&keyinfo.file_id).unwrap());
            let mut reader = reader::Reader::new(buf_reader);
            let mut record = Record::new();
            let mut next_offset = 0;

            reader.read_record(io::SeekFrom::Start(keyinfo.record_pos), &mut record, &mut next_offset);

            Ok(Some(record.value.clone()))
        } else {
            Ok(None)
        }
    }

    fn should_write_to_new_file(&self, file_name: &String) -> io::Result<bool> {
        let f = self.file_handles.get(file_name).unwrap();
        let metadata = f.metadata()?;
        Ok(metadata.len() > 1000)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let (mut file_id, mut file_to_write) = if self.should_write_to_new_file(self.file_names.last().unwrap())? {
            self.largest_segment_seq += 1;
            let file_name = format!("{:08}.bcd", self.largest_segment_seq);
            let file_path = self.path.join(&file_name);
            let file_path = file_path.as_path();

            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)?;
            self.file_handles.insert(file_name.clone(), f);
            self.file_names.push(file_name.clone());
            (file_name.clone(), self.file_handles.get(&file_name).unwrap())
        } else {
            (
                self.file_names.last().unwrap().clone(),
                self.file_handles.get(self.file_names.last().unwrap()).unwrap(),
            )
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
        let keyinfo = KeyInfo {
            file_id: file_id.clone(),
            record_pos: file_offset,
            timestamp: self.counter,
        };

        self.keydir.insert(key, keyinfo);

        self.counter += 1;

        if self.file_names.len() > 6 {
            let range: Vec<usize> = (0..self.file_names.len()).collect();
            self.compaction(&range)?;
        }

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.keydir.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let (_, mut file_to_write) = if self.should_write_to_new_file(self.file_names.last().unwrap())? {
            let file_name = format!("{:08}.bcd", self.largest_segment_seq + 1);
            let file_path = self.path.join(&file_name);
            let file_path = file_path.as_path();

            let f = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)?;
            self.file_handles.insert(file_name.clone(), f);
            self.file_names.push(file_name.clone());
            (file_name.clone(), self.file_handles.get(&file_name).unwrap())
        } else {
            (
                self.file_names.last().unwrap().clone(),
                self.file_handles.get(self.file_names.last().unwrap()).unwrap(),
            )
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

    fn compaction(&mut self, to_be_compacted: &[usize]) -> Result<()> {
        let mut list_of_merged_path: Vec<PathBuf> = Vec::new();
        let mut list_of_merge_file_names: Vec<String> = Vec::new();

        for &i in to_be_compacted.iter() {
            let file_name = &self.file_names[i];
            let file_path = self.path.join(file_name);
            let file_path = file_path.as_path();

            let mut segment_seq_str: String = file_path.file_stem().unwrap().to_str().unwrap().to_string();
            segment_seq_str.push('1');
            segment_seq_str.push_str(".merge");
            let merged_file_path: PathBuf = self.path.join(&segment_seq_str);
            list_of_merged_path.push(merged_file_path);
            list_of_merge_file_names.push(segment_seq_str);
        }

        let mut curr_idx: usize = 0;
        let mut merged_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&list_of_merged_path[curr_idx])?;

        let mut list_of_merge_files = Vec::new();

        for &i in to_be_compacted.iter() {
            let source_file_name = &self.file_names[i];
            let mut rdr = self.file_handles.get(source_file_name).unwrap();
            rdr.seek(io::SeekFrom::Start(0));

            let buf_reader = io::BufReader::with_capacity(1024, rdr);
            let mut reader = reader::Reader::new(buf_reader);
            let mut record = Record::new();

            let mut curr_offset = 0;
            let mut next_offset = 0;
            while reader.read_record(io::SeekFrom::Current(0), &mut record, &mut next_offset)? != false {
                if let Some(keyinfo) = self.keydir.get(&record.key) {
                    if keyinfo.timestamp == record.timestamp {
                        let file_offset = merged_file.seek(io::SeekFrom::End(0))?;
                        let mut writer = writer::Writer::new(&merged_file);
                        writer.write_record(&record)?;

                        let new_key_info = KeyInfo {
                            file_id: list_of_merge_file_names[curr_idx].clone(),
                            record_pos: file_offset,
                            timestamp: keyinfo.timestamp,
                        };

                        self.keydir.insert(record.key.clone(), new_key_info);

                        if curr_idx < list_of_merged_path.len()-1 {
                            let metadata = merged_file.metadata()?;
                            if metadata.len() > 1000 {
                                list_of_merge_files.push(merged_file);
                                curr_idx += 1;

                                merged_file = fs::OpenOptions::new()
                                    .read(true)
                                    .write(true)
                                    .append(true)
                                    .create(true)
                                    .open(&list_of_merged_path[curr_idx])?;
                            }
                        }
                    }
                }

                curr_offset = next_offset;
            }
        }

        list_of_merge_files.push(merged_file);
        curr_idx += 1;

        let mut j = 0;
        for (k, merged_file) in list_of_merge_files.into_iter().enumerate() {
            let file_name = &self.file_names[to_be_compacted[j]];
            let file_path = self.path.join(file_name);
            let file_path = file_path.as_path();

            fs::remove_file(file_path)?;
            fs::rename(&list_of_merged_path[k], file_path)?;

            self.file_handles.insert(file_name.clone(), merged_file);
            j += 1;
        }

        let jj = j;
        while j < to_be_compacted.len() {
            let idx = to_be_compacted[j];

            let file_name = &self.file_names[idx];
            let file_path = self.path.join(file_name);
            let file_path = file_path.as_path();

            self.file_handles.remove(file_name);
            fs::remove_file(file_path)?;

            j += 1;
        }

        if jj < to_be_compacted.len() {
            let drain_start = to_be_compacted[jj];
            self.file_names.drain(drain_start..);
        }

        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        for f in self.file_handles.iter() {
            f.1.sync_data();
        }
    }
}
