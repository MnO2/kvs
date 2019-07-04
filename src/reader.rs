use crate::record::Record;
use byteorder::{BigEndian, ReadBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::io;
use std::io::prelude::*;
use std::io::BufRead;
use std::io::Cursor;

#[derive(Debug)]
pub(crate) struct Reader<R> {
    rdr: io::BufReader<R>,
}

impl<R: io::Read + io::Seek> Reader<R> {
    pub(crate) fn new(rdr: R) -> Reader<R> {
        Reader {
            rdr: io::BufReader::with_capacity(100, rdr),
        }
    }

    pub(crate) fn read_record(
        &mut self,
        seek_from: io::SeekFrom,
        record: &mut Record,
        next_offset: &mut u64,
    ) -> io::Result<bool> {
        self.rdr.seek(seek_from)?;

        let mut buf: [u8; 8] = [0; 8];
        let num_of_bytes = self.rdr.read(&mut buf)?;
        dbg!(&num_of_bytes);
        if num_of_bytes == 0 {
            return Ok(false);
        }

        let mut cursor = Cursor::new(&buf);
        let record_size: usize = cursor.read_u64::<BigEndian>().unwrap() as usize;
        dbg!(&record_size);

        let mut buf = Vec::new();
        buf.resize(record_size, 0);

        let num_of_bytes = self.rdr.read(&mut buf)?;
        dbg!(&num_of_bytes);
        if num_of_bytes == 0 {
            return Ok(false);
        }

        let mut de = Deserializer::new(&buf[..]);
        let deseralized_record: Record = Deserialize::deserialize(&mut de).unwrap();

        record.timestamp = deseralized_record.timestamp;
        record.key = deseralized_record.key;
        record.value = deseralized_record.value;

        *next_offset = self.rdr.seek(io::SeekFrom::Current(0))?;

        Ok(true)
    }
}
