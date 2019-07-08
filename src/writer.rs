use crate::record::Record;
use byteorder::{BigEndian, WriteBytesExt};
use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Debug)]
pub(crate) struct Writer<W: io::Write> {
    wtr: W,
}


impl<W: io::Write> Writer<W> {
    pub fn new(wtr: W) -> Writer<W> {
        Writer {
            wtr,
        }
    }

    pub fn write_record(&mut self, record: &Record) -> io::Result<()> {
        let mut buf_record = Vec::new();

        record.serialize(&mut Serializer::new(&mut buf_record)).unwrap();

        let record_len: u64 = buf_record.len() as u64;
        let mut buf = Vec::new();
        buf.write_u64::<BigEndian>(record_len).unwrap();

        self.wtr.write(&buf)?;
        self.wtr.write(&buf_record)?;

        Ok(())
    }
}