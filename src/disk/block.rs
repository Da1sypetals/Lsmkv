use bytes::Bytes;
use std::{
    fs::File,
    io::{BufWriter, Seek, SeekFrom, Write},
};

use crate::memory::record::Record;

pub struct DataBlockWriter<'a> {
    len: usize,
    size: u16,
    pub(crate) buf_writer: BufWriter<&'a File>,
}

impl<'a> DataBlockWriter<'a> {
    pub fn new(file: &'a mut File) -> Self {
        file.seek(SeekFrom::End(0)).unwrap();
        Self {
            len: 0,
            size: 0,
            buf_writer: BufWriter::new(file),
        }
    }

    /// Returns the number of bytes written
    pub fn append(&mut self, key: &Bytes, value: &Record) -> u16 {
        let kv_size = match value {
            Record::Value(value) => {
                let key_size = key.len() as u16;
                let value_size = value.len() as u16;

                // for Value
                self.buf_writer.write_all(&[0u8]).unwrap();

                self.buf_writer.write_all(&key_size.to_le_bytes()).unwrap();
                self.buf_writer.write_all(&key).unwrap();

                self.buf_writer
                    .write_all(&value_size.to_le_bytes())
                    .unwrap();
                self.buf_writer.write_all(&value).unwrap();

                // type keysize key valuesize value
                1 + 2 + key_size + 2 + value_size
            }
            Record::Tomb => {
                // for Tomb
                self.buf_writer.write_all(&[1u8]).unwrap();
                let key_size = key.len() as u16;

                self.buf_writer.write_all(&key_size.to_le_bytes()).unwrap();
                self.buf_writer.write_all(&key).unwrap();

                // type key_size key
                1 + 2 + key_size
            }
        };

        self.len += 1;
        self.size += kv_size;
        kv_size
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn size(&self) -> u16 {
        self.size as u16
    }
}

impl<'a> Drop for DataBlockWriter<'a> {
    fn drop(&mut self) {
        self.buf_writer.flush().unwrap();
    }
}
