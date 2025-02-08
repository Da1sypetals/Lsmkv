use bytes::Bytes;

#[derive(Clone)]
pub enum Record {
    Value(Bytes),
    Tomb,
}

impl Record {
    pub fn as_search_result(self) -> Option<Bytes> {
        match self {
            Record::Value(bytes) => Some(bytes),
            Record::Tomb => None,
        }
    }
}

impl Into<Record> for Bytes {
    fn into(self) -> Record {
        Record::Value(self)
    }
}

impl Into<Record> for &[u8] {
    fn into(self) -> Record {
        Record::Value(Bytes::copy_from_slice(self))
    }
}
