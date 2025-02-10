pub mod read;
pub mod write;

use bytes::Bytes;

pub(crate) type Index = Vec<(Bytes, u16)>;
