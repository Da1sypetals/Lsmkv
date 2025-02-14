pub enum WalValue {
    Put(Vec<u8>),
    Delete,
}

pub struct WalLog {
    pub(crate) key: Vec<u8>,
    pub(crate) value: WalValue,
}

// utils
impl WalLog {
    pub fn encode(&self) -> Vec<u8> {
        todo!()
    }

    pub fn decode(&self, bytes: &[u8]) {
        todo!()
    }
}
