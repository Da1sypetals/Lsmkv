pub enum LogRecord<'a> {
    Value {
        timestamp: u64,
        key: &'a [u8],
        value: &'a [u8],
    },
    Tomb {
        timestamp: u64,
        key: &'a [u8],
    },
    TransactionStart {
        start_timestamp: u64,
        transaction_id: u64,
    },
    TransactionValue {
        transaction_id: u64,
        key: &'a [u8],
        value: &'a [u8],
    },
    TransactionTomb {
        transaction_id: u64,
        key: &'a [u8],
    },
    TransactionEnd {
        end_timestamp: u64,
        transaction_id: u64,
    },
}
