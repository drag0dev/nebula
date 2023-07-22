use super::entry::Entry;
use bytes::BufMut;

/// |CRC(u32),Timestamp(u128),Tombstone(u8),Key len(u64),Value len(8B),key, value|
/// contains pairs of binary keys and values
// TODO: could potentially be pub(super)
pub struct Block {
    pub data: Vec<u8>,
    pub block_size: usize,
}

/// entry size without the key and value
pub const ENTRY_SIZE: usize = 4+16+1+8+8;

impl Block {
    pub fn new(block_size: usize) -> Self {
        Block {
            data: Vec::new(),
            block_size
        }
    }

    pub fn add(&mut self, transaction: Entry) -> bool {
        let mut unwarpped_value = &Vec::new();
        let value_len;
        if let Some(value) = transaction.value.as_ref() {
            value_len = value.len();
            unwarpped_value = value;
        } else { value_len = 0; }

        // check if there is space for the transaction
        if (self.data.len() + ENTRY_SIZE + transaction.key.len() + value_len) > self.block_size {
            return false;
        }

        self.data.put_u32(transaction.crc);
        self.data.put_u128(transaction.timestamp);
        self.data.put_u8(transaction.tombstone);
        self.data.put_u64(transaction.key_len);
        self.data.put_u64(transaction.value_len);
        self.data.put(&transaction.key[..]);
        if value_len > 0 { self.data.put(&unwarpped_value[..]); }

        return true;
    }

    /// decode binary block into a block
    pub fn decode(data: &[u8]) -> Self {
        Block {
            data: data.to_owned(),
            block_size: data.len()
        }
    }
}
