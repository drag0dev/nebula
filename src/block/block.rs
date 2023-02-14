use super::entry::Entry;
use bytes::BufMut;

/// |CRC(u32),Timestamp(u128),Tombstone(u8),Key len(u64),Value len(8B),key, value|
/// contains pairs of binary keys and values
pub struct Block {
    data: Vec<u8>,
    block_size: usize,
}

/// entry size without the key and value
const ENTRY_SIZE: usize = 4+16+1+8+8;

impl Block {
    pub fn new(block_size: usize) -> Self {
        Block {
            data: Vec::new(),
            block_size
        }
    }

    pub fn add(&mut self, transaction: Entry) -> bool {
        // check if there is space for the transaction
        if (self.data.len() + ENTRY_SIZE + transaction.key.len() + transaction.value.len()
            ) > self.block_size {
            return false;
        }

        self.data.put_u32(transaction.crc);
        self.data.put_u128(transaction.timestamp);
        self.data.put_u8(transaction.tombstone);
        self.data.put_u64(transaction.key_len);
        self.data.put_u64(transaction.value_len);
        self.data.put(&transaction.key[..]);
        self.data.put(&transaction.value[..]);

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
