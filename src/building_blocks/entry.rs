/// |CRC(u32),Timestamp(u128),Tombstone(u8),Key len(u64),Value len(8B),key,value|
/// a single data entry
#[derive(Clone, PartialEq, Debug)]
pub struct Entry {
    pub crc: u32,

    /// nanos
    pub timestamp: u128,

    /// 1 - tombstone
    /// 0 - not tombstone
    pub tombstone: u8,

    /// key length
    pub key_len: u64,

    /// value length
    pub value_len: u64,

    pub key: Vec<u8>,

    /// value is optional incase of a tombstone
    pub value: Option<Vec<u8>>
}