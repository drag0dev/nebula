use bincode::{
    options,
    Options,
    config::{
        WithOtherIntEncoding,
        WithOtherEndian,
        WithOtherTrailing,
        DefaultOptions,
        RejectTrailing,
        LittleEndian,
        FixintEncoding
    }
};
type BincodeOptions = WithOtherIntEncoding<WithOtherEndian<WithOtherTrailing<DefaultOptions, RejectTrailing>, LittleEndian>, FixintEncoding>;
lazy_static::lazy_static! {
    pub static ref BINCODE_OPTIONS: BincodeOptions = options()
            .reject_trailing_bytes()
            .with_little_endian()
            .with_fixint_encoding();
}

/// max lenth of the key is 64kb
pub static MAX_KEY_LEN: u64 = 64*1024;
/// max lenth of the key is 512mb
pub static MAX_VAL_LEN: u64 = 512*1024*1024;

mod entry;
mod bloomfilter;
mod hyperloglog;
mod count_min_sketch;
mod token_bucket;
mod memtable;
mod sstable;
mod skip_list;
mod b_tree;

pub use entry::Entry;
pub use bloomfilter::BloomFilter;
pub use hyperloglog::HyperLogLog;
pub use count_min_sketch::CountMinSketch;
pub use token_bucket::TokenBucket;
pub use memtable::Memtable;
pub use memtable::StorageCRUD;
pub use memtable::MemtableEntry;
pub use sstable::{
    IndexBuilder, IndexIterator, IndexEntry,
    SummaryBuilder, SummaryEntry, SummaryIterator,
    SSTableConfig, FileOrganization,
    SSTableBuilderMultiFile, SSTableReaderMultiFile, SSTableIteratorMultiFile,
    SSTableBuilderSingleFile, SSTableReaderSingleFile, SSTableIteratorSingleFile
};
pub use skip_list::SkipList;
pub use skip_list::SkipListNode;
pub use b_tree::BTree;
