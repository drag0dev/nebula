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

mod entry;
mod bloomfilter;
mod hyperloglog;
mod count_min_sketch;
mod memtable;
mod sstable;

pub use entry::Entry;
pub use bloomfilter::BloomFilter;
pub use hyperloglog::HyperLogLog;
pub use count_min_sketch::CountMinSketch;
pub use memtable::Memtable;
pub use memtable::StorageCRUD;
pub use memtable::MemtableEntry;
pub use sstable::Filter;
