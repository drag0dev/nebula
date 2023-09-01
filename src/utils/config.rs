struct TokenBucket {

    // max bucket capacity
    capacity: usize,

    // current number of tokens
    tokens: usize,

    // timestamp of last refill
    last_reset: Instant,

    // bucket refill time
    reset_interval: Duration,
}

struct CountMinSketch {
    /// the hash functions should be "pair-wise independent" (?)
    hash_func_count: u64,
    column_count: u64,

    /// delta
    desired_accuracy: f64,

    /// certainty of achieving the desired accuracy
    certainty: f64,

    // k = ln(1/eps)
    seeds: Vec<u32>,

    matrix: Vec<Vec<u64>>,
}

struct BloomFilter{
    pub item_count: u64,
    /// false positive probability
    fp_prob: f64,
    hash_functions: u64,
    bit_arr: BitVec,
    bit_arr_len: u64,
    seeds: Vec<u32>,
}

struct SummaryBuilder {
    // FILENAME
    file: File,
}

struct SummaryIterator {
    // FILENAME
    pub (in crate::building_blocks::sstable) file: File,

    /// number of bytes read so far
    amount_read: i64,

    /// number of bytes to be read in total
    pub (in crate::building_blocks::sstable) amount_to_be_read: i64,

}

struct SSTableReaderMultiFile {
    pub filter: BloomFilter,
    // FILENAME
    index_file: File,
    // FILENAME
    summary_file: File,
    // metadata: ?,
    // FILENAME
    sstable_file: File,
}

struct SSTableIteratorMultiFile {
    // FILENAME
    sstable_file: File,
    pub (in crate::building_blocks::sstable) current_offset: u64,
}

struct SSTableBuilderMultiFile {
    index: IndexBuilder,
    summary: SummaryBuilder,
    //metadata: ?,
    filter: BloomFilter,
    // FILENAME
    filter_file: File,
    // FILENAME
    sstable_file: File,
    sstable_offset: u64,
    summary_offset: u64,
    summary_nth: u64,
    entries_written: u64,

    // the first entry in the current range of the summary
    first_entry_range: Option<Rc<Entry>>,

    // need to keep track in order to be able to write total range and last entry in the current range
    first_entry_written: Option<Rc<Entry>>,
    last_entry_written: Option<Rc<Entry>>,
}

struct SSTableReaderSingleFile {
    pub header: SSTableHeader,
    // FILENAME
    file: File,
}


struct SSTableBuilderSingleFile {
    header: SSTableHeader,

    /// used for reading previously written data
    reader_file: File,

    /// used for writing to the file, synced after each pass
    writer_file: File,

    filter: BloomFilter,

    /// last key written, used for generating summary
    last_key_global: Option<Vec<u8>>,
    summary_nth: u64,
}

struct SSTableConfig {
    file_organization: FileOrganization,

    // TODO: assert that this is > 2
    /// every n key make an entry in the summary
    summary_nth: u64,

    /// filter false positive probability
    filter_fp_prob: f64,
}

struct IndexIterator {
    // FILENAME
    file: File,
    pub (in crate::building_blocks::sstable) current_offset: u64,
}

struct IndexBuilder {
    // FILENAME
    file: File,

    /// tracks at which offset each index entry is written
    pub (in crate::building_blocks::sstable) index_offset: u64,
}

struct TableNode {
    // FILENAME
    pub(super) path: String,
}

struct MemtableEntry {
    /// nanos
    pub timestamp: u128,

    pub key: String,

    /// its value is None it means its a tombstone
    pub value: Option<String>
}

struct HyperLogLog {
    buckets: Vec<u8>,
    number_of_bits: u8,
    set: f64,
}

struct SimHash {
    simhash: u64,
    stopwords: HashSet<String>,
}

pub struct SkipList<T> {
    head: Rc<RefCell<SkipListNode<T>>>,
    max_level: usize,
}


pub struct LSMTree<S: LSMTreeUnderlying> {
    pub(super) levels: Vec<Level>,
    // level size ?
    // tier size ?
    // tables per tier ?
    pub(super) fp_prob: f64,
    pub(super) summary_nth: u64,
    pub(super) data_dir: String,
    pub(super) size_threshold: usize,
    pub(super) last_table: usize,
    pub(super) marker: std::marker::PhantomData<S>,
}


        fp_prob: f64,
        summary_nth: u64,
        data_dir: String,
        size_threshold: usize,
        number_of_levels: usize,



file_organization for sstable memtable and lsmtree

