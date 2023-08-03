use std::fs::File;
use crate::building_blocks::Filter;

/// SSTable builder where aiding structures are in the same file as the data itself
/// singlefile sstable is built in steps
/// 1. pass - write all sstable entries and the filter
/// 2. pass - write index by reading previously written entries
/// 3. pass - write summary by reading previously written index entries
/// file layout:
/// ----------------------
/// sstable header
/// data
/// filter
/// index
/// summary
/// ----------------------
/// this way of creating a single file sstable is very slow due to a lot of IO ops
/// but its the only way of being able to handle very large sstables
pub struct SSTableBuilderSingleFile {
    file: File,
    filter: Filter,
}


