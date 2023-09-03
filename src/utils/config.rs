use crate::building_blocks::{
    BloomFilter, CountMinSketch, Entry, FileOrganization, LSMTreeUnderlying, SSTableConfig,
    SkipListNode, TokenBucket, MF, SF,
};
use core::cell::RefCell;
use std::collections::HashSet;
use std::fs::File;
use std::rc::Rc;
use std::time::{Duration, Instant};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub token_bucket: TokenBucketConfig,
    pub cms: CountMinSketchConfig,
    pub bf: BloomFilterConfig,
    pub lsm: LSMTreeConfig,
    pub hll: HLLConfig,
    pub ssconfig: SSTableConfig,
}

impl Config {
    pub fn default() -> Self {
        Config {
            token_bucket: TokenBucketConfig::default(),
            cms: CountMinSketchConfig::default(),
            bf: BloomFilterConfig::default(),
            lsm: LSMTreeConfig::default(),
            hll: HLLConfig::default(),
            ssconfig: SSTableConfig::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct TokenBucketConfig {
    capacity: usize,
    reset_interval: Duration, // not directly serializable :D
}

impl TokenBucketConfig {
    pub fn default() -> Self {
        TokenBucketConfig {
            capacity: 5,
            reset_interval: Duration::from_secs(2),
        }
    }
    pub fn defaults(&self) -> (usize, Duration) {
        (self.capacity, self.reset_interval)
    }
}

#[derive(Serialize, Deserialize)]
pub struct CountMinSketchConfig {
    desired_accuracy: f64,
    certainty: f64,
}

impl CountMinSketchConfig {
    pub fn default() -> Self {
        CountMinSketchConfig {
            desired_accuracy: 0.01,
            certainty: 0.01,
        }
    }
    pub fn defaults(&self) -> (f64, f64) {
        (self.desired_accuracy, self.certainty)
    }
}

#[derive(Serialize, Deserialize)]
pub struct BloomFilterConfig {
    item_count: u64,
    fp_prob: f64,
}

impl BloomFilterConfig {
    pub fn default() -> Self {
        BloomFilterConfig {
            item_count: 10,
            fp_prob: 0.01,
        }
    }

    pub fn defaults(&self) -> (u64, f64) {
        (self.item_count, self.fp_prob)
    }
}

#[derive(Serialize, Deserialize)]
pub struct LSMTreeConfig {
    fp_prob: f64,
    summary_nth: u64,
    data_dir: String,
    size_threshold: usize,
    number_of_levels: usize,
}

impl LSMTreeConfig {
    pub fn default() -> Self {
        LSMTreeConfig {
            fp_prob: 0.01,
            summary_nth: 50,
            data_dir: String::from("data/table_data"),
            size_threshold: 20,
            number_of_levels: 5,
        }
    }

    pub fn defaults(&self) -> (f64, u64, String, usize, usize) {
        (
            self.fp_prob,
            self.summary_nth,
            self.data_dir,
            self.size_threshold,
            self.number_of_levels,
        )
    }
}

impl SSTableConfig {
    pub fn default() -> Self {
        SSTableConfig {
            file_organization: FileOrganization::MultiFile(()),
            filter_fp_prob: 0.01,
            summary_nth: 50,
        }
    }
    pub fn defaults(&self) -> (FileOrganization, f64, u64) {
        (
            self.file_organization,
            self.filter_fp_prob,
            self.summary_nth,
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct HLLConfig {
    number_of_bits: u8,
}

impl HLLConfig {
    pub fn default() -> Self {
        HLLConfig { number_of_bits: 10 }
    }

    pub fn defaults(&self) -> u8 {
        self.number_of_bits
    }
}

#[derive(Serialize, Deserialize)]
pub struct SimHashConfig {
    simhash: u64,
    stopwords: HashSet<String>,
}

impl SimHashConfig {
    pub fn default() -> Self {
        let stopwords: HashSet<String> = ["this", "is", "a", "with", "to", "the", "some"]
            .iter()
            .map(|&word| word.to_string())
            .collect();

        SimHashConfig {
            simhash: 0,
            stopwords,
        }
    }

    pub fn defaults(&self) -> (u64, HashSet<String>) {
        (self.simhash, self.stopwords)
    }
}

#[derive(Serialize, Deserialize)]
pub struct SkipListConfig {
    max_level: usize,
}

impl SkipListConfig {
    pub fn default() -> Self {
        SkipListConfig { max_level: 10 }
    }

    pub fn defaults(&self) -> usize {
        self.max_level
    }
}

// struct SummaryIterator {
//     // FILENAME
//     pub(in crate::building_blocks::sstable) file: File,
//
//     /// number of bytes read so far
//     amount_read: i64,
//
//     /// number of bytes to be read in total
//     pub(in crate::building_blocks::sstable) amount_to_be_read: i64,
// }
//
// struct SSTableReaderMultiFile {
//     pub filter: BloomFilter,
//     // FILENAME
//     index_file: File,
//     // FILENAME
//     summary_file: File,
//     // metadata: ?,
//     // FILENAME
//     sstable_file: File,
// }
//
// struct SSTableIteratorMultiFile {
//     // FILENAME
//     sstable_file: File,
//     pub(in crate::building_blocks::sstable) current_offset: u64,
// }
//
// struct SSTableBuilderMultiFile {
//     index: IndexBuilder,
//     summary: SummaryBuilder,
//     //metadata: ?,
//     filter: BloomFilter,
//     // FILENAME
//     filter_file: File,
//     // FILENAME
//     sstable_file: File,
//     sstable_offset: u64,
//     summary_offset: u64,
//     summary_nth: u64,
//     entries_written: u64,
//
//     // the first entry in the current range of the summary
//     first_entry_range: Option<Rc<Entry>>,
//
//     // need to keep track in order to be able to write total range and last entry in the current range
//     first_entry_written: Option<Rc<Entry>>,
//     last_entry_written: Option<Rc<Entry>>,
// }
//
// struct SSTableReaderSingleFile {
//     pub header: SSTableHeader,
//     // FILENAME
//     file: File,
// }
//
// struct SSTableBuilderSingleFile {
//     header: SSTableHeader,
//
//     /// used for reading previously written data
//     reader_file: File,
//
//     /// used for writing to the file, synced after each pass
//     writer_file: File,
//
//     filter: BloomFilter,
//
//     /// last key written, used for generating summary
//     last_key_global: Option<Vec<u8>>,
//     summary_nth: u64,
// }
//
// struct SSTableConfig {
//     file_organization: FileOrganization,
//
//     // TODO: assert that this is > 2
//     /// every n key make an entry in the summary
//     summary_nth: u64,
//
//     /// filter false positive probability
//     filter_fp_prob: f64,
// }
//
// struct IndexIterator {
//     // FILENAME
//     file: File,
//     pub(in crate::building_blocks::sstable) current_offset: u64,
// }
//
// struct IndexBuilder {
//     // FILENAME
//     file: File,
//
//     /// tracks at which offset each index entry is written
//     pub(in crate::building_blocks::sstable) index_offset: u64,
// }
//
// struct TableNode {
//     // FILENAME
//     pub(super) path: String,
// }
