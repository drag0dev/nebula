use crate::building_blocks::{
    BloomFilter, CountMinSketch, Entry, FileOrganization, LSMTreeUnderlying, SSTableConfig,
    SkipListNode, TokenBucket, MF, SF,
};
use core::cell::RefCell;
use crate::building_blocks::StorageCRUD;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::rc::Rc;
use std::time::{Duration, Instant};
use anyhow::{Result, Context};

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub token_bucket: TokenBucketConfig,
    pub cms: CountMinSketchConfig,
    pub bf: BloomFilterConfig,
    pub lsm: LSMTreeConfig,
    pub hll: HLLConfig,
    pub ssconfig: SSTableConfig,
    pub skiplist: SkipListConfig,
    pub simhash: SimHashConfig,
    pub memtable: MemtableConfig,
    pub wal: WALConfig,
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
            skiplist: SkipListConfig::default(),
            simhash: SimHashConfig::default(),
            memtable: MemtableConfig::default(),
            wal: WALConfig::default(),
        }
    }

    // Method to load the JSON configuration from a file into a Config struct
    pub fn load_from_file() -> Result<Self> {
        // Open the file
        let mut file = File::open("data/config.json").context("Unable to open file")?;

        // Create a string to hold the file contents
        let mut contents = String::new();

        // Read the file contents into the string
        file.read_to_string(&mut contents)
            .context("Unable to read data")?;

        // Deserialize the JSON string into a Config struct
        let config: Config = serde_json::from_str(&contents).context("deserializing config")?;

        Ok(config)
    }

    pub fn write_defaults_to_file() -> Result<()> {
        // Create a default Config
        let config = Config::default();

        // Serialize it to a JSON string
        let json_str = serde_json::to_string_pretty(&config)?;

        // Open a new file or overwrite an existing one named "config.json"
        let mut file = File::create("data/config.json").context("Unable to create file")?;

        // Write the JSON string to the file
        file.write_all(json_str.as_bytes())
            .context("Unable to write data")?;

        println!("Serialized Config to JSON file: data/config.json");
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
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
    pub fn get_values(&self) -> (usize, Duration) {
        (self.capacity, self.reset_interval)
    }
}

#[derive(Serialize, Deserialize, Debug)]
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
    pub fn get_values(&self) -> (f64, f64) {
        (self.desired_accuracy, self.certainty)
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

    pub fn get_values(&self) -> (u64, f64) {
        (self.item_count, self.fp_prob)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LSMTreeConfig {
    file_organization: FileOrganization,
    fp_prob: f64,
    summary_nth: u64,
    data_dir: String,
    size_threshold: usize,
    number_of_levels: usize,
}

impl LSMTreeConfig {
    pub fn default() -> Self {
        LSMTreeConfig {
            file_organization: FileOrganization::MultiFile(()),
            fp_prob: 0.01,
            summary_nth: 50,
            data_dir: String::from("data/table_data"),
            size_threshold: 20,
            number_of_levels: 5,
        }
    }

    pub fn get_values(&self) -> (FileOrganization, f64, u64, String, usize, usize) {
        (
            self.file_organization.clone(),
            self.fp_prob,
            self.summary_nth,
            self.data_dir.clone(),
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
    pub fn get_values(&self) -> (FileOrganization, f64, u64) {
        (
            self.file_organization.clone(),
            self.filter_fp_prob,
            self.summary_nth,
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HLLConfig {
    number_of_bits: u8,
}

impl HLLConfig {
    pub fn default() -> Self {
        HLLConfig { number_of_bits: 10 }
    }

    pub fn get_values(&self) -> u8 {
        self.number_of_bits
    }
}


#[derive(Serialize, Deserialize, Debug)]
pub struct MemtableConfig {
    // TODO BOX DYN TRAIT DOESNT IMPL SERIALIEZ
    // storage: Box<dyn StorageCRUD>,
    capacity: u64,
    sstable_type: FileOrganization,
    fp_prob: f64,
    summary_nth: u64,
    data_folder: String,
}

impl MemtableConfig {
    pub fn default() -> Self {
        MemtableConfig {
            capacity: 50,
            sstable_type: FileOrganization::MultiFile(()),
            fp_prob: 0.01,
            summary_nth: 50,
            data_folder: String::from("data/table_data"),
        }
    }
    pub fn get_values(&self) -> (u64, FileOrganization, f64, u64, String) {
        (
            self.capacity,
            self.sstable_type.clone(),
            self.fp_prob,
            self.summary_nth,
            self.data_folder.clone(),
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

    pub fn get_values(&self) -> (u64, HashSet<String>) {
        (self.simhash, self.stopwords.clone())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SkipListConfig {
    max_level: usize,
}

impl SkipListConfig {
    pub fn default() -> Self {
        SkipListConfig { max_level: 10 }
    }

    pub fn get_values(&self) -> usize {
        self.max_level
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct WALConfig {
    segment_size: u64,
    path: String
}

impl WALConfig {
    pub fn default() -> Self {
        WALConfig { segment_size: 2000, path: String::from("data_dir/WAL") } 
    }
    pub fn get_values(&self) -> (String, u64) {
        (self.path.clone(), self.segment_size)
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
