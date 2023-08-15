use clap::{Parser, Subcommand};

// special prefixes:
// BloomFilter - bf_
// HLL - hll_
// CMS - cms_
// when putting a special structure its expected to be in binary repr that respects global
// bincode ser/deser options
#[derive(Debug, Parser)]
#[command(name = "")]
pub struct Repl {
    #[command(subcommand)]
    pub commands: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },

    List {
        key_prefix: String,

        #[arg(num_args=2, value_names = ["PAGE NUMBER", "PAGE SIZE"])]
        /// page number and page size
        pagination: Option<Vec<u64>>,
    },

    RangeScan {
        start_key: String,
        end_key: String,

        #[arg(num_args=2, value_names = ["PAGE NUMBER", "PAGE SIZE"])]
        /// page number and page size
        pagination: Option<Vec<u64>>,
    },

    #[command(subcommand)]
    Bf(BloomFilterCommands),

    #[command(subcommand)]
    Sh(SimHashCommands),

    #[command(subcommand)]
    Hll(HLLCommands),

    #[command(subcommand)]
    Cms(CMSCommands),

    Quit,
}

#[derive(Debug, Subcommand)]
pub enum BloomFilterCommands {
    Add { bloom_filter_key: String, value: String },
    Check { key: String },
}

// doesnt require an instance
// the values are therefore provided in the command in hex format
#[derive(Debug, Subcommand)]
pub enum SimHashCommands {
    Hash { value: String },
    Similarity {left: String, right: String, }
}

#[derive(Debug, Subcommand)]
pub enum HLLCommands {
    Add { hll_key: String, value: String },
    Count { hll_key: String }
}

#[derive(Debug, Subcommand)]
pub enum CMSCommands {
    Add { cms_key: String, value: String },
    Count { cms_key: String }
}
