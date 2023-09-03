use clap::{Parser, Subcommand};

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
    New { bloom_filter_key: String },
    Add { bloom_filter_key: String, value: String },
    Check { bloom_filter_key: String, value: String },
}

// doesnt require an instance
// the values are therefore provided in the command in hex format
#[derive(Debug, Subcommand)]
pub enum SimHashCommands {
    Hash { key: String, value: String },
    Similarity {left_key: String, right_key: String, }
}

#[derive(Debug, Subcommand)]
pub enum HLLCommands {
    New { hll_key: String },
    Add { hll_key: String, value: String },
    Count { hll_key: String }
}

#[derive(Debug, Subcommand)]
pub enum CMSCommands {
    New { cms_key: String },
    Add { cms_key: String, value: String },
    Count { cms_key: String }
}
