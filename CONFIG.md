```
"token_bucket": {
    "capacity": 5,
    "reset_interval": {
        "secs": 2,
        "nanos": 0
    }
},

# CountMinSketch
"cms": { 
    "desired_accuracy": 0.01,
    "certainty": 0.01
},

# BloomFilter
"bf": { 
    "item_count": 10,
    "fp_prob": 0.01 # false positive probability
},

# LSMTree
"lsm": { 
    "file_organization": {
        "MultiFile": null
    },
    "fp_prob": 0.01, # false positive probability
    "summary_nth": 50, # summary range size
    "data_dir": "data/table_data",
    "size_threshold": 20,
    "number_of_levels": 5
},

# HyperLogLog
"hll": { 
    "number_of_bits": 10
},

# SSTable
"ssconfig": { 
    "file_organization": {
        "MultiFile": null
    },
    "summary_nth": 50, # summary range size
    "filter_fp_prob": 0.01 # false positive probability
},

# SkipList
"skiplist": {
    "max_level": 10
},

# SimHash
"simhash": {
    "simhash": 0,
    "stopwords": [
        "the",
        "is",
        "this",
        "some",
        "a",
        "with",
        "to"
    ]
},

# Memtable
"memtable": {
    "storage": "BTree",
    "capacity": 50,
    "sstable_type": {
        "MultiFile": null
    },
    "fp_prob": 0.01,
    "summary_nth": 50, # summary range size
    "data_folder": "data/table_data"
},

# WriteAheadLog
"wal": { 
    "segment_size": 20000,
    "path": "data/WAL"
},

# Cache
"cache": {
    "capacity": 1000
}
```