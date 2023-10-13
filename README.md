# <p style="text-align:center; font-size: 30px; font-weight:bold;">Nebula</p>

<p style="text-align:center;"> 
NoSQL write-heavy key-value database with support for Bloom Filter, HyperLogLog, CountMinSketch and SimHash data structures.
</p>

## Table of Contents
- [CLI](#cli)
- [REPL](#repl)
    - [General commands](#general-commands)
    - [Special data structure commands](#special-data-structure-commands)
- [Configuration](#configuration)
- [Installation](#installation)

## CLI
The database itself has a console line interface which supports operations:
- **init** - crates a new instance of the database, which encompasses creating the required directories for db to work and the config file

- **clear** - removes any database data except the config file, keeping the required directories

- **start** - start the database and drops user into the REPL

- **generatetestdata** - generates all data required for the unit tests to work

- **dummydata** [filename] - runs all queries inside the provided file against the db 

## REPL
### General commands
- **get** \<KEY>
- **put** \<KEY>, \<VALUE>
- **delete** \<KEY>
- **list** \<KEY_PREFIX> [PAGE NUMBER] [PAGE SIZE]
    - finds all entries that have the provided prefix in their key
    - PAGE NUMBER and PAGE SIZE used to specify pagination
- **range-scan** \<START_KEY> <END_KEY> [PAGE NUMBER] [PAGE SIZE]
    - find all entries that have a key for which stands START_KEY >= key <= END_KEY
    - PAGE NUMBER and PAGE SIZE used to specify pagination
- **quit**
- **help**

### Special data structure commands
- **BloomFilter** - keys must start with "bf_"
    - **bf new** \<KEY>
        - creates a new BloomFilter and saves it under the provided key 
        - BloomFilter is instantiated with item count and false positive probability taken from the config
    - **bf add** \<KEY> \<VALUE>
        - adds the provided value to the BloomFilter stored under the provided key
    - **bf check** \<KEY> \<VALUE>
        - checks if the provided value is present in the BloomFilter stored under the provided key

<br>

- **SimHash** - keys must start with "sh_"
    - **sh hash** \<KEY> \<VALUE>
        - hash the provided value using SimHash and store it under the provided key
    - **sh similarity** \<LEFT_KEY> \<RIGHT_KEY>
        - compares hashes which are stored under the provided keys

<br>

- **HyperLogLog** - keys must start with "hll_"
    - **hll new** \<KEY>
        - creates a new HyperLogLog and saves it under the provided key
        - HyperLogLog is instantiated with size taken from the config
    - **hll add** \<KEY> \<VALUE>
        - adds the provided value to the HyperLogLog stored under the provided key
    - **hll count** \<KEY>
        - returns the count from the HyperLogLog stored under the provided key

<br>

- **CountMinSketch** - keys must start with "cms_"
    - **cms new** \<KEY>
        - creates a new CountMinSketch and saves it under the provided key
        - CountMinSketch is instantiated with desired accuracy and certainty taken from the config
    - **cms add** \<KEY> \<VALUE>
        - adds the provided value to the CountMinSketch stored under the provided key
    - **cms count** \<KEY>
        - returns the count from the CountMinSketch stored under the provided key

## Configuration
When initializing the database a config file will be created at "./data/config.json"
with the default configuration. Any changes, if needed, should be done in that file.  
[Default config and config explanation](CONFIG.md)

# Installation
installing via cargo:
```
cargo install --path ./
```
just building from source:

```
cargo build --release
```