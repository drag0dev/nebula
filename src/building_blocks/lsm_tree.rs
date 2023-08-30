// TODO: replace the unwraps with context()? if I have the time

use anyhow::Context;
use anyhow::Result;
use sstable::SSTableReaderSingleFile;
use std::fs::create_dir;
use std::fs::remove_dir_all;
use std::fs::rename;
use std::ops::Range;
use std::path::Path;
use std::rc::Rc;

use crate::building_blocks::sstable;
use crate::building_blocks::Entry;

use super::SSTableIteratorSingleFile;

fn s(k: &Vec<u8>) -> String {
    String::from_utf8(k.clone()).unwrap()
}

fn print_table(dir: &str, lsm: &LSMTree, l: usize, t: usize) {
    SSTableReaderSingleFile::load(&format!("{dir}/{}", &lsm.levels[l].nodes[t].path))
        .unwrap()
        .iter()
        .unwrap()
        .for_each(|x| {
            let xx = &x.unwrap();
            println!(
                "{:?}",
                (
                    s(&xx.key),
                    if xx.value.is_some().clone() {
                        "entr"
                    } else {
                        "tomb"
                    }
                )
            );
        });
}

#[derive(Debug)]
struct TableNode {
    path: String,
}

// NOTE: this could be replaced with a BTree for increased efficiency
// but this is simpler and just works for now.
#[derive(Debug)]
struct Level {
    nodes: Vec<TableNode>,
}

pub struct LSMTree {
    levels: Vec<Level>,
    // level size ?
    // tier size ?
    // tables per tier ?
    fp_prob: f64,     // bloomfilter false positive probability
    item_count: u64,  // bloomfilter item count
    summary_nth: u64, // idk
    data_dir: String,
    size_threshold: usize,
    last_table: usize,
}

impl LSMTree {
    pub fn new(
        item_count: u64,
        fp_prob: f64,
        summary_nth: u64,
        data_dir: String,
        size_threshold: usize,
    ) -> Self {
        LSMTree {
            levels: vec![
                Level { nodes: vec![] },
                Level { nodes: vec![] },
                Level { nodes: vec![] },
            ],
            fp_prob,
            item_count,
            summary_nth,
            data_dir,
            size_threshold,
            last_table: 0,
        }
    }

    // NOTE:?
    // Can't use ? if func returns Option<T>
    /// Tries to find an `Entry` base on the `key`
    ///
    /// Returns None if it encounters a tombstone
    /// Returns None if it finds nothing even after a full traversal
    /// # Examples:
    /// ```
    /// let dir = String::from("data");
    ///
    /// let mut lsm = LSMTree::new(
    ///     100, // item_count: u64,
    ///     0.1, // fp_prob: f64,
    ///     10,  // summary_nth: u64,
    ///     dir, // data_dir: String,
    ///     3    // size_threshold: usize,
    /// );
    /// lsm.insert("new_sstable").unwrap();
    ///
    /// let key: Vec<u8> = Vec::from("joe");
    /// let out = lsm.get(key);
    /// assert!(out.is_some());
    ///
    /// let key: Vec<u8> = Vec::from("mama");
    /// let out = lsm.get(key);
    /// assert!(out.is_some());
    ///
    /// ```
    pub fn get(&self, key: Vec<u8>) -> Option<Entry> {
        for level in &self.levels {
            for table in &level.nodes.iter().rev().collect::<Vec<&TableNode>>() {
                let path = format!("{}/{}", self.data_dir, table.path);
                let msg = format!("Failed to open file {path}");
                let reader = SSTableReaderSingleFile::load(&path).context(msg).unwrap();
                let filter = reader.read_filter().unwrap();

                let mut sum_offset: Option<u64> = None;

                // if filter says no just go on
                if !filter.check(&key).unwrap() {
                    continue;
                }

                let mut index = reader.index_iter().unwrap();
                let (sum_iter, sum_range) = reader.summary_iter().unwrap();

                if sum_range.first_key > key {
                    continue;
                }
                if sum_range.last_key < key {
                    continue;
                }

                for e in sum_iter {
                    let sum_entry = e.unwrap();

                    if sum_entry.first_key <= key {
                        if sum_entry.last_key >= key {
                            sum_offset = Some(sum_entry.offset);
                            break;
                        }
                    }
                }

                if sum_offset.is_none() {
                    return None;
                }

                let entries = reader.iter();
                assert!(entries.is_ok());
                let mut entries = entries.unwrap();

                assert!(sum_offset.is_some());
                let offset = sum_offset.unwrap();
                index.move_iter(offset).unwrap();
                for entry in index {
                    let entry = entry.unwrap();
                    let offset = entry.offset;
                    if entry.key != key {
                        continue;
                    }

                    entries.move_iter(offset).unwrap();
                    let entry_ok = entries.next().unwrap().unwrap();

                    // if it's a tombstone, assume byebye
                    if entry_ok.value.is_none() {
                        return None;
                    }

                    return Some(entry_ok);
                }
                return None;
            }
        }
        println!();
        None
    }

    fn count_tables(&self, level_num: usize) -> usize {
        self.levels[level_num].nodes.len()
    }

    fn count_entries(&self, table_path: String) -> usize {
        let reader = SSTableReaderSingleFile::load(&(format!("test-data/{}", table_path)));
        println!("Counting entries of table: {}", table_path);
        let reader = reader.unwrap();
        println!(
            "{:?}",
            reader
                .iter()
                .unwrap()
                .map(|e| s(&e.unwrap().key))
                .collect::<Vec<String>>()
        );
        reader.iter().unwrap().collect::<Vec<_>>().len()
    }

    /// Private function for appending the table names to each level
    fn append_table(&mut self, path: &str) -> Result<()> {
        let node = TableNode {
            path: String::from(path),
        };
        self.levels[0].nodes.push(node);

        Ok(())
    }

    fn _insert(&mut self, path: &str) -> Result<()> {
        self.append_table(path).context("appending table")?;

        self.levels
            .iter()
            .enumerate()
            .map(|(idx, l)| {
                (
                    idx,
                    l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
                )
            })
            .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));

        let dir = { self.data_dir.clone() };

        if self.levels[0].nodes.len() >= self.size_threshold {
            println!("MERGING 0");
            self.merge(0, &dir).context("merging")?;
        }
        Ok(())
    }

    /// Inserts a new sstable into the LSM structure by passing a filepath
    ///
    /// # NOTE:
    /// `path` is relative to the `data_dir` field of the instantiated LSMTree
    /// Will probably die if the `path` is incorrect
    ///
    /// # NOTE:
    /// Do _not_ pass `path` = "sstable-n-n" as it _will_ die
    /// Pass anything else
    ///
    /// # Examples:
    /// ```
    /// let dir = String::from("data");
    ///
    /// let mut lsm = LSMTree::new(
    ///     100, // item_count: u64,
    ///     0.1, // fp_prob: f64,
    ///     10,  // summary_nth: u64,
    ///     dir, // data_dir: String,
    ///     3    // size_threshold: usize,
    /// );
    /// lsm.insert("new_sstable").unwrap();
    ///
    /// let key: Vec<u8> = Vec::from("joe");
    /// let out = lsm.get(key);
    /// assert!(out.is_some());
    ///
    /// let key: Vec<u8> = Vec::from("mama");
    /// let out = lsm.get(key);
    /// assert!(out.is_some());
    ///
    /// ```
    pub fn insert(&mut self, table_name: &str) -> Result<()> {
        let path = format!("{}/{}", self.data_dir, table_name);

        let new_idx = self.last_table + 1;

        let new_name = format!("sstable-0-{}", new_idx);
        let new_path = format!("{}/{}", self.data_dir, new_name);
        // create_dir(&new_path).context("creating empty dir").unwrap();

        rename(path, new_path).context("renaming sstable")?;

        println!("okRENAMED {table_name}");

        self.last_table += 1;

        self._insert(&new_name)
    }

    /// Function to resolve a sequence of entries with the same key
    ///
    /// # NOTE:
    /// Not memory efficient, could fill memory up indefinitely
    fn resolve_entries(
        &mut self,
        entries: &mut Vec<Rc<Entry>>,
        level_num: usize,
    ) -> Option<Rc<Entry>> {
        // Sort the entries by timestamp
        entries.sort_by_key(|e| e.timestamp);

        // if the level is not the last one, just return newest entry
        // if there are no entries return None
        if level_num < self.levels.len() {
            let last = entries.last();
            if let Some(entry) = last {
                return Some(Rc::clone(entry));
            }
            return None;
        }

        // else, traverse the entries backwards
        // and get the newest non-tombstone entry
        for entry in entries.iter().rev() {
            if entry.value.is_none() {
                continue;
            }
            return Some(Rc::clone(entry));
        }
        None
    }

    /// Merges all sstables assigned to a specified level into
    /// an sstable specified by filename
    fn merge(&mut self, level_num: usize, dirname: &str) -> Result<()> {
        if level_num == self.levels.len() - 1 {
            return Ok(());
        }

        let previous = self.levels[level_num + 1].nodes.last();
        let mut last = -1;
        if let Some(filename) = previous {
            if let Some(num) = filename.path.split("-").last() {
                last = num.parse().context("parsing last")?;
            }
        }

        let tablename = &format!("sstable-{}-{}", level_num + 1, last + 1);

        let mut builder = sstable::SSTableBuilderSingleFile::new(
            dirname,
            tablename,
            self.item_count,
            self.fp_prob,
            self.summary_nth,
        )
        .context("creating builder")?;

        let mut iterators: Vec<_> = self.levels[level_num]
            .nodes
            .iter()
            .map(|table| {
                SSTableReaderSingleFile::load(&(format!("{}/{}", dirname, table.path)))
                    .with_context(|| format!("loading {dirname}/{}", table.path))
                    .unwrap()
                    .iter()
                    .unwrap()
                    .into_iter()
                    .peekable()
            })
            .collect();

        let mut last_key: Option<Vec<u8>> = None;
        let mut relevant_entries: Vec<Rc<Entry>> = Vec::new();

        loop {
            // Get the smallest entry
            let smallest = iterators
                .iter_mut()
                .enumerate()
                .filter_map(|(idx, iter)| iter.peek().map(|value| (value, idx)))
                .min_by_key(|&(value, _)| value.as_ref().unwrap().key.clone());

            match smallest {
                Some((_, idx)) => {
                    // Consume the value from the corresponding iterator
                    let entry = iterators[idx].next().unwrap().unwrap();
                    let key = { entry.key.clone() };

                    let entry_ref = Rc::new(entry);

                    // Check if this entry has the same key as the last key
                    if last_key.as_ref() == Some(&key) {
                        relevant_entries.push(Rc::clone(&entry_ref));
                    } else {
                        if let Some(resolved_entry) =
                            self.resolve_entries(&mut relevant_entries, level_num)
                        {
                            builder.insert_entry(&resolved_entry).unwrap();
                        }
                        relevant_entries.clear();
                        relevant_entries.push(Rc::clone(&entry_ref));
                        last_key = Some(key);
                    }
                }
                None => {
                    // If there are no more entries, resolve the remaining entries
                    println!("Resolving remaining entries...\n");
                    if let Some(resolved_entry) =
                        self.resolve_entries(&mut relevant_entries, level_num)
                    {
                        builder.insert_entry(&resolved_entry).unwrap();
                    }
                    break; // Break when all iterators are exhausted
                }
            }
        }

        builder.finish_data().expect("finishing big sstable");

        // clear level and remove from disk
        self.levels[level_num].nodes.iter().for_each(|node| {
            let filename = node.path.clone();
            let path = format!("{dirname}/{filename}");
            remove_dir_all(path)
                .context("removing {node.path}")
                .unwrap();
        });

        self.levels[level_num].nodes.clear();

        self.levels[level_num + 1].nodes.push(TableNode {
            path: String::from(tablename),
        });

        if self.levels[level_num + 1].nodes.len() >= self.size_threshold {
            let msg = format!("MERGING RECURSE {level_num} -> {}", level_num + 1);
            println!("{}", msg);
            self.merge(level_num + 1, dirname).context(msg)?;
        }

        Ok(())
    }
}

fn insert_range(
    range: &mut Range<i32>,
    dir: &str,
    lsm: &mut LSMTree,
    tombstone: bool,
    auto_merge: bool,
    base: &str,
) -> Result<(), ()> {
    // must sort entries
    let mut entries: Vec<String> = range.map(|i| i.to_string()).collect();

    entries.sort();

    let mut path;
    if base.is_empty() {
        if tombstone {
            path = String::from("test-tombs-0-0");
        } else {
            path = String::from("test-sstable-0-0");
        }
    } else {
        path = String::from(format!("test-{base}-0-0"));
    }

    let mut builder = sstable::SSTableBuilderSingleFile::new(dir, &path, 100, 0.1, 10)
        .expect("creating a sstable");

    println!("created builder: {dir}/{path}");
    for (idx, key) in entries.iter().enumerate() {
        let val;
        let timestmp;

        if tombstone {
            val = None;
            timestmp = ((idx + 1) * 1000) as u128;
        } else {
            val = Some(key.to_string().into_bytes());
            timestmp = idx as u128;
        }

        let entry = Entry {
            timestamp: timestmp,
            key: key.to_string().into_bytes(),
            value: val,
        };

        builder
            .insert_entry(&entry)
            .context("inserting entry into the sstable")
            .unwrap();

        if idx < 100 {
            continue;
        }

        // finish previous and start new
        if idx % 100 == 0 {
            builder
                .finish_data()
                .context(format!("finishing sstable {dir}/{path}"))
                .unwrap();
            println!("finished builder: {dir}/{path}");

            if auto_merge {
                lsm.insert(&path)
                    .context(format!("inserting sstable {dir}/{path}"))
                    .unwrap();
            } else {
                lsm.append_table(&path)
                    .context(format!("inserting sstable {dir}/{path}"))
                    .unwrap();
            }
            println!("inserted sstable: {dir}/{path}");

            if tombstone {
                path = format!("test-tombs-0-{}", idx / 100);
            } else {
                path = format!("test-sstable-0-{}", idx / 100);
            }

            builder = sstable::SSTableBuilderSingleFile::new(dir, &path, 100, 0.1, 10)
                .context(format!("opening sstable {dir}/{path}"))
                .unwrap();
            println!("created builder: {dir}/{path}");
        }
    }

    builder.finish_data().expect("finishing big sstable");
    println!("finished builder: {dir}/{path}");

    if auto_merge {
        lsm.insert(&path)
            .context(format!("inserting sstable {dir}/{path}"))
            .unwrap();
    } else {
        lsm.append_table(&path)
            .context(format!("inserting sstable {dir}/{path}"))
            .unwrap();
    }
    println!("inserted {path}");

    Ok(())
}

macro_rules! redo_dirs {
    ($expr:expr) => {
        let test_path = $expr;
        let exists = Path::new(test_path).is_dir();
        if exists {
            remove_dir_all(test_path).expect("removing old data");
        }
        create_dir(test_path)
            .context("creating the test directory")
            .expect("idk");
    };
}

macro_rules! are_tombstones {
    ($lsm:expr, $keys:expr, true) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());

            let entry = out.unwrap();
            assert_eq!(Vec::from(key), entry.key);
            assert!(entry.value.is_none());
        }
    };

    ($lsm:expr, $keys:expr, false) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());

            let entry = out.unwrap();
            assert_eq!(Vec::from(key), entry.key);
            assert!(entry.value.is_some());
        }
    };
}

macro_rules! keys_exist {
    ($lsm:expr, $keys:expr, true) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert!(out.is_some());
        }
    };

    ($lsm:expr, $keys:expr, false) => {
        for key in $keys {
            let out = $lsm.get(Vec::from(key));
            assert_eq!(out, None);
        }
    };
}

#[test]
fn lsm_insert() -> Result<(), ()> {
    let test_path = "test-data/lsm-insert";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "")
}

#[test]
fn lsm_read() -> Result<(), ()> {
    let test_path = "./test-data/lsm-read";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let keys: Vec<&str> = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "1347", "2659", "8531", "4752", "9812", "5763", "3498", "6781", "9123", "1245", "6537",
        "3091", "7845", "2893", "6534", "1829", "9876", "5432", "1234", "9087", "7654", "2345",
        "8765", "5432", "2134", "7865", "1249", "4301", "8976", "2098", "6234", "4567", "9564",
        "7532", "9987", "5703", "8765", "9284", "6092", "4531", "8635", "4398", "7612", "2089",
        "6152", "3546", "8790", "9843", "5261", "4215", "6714", "3156", "1065", "7834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "-347", "-659", "-531", "-752", "-812", "-763", "-498", "-781", "-123", "-245", "-537",
        "-091", "-845", "-893", "-534", "-829", "-876", "-432", "-234", "-087", "-654", "-345",
        "-765", "-432", "-134", "-865", "-249", "-301", "-976", "-098", "-234", "-567", "-564",
        "-532", "-987", "-703", "-765", "-284", "-092", "-531", "-635", "-398", "-612", "-089",
        "-152", "-546", "-790", "-843", "-261", "-215", "-714", "-156", "-065", "-834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0047", "0059", "0031", "0052", "0012", "0063", "0098", "0081", "0023", "0045", "0037",
        "0091", "0045", "0093", "0034", "0029", "0076", "0032", "0034", "0087", "0054", "0045",
        "0065", "0032", "0034", "0065", "0049", "0001", "0076", "0098", "0034", "0067", "0064",
        "0032", "0087", "0003", "0065", "0084", "0092", "0031", "0035", "0098", "0012", "0089",
        "0052", "0046", "0090", "0043", "0061", "0015", "0014", "0056", "0065", "0034",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999", "1234",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "qX2t", "pY9s", "mZ7r", "kA3v", "nB6w", "eC8x", "fD1y", "gE5z", "hF4u", "iG0t", "jH6q",
        "rI3w", "tJ8e", "yK1r", "uL4t", "wM7y", "oN9u", "zO2i", "xP5o", "vQ0p", "aR7s", "bS3d",
        "cT6f", "dU1g", "eV4h", "fW8j", "gX5k", "hY2l", "iZ9m", "jA6n", "kB0p", "lC7q", "mD3r",
        "nE5s", "oF1t", "pG4u", "qH7v", "rI2w", "sJ9x", "tK6y",
    ];
    keys_exist!(lsm, keys, false);

    Ok(())
}

#[test]
fn lsm_merge_simple() {
    let test_path = "./test-data/lsm-merge-simple";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys: Vec<&str> = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    let keys: Vec<&str> = vec![
        "1347", "2659", "8531", "4752", "9812", "5763", "3498", "6781", "9123", "1245", "6537",
        "3091", "7845", "2893", "6534", "1829", "9876", "5432", "1234", "9087", "7654", "2345",
        "8765", "5432", "2134", "7865", "1249", "4301", "8976", "2098", "6234", "4567", "9564",
        "7532", "9987", "5703", "8765", "9284", "6092", "4531", "8635", "4398", "7612", "2089",
        "6152", "3546", "8790", "9843", "5261", "4215", "6714", "3156", "1065", "7834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "-347", "-659", "-531", "-752", "-812", "-763", "-498", "-781", "-123", "-245", "-537",
        "-091", "-845", "-893", "-534", "-829", "-876", "-432", "-234", "-087", "-654", "-345",
        "-765", "-432", "-134", "-865", "-249", "-301", "-976", "-098", "-234", "-567", "-564",
        "-532", "-987", "-703", "-765", "-284", "-092", "-531", "-635", "-398", "-612", "-089",
        "-152", "-546", "-790", "-843", "-261", "-215", "-714", "-156", "-065", "-834",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0047", "0059", "0031", "0052", "0012", "0063", "0098", "0081", "0023", "0045", "0037",
        "0091", "0045", "0093", "0034", "0029", "0076", "0032", "0034", "0087", "0054", "0045",
        "0065", "0032", "0034", "0065", "0049", "0001", "0076", "0098", "0034", "0067", "0064",
        "0032", "0087", "0003", "0065", "0084", "0092", "0031", "0035", "0098", "0012", "0089",
        "0052", "0046", "0090", "0043", "0061", "0015", "0014", "0056", "0065", "0034",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999", "1234",
    ];
    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "qX2t", "pY9s", "mZ7r", "kA3v", "nB6w", "eC8x", "fD1y", "gE5z", "hF4u", "iG0t", "jH6q",
        "rI3w", "tJ8e", "yK1r", "uL4t", "wM7y", "oN9u", "zO2i", "xP5o", "vQ0p", "aR7s", "bS3d",
        "cT6f", "dU1g", "eV4h", "fW8j", "gX5k", "hY2l", "iZ9m", "jA6n", "kB0p", "lC7q", "mD3r",
        "nE5s", "oF1t", "pG4u", "qH7v", "rI2w", "sJ9x", "tK6y",
    ];
    keys_exist!(lsm, keys, false);
}

#[test]
fn lsm_merge_tombstones() {
    let test_path = "./test-data/lsm-merge-tombstones";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let t = lsm.count_tables(0);
    let t = lsm.count_tables(1);

    println!("tables in tree: {t}");
    println!("entries in table900: {t}");
    println!("level0 {:?}", lsm.levels[0]);
    println!("level1 {:?}", lsm.levels[1]);

    let keys = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);
    are_tombstones!(lsm, keys, false);

    // tombstones
    insert_range(&mut (501..600), test_path, &mut lsm, true, false, "").unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys = vec![
        "501", "589", "534", "567", "590", "501", "545", "578", "523", "532", "565", "510", "543",
        "576", "509", "587", "554", "521", "545", "578", "501", "534", "567", "590", "523", "556",
        "589", "532", "565", "510", "543", "576", "509", "587", "554", "521", "545", "578", "501",
        "534", "567", "590", "523", "599",
    ];

    keys_exist!(lsm, keys, false);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);
}

#[test]
fn lsm_merge_mix_tomb() {
    let test_path = "./test-data/lsm-merge-mix-tomb";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..1000), test_path, &mut lsm, false, false, "").unwrap();

    let t = lsm.count_tables(0);
    let t = lsm.count_tables(1);

    println!("tables in tree: {t}");
    println!("entries in table900: {t}");
    println!("level0 {:?}", lsm.levels[0]);
    println!("level1 {:?}", lsm.levels[1]);

    let keys = vec![
        "456", "789", "234", "567", "890", "901", "345", "678", "123", "432", "765", "210", "543",
        "876", "109", "987", "654", "321", "345", "678", "901", "234", "567", "890", "123", "456",
        "789", "432", "765", "210", "543", "876", "109", "987", "654", "321", "345", "678", "901",
        "234", "567", "890", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);
    are_tombstones!(lsm, keys, false); // this is reduntant now ?

    // tombstones for existing entries
    insert_range(&mut (501..600), test_path, &mut lsm, true, false, "applied").unwrap();

    // tombstones for fun
    insert_range(
        &mut (2001..2100),
        test_path,
        &mut lsm,
        true,
        false,
        "propagated",
    )
    .unwrap();

    lsm.merge(0, test_path).unwrap();

    let keys = vec![
        "501", "589", "534", "567", "590", "501", "545", "578", "523", "532", "565", "510", "543",
        "576", "509", "587", "554", "521", "545", "578", "501", "534", "567", "590", "523", "556",
        "589", "532", "565", "510", "543", "576", "509", "587", "554", "521", "545", "578", "501",
        "534", "567", "590", "523", "599",
    ];
    keys_exist!(lsm, keys.clone(), false);

    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    // goofy tombstones
    let keys: Vec<&str> = vec![
        "2087", "2054", "2021", "2045", "2078", "2071", "2034", "2067", "2090", "2023", "2056",
        "2089", "2032", "2065", "2010", "2043", "2076", "2099", "2087", "2054", "2021", "2045",
        "2078", "2010", "2034", "2067", "2090", "2023",
    ];

    // keys_exist!(false) == doesn't exist, or is tombstone because
    // both failed searches and found tombstones return None
    keys_exist!(lsm, keys.clone(), false);

    assert_eq!(lsm.get(Vec::from("2002")), None);

    assert_eq!(lsm.get(Vec::from("2012")), None);

    assert_eq!(lsm.get(Vec::from("2021")), None);
}

#[test]
fn lsm_merge_mix_tomb_auto() {
    let test_path = "./test-data/lsm-merge-mix-tomb-auto";
    redo_dirs!(test_path);

    let mut lsm = LSMTree::new(100, 0.1, 10, String::from(test_path), 3);

    insert_range(&mut (0..700), test_path, &mut lsm, false, true, "").unwrap();

    let keys = vec![
        "456", "469", "234", "567", "690", "421", "345", "648", "123", "432", "465", "210", "543",
        "646", "109", "687", "654", "321", "345", "648", "301", "234", "567", "690", "123", "456",
        "469", "432", "465", "210", "543", "646", "109", "267", "654", "321", "345", "648", "301",
        "234", "567", "690", "123",
    ];

    keys_exist!(lsm, keys.clone(), true);

    // tombstones for existing keys
    insert_range(&mut (201..300), test_path, &mut lsm, true, true, "applied").unwrap();

    // tombstones for fun
    insert_range(
        &mut (2001..2100),
        test_path,
        &mut lsm,
        true,
        true,
        "propagated",
    )
    .unwrap();

    lsm.levels
        .iter()
        .enumerate()
        .map(|(idx, l)| {
            (
                idx,
                l.nodes.iter().map(|n| n.path.clone()).collect::<Vec<_>>(),
            )
        })
        .for_each(|p| println!("STATUS {} {:?}", p.0, p.1));

    // print_table(test_path, &lsm, 1, 0);
    // print_table(test_path, &lsm, 2, 0);

    // deleted
    let keys = vec![
        "201", "289", "234", "267", "290", "201", "242", "278", "223", "232", "262", "210", "243",
        "276", "209", "287", "224", "221", "242", "278", "201", "234", "267", "290", "223", "226",
        "289", "232", "262", "210", "243", "276", "209", "287", "224", "221", "242", "278", "201",
        "234", "267", "290", "223", "299",
    ];
    keys_exist!(lsm, keys.clone(), false);

    // kept
    let keys: Vec<&str> = vec![
        "0", "1", "4", "7", "3", "9", "54", "67", "12", "43", "76", "21", "54", "32", "76", "29",
        "87", "54", "21", "45", "78", "71", "34", "67", "90", "23", "56", "89", "32", "65", "10",
        "43", "76", "99", "87", "54", "21", "45", "78", "10", "34", "67", "90", "23",
    ];
    keys_exist!(lsm, keys, true);

    // goofy tombstones
    let keys: Vec<&str> = vec![
        "2087", "2054", "2021", "2045", "2078", "2071", "2034", "2067", "2090", "2023", "2056",
        "2089", "2032", "2065", "2010", "2043", "2076", "2099", "2087", "2054", "2021", "2045",
        "2078", "2010", "2034", "2067", "2090", "2023",
    ];

    keys_exist!(lsm, keys.clone(), false);

    assert_eq!(lsm.get(Vec::from("2002")), None);

    assert_eq!(lsm.get(Vec::from("2012")), None);

    assert_eq!(lsm.get(Vec::from("2021")), None);
}
