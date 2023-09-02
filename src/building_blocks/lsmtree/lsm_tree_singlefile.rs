// TODO: replace the unwraps with context()? if I have the time
use anyhow::{Context, Result};
use crate::building_blocks::sstable::{
    SSTableReaderSingleFile as SSTableReader,
    SSTableBuilderSingleFile as SSTableBuilder, SF};
use crate::building_blocks::Entry;
use std::fs::{remove_dir_all, rename};
use std::rc::Rc;
use super::{LSMTree, TableNode, Level};

impl LSMTree<SF> {
    pub fn new(
        fp_prob: f64,
        summary_nth: u64,
        data_dir: String,
        size_threshold: usize,
        number_of_levels: usize,
    ) -> Self {

        let marker: std::marker::PhantomData<SF> = Default::default();
        let mut levels = vec![];
        for _ in 0..number_of_levels {
            levels.push(Level { nodes: vec![] });
        }
        LSMTree {
            levels,
            fp_prob,
            summary_nth,
            data_dir,
            size_threshold,
            last_table: 0,
            marker,
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
    ///     0.1, // fp_prob: f64,
    ///     10,  // summary_nth: u64,
    ///     dir, // data_dir: String,
    ///     3    // size_threshold: usize,
    ///     3    // number_of_levels: usize,
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
                let reader = SSTableReader::load(&path).context(msg).unwrap();
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
        None
    }

    /// Private function for appending the table names to each level
    pub(super) fn append_table(&mut self, path: &str) -> Result<()> {
        let node = TableNode {
            path: String::from(path),
        };
        self.levels[0].nodes.push(node);

        Ok(())
    }

    fn _insert(&mut self, path: &str) -> Result<()> {
        self.append_table(path).context("appending table")?;

        let dir = { self.data_dir.clone() };

        if self.levels[0].nodes.len() >= self.size_threshold {
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

        rename(path, new_path).context("renaming sstable")?;

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
    pub(super) fn merge(&mut self, level_num: usize, dirname: &str) -> Result<()> {
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

        let mut iterators: Vec<_> = self.levels[level_num]
            .nodes
            .iter()
            .map(|table| {
                SSTableReader::load(&(format!("{}/{}", dirname, table.path)))
                    .with_context(|| format!("loading {dirname}/{}", table.path))
                    .unwrap()
                    .iter()
                    .unwrap()
                    .into_iter()
                    .peekable()
            })
            .collect();

        let mut sum_item_counts = 0;
        {
            for table in &self.levels[level_num].nodes {
                let sstable = SSTableReader::load(&(format!("{}/{}", dirname, table.path)));
                let sstable = sstable.context("unwrapping for bf")?;
                let bf = sstable.read_filter().context("unwrapping bf")?;
                sum_item_counts += bf.item_count;
            }
        }

        let mut builder = SSTableBuilder::new(
            dirname,
            tablename,
            sum_item_counts,
            self.fp_prob,
            self.summary_nth,
        )
        .context("creating builder")?;



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
                            builder.insert((*resolved_entry).clone()).unwrap();
                        }
                        relevant_entries.clear();
                        relevant_entries.push(Rc::clone(&entry_ref));
                        last_key = Some(key);
                    }
                }
                None => {
                    // If there are no more entries, resolve the remaining entries
                    // println!("Resolving remaining entries...\n");
                    if let Some(resolved_entry) =
                        self.resolve_entries(&mut relevant_entries, level_num)
                    {
                        builder.insert((*resolved_entry).clone()).unwrap();
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
            self.merge(level_num + 1, dirname).context(msg)?;
        }

        Ok(())
    }

    pub fn load(&mut self) -> Result<()> {
        let paths =
            std::fs::read_dir(self.data_dir.clone()).context("reading directory contents")?;

        println!("dir mfw {}", self.data_dir.clone());

        for file in paths {
            let filepath = file.context("reading filename").unwrap().path();

            let dir_name = filepath
                .file_name()
                .and_then(|name| name.to_str())
                .expect("Failed to convert OsStr to String");

            println!("paths mfw {dir_name}");

            let mut tokens: Vec<&str> = dir_name.split("-").collect();
            tokens.reverse();
            println!("{}", tokens[1]);
            let level = tokens[1].parse::<usize>().context("parsing level num")?;

            if self.levels.len() > level {
                self.levels[level].nodes.push(TableNode {
                    path: String::from(dir_name.clone()),
                });

                self.levels[level]
                    .nodes
                    .sort_by_key(|node| node.path.clone());
            } else {
                self.levels.push(Level {
                    nodes: vec![TableNode {
                        path: String::from(dir_name.clone()),
                    }],
                });
            }
        }
        Ok(())
    }
}
