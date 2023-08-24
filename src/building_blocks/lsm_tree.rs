use sstable::SSTableReaderSingleFile;
use std::time::SystemTime;

use crate::building_blocks::bloomfilter;
use crate::building_blocks::sstable;
use crate::building_blocks::Memtable;
use crate::utils::merge_sort;

use super::MemtableEntry;
use super::SummaryEntry;

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

// will remove I was confused 
impl Clone for TableNode {
    fn clone(&self) -> TableNode {
        TableNode {
            path: self.path.clone()
        }
    }
}

impl Clone for Level {
    fn clone(&self) -> Level {
        Level {
            nodes: self.nodes.clone()
        }
    }
}

pub struct LSMTree {
    //    wal: WriteAheadLog,
    levels: Vec<Level>,
    // level size
    // tier size ?
    // tables per tier ?
    fp_prob: f64,     // bloomfilter false positive probability
    item_count: u64,  // bloomfilter item count
    summary_nth: u64, // idk
}

impl LSMTree {
    pub fn new(item_count: u64, fp_prob: f64, summary_nth: u64) -> Self {
        LSMTree {
            levels: vec![
                Level { nodes: vec![] },
                Level { nodes: vec![] },
                Level { nodes: vec![] },
            ],
            fp_prob,
            item_count,
            summary_nth,
        }
    }

    fn insert(&mut self, path: &str) {
        let node = TableNode {
            path: String::from(path),
        };
        self.levels[0].nodes.push(node);
    }

    // TODO: RESOLVE TOMBSTONES AND UPDATES
    fn compact(&mut self, level_num: usize, data_dir: &str, generation: &str) -> Result<(), ()> {
        let mut builder = sstable::SSTableBuilderSingleFile::new(
            "test-data",
            "mama",
            self.item_count,
            self.fp_prob,
            self.summary_nth,
        )
        .unwrap();

        let mut iterators: Vec<_> = self.levels[level_num]
            .nodes
            .iter()
            .map(|table| {
                SSTableReaderSingleFile::load(&(format!("test-data/{}", table.path)))
                    .unwrap()
                    .iter()
                    .unwrap()
                    .into_iter()
                    .peekable()
            })
            .collect();

        loop {
            let smallest = iterators
                .iter_mut()
                .enumerate()
                .filter_map(|(idx, iter)| iter.peek().map(|value| (value, idx)))
                .min_by_key(|&(value, _)| value.as_ref().unwrap().key.clone());

            match smallest {
                Some((_, idx)) => {
                    // consume the value from the corresponding iterator
                    let entry = iterators[idx].next().unwrap().unwrap();

                    builder.insert_entry(&entry).unwrap();
                }
                None => break, // break when all iterators are exhausted
            }
        }

        builder.finish_data().expect("finishing big sstable");

        self.levels[level_num].nodes.clear();
        self.levels[level_num + 1].nodes.push(TableNode { path: String::from("mama") });

        // let it = sstable::SSTableReaderSingleFile::load(&format!("test-data/{}", self.levels[level_num + 1].nodes[0].path)).unwrap().iter().unwrap();
        let it = sstable::SSTableReaderSingleFile::load(&"test-data/mama").unwrap().iter().unwrap();
        println!("ok reading joe");
        let mut i = 0;
        
        for n in it {
            if i > 30 { break; }
            let nn = n.unwrap();
            println!("entry: {nn:?}");
            i += 1;
        }


        Ok(())
    }
}

#[test]
fn lsm_insert() {
    let mut lsm = LSMTree::new(100, 0.1, 10);

    for j in 0..20 {
        let path = format!("lsm-write-singlefile{}", j);
        let mut builder = sstable::SSTableBuilderSingleFile::new("test-data", &path, 100, 0.1, 10)
            .expect("creating a sstable");

        for i in 0..100 {
            let k = i + (j * 100);
            let entry = crate::building_blocks::Entry {
                timestamp: k,
                key: k.to_string().into_bytes(),
                value: Some(k.to_string().into_bytes()),
            };

            builder
                .insert_entry(&entry)
                .expect("inserting entry into the sstable");
        }
        builder.finish_data().expect("finishing big sstable");

        lsm.insert(&path);
    }

    println!("joe {:?}", lsm.levels);

    lsm.compact(0, "joe", "mama").unwrap();
    println!();

    println!("joe {:?}", lsm.levels);
        assert!(1+1==1);
}

#[test]
fn check_validity() {
    let sstable_reader = SSTableReaderSingleFile::load("test-data/mama")
        .expect("reading sstable");

    // test sstable entries
    let mut i = 0;
    for entry in sstable_reader.iter().expect("getting sstable iter") {
        let entry = entry.expect("reading entry");
        let expected_entry = crate::building_blocks::Entry {
            key: i.to_string().into_bytes(),
            value: Some(i.to_string().into_bytes()), timestamp: i,
        };

        println!("i: {i} entry: {entry:?} expct: {expected_entry:?}");

        assert_eq!(entry, expected_entry);
        i += 1;
    }

    // test index
    let mut data_iter = sstable_reader.iter().expect("getting data iter");
    let random_entry_index = sstable_reader.index_iter().expect("getting index iter")
        .nth(10)
        .unwrap()
        .expect("reading eleventh entry in the index");

    data_iter.move_iter(random_entry_index.offset)
        .expect("moving sstable iter");

    let random_entry_read = data_iter.next().unwrap().expect("reading random sstable entry");
    assert_eq!(random_entry_read.key, 10.to_string().into_bytes());

    // test summary
    let (mut summary_iter, _) = sstable_reader
        .summary_iter()
        .expect("getting summary iter");

    let random_entry_summary = summary_iter
        .nth(5)
        .unwrap()
        .expect("getting fifth entry in the summary");

    let mut index_iter = sstable_reader.index_iter().expect("getting index iter");
    index_iter.move_iter(random_entry_summary.offset).expect("moving index iter");

    let index_entry = index_iter.next().unwrap().expect("reading random index entry");
    assert_eq!(index_entry.key, 50.to_string().into_bytes());

    let filter = sstable_reader.read_filter().expect("getting filter");
    // test filter
    for i in 0..100 {
        let check = filter.check(&i.to_string().into_bytes()).expect("checking key in the filter");
        assert_eq!(check, true);
    }
}
