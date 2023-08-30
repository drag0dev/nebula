use crate::building_blocks::sstable::LSMTreeUnderlying;

pub struct TableNode {
    path: String,
}

// NOTE: this could be replaced with a BTree for increased efficiency
// but this is simpler and just works for now.
pub struct Level {
    nodes: Vec<TableNode>,
}

pub struct LSMTree<S: LSMTreeUnderlying> {
    levels: Vec<Level>,
    // level size ?
    // tier size ?
    // tables per tier ?
    fp_prob: f64,     // bloomfilter false positive probability
    summary_nth: u64, // idk
    data_dir: String,
    size_threshold: usize,
    last_table: usize,
    tables_item_counts: Vec<u64>,
    marker: std::marker::PhantomData<S>,
}
