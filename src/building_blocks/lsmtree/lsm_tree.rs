use crate::building_blocks::sstable::LSMTreeUnderlying;

#[derive(Debug)]
pub struct TableNode {
    pub(super) path: String,
}

// NOTE: this could be replaced with a BTree for increased efficiency
// but this is simpler and just works for now.
#[derive(Debug)]
pub struct Level {
    pub(super) nodes: Vec<TableNode>,
}

pub struct LSMTree<S: LSMTreeUnderlying> {
    pub(super) levels: Vec<Level>,
    // level size ?
    // tier size ?
    // tables per tier ?
    pub(super) fp_prob: f64,
    pub(super) summary_nth: u64,
    pub(super) data_dir: String,
    pub(super) size_threshold: usize,
    pub(super) last_table: usize,
    pub(super) marker: std::marker::PhantomData<S>,
}
