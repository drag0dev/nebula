// TODO: derive serialization for RON?
pub struct SSTableConfig {
    file_organization: FileOrganization,

    // TODO: assert that this is > 2
    /// every n key make an entry in the summary
    summary_nth: u64,

    /// filter false positive probability
    filter_fp_prob: f64,
}

#[derive(PartialEq)]
pub enum FileOrganization {
    SingleFile(()),
    MultiFile(()),
}

#[derive(PartialEq)]
pub struct SF(());

#[derive(PartialEq)]
pub struct MF(());

pub trait LSMTreeUnderlying {}
impl LSMTreeUnderlying for SF {}
impl LSMTreeUnderlying for MF {}
