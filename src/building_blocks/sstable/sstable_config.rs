use serde::{Serialize,Deserialize};
// TODO: derive serialization for RON?

#[derive(Serialize, Deserialize, Debug)]
pub struct SSTableConfig {
    pub file_organization: FileOrganization,

    // TODO: assert that this is > 2
    /// every n key make an entry in the summary
    pub summary_nth: u64,

    /// filter false positive probability
    pub filter_fp_prob: f64,
}

#[derive(Clone)]
#[derive(PartialEq)]
#[derive(Serialize, Deserialize, Debug)]
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
