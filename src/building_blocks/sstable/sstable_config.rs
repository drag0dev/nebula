// TODO: derive serialization for RON?
pub struct SSTableConfig {
    file_organization: FileOrganization,

    // TODO: assert that this is > 2
    /// every n key make an entry in the summary
    summary_nth: u64,

    /// filter false positive probability
    filter_fp_prob: f64,
}

pub enum FileOrganization {
    SingleFile,
    MultipleFiles,
}
