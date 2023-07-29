// TODO: derive serialization for RON?
pub struct SSTableConfig {
    file_organization: FileOrganization,
}

pub enum FileOrganization {
    SingleFile,
    MultipleFiles,
}
