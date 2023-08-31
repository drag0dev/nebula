mod wal;
mod utils;

pub use wal::WriteAheadLog;
pub(super) use utils::get_next_index_avaiable;
pub(super) use utils::create_file;
pub(super) use utils::purge_all_files;
