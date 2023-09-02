use std::io::Write;
use anyhow::{Result, Context};
use memmap2::MmapMut;
use crate::building_blocks::Entry;
use super::{get_next_index, create_file, purge_all_files};

pub struct WriteAheadLog {
    current_file: Option<MmapMut>,

    // amount of bytes written to the current file
    current_file_len: usize,

    /// in bytes
    segment_size: u64,

    // path to the wal segments
    path: String
}

impl WriteAheadLog {
    /// wal_folder - where the segments are stored
    /// segment_size - size of each segment in bytes
    pub fn new(wal_folder: &str, segment_size: u64) -> Result<Self> {
        let mut s = WriteAheadLog {
            current_file: None,
            current_file_len: 0,
            segment_size,
            path: wal_folder.to_owned()
        };
        s.generate_next_file().context("creating a segment")?;
        Ok(s)
    }

    pub fn add(&mut self, entry: &Entry) -> Result<()> {
        let entry_ser = entry.serialize()?;

        if (entry_ser.len() + self.current_file_len) as u64 > self.segment_size || self.current_file.is_none() {
            self.generate_next_file().context("creating a new segment")?;
        }

        if let Some(file) = self.current_file.as_mut() {
            (&mut file[self.current_file_len..])
                .write_all(&entry_ser)
                .context("writing entry")?;
        } else {unreachable!()}

        self.current_file_len += entry_ser.len();
        Ok(())
    }

    /// remove all files on the disk inlcuding the one that is mapped to mem
    /// only to be called once the memtable is flushed successfully
    pub fn purge(&mut self) -> Result<()> {
        self.current_file = None;
        self.current_file_len = 0;
        Ok(purge_all_files(&self.path)?)
    }

    fn generate_next_file(&mut self) -> Result<()> {
        let next_index = get_next_index(&self.path)
            .context("getting the next index available")?;

        let current_file = create_file(&self.path, next_index, self.segment_size)
                .context("creating a new file")?;

        self.current_file = Some(current_file);
        self.current_file_len = 0;
        Ok(())
    }
}
