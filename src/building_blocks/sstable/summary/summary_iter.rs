use crate::building_blocks::BINCODE_OPTIONS;
use super::summary::MAX_SUMMARY_ENTRY_LEN;
use anyhow::{Result, Context, anyhow};
use bincode::Options;
use std::{fs::File, io::Read};

use super::SummaryEntry;


pub struct SummaryIterator {
    file: File,
}

impl SummaryIterator {
    pub fn iter(file: File) -> Self {
        SummaryIterator { file }
    }
}

impl Iterator for SummaryIterator {
    type Item = Result<SummaryEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut len_ser = vec![0; 8];
        let res = self.file.read_exact(&mut len_ser);
        if let Err(e) = res.as_ref() {
            return match e.kind() {
                std::io::ErrorKind::UnexpectedEof => None,
                _ => Some(Err(res.context("reading summary entry len").err().unwrap()))
            };
        }

        let len = BINCODE_OPTIONS
            .deserialize(&len_ser)
            .context("deserializing summary entry len");
        if let Err(e) = len { return Some(Err(e)); }
        let len: u64 = len.unwrap();

        if len > MAX_SUMMARY_ENTRY_LEN {
            let e = anyhow!("corrupted summary entry len");
            return Some(Err(e));
        }

        let mut entry_ser = vec![0; len as usize];
        let res = self.file.read_exact(&mut entry_ser)
            .context("reading summary entry");
        if let Err(e) = res { return Some(Err(e)) }

        let entry = BINCODE_OPTIONS
            .deserialize(&entry_ser)
            .context("deserializing summary entry");
        if let Err(e) = entry { return Some(Err(e)); }

        Some(entry)
    }
}
