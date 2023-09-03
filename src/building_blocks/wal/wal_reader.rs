use anyhow::{Result, Context};
use bincode::Options;
use std::{fs::{read_dir, File, OpenOptions}, io::Read};
use crate::building_blocks::{Entry, BINCODE_OPTIONS};
use super::{get_next_index_avaiable, get_valid_path_names};

/// reads the wal folders and yields a vector of entries for each segment
/// going from oldest to the newest segment, but entries themsevles are
/// going from newest to the oldest
pub struct WriteAheadLogReader {
    files: Vec<String>,
    path: String,
}

impl WriteAheadLogReader {
    /// path to the wal folder
    pub fn iter(path: &str) -> Result<Self> {
        let paths = read_dir(path)
            .context("reading wal folder")?;

        let mut file_names = get_valid_path_names(paths)?;
        _ = get_next_index_avaiable(&file_names)
            .context("check if there are files missing")?;

        file_names.sort_unstable_by(|a, b| {
            let a_index = a.split('-').last().unwrap().parse::<u64>().unwrap();
            let b_index = b.split('-').last().unwrap().parse::<u64>().unwrap();
            a_index.partial_cmp(&b_index).unwrap()
        });
        file_names.reverse();

        Ok(WriteAheadLogReader {
            files: file_names,
            path: path.to_owned()
        })
    }
}

impl Iterator for WriteAheadLogReader {
    type Item = Result<Vec<Entry>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut entries = Vec::new();
        let file_name = if let Some(_) = self.files.last() {
            self.files.pop().unwrap()
        } else {
            return None;
        };

        let file_name = format!("{}/{}", self.path, file_name);
        let file = OpenOptions::new()
            .read(true)
            .open(file_name)
            .context("opening file '{file_name}'");
        if let Err(e) = file { return Some(Err(e)); }
        let mut file = file.unwrap();

        loop {
            let entry = read_entry(&mut file)
                .context("reading entry");
            if let Err(e) = entry { return Some(Err(e)); }
            else {
                let entry = entry.unwrap();
                if entry.is_none() { break; }
                else { entries.push(entry.unwrap()); }
            }
        }

        Some(Ok(entries))
    }
}

/// reads until it reaches end of the file or len 0
fn read_entry(file: &mut File) -> Result<Option<Entry>> {
    let mut len_ser = vec![0; 8];
    let res = file.read_exact(&mut len_ser);
    if let Err(e) = res.as_ref() {
        return match e.kind() {
            std::io::ErrorKind::UnexpectedEof => Ok(None),
            _ => Err(res.context("reading index entry len").err().unwrap())
        };
    }

    let len: u64 = BINCODE_OPTIONS
        .deserialize(&len_ser)
        .context("deserializing entry len")?;

    if len == 0 {
        return Ok(None);
    }

    let mut entry_ser = vec![0; (len + 4) as usize];
    file.read_exact(&mut entry_ser[..])
        .context("reading entry")?;

    let entry = Entry::deserialize(&entry_ser[..])
        .context("deserializing entry")?;

    Ok(Some(entry))
}
