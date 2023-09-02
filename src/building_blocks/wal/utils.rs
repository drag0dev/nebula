use anyhow::{Result, Context, anyhow};
use std::fs::{read_dir, ReadDir, remove_file};
use regex::Regex;
use memmap2::MmapMut;

/// reads the wal folder, checks that all files have a valid name
/// returns index of the last file written + 1
pub fn get_next_index(wal_folder: &str) -> Result<usize> {
    let paths = read_dir(wal_folder)
        .context("reading wal folder")?;
    let file_names = get_valid_path_names(paths)?;
    get_next_index_avaiable(&file_names)
}

pub fn get_valid_path_names(input: ReadDir) -> Result<Vec<String>> {
    let segment_name_re = Regex::new(r"^segment-\d+$").unwrap();
    let mut file_names = Vec::new();

    for path in input {
        let path = path.context("reading a path in wal folder")?;

        let file_name: String = path
            .file_name()
            .to_str().context("getting file name")?
            .to_owned();
        if !segment_name_re.is_match(&file_name) {
            let e = anyhow!("'{}' is not a segment file", file_name);
            return Err(e);
        }

        let file_meta = path.metadata().context("getting metadata")?;
        if file_meta.is_file() {
            file_names.push(file_name);
        } else {
            let e = anyhow!("fs entry '{}' is not a file", file_name);
            return Err(e);
        }
    }

    Ok(file_names)
}

pub fn get_next_index_avaiable(input: &Vec<String>) -> Result<usize> {
    let mut indices: Vec<_> = input
        .iter()
        .map(|path| {
            let parts = path.split('-');
            // its safe to unwrap since regex guarantees that there is exactly one '-'
            // and a number after it
            parts.last().unwrap().parse::<usize>().unwrap()
        })
    .collect();
    indices.sort_unstable();

    // check if all indices exist
    for (i, index) in indices.iter().enumerate() {
        if *index != i {
            let e = anyhow!("segment with index '{i}' is missing");
            return Err(e);
        }
    }

    if indices.len() >= 1 {
        Ok(*indices.last().unwrap()+1)
    } else {
        Ok(0)
    }
}

pub fn create_file(dir: &str, index: usize, file_size: u64) -> Result<MmapMut> {
    let file_name = format!("{dir}/segment-{index}");

    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(file_name)
        .context("creating and opening file '{file_name}'")?;

    file.set_len(file_size)
        .context("setting length of the file '{file_name}'")?;

    let map = unsafe {
        MmapMut::map_mut(&file)
            .context("mapping file '{file_name}'")?
    };

    Ok(map)
}

/// the currently opened file has to be closed before calling this
pub fn purge_all_files(dir: &str) -> Result<()> {
    let paths = read_dir(dir)
        .context("reading wal folder")?;
    for path in paths {
        let path = path.context("reading a path in wal folder")?;
        remove_file(path.path())
            .context("removing file '{path}'")?;
    }
    Ok(())
}
