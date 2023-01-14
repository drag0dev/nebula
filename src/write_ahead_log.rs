use anyhow::{Context, Result};
use crc::{Crc, CRC_32_ISCSI};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::io::Write;
use std::rc::Rc;

struct WALEntry {
    crc: u32,
    timestamp: u128,
    tombstone: bool,
    key_size: u64,
    value_size: u32,
    key: Rc<[u8]>,
    value: Rc<[u8]>,
}

pub struct WriteAheadLog {
    crc: Crc<u32>,
    file_size: u64,   // size of the wal file
    file: MmapMut,    // wal mapped to mem
    write_start: u64, // byte offeset when to insert next wal record
}

impl WriteAheadLog {
    pub fn new(file_size: u64) -> Result<Self> {
        let crc = Crc::<u32>::new(&CRC_32_ISCSI);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .open("ker.dat")
            .context("creating WAL")?;

        file.set_len(file_size).context("set file length")?;

        let mut mmap = unsafe { MmapMut::map_mut(&file).context("mmaping a file")? };

        Ok(WriteAheadLog {
            crc,
            file_size,
            file: mmap,
            write_start: 0,
        })
    }

    //pub fn transaction(&mut self, timestamp: u128, key: Rc<[u8]>, value: Rc<[u8]>) -> Result<()> {
    // TODO: log if we try to write more or make sure memtable never does it to
    // know that there is a potential error
    //   Ok(())
    //}
    // TODO: need parse query types

    pub fn flush(&mut self) -> Result<()> {
        // flush wal in memory to disk
        // reset byte offset
        self.write_start = 0;

        todo!();
    }
}
