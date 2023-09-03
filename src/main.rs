mod building_blocks;
mod cli;
mod engine;
mod repl;
mod test_data_gen;
mod utils;

use std::{path::Path, fs::{create_dir, remove_file, remove_dir_all}};
use crate::engine::Engine;
use anyhow::{Context, Error, Result, anyhow};
use clap::Parser;
use cli::CliCommands;
use test_data_gen::generate_test_data;

fn main() {
    let args = cli::CliCommands::parse();
    match args {
        CliCommands::Init => {
            if let Err(e) = initialize_fs() {
                print_err(e);
            } else {
                println!("successfully initialized");
            }
        }
        CliCommands::Clear => {
            if let Err(e) = clear_fs() {
                print_err(e);
            } else {
                println!("successfully cleared");
            }
        }
        CliCommands::Start => {
            let engine = Engine::new().context("instantiating engine");
            if let Err(e) = engine {
                print_err(e);
            } else {
                let mut engine = engine.unwrap();

                if let Err(e) =  engine.start().context("starting engine") {
                    print_err(e);
                }
            }

        }
        CliCommands::GenerateTestData => {
            if let Err(e) = generate_test_data() {
                print_err(e);
            } else {
                println!("successfully generated test data");
            }
        }
        CliCommands::DummyData { file_name } => todo!(),
    }
}

fn print_err(e: Error) {
    println!("error: {}", e);
    for (i, small_e) in e.chain().enumerate().skip(1) {
        println!("{}{small_e}", "\t".repeat(i));
    }
}

// creates all the directories and generates a default config
// data -
//      -> table_data
//      -> WAL
// TODO: generate default config
fn initialize_fs() -> Result<()> {
    // top "data" dir
    let path = Path::new("./data");
    if !path.is_dir() {
        create_dir("./data").context("creating './data' dir")?;
    }

    // "table_data" dir
    let path = Path::new("./data/table_data");
    if !path.is_dir() {
        create_dir("./data/table_data").context("creating './data/table_data' dir")?;
    }

    // "WAL" dir
    let path = Path::new("./data/WAL");
    if !path.is_dir() {
        create_dir("./data/WAL").context("creating './data/WAL' dir")?;
    }
    Ok(())
}

fn clear_fs() -> Result<()> {
    if Path::new("./data").is_dir() {
        let path = Path::new("./data/table_data");
        if path.is_dir() {
            let entries = path.read_dir().context("reading 'table_data'")?;
            for entry in entries {
                let entry = entry.context("reading dir in 'table_data'")?;
                remove_dir_all(entry.path()).context("removing file in 'table_data'")?;
            }
        } else {
            return Err(anyhow!("missing dir './data/table_data'"));
        }

        let path = Path::new("./data/WAL");
        if path.is_dir() {
            let entries = path.read_dir().context("reading 'WAL'")?;
            for entry in entries {
                let entry = entry.context("reading file in 'WAL'")?;
                remove_file(entry.path()).context("removing file in 'WAL'")?;
            }
        } else {
            return Err(anyhow!("missing dir './data/WAL'"));
        }
    } else {
        return Err(anyhow!("missing dir './data'"));
    }
    Ok(())
}
