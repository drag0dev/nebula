mod building_blocks;
mod cli;
mod engine;
mod repl;
mod test_data_gen;
mod utils;

use crate::engine::Engine;
use anyhow::{Context, Error, Result};
use clap::Parser;
use cli::CliCommands;
use test_data_gen::generate_test_data;

fn main() -> Result<()> {
    let args = cli::CliCommands::parse();
    match args {
        CliCommands::Initialize => todo!(),
        CliCommands::Delete => todo!(),
        CliCommands::Start => {
            let engine = Engine::new().context("oh goober!");
            if let Err(e) = engine {
                print_err(e);
            } else {
                let mut engine = engine.unwrap();

                if let Err(e) =  engine.start().context("starting engine") {
                    print_err(e);
                }
            }

            Ok(())
        }
        CliCommands::GenerateTestData => {
            if let Err(e) = generate_test_data() {
                print_err(e);
            } else {
                println!("successfully generated test data");
            }
            Ok(())
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
