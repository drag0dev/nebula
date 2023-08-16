mod utils;
mod building_blocks;
mod repl;
mod cli;
mod test_data_gen;

use clap::Parser;
use cli::CliCommands;
use test_data_gen::generate_test_data;
use anyhow::Error;

fn main() {
    let args  = cli::CliCommands::parse();
    match args {
        CliCommands::Initialize => todo!(),
        CliCommands::Delete => todo!(),
        CliCommands::Start => todo!(),
        CliCommands::GenerateTestData => {
            if let Err(e) = generate_test_data() {
                print_err(e);
            } else {
                println!("successfully generated test data");
            }
        },
        CliCommands::DummyData { file_name } => todo!(),
    }
}

fn print_err(e: Error) {
    for (i, small_e) in e.chain().enumerate() {
        println!("{}{small_e}", "\t".repeat(i));
    }
}
