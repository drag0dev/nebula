mod utils;
mod building_blocks;
mod repl;
mod cli;

use clap::Parser;

fn main() {
    let args  = cli::CliCommands::parse();
}
