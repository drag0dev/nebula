use std::path::PathBuf;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "nebula")]
pub enum CliCommands {
    /// create a new instance of the database, if it already exists it will fail
    Init,

    /// deletes all the data excluding the config
    Clear,

    /// start the database
    Start,

    /// generates test data
    GenerateTestData,

    /// executes query from a prepared file against the database
    DummyData { file_name: PathBuf }
}
