use std::io::{stdin, stdout, Write};
use anyhow::{Result, Context};
use clap::Parser;
use super::Repl;

pub fn repl() -> Result<Repl> {
    let stdin = stdin();
    let mut stdout = stdout();
    let mut buff = String::new();
    loop {
        print!(">> ");
        stdout.flush()
            .context("flushing prompt to stdout")?;

        buff.clear();
        stdin.read_line(&mut buff)
            .context("reading user input")?;

        let query = parse_from_str(&buff);
        if let Err(e) = query {
            println!("{}", e);
        } else {
            return Ok(query.unwrap())
        }
    }
}

pub fn parse_from_str(input: &str) -> Result<Repl> {
    let mut command = vec![""];
    command.extend(input
            .split(' ')
            .map(|p| p.trim()));
    Ok(Repl::try_parse_from(command)?)
}
