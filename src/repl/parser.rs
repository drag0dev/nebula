use std::io::{stdin, stdout, Write, Stdin, Stdout};
use anyhow::{Result, Context};
use clap::Parser;
use super::Repl;

pub struct REPL {
    stdin: Stdin,
    stdout: Stdout,
}

impl REPL {
    pub fn new() -> Self {
        REPL {
            stdin: stdin(),
            stdout: stdout(),
        }
    }

    pub fn get_query(&mut self) -> Result<Repl> {
        let mut buff = String::new();
        loop {
            print!(">> ");
            self.stdout.flush()
                .context("flushing prompt to stdout")?;

            buff.clear();
            self.stdin.read_line(&mut buff)
                .context("reading user input")?;

            let query = parse_from_str(&buff);
            if let Err(e) = query {
                println!("{}", e);
            } else {
                return Ok(query.unwrap())
            }
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
