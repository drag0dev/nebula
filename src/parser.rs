use anyhow::{Result, Context};
use std::io::{stdin, stdout};
use std::io::prelude::*;

// TODO: add commands that would rely on Bloom Filter, SimHash, HLL and CMS
// TODO: only accept printable chars as keys
// TODO: clear?

#[derive(Debug, PartialEq)]
pub enum CommandType{
    GET, PUT, DELETE, LIST, RANGE_SCAN, QUIT, HELP
}

/// interactiv prompt where user types commands
/// the functions ends if the user types QUIT or there is an error
pub fn cli() -> Result<()>{
    let stdin = stdin();
    let mut stdout = stdout();
    let mut input: String = String::new();
    loop{
        print!("> ");
        stdout.flush().context("flushing '>' to the stdout")?;
        input.clear();
        stdin.read_line(&mut input).context("reading user input")?;
        let trimmed_input = input.trim();
        let cmd_type = command_type(trimmed_input);
        if cmd_type == None{
            println!("error: invalid command");
            println!("usage: command [...] (see HELP)");
        }else if cmd_type == Some(CommandType::QUIT){
            println!("quitting");
            break;
        }else if cmd_type == Some(CommandType::HELP){
            help_print();
        }else{

        }
    }
    Ok(())
}

/// case insensitive
fn command_type(input: &str) -> Option<CommandType>{
    let first_word = input.split(" ").nth(0);
    if let Some(cmd) = first_word{
        let cmd = cmd.to_uppercase();
        return match &cmd[..] {
            "GET" => Some(CommandType::GET),
            "PUT" => Some(CommandType::PUT),
            "DELETE" => Some(CommandType::DELETE),
            "LIST" => Some(CommandType::LIST),
            "RANGESCAN" => Some(CommandType::RANGE_SCAN),
            "QUIT" => Some(CommandType::QUIT),
            "HELP" => Some(CommandType::HELP),
            _ => None,
        }
    }else{
        return None;
    }
}

fn help_print(){
    println!("----------HELP----------");
    println!("GET key");
    println!("PUT key value");
    println!("DELETE key");
    println!("LIST prefix [page-size page-number]");
    println!("RANGESCAN min max [page-size page-number]");
    println!("QUIT");
    println!("HELP");
    println!("------------------------");
}

#[cfg(test)]
mod tests{
    use super::*;
    #[test]
    fn uppercase_cmd_type(){
        assert_eq!(Some(CommandType::GET), command_type("GET"));
        assert_eq!(Some(CommandType::PUT), command_type("PUT"));
        assert_eq!(Some(CommandType::DELETE), command_type("DELETE"));
        assert_eq!(Some(CommandType::LIST), command_type("LIST"));
        assert_eq!(Some(CommandType::RANGE_SCAN), command_type("RANGESCAN"));
        assert_eq!(Some(CommandType::QUIT), command_type("QUIT"));
        assert_eq!(Some(CommandType::HELP), command_type("HELP"));
    }

    #[test]
    fn lowercase_cmd_type(){
        assert_eq!(Some(CommandType::GET), command_type("get"));
        assert_eq!(Some(CommandType::PUT), command_type("put"));
        assert_eq!(Some(CommandType::DELETE), command_type("delete"));
        assert_eq!(Some(CommandType::LIST), command_type("list"));
        assert_eq!(Some(CommandType::RANGE_SCAN), command_type("rangescan"));
        assert_eq!(Some(CommandType::QUIT), command_type("quit"));
        assert_eq!(Some(CommandType::HELP), command_type("help"));
    }

    #[test]
    fn mixed_case_cmd_type(){
        assert_eq!(Some(CommandType::GET), command_type("gEt"));
        assert_eq!(Some(CommandType::PUT), command_type("PuT"));
        assert_eq!(Some(CommandType::DELETE), command_type("deLETe"));
        assert_eq!(Some(CommandType::LIST), command_type("lIsT"));
        assert_eq!(Some(CommandType::RANGE_SCAN), command_type("RANGEscan"));
        assert_eq!(Some(CommandType::QUIT), command_type("qUIT"));
        assert_eq!(Some(CommandType::HELP), command_type("heLP"));
    }

    #[test]
    fn wrong_cmd_type(){
        assert_eq!(None, command_type("123"));
        assert_eq!(None, command_type("gett"));
        assert_eq!(None, command_type("1get"));
        assert_eq!(None, command_type("RANGE SCAN"));
    }
}
