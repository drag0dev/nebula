use anyhow::{Result, Context};
use std::io::prelude::*;
use std::io::{stdin, stdout};

// TODO: add commands that would rely on Bloom Filter, SimHash, HLL and CMS
// TODO: ignore extra spacing between words in user input
// TODO: what can value be/what checks are supposed to be done
// TODO: error on extra words in command
// TODO: todo macros all are going to be replaced with actual calls to the write/read path

// aditional cli qol
// TODO: clear
// TODO: up arrow for previous commands
// TODO: tab
// TODO: left right arrows

#[derive(Debug, PartialEq)]
#[allow(non_camel_case_types)]
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
        if trimmed_input.len() == 0{ // skipping empty input
            continue;
        }

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
            let cmd_type = cmd_type.unwrap();
            if execute_command(trimmed_input, cmd_type).is_err(){
                return Ok(());
            }
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

/// returns error only when there was error executing command in write/read path
/// where the error handling is done, here used as an indicator to stop user input
fn execute_command(input: &str, cmd_type: CommandType) -> Result<(), ()>{
    match cmd_type{
        CommandType::GET => {
            let key = input.split(" ").nth(1);
            if let Some(key) = key{
                if !key.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }
                todo!();
            }else {
                println!("error: missing key");
                println!("usage: GET key (see HELP)");
                return Ok(());
            }
        },

        CommandType::PUT => {
            let key = input.split(" ").nth(1);
            if let Some(key) = key{
                if !key.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }
            }else{
                println!("error: missing key");
                println!("usage: PUT key value (see HELP)");
                return Ok(());
            }
            let key = key.unwrap();

            let value = input.split(" ").nth(2);
            if let Some(value) = value{
            }else{
                println!("error: missing value");
                println!("usage: PUT key value (see HELP)");
                return Ok(());
            }
            let value = value.unwrap();
            todo!();
        },

        CommandType::DELETE => {
            let key = input.split(" ").nth(1);
            if let Some(key) = key{
                if !key.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }
                todo!();
            } else{
                println!("error: missing key");
                println!("usage: DELETE key (see HELP)");
                return Ok(());
            }
        },
        CommandType::LIST => {
            let prefix = input.split(" ").nth(1);
            if let Some(prefix) = prefix{
                if !prefix.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: prefix must only contain printable ascii characters");
                    return Ok(());
                }
                // check for pagination
                let page_size = input.split(" ").nth(2);
                let page_number = input.split(" ").nth(3);

                if page_size.is_some() && page_number.is_some(){ // pagination
                    let page_size = page_size.unwrap();
                    let page_number = page_number.unwrap();
                    if !page_size.chars().all(|c| c.is_ascii_digit()){
                        println!("error: page-size must be whole number");
                        return Ok(());
                    }
                    if !page_number.chars().all(|c| c.is_ascii_digit()){
                        println!("error: page-number must be whole number");
                        return Ok(());
                    }
                    todo!();
                }else if page_size.is_some() || page_number.is_some(){
                        println!("error: missing pagination arguments");
                        println!("usage: LIST key [page-size page-number] (see HELP)");
                        return Ok(());
                }else{ // no pagination
                    todo!();
                }
            } else{
                println!("error: missing key");
                println!("usage: LIST key [page-size page-number] (see HELP)");
                return Ok(());
            }
        },
        CommandType::RANGE_SCAN => {
            let min = input.split(" ").nth(1);
            if let Some(min) = min{
                if !min.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: min must only contain printable ascii characters");
                    return Ok(());
                }
            }else{
                println!("error: missing min");
                println!("usage: RANGESCAN min max [page-size page-number]");
                return Ok(());
            }
            let min = min.unwrap();

            let max = input.split(" ").nth(2);
            if let Some(max) = max{
                if !min.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: min must only contain printable ascii characters");
                    return Ok(());
                }
            }else{
                println!("error: missing max");
                println!("usage: RANGESCAN min max [page-size page-number]");
                return Ok(());
            }
            let max = max.unwrap();

            // check for pagination
            let page_size = input.split(" ").nth(3);
            let page_number = input.split(" ").nth(4);

            if page_size.is_some() && page_number.is_some(){ // pagination
                let page_size = page_size.unwrap();
                let page_number = page_number.unwrap();
                if !page_size.chars().all(|c| c.is_ascii_digit()){
                    println!("error: page-size must be whole number");
                    return Ok(());
                }
                if !page_number.chars().all(|c| c.is_ascii_digit()){
                    println!("error: page-number must be whole number");
                    return Ok(());
                }
                todo!();
            }else if page_size.is_some() || page_number.is_some(){
                println!("error: missing pagination arguments");
                println!("usage: RANGESCAN min max [page-size page-number]");
                return Ok(());
            }else{ // no pagination
                todo!();
            }
        },
        _ => unreachable!(),
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
