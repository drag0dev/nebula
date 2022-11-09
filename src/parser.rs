use anyhow::{Result, Context};
use std::io::prelude::*;
use std::io::{stdin, stdout};

// TODO: add commands that would rely on Bloom Filter, SimHash, HLL and CMS
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

        let tokens = get_tokens(trimmed_input);

        let cmd = tokens.get(0);
        if cmd.is_none(){
            println!("error: missing command");
            println!("usage: command [...] (see HELP)");
            continue;
        }

        let cmd_type = command_type(cmd.unwrap());
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
            if execute_command(&tokens, cmd_type).is_err(){
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
fn execute_command(tokens: &Vec<&str>, cmd_type: CommandType) -> Result<(), ()>{
    match cmd_type{
        CommandType::GET => {
            if tokens.len() < 2{
                println!("error: missing key");
                println!("usage: GET key (see HELP)");
                return Ok(());
            }
            let mut keys: Vec<&str> = Vec::with_capacity(tokens.len() - 1);

            for key in tokens.iter().skip(1){
                if !key.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }
                keys.push(key);
            }
            todo!();
        },

        CommandType::PUT => {
            if tokens.len() < 2 {
                println!("error: missing key");
                println!("usage: PUT key value (see HELP)");
                return Ok(());
            }else if tokens.len() < 3{
                println!("error: missing value");
                println!("usage: PUT key value (see HELP)");
                return Ok(());
            }else if ((tokens.len() - 1) % 2) !=  0 {
                println!("error: missing value");
                println!("usage: PUT key value (see HELP)");
                return Ok(());
            }
            let mut keys: Vec<&str> = Vec::with_capacity((tokens.len() - 1) / 2);
            let mut values: Vec<&str> = Vec::with_capacity((tokens.len() - 1) / 2);

            for (i, item) in tokens.iter().skip(1).enumerate(){
                if i % 2 == 0 && !item.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }else {}

                if i % 2 == 0{
                    keys.push(item);
                }else{
                    values.push(item);
                }
            }
            todo!();
        },

        CommandType::DELETE => {
            if tokens.len() < 2{
                println!("error: missing key");
                println!("usage: DELETE key (see HELP)");
                return Ok(());
            }
            let mut keys: Vec<&str> = Vec::with_capacity(tokens.len() - 1);

            for key in tokens.iter().skip(1){
                if !key.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: key must only contain printable ascii characters");
                    return Ok(());
                }
                keys.push(key);

            }
            todo!();
        },
        CommandType::LIST => {
            if tokens.len() > 4 {
                    println!("error: unrecognized additional arguments");
                    println!("usage: LIST key [page-size page-number] (see HELP)");
                    return Ok(());
            }
            let prefix = tokens.get(1);
            if let Some(prefix) = prefix{
                if !prefix.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: prefix must only contain printable ascii characters");
                    return Ok(());
                }
                // check for pagination
                let page_size = tokens.get(2);
                let page_number = tokens.get(3);

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
            if tokens.len() > 5{
                println!("error: unrecognized additional arguments");
                println!("usage: RANGESCAN min max [page-size page-number]");
                return Ok(());
            }
            let min = tokens.get(1);
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

            let max = tokens.get(2);
            if let Some(max) = max{
                if !min.chars().all(|c| c.is_ascii_graphic()){
                    println!("error: max must only contain printable ascii characters");
                    return Ok(());
                }
            }else{
                println!("error: missing max");
                println!("usage: RANGESCAN min max [page-size page-number]");
                return Ok(());
            }
            let max = max.unwrap();

            // check for pagination
            let page_size = tokens.get(3);
            let page_number = tokens.get(4);

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

/// get vec of slices of all tokens in the provided String
fn get_tokens<'a>(input: &'a str) -> Vec<&'a str>{
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut end;
    let mut iter = input.chars().peekable();

    let mut next_char: Option<&char>;
    'outer: loop{
        // get to the start of a word
        next_char = iter.peek();
        if next_char.is_none(){ // if there is no more chars break
            break 'outer;
        }else if !next_char.unwrap().is_ascii_graphic(){
            loop{ // get to the first printable char
                iter.next();
                start += 1;
                if let Some(c) = iter.by_ref().peek(){ // if next char
                    if c.is_ascii_graphic(){
                        break;
                    }
                }else{
                    break 'outer; // if no printable char was found, break
                }
            }
        }

        // get to the end of a word
        end = start;
        for c in iter.by_ref(){
            if !c.is_ascii_graphic(){
                break;
            }
            end += 1;
        }

        tokens.push(&input[start..end]);
        start = end+1; // step over the non graphic char
    }
    tokens
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

        // mixed case
        assert_eq!(Some(CommandType::GET), command_type("gEt"));
        assert_eq!(Some(CommandType::PUT), command_type("PuT"));
        assert_eq!(Some(CommandType::DELETE), command_type("deLETe"));
        assert_eq!(Some(CommandType::LIST), command_type("lIsT"));
        assert_eq!(Some(CommandType::RANGE_SCAN), command_type("RANGEscan"));
        assert_eq!(Some(CommandType::QUIT), command_type("qUIT"));
        assert_eq!(Some(CommandType::HELP), command_type("heLP"));

        // wrong cmd
        assert_eq!(None, command_type("123"));
        assert_eq!(None, command_type("gett"));
        assert_eq!(None, command_type("1get"));
        assert_eq!(None, command_type("RANGE SCAN"));
    }

    #[test]
    fn tokens(){
        assert_eq!(get_tokens(" aa aa aa"), vec!["aa", "aa", "aa"]);
        assert_eq!(get_tokens("aaaaaa"), vec!["aaaaaa"]);
        assert_eq!(get_tokens("").len(), 0);
        assert_eq!(get_tokens("   aa    aa aa    "), vec!["aa", "aa", "aa"]);
        assert_eq!(get_tokens("   aa \n\t\t\t\0                           a aa "), vec!["aa", "a", "aa"]);
        assert_eq!(get_tokens("aa\taa\naa\0aa"), vec!["aa", "aa", "aa", "aa"]);
    }
}
