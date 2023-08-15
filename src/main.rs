mod utils;
mod building_blocks;
mod repl;

fn main() {
    loop {
        let q = repl::repl().unwrap();
        match q.commands {
            repl::Commands::Quit => break,
            _ => {},
        }
    }
}
