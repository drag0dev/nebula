mod utils;
mod parser;
mod building_blocks;

fn main() {
    let quit_status = parser::cli();
    if let Some(err) = quit_status.err(){
        println!("error: encountered while parsing command\n{:?}", err);
    }
}
