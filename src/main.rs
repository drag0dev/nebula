mod bloomfilter;
mod count_min_sketch;
mod utils;
mod hyperloglog;
mod parser;

fn main() {
    let quit_status = parser::cli();
    if let Some(err) = quit_status.err(){
        println!("error: encountered while parsing command\n{:?}", err);
    }
}
