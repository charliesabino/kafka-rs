#![allow(unused_imports)]
mod broker;

use broker::Broker;
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let broker = Broker::new();
    broker.run();
}
