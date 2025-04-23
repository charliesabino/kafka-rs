#![allow(unused_imports)]
mod broker;

use broker::Broker;
use std::net::TcpListener;

fn main() {
    env_logger::init();

    let broker = Broker::new();
    broker.run();
}
