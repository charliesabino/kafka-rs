#![allow(unused_imports)]
mod broker;

use broker::Broker;
use std::net::TcpListener;

const BROKER_ADDRESS: &str = "127.0.0.1:9092";

fn main() {
    env_logger::init();

    let broker = Broker::new(&BROKER_ADDRESS).unwrap();
    broker.listen().unwrap();
}
