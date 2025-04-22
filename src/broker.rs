use std::{
    io::{Read, Write},
    net::TcpListener,
};

type MessageSize = i32;

#[derive(Debug)]
struct ResponseHeader {
    correlation_id: i32,
}

#[derive(Debug)]
pub struct ResponseMessage {
    message_size: MessageSize,
    response_header: ResponseHeader,
}

#[derive(Debug)]
struct RequestHeader {
    correlation_id: i32,
}

#[derive(Debug)]
pub struct RequestMessage {
    message_size: MessageSize,
    response_header: RequestHeader,
}

impl ResponseMessage {
    fn new(message_size: MessageSize, response_header: ResponseHeader) -> Self {
        Self {
            message_size,
            response_header,
        }
    }

    fn to_bytes(&self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[..4].copy_from_slice(&self.message_size.to_be_bytes());
        buf[4..8].copy_from_slice(&self.response_header.correlation_id.to_be_bytes());
        buf
    }
}

pub struct Broker {
    listener: TcpListener,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            listener: TcpListener::bind("127.0.0.1:9092").unwrap(),
        }
    }

    pub fn run(&self) {
        for stream in self.listener.incoming() {
            match stream {
                Ok(mut _stream) => {
                    let msg = ResponseMessage::new(0, ResponseHeader { correlation_id: 7 });
                    _stream.write(&msg.to_bytes()).unwrap();
                }
                Err(e) => {}
            }
        }
    }
}
