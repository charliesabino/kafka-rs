use log::trace;

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

#[derive(Debug)]
struct RequestHeader {
    request_api_key: i16,
    request_api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

impl RequestHeader {
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut request_api_key_bytes = [0u8; 2];
        let mut request_api_version_bytes = [0u8; 2];
        let mut correlation_bytes = [0u8; 4];

        request_api_key_bytes.copy_from_slice(&bytes[..2]);
        request_api_version_bytes.copy_from_slice(&bytes[2..4]);
        correlation_bytes.copy_from_slice(&bytes[4..8]);

        Self {
            request_api_key: i16::from_be_bytes(request_api_key_bytes),
            request_api_version: i16::from_be_bytes(request_api_version_bytes),
            correlation_id: i32::from_be_bytes(correlation_bytes),
            client_id: None,
        }
    }
}

#[derive(Debug)]
pub struct RequestMessage {
    message_size: MessageSize,
    request_header: RequestHeader,
}

impl RequestMessage {
    fn from_bytes(bytes: &[u8]) -> Self {
        let mut size_bytes = [0u8; 4];
        size_bytes.copy_from_slice(&bytes[0..4]);

        Self {
            message_size: i32::from_be_bytes(size_bytes),
            request_header: RequestHeader::from_bytes(&bytes[4..]),
        }
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
                    let mut req = vec![0u8; 1024];
                    let mut bytes_read = 0;

                    trace!("Reading bytes");

                    while bytes_read < std::mem::size_of::<MessageSize>() {
                        bytes_read += _stream.read(&mut req[bytes_read..]).unwrap();
                    }

                    let mut message_size_bytes = [0u8; 4];
                    message_size_bytes.copy_from_slice(&req[0..4]);
                    let message_size = i32::from_be_bytes(message_size_bytes);

                    trace!("Message size: {message_size}");

                    while bytes_read < message_size as usize {
                        bytes_read += _stream.read(&mut req[bytes_read..]).unwrap();
                    }

                    let req = RequestMessage::from_bytes(&req);
                    trace!("Received request: {:?}", req);

                    let resp_header = ResponseHeader {
                        correlation_id: req.request_header.correlation_id,
                    };

                    let resp = ResponseMessage::new(0, resp_header);
                    trace!("Sending response: {:?}", resp);

                    _stream.write(&resp.to_bytes()).unwrap();
                }
                Err(_e) => {}
            }
        }
    }
}
