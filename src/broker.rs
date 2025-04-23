use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{error, trace};
use std::{
    io::{self, Cursor, Read, Write},
    net::{TcpListener, TcpStream},
};

type MessageSize = i32;
const RESPONSE_HEADER_SIZE: MessageSize = 4;
const REQUEST_HEADER_BASE_SIZE: usize = 8;
const MESSAGE_SIZE_FIELD_LEN: usize = std::mem::size_of::<MessageSize>();

#[derive(Debug, Clone)]
struct ResponseHeader {
    correlation_id: i32,
}

impl ResponseHeader {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_i32::<BigEndian>(self.correlation_id)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResponseMessage {
    response_header: ResponseHeader,
}

impl ResponseMessage {
    fn new(response_header: ResponseHeader /*, payload: Vec<u8> */) -> Self {
        Self {
            response_header,
            /* payload */
        }
    }

    fn to_bytes(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::new();
        buffer.write_i32::<BigEndian>(0)?;

        self.response_header.write_to(&mut buffer)?;

        // buffer.write_all(&self.payload)?;

        let message_size = (buffer.len() - MESSAGE_SIZE_FIELD_LEN) as MessageSize;

        let mut cursor = Cursor::new(&mut buffer);
        cursor.write_i32::<BigEndian>(message_size)?;

        Ok(buffer)
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
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let request_api_key = reader.read_i16::<BigEndian>()?;
        let request_api_version = reader.read_i16::<BigEndian>()?;
        let correlation_id = reader.read_i32::<BigEndian>()?;

        let client_id = None;

        Ok(Self {
            request_api_key,
            request_api_version,
            correlation_id,
            client_id,
        })
    }
}

#[derive(Debug)]
pub struct RequestMessage {
    request_header: RequestHeader,
    // payload: Vec<u8>,
}

impl RequestMessage {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let request_header = RequestHeader::read_from(reader)?;

        // let mut payload = Vec::new();
        // reader.read_to_end(&mut payload)?;

        Ok(Self {
            request_header,
            /* payload */
        })
    }
}

pub struct Broker {
    listener: TcpListener,
}

impl Broker {
    pub fn new(bind_addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(bind_addr)?;
        println!("Broker listening on {}", bind_addr);
        Ok(Self { listener })
    }

    pub fn listen(self) -> io::Result<()> {
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    std::thread::spawn(move || {
                        if let Err(e) = Self::handle_connection(stream) {
                            error!("Error handling connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                    if e.kind() == io::ErrorKind::InvalidData {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
        trace!("Connection established from: {:?}", stream.peer_addr());

        let message_size = stream.read_i32::<BigEndian>()?;
        trace!("Advertised message size: {}", message_size);

        let mut message_buffer = vec![0u8; message_size as usize];
        stream.read_exact(&mut message_buffer)?;

        let mut message_cursor = Cursor::new(message_buffer);

        let req = RequestMessage::read_from(&mut message_cursor)?;
        trace!("Received request: {:?}", req);

        let resp_header = ResponseHeader {
            correlation_id: req.request_header.correlation_id,
        };

        let resp = ResponseMessage::new(resp_header);
        trace!("Sending response: {:?}", resp);

        let resp_bytes = resp.to_bytes()?;

        stream.write_all(&resp_bytes)?;
        trace!("Response sent successfully");

        Ok(())
    }
}
