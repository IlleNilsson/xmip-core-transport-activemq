//! The client's side of one connection to a broker.

use std::collections::VecDeque;
use std::io::{BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;
use transport::wire::host_of;

use crate::frame::{Frame, encode, read};

/// What a Location presents in CONNECT.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Login {
    pub user: String,
    pub password: String,
}

/// One MESSAGE as the broker delivered it: the Stream, and the id to
/// acknowledge it by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub arrived: Arrived,
    pub ack: String,
}

/// One connected client: sends, subscribes, takes what the broker sends.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    server: String,
    session: String,
    next_id: u64,
    pending: VecDeque<Frame>,
}

impl Client {
    /// Connect to `server`, CONNECT as STOMP 1.2 and take CONNECTED.
    ///
    /// # Errors
    /// Where the server could not be reached, refused the login, or did
    /// not answer with CONNECTED.
    pub fn connect(server: &str, login: Option<&Login>, timeout: Option<Duration>) -> Result<Self> {
        let stream = socket::connect_tcp(server, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut client = Self {
            reader,
            writer,
            server: server.to_string(),
            session: String::new(),
            next_id: 0,
            pending: VecDeque::new(),
        };
        let mut connect = Frame::new("CONNECT")
            .with_header("accept-version", "1.2")
            .with_header("host", host_of(server))
            .with_header("heart-beat", "0,0");
        if let Some(login) = login {
            connect = connect
                .with_header("login", &login.user)
                .with_header("passcode", &login.password);
        }
        client.write(&connect)?;
        match read(&mut client.reader)? {
            Some(frame) if frame.command == "CONNECTED" => {
                client.session = frame.header("session").unwrap_or_default().to_string();
            }
            Some(frame) if frame.command == "ERROR" => return Err(refused(&frame)),
            Some(frame) => {
                return Err(protocol_error(format!(
                    "the server answered CONNECT with {}",
                    frame.command
                )));
            }
            None => return Err(protocol_error("the server closed on CONNECT")),
        }
        Ok(client)
    }

    /// The session the broker named in CONNECTED, or empty.
    #[must_use]
    pub fn session(&self) -> &str {
        &self.session
    }

    /// SEND `bytes` to `destination` and wait for the RECEIPT that says
    /// the broker has it.
    ///
    /// # Errors
    /// Where the broker went away or answered with ERROR.
    pub fn send(&mut self, destination: &str, bytes: &[u8]) -> Result<()> {
        let receipt = self.next_id();
        let frame = Frame::new("SEND")
            .with_header("destination", destination)
            .with_header("receipt", &receipt)
            .with_header("content-type", "application/octet-stream")
            .with_body(bytes);
        self.write(&frame)?;
        self.await_receipt(&receipt)
    }

    /// SUBSCRIBE to `destination` with client-individual acknowledgement;
    /// the id that names the subscription.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn subscribe(&mut self, destination: &str) -> Result<String> {
        let id = self.next_id();
        self.write(
            &Frame::new("SUBSCRIBE")
                .with_header("id", &id)
                .with_header("destination", destination)
                .with_header("ack", "client-individual"),
        )?;
        Ok(id)
    }

    /// The next MESSAGE the broker delivers, or `None` when it closed.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the broker sent ERROR.
    pub fn next_message(&mut self) -> Result<Option<Message>> {
        loop {
            let frame = match self.pending.pop_front() {
                Some(frame) => frame,
                None => match read(&mut self.reader)? {
                    Some(frame) => frame,
                    None => return Ok(None),
                },
            };
            match frame.command.as_str() {
                "MESSAGE" => return Ok(Some(self.message(&frame))),
                "ERROR" => return Err(refused(&frame)),
                _ => {}
            }
        }
    }

    /// ACK the message delivered under `ack`.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn ack(&mut self, ack: &str) -> Result<()> {
        self.write(&Frame::new("ACK").with_header("id", ack))
    }

    /// DISCONNECT with a receipt, and wait for it.
    ///
    /// # Errors
    /// Where the broker went away before the receipt.
    pub fn disconnect(mut self) -> Result<()> {
        let receipt = self.next_id();
        self.write(&Frame::new("DISCONNECT").with_header("receipt", &receipt))?;
        self.await_receipt(&receipt)
    }

    fn message(&self, frame: &Frame) -> Message {
        let destination = frame.header("destination").unwrap_or_default();
        let id = frame.header("message-id").unwrap_or_default();
        let ack = frame.header("ack").unwrap_or(id).to_string();
        let slash = if destination.starts_with('/') {
            ""
        } else {
            "/"
        };
        Message {
            arrived: Arrived::new(
                format!(
                    "activemq://{}{slash}{destination}?message-id={id}",
                    self.server
                ),
                frame.body.clone(),
            ),
            ack,
        }
    }

    /// Read until the RECEIPT for `receipt`, keeping any MESSAGE that
    /// arrives on the way.
    fn await_receipt(&mut self, receipt: &str) -> Result<()> {
        loop {
            match read(&mut self.reader)? {
                Some(frame) if frame.command == "RECEIPT" => {
                    if frame.header("receipt-id") == Some(receipt) {
                        return Ok(());
                    }
                }
                Some(frame) if frame.command == "ERROR" => return Err(refused(&frame)),
                Some(frame) if frame.command == "MESSAGE" => self.pending.push_back(frame),
                Some(_) => {}
                None => return Err(protocol_error("the broker closed before the receipt")),
            }
        }
    }

    fn next_id(&mut self) -> String {
        self.next_id += 1;
        self.next_id.to_string()
    }

    fn write(&mut self, frame: &Frame) -> Result<()> {
        self.writer
            .write_all(&encode(frame))
            .map_err(|e| classify("writing a frame", &e))?;
        self.writer
            .flush()
            .map_err(|e| classify("flushing a frame", &e))
    }
}

/// An ERROR frame as the failure it is: the broker said no, and will
/// again.
fn refused(frame: &Frame) -> transport::TransportError {
    let message = frame.header("message").unwrap_or("no message");
    let detail = String::from_utf8_lossy(&frame.body);
    protocol_error(format!(
        "the broker answered ERROR: {message} {}",
        detail.trim()
    ))
}
