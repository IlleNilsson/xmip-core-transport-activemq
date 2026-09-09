//! The broker's side of one connection: what a test puts at the far end,
//! and what the playground stands up in place of a broker.
//!
//! Not a broker. One session serves one client and keeps its queues in
//! memory: what is sent is stored per destination, or delivered at once
//! where the client has subscribed to it; what is subscribed to is drained
//! to the client under the id it chose. A Location talks to a real broker
//! through [`crate::Client`].

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::client::Login;
use crate::frame::{Frame, encode, read};

/// What the client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client sent to a destination; here is the Stream.
    Sent(Arrived),
    /// The client subscribed to `destination` under `id`.
    Subscribed { id: String, destination: String },
    /// The client unsubscribed `id`.
    Unsubscribed(String),
    /// The client acknowledged this message.
    Acked(String),
}

/// What the session holds: messages per destination, not yet delivered.
pub type Queues = BTreeMap<String, Vec<Vec<u8>>>;

pub struct Session {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    peer: SocketAddr,
    connect: Frame,
    queues: Queues,
    subscriptions: Vec<(String, String)>,
    next_message: u64,
}

impl Session {
    /// Accept one client on `listener`, take its CONNECT and answer
    /// CONNECTED — or ERROR, where it does not speak 1.2 or the login is
    /// not `expected`.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the client did not open
    /// with CONNECT, or it was refused.
    pub fn accept(
        listener: &TcpListener,
        expected: Option<&Login>,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let (reader, writer) = socket::split(stream)?;
        let mut session = Self {
            reader,
            writer,
            peer,
            connect: Frame::new("CONNECT"),
            queues: Queues::new(),
            subscriptions: Vec::new(),
            next_message: 0,
        };
        let connect = match read(&mut session.reader)? {
            Some(frame) if frame.command == "CONNECT" || frame.command == "STOMP" => frame,
            _ => return Err(protocol_error("the client did not open with CONNECT")),
        };
        let versions = connect.header("accept-version").unwrap_or("1.0");
        if !versions.split(',').any(|v| v.trim() == "1.2") {
            return session.refuse("only STOMP 1.2 is spoken here");
        }
        let presented = match (connect.header("login"), connect.header("passcode")) {
            (Some(user), Some(password)) => Some(Login {
                user: user.to_string(),
                password: password.to_string(),
            }),
            _ => None,
        };
        if expected.is_some() && presented.as_ref() != expected {
            return session.refuse("not authorised");
        }
        session.connect = connect;
        session.write(
            &Frame::new("CONNECTED")
                .with_header("version", "1.2")
                .with_header("server", "xmip/0.1.0")
                .with_header("heart-beat", "0,0")
                .with_header("session", &format!("xmip-{}", peer.port())),
        )?;
        Ok(session)
    }

    fn refuse(mut self, message: &str) -> Result<Self> {
        self.write(&Frame::new("ERROR").with_header("message", message))?;
        Err(protocol_error(format!("the client was refused: {message}")))
    }

    /// Hold these queues, the way one session hands its state to the next.
    #[must_use]
    pub fn with_queues(mut self, queues: Queues) -> Self {
        self.queues = queues;
        self
    }

    /// The CONNECT the client sent.
    #[must_use]
    pub fn connect(&self) -> &Frame {
        &self.connect
    }

    /// What is queued and not yet delivered.
    #[must_use]
    pub fn queues(&self) -> &Queues {
        &self.queues
    }

    /// The queues, for the next session to carry on with.
    #[must_use]
    pub fn into_queues(self) -> Queues {
        self.queues
    }

    /// The next message the client sends, or `None` when it disconnected.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_send(&mut self) -> Result<Option<Arrived>> {
        loop {
            match self.next_event()? {
                Some(Event::Sent(arrived)) => return Ok(Some(arrived)),
                Some(_) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it disconnected.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or
    /// the client sent what only a broker sends.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            let Some(frame) = read(&mut self.reader)? else {
                return Ok(None);
            };
            let event = match frame.command.as_str() {
                "SEND" => {
                    let destination = frame.header("destination").unwrap_or_default().to_string();
                    self.deliver(&destination, &frame.body)?;
                    self.receipt(&frame)?;
                    let slash = if destination.starts_with('/') {
                        ""
                    } else {
                        "/"
                    };
                    let origin = format!("activemq://{}{slash}{destination}", self.peer);
                    Event::Sent(Arrived::new(origin, frame.body.clone()))
                }
                "SUBSCRIBE" => {
                    let id = frame.header("id").unwrap_or_default().to_string();
                    let destination = frame.header("destination").unwrap_or_default().to_string();
                    self.subscriptions.push((id.clone(), destination.clone()));
                    self.receipt(&frame)?;
                    for body in self.queues.remove(&destination).unwrap_or_default() {
                        self.deliver(&destination, &body)?;
                    }
                    Event::Subscribed { id, destination }
                }
                "UNSUBSCRIBE" => {
                    let id = frame.header("id").unwrap_or_default().to_string();
                    self.subscriptions.retain(|(s, _)| *s != id);
                    self.receipt(&frame)?;
                    Event::Unsubscribed(id)
                }
                "ACK" | "NACK" => {
                    let id = frame.header("id").unwrap_or_default().to_string();
                    self.receipt(&frame)?;
                    Event::Acked(id)
                }
                "DISCONNECT" => {
                    self.receipt(&frame)?;
                    return Ok(None);
                }
                "BEGIN" | "COMMIT" | "ABORT" => {
                    self.receipt(&frame)?;
                    continue;
                }
                other => {
                    self.write(&Frame::new("ERROR").with_header("message", "unknown command"))?;
                    return Err(protocol_error(format!("{other} from a client")));
                }
            };
            return Ok(Some(event));
        }
    }

    /// Deliver `body` on `destination`: as a MESSAGE where the client has
    /// subscribed to it, else into the queue for when it does.
    ///
    /// # Errors
    /// Where the client went away.
    pub fn deliver(&mut self, destination: &str, body: &[u8]) -> Result<()> {
        let Some((id, _)) = self
            .subscriptions
            .iter()
            .find(|(_, d)| d == destination)
            .cloned()
        else {
            self.queues
                .entry(destination.to_string())
                .or_default()
                .push(body.to_vec());
            return Ok(());
        };
        self.next_message += 1;
        let message_id = self.next_message.to_string();
        self.write(
            &Frame::new("MESSAGE")
                .with_header("subscription", &id)
                .with_header("message-id", &message_id)
                .with_header("ack", &message_id)
                .with_header("destination", destination)
                .with_header("content-type", "application/octet-stream")
                .with_body(body),
        )
    }

    /// RECEIPT, where the frame asked for one.
    fn receipt(&mut self, frame: &Frame) -> Result<()> {
        match frame.header("receipt") {
            Some(receipt) => self.write(&Frame::new("RECEIPT").with_header("receipt-id", receipt)),
            None => Ok(()),
        }
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
