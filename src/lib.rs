#![forbid(unsafe_code)]

//! Streams that arrive as messages from an `ActiveMQ` broker. One MESSAGE
//! is one Stream, its destination kept beside it.
//!
//! `ActiveMQ` Classic and Artemis both listen for STOMP on port 61613, and
//! STOMP 1.2 is the protocol here: CONNECT and CONNECTED, SEND with a
//! receipt so the broker has said it holds the message before the send is
//! counted done, SUBSCRIBE with client-individual acknowledgement so each
//! message is acknowledged once Xmip has it and never before. A queue or a
//! topic is a Location: `/queue/orders`, `/topic/prices`. A Receive
//! Location subscribes and takes what is delivered; a Send Location sends.
//! Either may instead accept clients directly through [`Session`], one
//! client's worth of broker.
//!
//! TLS is `xmip-core-library-tls`'s, per ADR-0033. `OpenWire`, the
//! brokers' native protocol, is binary and versioned and would be its own
//! technology; STOMP is what both brokers document for other languages.
//!
//! A send target is `activemq://host:61613/queue/orders`,
//! `host:61613/queue/orders`, or a destination alone on this transport's
//! broker. The origin URI carries what the frame knew:
//! `activemq://server/queue/orders?message-id=ID:broker-1234`.

pub mod client;
pub mod frame;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Message};
pub use frame::Frame;
pub use session::{Event, Queues, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Configured, Directions, Login, Pool, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct ActiveMqTransport {
    server: String,
    queue: String,
    login: Option<Login>,
    timeout: Option<Duration>,
    /// The clients a send goes on, connected once per broker and kept.
    clients: Pool<Client>,
}

impl ActiveMqTransport {
    /// Speak to the broker at `server` about `queue` — a destination as
    /// the broker names it, `/queue/orders` or `/topic/prices`.
    #[must_use]
    pub fn new(server: impl Into<String>, queue: impl Into<String>) -> Self {
        Self {
            server: server.into(),
            queue: queue.into(),
            login: None,
            timeout: None,
            clients: Pool::new(),
        }
    }

    /// CONNECT as this user.
    #[must_use]
    pub fn with_login(mut self, user: &str, password: &str) -> Self {
        self.login = Some(Login::new(user, password));
        self
    }

    /// Give up on a broker that stops mid-frame, and stop receiving when
    /// it has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the broker as a client.
    ///
    /// # Errors
    /// Where the broker could not be reached, refused the login, or did
    /// not speak STOMP.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.server, self.login.as_ref(), self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.server)
    }

    /// Accept one client on an already-bound listener, expecting this
    /// transport's login where it has one.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client refused.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.login.as_ref(), self.timeout)
    }

    /// Where a target names the broker and destination itself —
    /// `activemq://host:61613/queue/orders`, `host:61613/queue/orders` —
    /// or is a destination alone on this transport's broker.
    fn resolve(&self, target: &str) -> (String, String) {
        if let Some((server, destination)) = socket::target("activemq", target) {
            return match destination {
                "" => (server.to_string(), self.queue.clone()),
                _ => (server.to_string(), format!("/{destination}")),
            };
        }
        match target.split_once('/') {
            Some((server, destination)) if !server.is_empty() && server.contains(':') => {
                (server.to_string(), format!("/{destination}"))
            }
            _ => (self.server.clone(), target.to_string()),
        }
    }
}

impl Transport for ActiveMqTransport {
    fn name(&self) -> &'static str {
        "activemq"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Subscribe to the queue and take what the broker delivers, each
    /// acknowledged, until it has been quiet for the timeout or closes.
    /// A quiet queue is an empty vector, not an error.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.subscribe(&self.queue)?;
        let mut arrived = Vec::new();
        loop {
            match client.next_message() {
                Ok(Some(message)) => {
                    client.ack(&message.ack)?;
                    arrived.push(message.arrived);
                }
                Ok(None) => return Ok(arrived),
                Err(error) if error.retryable => break,
                Err(error) => return Err(error),
            }
        }
        client.disconnect()?;
        Ok(arrived)
    }

    /// SEND on the client kept for the broker, connected on the first send
    /// to it, and wait for the receipt.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (server, destination) = self.resolve(target);
        self.clients.exchange(
            &server,
            || Client::connect(&server, self.login.as_ref(), self.timeout),
            |client| client.send(&destination, bytes),
        )
    }
}

impl Configured for ActiveMqTransport {
    /// The address is the broker's STOMP listener, `host:61613`. The login
    /// is the Location's credentials, not a setting: a secret never is.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[
            Setting {
                name: "destination",
                kind: Kind::Text,
                presence: Presence::Required,
                meaning: "The queue or topic as the broker names it, /queue/orders or \
                          /topic/prices: subscribed to on receive, the default target on send.",
                applies: Applies::Both,
            },
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a broker that stops mid-frame is waited on, and how long \
                          a receive waits on a quiet destination; unbounded when left out.",
                applies: Applies::Both,
            },
        ],
    };

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The login comes through the Location's credentials, not a setting.
        let transport = Self::new(address, settings.text("destination"));
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl ActiveMqTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one queue called `/queue/probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "/queue/probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for ActiveMqTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        // The receipt goes out before the send is reported; the client keeps
        // its connection for the next.
        session
            .next_send()?
            .ok_or_else(|| protocol_error("the client disconnected without sending"))
    }
}

impl Loopback for ActiveMqTransport {
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A client to `address`, one SEND with a receipt to this transport's
    /// queue, receipted before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            server: address.to_string(),
            ..self.clone()
        }
        .send(&self.queue, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::payload::{edge_payloads, sized_payloads};
    use xcore::settings::Given;

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn activemq_declares_its_settings_and_reads_through_them() {
        assert_eq!(ActiveMqTransport::SETTINGS.problems(), Vec::<String>::new());
        let given = [
            (
                "destination".to_string(),
                Given::Text("/queue/orders".to_string()),
            ),
            ("timeout".to_string(), Given::Text("2s".to_string())),
        ];
        let transport =
            ActiveMqTransport::open("broker:61613", Applies::Receive, &given).expect("built");
        assert_eq!(transport.server, "broker:61613");
        assert_eq!(transport.queue, "/queue/orders");
        assert_eq!(transport.timeout, Some(secs(2)));
        assert!(transport.login.is_none(), "the login is the credentials'");
        let Err(refused) = ActiveMqTransport::open("broker:61613", Applies::Send, &[]) else {
            panic!("destination is required");
        };
        assert!(refused.message.contains("\"destination\""), "{refused}");
    }

    #[test]
    fn a_client_sends_to_a_session_and_the_session_delivers_to_a_receiver() {
        let far_end = ActiveMqTransport::new("127.0.0.1:0", "/queue/orders")
            .with_login("xmip", "secret")
            .timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near = ActiveMqTransport::new(address.clone(), "/queue/orders")
                .with_login("xmip", "secret")
                .timing_out_after(secs(2));
            near.send("/queue/orders", b"order 1\r\nline 2")?;
            near.send(&format!("activemq://{address}/queue/orders"), b"")?;
            near.send(&format!("{address}/queue/other"), b"other")?;
            ActiveMqTransport::new(address, "/queue/orders")
                .with_login("xmip", "secret")
                .timing_out_after(Duration::from_millis(300))
                .receive()
        });
        // One broker, so one client for all three sends: connected once.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.connect().header("login"), Some("xmip"));
        for expected in [&b"order 1\r\nline 2"[..], b"", b"other"] {
            let sent = session.next_send().expect("sent").expect("one");
            assert_eq!(sent.bytes, expected);
            assert!(sent.origin_uri.starts_with("activemq://127.0.0.1:"));
        }
        let queues = session.into_queues();
        assert_eq!(queues["/queue/orders"].len(), 2);
        assert_eq!(queues["/queue/other"].len(), 1);
        let mut session = far_end
            .accept_one(&listener)
            .expect("receiver")
            .with_queues(queues);
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            events.push(event);
        }
        assert!(matches!(&events[0], Event::Subscribed { destination, .. }
            if destination == "/queue/orders"));
        assert_eq!(events[1], Event::Acked("1".to_string()));
        assert_eq!(events[2], Event::Acked("2".to_string()));
        assert_eq!(events.len(), 3);
        assert!(
            session.queues()["/queue/other"].len() == 1,
            "not subscribed"
        );
        let arrived = sender.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert_eq!(arrived[0].bytes, b"order 1\r\nline 2");
        assert!(
            arrived[0]
                .origin_uri
                .ends_with("/queue/orders?message-id=1")
        );
        assert!(arrived[1].bytes.is_empty());
        assert!(
            arrived[1]
                .origin_uri
                .ends_with("/queue/orders?message-id=2")
        );
    }

    #[test]
    fn a_thousand_sends_connect_once_and_a_client_the_broker_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = ActiveMqTransport::new("127.0.0.1:0", "/queue/orders")
            .with_login("xmip", "secret")
            .timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = ActiveMqTransport::new(address, "/queue/orders")
            .with_login("xmip", "secret")
            .timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("/queue/orders", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a send.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
            sending.send("/queue/orders", b"after the close")
        });
        // One CONNECT for every send: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let sent = session.next_send().expect("sent").expect("one");
            assert_eq!(sent.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        let last = again.next_send().expect("sent").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.clients.opened(), 2);
    }

    #[test]
    fn a_session_delivers_what_it_is_given_while_the_client_listens() {
        let far_end =
            ActiveMqTransport::new("127.0.0.1:0", "/topic/prices").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            let near = ActiveMqTransport::new(address, "/topic/prices").timing_out_after(secs(2));
            let mut client = near.connect()?;
            client.subscribe("/topic/prices")?;
            let first = client.next_message()?.expect("first");
            client.ack(&first.ack)?;
            let second = client.next_message()?;
            Ok::<_, transport::TransportError>((first, second))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        session.next_event().expect("subscribed");
        session.deliver("/topic/prices", b"42").expect("delivered");
        assert_eq!(
            session.next_event().expect("acked"),
            Some(Event::Acked("1".to_string()))
        );
        drop(session);
        let (first, second) = receiver.join().expect("thread").expect("listening");
        assert_eq!(first.arrived.bytes, b"42");
        assert_eq!(first.ack, "1");
        assert!(second.is_none(), "the broker closed");
    }

    #[test]
    fn a_refused_login_and_a_broker_that_is_not_stomp_are_permanent() {
        let far_end = ActiveMqTransport::new("127.0.0.1:0", "/queue/x")
            .with_login("xmip", "secret")
            .timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let stranger = std::thread::spawn(move || {
            ActiveMqTransport::new(address, "/queue/x")
                .with_login("xmip", "wrong")
                .timing_out_after(secs(2))
                .connect()
                .err()
                .expect("refused")
        });
        assert!(
            far_end.accept_one(&listener).is_err(),
            "refused at the far end too"
        );
        let error = stranger.join().expect("thread");
        assert!(!error.retryable, "{error}");
        assert!(error.message.contains("not authorised"));

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            std::io::Write::write_all(&mut stream, b"220 mail.example ESMTP\r\n\r\n\0").expect("w");
            // Read to the end rather than closing: a reset can discard bytes
            // the peer has not read yet, and then the failure under test is a
            // dropped connection rather than the answer that is not STOMP.
            let _ = std::io::Read::read_to_end(&mut stream, &mut Vec::new());
        });
        let error = ActiveMqTransport::new(address, "/queue/x")
            .timing_out_after(secs(2))
            .connect()
            .err()
            .expect("not stomp");
        assert!(!error.retryable, "{error}");
        let transport = ActiveMqTransport::new("127.0.0.1:0", "/queue/x");
        assert!(transport.claims().is_none());
        assert_eq!(transport.name(), "activemq");
        assert_eq!(
            transport.resolve("activemq://h:1/queue/a"),
            ("h:1".to_string(), "/queue/a".to_string())
        );
        assert_eq!(
            transport.resolve("h:1/topic/b"),
            ("h:1".to_string(), "/topic/b".to_string())
        );
        assert_eq!(
            transport.resolve("/queue/c"),
            ("127.0.0.1:0".to_string(), "/queue/c".to_string())
        );
        assert_eq!(
            transport.resolve("activemq://h:1"),
            ("h:1".to_string(), "/queue/x".to_string())
        );
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = ActiveMqTransport::loopback();
        let arrived = loopback.round(b"sent").expect("round");
        assert_eq!(arrived.bytes, b"sent");
        assert!(arrived.origin_uri.starts_with("activemq://127.0.0.1:"));
        assert!(arrived.origin_uri.contains("/queue/probe"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = ActiveMqTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }
}
