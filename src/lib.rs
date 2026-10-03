#![forbid(unsafe_code)]

//! Streams that arrive as messages from an `ActiveMQ` broker. One MESSAGE
//! is one Stream, its destination kept beside it.
//!
//! `ActiveMQ` Classic and Artemis both listen for STOMP on port 61613, and
//! STOMP 1.2 is the protocol here: CONNECT and CONNECTED, SEND with a
//! receipt so the broker has said it holds the message before the send is
//! counted done, SUBSCRIBE with client-individual acknowledgement so each
//! message is answered after the runtime's receive cycle and never before:
//! ACK when it accepted the message, NACK when it refused it, and nothing
//! when the cycle failed, the subscription renewed so the broker delivers
//! the message again. A queue or a
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
use net::Target;
pub use session::{Event, Queues, Session};
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::pool::delivered;
use transport::socket;
use transport::{
    Acknowledgement, Arrived, Configured, Directions, Login, Pool, Taken, Transport, Verdict,
};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct ActiveMqTransport {
    server: String,
    queue: String,
    login: Option<Login>,
    timeout: Option<Duration>,
    /// The clients a send goes on, connected once per broker and kept.
    clients: Pool<Client>,
    /// The client a receive takes from, connected and subscribed on the
    /// first receive and kept subscribed.
    subscriptions: Pool<Client>,
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
            subscriptions: Pool::new(),
        }
    }

    /// CONNECT as this user.
    #[must_use]
    #[cfg(test)]
    fn with_login(mut self, user: &str, password: &str) -> Self {
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
        match Target::naming_server(&["activemq"], target) {
            Some(named) if named.path().is_empty() && !named.scheme().is_empty() => {
                (named.authority().to_string(), self.queue.clone())
            }
            Some(named) => (named.authority().to_string(), format!("/{}", named.path())),
            None => (self.server.clone(), target.to_string()),
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

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered(
            "the acknowledgement goes on the session the receive reads from",
        )
    }

    /// Take what the broker delivers until it has been quiet for the
    /// timeout or closes, on the subscription the first receive made and
    /// kept: what the broker delivered between two receives waits in the
    /// socket. A quiet queue is an empty vector, not an error. Nothing is
    /// acknowledged here: each message's acknowledgement ([`answering`])
    /// sends ACK on that subscription when the receive cycle accepted it,
    /// NACK when it refused it, and nothing when the cycle failed: that
    /// subscription is let go here, at the next receive, and a new one made,
    /// so the broker delivers again what was left unanswered.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let messages = self.subscriptions.exchange(
            self.server.as_str(),
            || {
                let mut client = self.connect()?;
                client.subscribe(&self.queue)?;
                Ok(client)
            },
            |client| {
                if client.withholds() {
                    // Let go, so that the broker delivers again what was
                    // left unanswered on it; a new subscription takes over.
                    return Err(protocol_error(
                        "a message was left for the broker to deliver again",
                    ));
                }
                delivered(client, Client::next_message)
            },
        )?;
        Ok(messages
            .into_iter()
            .map(|message| {
                let acknowledgement = answering(&self.subscriptions, &self.server, message.ack);
                Arrived::whole(message.origin_uri, message.body, acknowledgement)
            })
            .collect())
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

/// The acknowledgement of the message delivered under `ack` on the
/// subscription `subscriptions` keeps for `server`: ACK on
/// [`Verdict::Accepted`]; NACK on [`Verdict::Refused`] — the client did not
/// consume it, and the broker discards or dead-letters it rather than
/// deliver it again (STOMP 1.2, *NACK*); on [`Verdict::Failed`] nothing is
/// written, and the subscription is let go at the next receive, so the
/// broker delivers it again ([`Client::withhold`]). One frame written at
/// most, nothing waited for. An ack id names a message on one connection
/// only, so the answer goes on the kept subscription or not at all: where
/// the broker closed it meanwhile, no other is opened, and the broker
/// delivers again what that connection had not answered.
fn answering(subscriptions: &Pool<Client>, server: &str, ack: String) -> Acknowledgement {
    let subscriptions = subscriptions.clone();
    let server = server.to_string();
    Acknowledgement::deferred(move |verdict| {
        subscriptions.kept(
            server.as_str(),
            "the subscription that received the message is closed; \
             the broker delivers it again",
            |client| match verdict {
                Verdict::Accepted => client.ack(&ack),
                Verdict::Refused(_) => client.nack(&ack),
                Verdict::Failed => {
                    client.withhold();
                    Ok(())
                }
            },
        )
    })
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
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
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
    use transport::arrived::next_arrival;
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
    fn a_receiver_acks_the_accepted_nacks_the_refused_and_leaves_the_failed_to_come_again() {
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
            near.send("/queue/orders", b"order 3")?;
            let receiving = ActiveMqTransport::new(address, "/queue/orders")
                .with_login("xmip", "secret")
                .timing_out_after(Duration::from_millis(300));
            let mut arrived = receiving.receive()?.into_iter();
            let one = arrived.next().expect("one").taken()?;
            let two = arrived.next().expect("two");
            let origin = two.origin_uri.clone();
            two.refused(transport::Refusal::Unacceptable)?;
            arrived.next().expect("three").failed()?;
            // The failed one comes again, on a new subscription.
            let again = next_arrival(receiving.receive()?, "three again")?.taken()?;
            Ok::<_, transport::TransportError>((one, origin, again))
        });
        // One broker, so one client for all four sends: connected once.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.connect().header("login"), Some("xmip"));
        for expected in [&b"order 1\r\nline 2"[..], b"", b"other", b"order 3"] {
            let sent = session.next_send().expect("sent").expect("one");
            assert_eq!(sent.bytes, expected);
            assert!(sent.origin_uri.starts_with("activemq://127.0.0.1:"));
        }
        let queues = session.into_queues();
        assert_eq!(queues["/queue/orders"].len(), 3);
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
        // Accepted, ACK; refused, NACK, not delivered again; failed,
        // nothing, and the connection let go.
        assert_eq!(events[1], Event::Acked("1".to_string()));
        assert_eq!(events[2], Event::Nacked("2".to_string()));
        assert_eq!(events.len(), 3);
        assert!(
            session.queues()["/queue/other"].len() == 1,
            "not subscribed"
        );
        let queues = session.into_queues();
        assert_eq!(
            queues["/queue/orders"],
            [b"order 3".to_vec()],
            "the unanswered one is the broker's again"
        );
        let mut session = far_end
            .accept_one(&listener)
            .expect("a new subscription")
            .with_queues(queues);
        let mut events = Vec::new();
        while let Some(event) = session.next_event().expect("serving") {
            events.push(event);
        }
        assert_eq!(events[1..], [Event::Acked("1".to_string())]);
        let (one, two, again) = sender.join().expect("thread").expect("receiving");
        assert_eq!(again.bytes, b"order 3");
        assert_eq!(one.bytes, b"order 1\r\nline 2");
        assert!(one.origin_uri.ends_with("/queue/orders?message-id=1"));
        assert!(two.ends_with("/queue/orders?message-id=2"));
    }

    #[test]
    fn five_receives_subscribe_once_and_a_subscription_the_broker_closed_is_replaced() {
        let far_end = ActiveMqTransport::new("127.0.0.1:0", "/queue/orders")
            .with_login("xmip", "secret")
            .timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = ActiveMqTransport::new(address, "/queue/orders")
            .with_login("xmip", "secret")
            .timing_out_after(Duration::from_millis(100));
        let receiving = near.clone();
        let (taken, told) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            while arrived.len() < 6 {
                let now = receiving.receive()?;
                if !now.is_empty() {
                    taken.send(()).expect("told");
                }
                for one in now {
                    arrived.push(one.taken()?.bytes);
                }
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let subscribed = |session: &mut Session| {
            let event = session.next_event().expect("subscribed");
            assert!(matches!(event, Some(Event::Subscribed { .. })), "{event:?}");
        };
        // One CONNECT and SUBSCRIBE for every receive: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        subscribed(&mut session);
        for round in 0..5u8 {
            session
                .deliver("/queue/orders", &[round])
                .expect("delivered");
            told.recv().expect("taken");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        subscribed(&mut again);
        again.deliver("/queue/orders", &[5]).expect("delivered");
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived, (0..6u8).map(|n| vec![n]).collect::<Vec<_>>());
        assert_eq!(near.subscriptions.opened(), 2);
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
        assert_eq!(first.body, b"42");
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
