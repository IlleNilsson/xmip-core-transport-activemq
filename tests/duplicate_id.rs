//! A keyed send carries its deduplication key in the `_AMQ_DUPL_ID`
//! header, the property Artemis detects duplicates by, the same on every
//! attempt of one Journey; an unkeyed send carries none.
//!
//! The far end is the broker's side read frame by frame, through the
//! crate's own codec: the in-process `Session` reports what was sent, not
//! the headers it was sent with.

use std::io::Write;
use std::thread;

use transport::Transport;
use transport::loopback::LOOPBACK_TIMEOUT;
use transport::socket;
use xmip_core_transport_activemq::ActiveMqTransport;
use xmip_core_transport_activemq::client::DUPLICATE_ID;
use xmip_core_transport_activemq::frame::{Frame, encode, read};

/// A Journey's identifier, as the runtime hands it.
const KEY: &str = "0b6f5a52-7c1e-4d0a-9a4e-3f1d2c8b9e70";

#[test]
fn a_keyed_send_carries_the_journey_id_as_its_duplicate_id_on_every_attempt() {
    let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bound");
    let sender = thread::spawn(move || {
        let near = ActiveMqTransport::loopback();
        let target = format!("activemq://{address}/queue/probe");
        near.send_keyed(&target, b"order", KEY)?;
        near.send_keyed(&target, b"order", KEY)?;
        near.send(&target, b"order")
    });
    let (stream, _) = socket::accept_tcp(&listener, Some(LOOPBACK_TIMEOUT)).expect("accepted");
    let (mut reader, mut writer) = socket::split(stream).expect("split");
    let mut answer = |frame: Frame| writer.write_all(&encode(&frame)).expect("answered");
    let connect = read(&mut reader).expect("read").expect("CONNECT");
    assert_eq!(connect.command, "CONNECT");
    answer(Frame::new("CONNECTED").with_header("version", "1.2"));
    let heard: Vec<Option<String>> = (0..3)
        .map(|_| {
            let send = read(&mut reader).expect("read").expect("SEND");
            assert_eq!(
                (send.command.as_str(), send.body.as_slice()),
                ("SEND", &b"order"[..])
            );
            let receipt = send.header("receipt").expect("a receipt asked for");
            answer(Frame::new("RECEIPT").with_header("receipt-id", receipt));
            send.header(DUPLICATE_ID).map(str::to_string)
        })
        .collect();
    sender.join().expect("sender").expect("sent");
    let key = Some(KEY.to_string());
    assert_eq!(heard, [key.clone(), key, None]);
}
