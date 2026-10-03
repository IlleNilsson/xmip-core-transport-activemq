# xmip-core-transport-activemq

ActiveMQ transport: STOMP 1.2 to ActiveMQ Classic and Artemis — connect, send with receipt, subscribe with client-individual acknowledgment — a queue or topic is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location sends on a client connected once per broker and kept (`transport::Pool`), each SEND receipted; the login is the transport capability's `Login`. Until 2026-09-27 every send connected and disconnected.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives; each receive takes what came until the broker is quiet for the timeout (`transport::pool::delivered`). A subscription the broker closed is replaced. Until 2026-09-28 every receive connected, subscribed and disconnected.

A message is answered after the runtime's whole receive cycle, never as it arrives: ACK on the subscription that received it when the cycle accepted it; NACK when it refused it — the client did not consume it, and the broker discards it or puts it in its dead letter queue as its policy says (STOMP 1.2, NACK), ActiveMQ Classic at once; nothing when the cycle failed. STOMP 1.2 has no frame that asks a broker to deliver a message again: a message a `client-individual` subscription left unanswered is the broker's again once its connection ends, so a failed message marks the subscription (`Client::withhold`), later verdicts of the same cycle still answer on it, and the next receive lets it go and subscribes anew; the broker delivers the failed message again. `Session` puts what a connection left unanswered back on its queue when the connection ends, as a broker does. An ack id names a message on one connection only, so where the broker closed that subscription meanwhile no other is opened: the broker delivers again what that connection had not answered. The answer is one frame written on the kept connection, nothing waited for; `Session` reports a NACK as `Event::Nacked`. Until 2026-10-02 a message was acknowledged as it was received.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
