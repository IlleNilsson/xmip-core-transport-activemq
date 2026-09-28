# xmip-core-transport-activemq

ActiveMQ transport: STOMP 1.2 to ActiveMQ Classic and Artemis — connect, send with receipt, subscribe with client-individual acknowledgment — a queue or topic is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location sends on a client connected once per broker and kept (`transport::Pool`), each SEND receipted; the login is the transport capability's `Login`. Until 2026-09-27 every send connected and disconnected.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives; each receive takes and acknowledges what came until the broker is quiet for the timeout (`transport::pool::delivered`). A subscription the broker closed is replaced. Until 2026-09-28 every receive connected, subscribed and disconnected.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
