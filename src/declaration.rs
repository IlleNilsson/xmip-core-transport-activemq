//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48).

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the broker's STOMP
/// listener, `host:61613`. The passcode is the secret the Location names,
/// never a setting.
pub const SETTINGS: &Settings = &Settings {
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
            name: "user",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The login CONNECT presents, its passcode the Location's secret; no \
                      login when left out.",
            applies: Applies::Both,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a broker that stops mid-frame is waited on, and how long \
             a receive waits on a quiet destination; unbounded when left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["activemq"];
