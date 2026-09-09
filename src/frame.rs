//! STOMP 1.2 on the wire: a command line, header lines, a blank line, a
//! body, and the NUL that ends the frame.
//!
//! Header values escape `\r`, `\n`, `:` and `\` as `\r`, `\n`, `\c` and
//! `\\` — except in CONNECT and CONNECTED, which the specification leaves
//! unescaped for the sake of STOMP 1.0 brokers. A `content-length` header
//! says how long the body is, which is what lets a body carry NUL; without
//! one the body runs to the first NUL. Bare newlines between frames are
//! heart-beats and are skipped.

use std::io::BufRead;

use transport::error::{Result, classify, protocol_error};

/// The most a frame body may say it is before it is refused.
pub const MAX_BODY: usize = 64 * 1024 * 1024;

/// One frame, either direction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub command: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Frame {
    /// `command`, no headers, no body.
    #[must_use]
    pub fn new(command: &str) -> Self {
        Self {
            command: command.to_string(),
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// With this header too.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// With this body.
    #[must_use]
    pub fn with_body(mut self, body: &[u8]) -> Self {
        self.body = body.to_vec();
        self
    }

    /// The first value of `name`: STOMP says the first repetition wins.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Whether the headers of this command are written bare.
    fn unescaped(&self) -> bool {
        self.command == "CONNECT" || self.command == "CONNECTED"
    }
}

/// `frame` as bytes on the wire, `content-length` added where the body is
/// not empty.
#[must_use]
pub fn encode(frame: &Frame) -> Vec<u8> {
    let mut out = format!("{}\n", frame.command).into_bytes();
    for (name, value) in &frame.headers {
        let line = if frame.unescaped() {
            format!("{name}:{value}\n")
        } else {
            format!("{}:{}\n", escape(name), escape(value))
        };
        out.extend_from_slice(line.as_bytes());
    }
    if !frame.body.is_empty() && frame.header("content-length").is_none() {
        out.extend_from_slice(format!("content-length:{}\n", frame.body.len()).as_bytes());
    }
    out.push(b'\n');
    out.extend_from_slice(&frame.body);
    out.push(0);
    out
}

/// Read one frame, or `None` when the peer closed between frames.
///
/// # Errors
/// A connection that closes mid-frame, a header line without a colon, a
/// body over [`MAX_BODY`], or a body that does not end in NUL.
pub fn read(reader: &mut impl BufRead) -> Result<Option<Frame>> {
    let command = loop {
        let Some(line) = line(reader)? else {
            return Ok(None);
        };
        if !line.is_empty() {
            break line;
        }
    };
    let mut frame = Frame::new(&command);
    loop {
        let line =
            line(reader)?.ok_or_else(|| protocol_error("a frame that ends in its headers"))?;
        if line.is_empty() {
            break;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| protocol_error(format!("a header line without a colon: {line}")))?;
        if frame.unescaped() {
            frame.headers.push((name.to_string(), value.to_string()));
        } else {
            frame.headers.push((unescape(name)?, unescape(value)?));
        }
    }
    frame.body = match frame.header("content-length") {
        Some(length) => counted(reader, length)?,
        None => bare(reader)?,
    };
    Ok(Some(frame))
}

/// A body of `length` bytes, and the NUL after it.
fn counted(reader: &mut impl BufRead, length: &str) -> Result<Vec<u8>> {
    let length: usize = length
        .parse()
        .map_err(|_| protocol_error(format!("a content-length that is not a number: {length}")))?;
    if length > MAX_BODY {
        return Err(protocol_error("a body over what Xmip will read"));
    }
    let mut body = vec![0u8; length + 1];
    reader
        .read_exact(&mut body)
        .map_err(|e| classify("reading a frame body", &e))?;
    if body.pop() != Some(0) {
        return Err(protocol_error("a body not followed by NUL"));
    }
    Ok(body)
}

/// A body that runs to the first NUL.
fn bare(reader: &mut impl BufRead) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    reader
        .read_until(0, &mut body)
        .map_err(|e| classify("reading a frame body", &e))?;
    if body.pop() != Some(0) {
        return Err(protocol_error("a frame that ends before its NUL"));
    }
    Ok(body)
}

/// One line without its EOL, or `None` at the end of the connection.
fn line(reader: &mut impl BufRead) -> Result<Option<String>> {
    let mut raw = Vec::new();
    let read = reader
        .read_until(b'\n', &mut raw)
        .map_err(|e| classify("reading a frame line", &e))?;
    if read == 0 {
        return Ok(None);
    }
    if raw.last() == Some(&b'\n') {
        raw.pop();
    }
    if raw.last() == Some(&b'\r') {
        raw.pop();
    }
    String::from_utf8(raw)
        .map(Some)
        .map_err(|_| protocol_error("a frame line that is not UTF-8"))
}

fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\r' => out.push_str("\\r"),
            '\n' => out.push_str("\\n"),
            ':' => out.push_str("\\c"),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out
}

fn unescape(text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('r') => out.push('\r'),
            Some('n') => out.push('\n'),
            Some('c') => out.push(':'),
            Some('\\') => out.push('\\'),
            other => {
                return Err(protocol_error(format!(
                    "an escape STOMP does not define: \\{}",
                    other.unwrap_or(' ')
                )));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: &Frame) -> Frame {
        let bytes = encode(frame);
        read(&mut bytes.as_slice()).expect("read").expect("one")
    }

    #[test]
    fn a_frame_round_trips_with_its_escapes_and_its_nul() {
        let send = Frame::new("SEND")
            .with_header("destination", "/queue/orders")
            .with_header("odd", "a:b\\c\nd\re")
            .with_body(b"one\0two");
        let bytes = encode(&send);
        let text = String::from_utf8_lossy(&bytes).into_owned();
        assert!(text.starts_with("SEND\ndestination:/queue/orders\nodd:a\\cb\\\\c\\nd\\re\n"));
        assert!(text.contains("content-length:7\n\n"));
        assert!(bytes.ends_with(b"one\0two\0"));
        let back = round_trip(&send);
        assert_eq!(back.header("odd"), Some("a:b\\c\nd\re"));
        assert_eq!(back.body, b"one\0two");
        let connect = Frame::new("CONNECT").with_header("host", "a:b");
        assert!(encode(&connect).starts_with(b"CONNECT\nhost:a:b\n\n"));
        assert_eq!(round_trip(&connect).header("host"), Some("a:b"));
        assert!(round_trip(&Frame::new("DISCONNECT")).body.is_empty());
    }

    #[test]
    fn heart_beats_are_skipped_and_a_bare_body_runs_to_nul() {
        let wire = b"\n\r\n\nMESSAGE\r\nmessage-id:1\r\n\r\nhello\0\n\n";
        let mut reader = &wire[..];
        let frame = read(&mut reader).expect("read").expect("one");
        assert_eq!(frame.command, "MESSAGE");
        assert_eq!(frame.body, b"hello");
        assert!(read(&mut reader).expect("closed").is_none());
    }

    #[test]
    fn what_is_not_stomp_is_refused() {
        assert!(read(&mut &b"SEND\nno colon\n\n\0"[..]).is_err(), "colon");
        assert!(read(&mut &b"SEND\n"[..]).is_err(), "ends in headers");
        assert!(read(&mut &b"SEND\n\nbody without nul"[..]).is_err());
        assert!(read(&mut &b"SEND\ncontent-length:3\n\nabcd"[..]).is_err());
        assert!(read(&mut &b"SEND\ncontent-length:x\n\n\0"[..]).is_err());
        assert!(read(&mut &b"SEND\na:\\q\n\n\0"[..]).is_err(), "escape");
        assert!(read(&mut &b"SEND\ncontent-length:99999999999\n\n"[..]).is_err());
    }
}
