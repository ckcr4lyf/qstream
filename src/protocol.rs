//! Wire protocol codec for qstream.
//!
//! Every datagram is one message: fixed 8-byte header + optional payload.
//! All integers are big-endian (network byte order). See SPEC.md §5.

use std::fmt;

pub const MAGIC: [u8; 4] = *b"QSTR";
pub const PROTOCOL_VERSION: u8 = 0x01;
pub const HEADER_SIZE: usize = 8;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    HandshakeRequest = 0x01,
    HandshakeResponse = 0x02,
    // Planned (SPEC §5.2) — reserved codes:
    // Ping = 0x10, Pong = 0x11,
    // ManifestRequest = 0x20, ManifestResponse = 0x21,
    // SegmentRequest = 0x30, SegmentContents = 0x31, SegmentNotFound = 0x32,
    // Ack = 0x40,
    // PeerlistRequest = 0x50, PeerlistResponse = 0x51,
}

impl MessageType {
    pub fn from_u8(code: u8) -> Option<MessageType> {
        match code {
            0x01 => Some(MessageType::HandshakeRequest),
            0x02 => Some(MessageType::HandshakeResponse),
            _ => None,
        }
    }
}

/// A decoded message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// HANDSHAKE_REQUEST — payload: node name (UTF-8).
    HandshakeRequest { name: String },
    /// HANDSHAKE_RESPONSE — payload: node name (UTF-8).
    HandshakeResponse { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Datagram shorter than the fixed header.
    TruncatedHeader { len: usize },
    /// Magic bytes did not match `QSTR`.
    BadMagic { got: [u8; 4] },
    /// Unsupported protocol version.
    BadVersion { got: u8 },
    /// Unknown message type code.
    UnknownMessageType { code: u8 },
    /// Header's data length exceeds the datagram.
    TruncatedPayload { declared: u16, actual: usize },
    /// Node names must be valid UTF-8.
    InvalidUtf8,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::TruncatedHeader { len } => {
                write!(f, "datagram too short for header: {len} bytes")
            }
            ProtocolError::BadMagic { got } => {
                write!(f, "bad magic: {got:02x?} (expected QSTR)")
            }
            ProtocolError::BadVersion { got } => {
                write!(f, "unsupported protocol version: {got:#04x}")
            }
            ProtocolError::UnknownMessageType { code } => {
                write!(f, "unknown message type: {code:#04x}")
            }
            ProtocolError::TruncatedPayload { declared, actual } => {
                write!(f, "payload truncated: header says {declared} bytes, datagram has {actual}")
            }
            ProtocolError::InvalidUtf8 => write!(f, "payload is not valid UTF-8"),
        }
    }
}

impl std::error::Error for ProtocolError {}

fn encode_header(message_type: MessageType, data_length: u16) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4] = PROTOCOL_VERSION;
    buf[5] = message_type as u8;
    buf[6..8].copy_from_slice(&data_length.to_be_bytes());
    buf
}

/// Encode a message into a datagram buffer.
pub fn encode(message: &Message) -> Vec<u8> {
    match message {
        Message::HandshakeRequest { name } | Message::HandshakeResponse { name } => {
            let payload = name.as_bytes();
            let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
            buf.extend_from_slice(&encode_header(
                match message {
                    Message::HandshakeRequest { .. } => MessageType::HandshakeRequest,
                    Message::HandshakeResponse { .. } => MessageType::HandshakeResponse,
                },
                payload.len() as u16,
            ));
            buf.extend_from_slice(payload);
            buf
        }
    }
}

/// Decode a datagram into a message, validating magic, version and lengths.
pub fn decode(datagram: &[u8]) -> Result<Message, ProtocolError> {
    if datagram.len() < HEADER_SIZE {
        return Err(ProtocolError::TruncatedHeader { len: datagram.len() });
    }

    let mut magic = [0u8; 4];
    magic.copy_from_slice(&datagram[0..4]);
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic { got: magic });
    }

    let version = datagram[4];
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::BadVersion { got: version });
    }

    let message_type = MessageType::from_u8(datagram[5])
        .ok_or(ProtocolError::UnknownMessageType { code: datagram[5] })?;

    let data_length = u16::from_be_bytes([datagram[6], datagram[7]]) as usize;
    let payload = &datagram[HEADER_SIZE..];
    if payload.len() < data_length {
        return Err(ProtocolError::TruncatedPayload {
            declared: data_length as u16,
            actual: payload.len(),
        });
    }
    let payload = &payload[..data_length];

    let name = String::from_utf8(payload.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)?;

    Ok(match message_type {
        MessageType::HandshakeRequest => Message::HandshakeRequest { name },
        MessageType::HandshakeResponse => Message::HandshakeResponse { name },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_handshake_request() {
        let msg = Message::HandshakeRequest {
            name: "peer-1".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_handshake_response() {
        let msg = Message::HandshakeResponse {
            name: "master".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_empty_name() {
        let msg = Message::HandshakeRequest { name: String::new() };
        let datagram = encode(&msg);
        assert_eq!(datagram.len(), HEADER_SIZE); // zero-length payload
        assert_eq!(decode(&datagram).unwrap(), msg);
    }

    #[test]
    fn rejects_short_datagram() {
        assert!(matches!(
            decode(&[0x51, 0x53, 0x54]),
            Err(ProtocolError::TruncatedHeader { .. })
        ));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut datagram = encode(&Message::HandshakeRequest {
            name: "x".to_string(),
        });
        datagram[0] = 0x00;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadMagic { .. })
        ));
    }

    #[test]
    fn rejects_bad_version() {
        let mut datagram = encode(&Message::HandshakeRequest {
            name: "x".to_string(),
        });
        datagram[4] = 0x63;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadVersion { .. })
        ));
    }

    #[test]
    fn rejects_unknown_message_type() {
        let mut datagram = encode(&Message::HandshakeRequest {
            name: "x".to_string(),
        });
        datagram[5] = 0x99;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::UnknownMessageType { code: 0x99 })
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let datagram = encode(&Message::HandshakeRequest {
            name: "peer-1".to_string(),
        });
        let truncated = &datagram[..datagram.len() - 2];
        assert!(matches!(
            decode(truncated),
            Err(ProtocolError::TruncatedPayload { .. })
        ));
    }

    #[test]
    fn ignores_trailing_bytes() {
        // Decoding should use only `data_length` bytes of payload; extra
        // trailing bytes are tolerated (future-proofing for appended data).
        let mut datagram = encode(&Message::HandshakeRequest {
            name: "x".to_string(),
        });
        datagram.push(0xAA);
        assert!(decode(&datagram).is_ok());
    }
}
