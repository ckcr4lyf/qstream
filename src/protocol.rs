//! Wire protocol codec for qstream.
//!
//! Every datagram is one message: fixed 14-byte header + optional payload.
//! All integers are big-endian (network byte order). See PROTOCOL.pdf §3.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

pub const MAGIC: [u8; 3] = *b"QST";
pub const PROTOCOL_VERSION: u8 = 0x03;
pub const HEADER_SIZE: usize = 14;
/// Number of recent segment positions represented by an availability mask.
pub const AVAILABILITY_MASK_BITS: u32 = 16;

/// Peerlist entry flags (N1):
pub const PEER_UPNP_MAPPED: u8 = 0x01; // claimed endpoint == observed (verified mapping)
pub const PEER_SAME_IP: u8 = 0x02; // same public IP as the requester (likely same NAT)
pub const PEER_PARENT: u8 = 0x04; // assigned preferred source by the master
pub const SEGMENT_NOT_READY: u8 = 0x01; // temporary source admission limit

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    HandshakeRequest = 0x01,
    HandshakeResponse = 0x02,
    ManifestRequest = 0x20,
    ManifestResponse = 0x21,
    SegmentRequest = 0x30,
    SegmentContents = 0x31,
    SegmentNotFound = 0x32,
    Ack = 0x40,
    PeerlistRequest = 0x50,
    PeerlistResponse = 0x51,
    Ping = 0x60,
    Pong = 0x61,
}

impl MessageType {
    pub fn from_u8(code: u8) -> Option<MessageType> {
        match code {
            0x01 => Some(Self::HandshakeRequest),
            0x02 => Some(Self::HandshakeResponse),
            0x20 => Some(Self::ManifestRequest),
            0x21 => Some(Self::ManifestResponse),
            0x30 => Some(Self::SegmentRequest),
            0x31 => Some(Self::SegmentContents),
            0x32 => Some(Self::SegmentNotFound),
            0x40 => Some(Self::Ack),
            0x50 => Some(Self::PeerlistRequest),
            0x51 => Some(Self::PeerlistResponse),
            0x60 => Some(Self::Ping),
            0x61 => Some(Self::Pong),
            _ => None,
        }
    }
}

/// ACK subtype carried in the header `flags` byte (PROTOCOL.pdf §6).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckType {
    /// ACK with a 4-byte payload `(next_start, next_count)` — request the
    /// next packet range.
    Progress = 0x00,
    /// ACK with empty payload — transfer complete.
    Complete = 0x04,
}

impl AckType {
    pub fn from_u8(v: u8) -> Option<AckType> {
        match v {
            0x00 => Some(Self::Progress),
            0x04 => Some(Self::Complete),
            _ => None,
        }
    }
}

/// Compact recent segment inventory. Bit 0 represents `newest`, bit 1
/// `newest - 1`, through bit 15. Segment numbers map to `seg_<number>.ts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentAvailability {
    pub newest: u64,
    pub mask: u16,
}

impl SegmentAvailability {
    /// Whether this inventory explicitly covers `segment` and says it exists.
    /// A segment outside the 16-entry window is unknown, not absent.
    pub fn contains(&self, segment: u64) -> Option<bool> {
        if segment > self.newest {
            return None;
        }
        let distance = self.newest - segment;
        if distance >= AVAILABILITY_MASK_BITS as u64 {
            return None;
        }
        Some(self.mask & (1 << distance) != 0)
    }
}

/// A decoded message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// HANDSHAKE_REQUEST — payload: claimed endpoint (6 B) + name (UTF-8).
    /// The claimed endpoint is a UPnP/NAT-PMP mapping, if any (0.0.0.0:0 if
    /// none).
    HandshakeRequest { claimed: SocketAddr, name: String },
    /// HANDSHAKE_RESPONSE — payload: observed endpoint (6 B) + name (UTF-8).
    /// The observed endpoint is the requester's public endpoint as seen by
    /// the responder (in-band STUN).
    HandshakeResponse { observed: SocketAddr, name: String },
    /// MANIFEST_REQUEST — no payload.
    ManifestRequest,
    /// MANIFEST_RESPONSE — payload: raw manifest (m3u8) bytes.
    ManifestResponse { data: Vec<u8> },
    /// SEGMENT_REQUEST — payload: filename (UTF-8).
    SegmentRequest { transfer_id: u16, filename: String },
    /// SEGMENT_CONTENTS — payload: file chunk ≤ SEGMENT_PACKET_SIZE.
    SegmentContents {
        transfer_id: u16,
        packet_number: u16,
        total_packets: u16,
        data: Vec<u8>,
    },
    /// SEGMENT_NOT_FOUND — optionally carries the responder's compact recent
    /// segment inventory. Empty payload remains compatible with older nodes.
    SegmentNotFound {
        transfer_id: u16,
        availability: Option<SegmentAvailability>,
        retryable: bool,
    },
    /// ACK — for Progress, payload is (next_start, next_count); for
    /// Complete, payload is empty.
    Ack {
        transfer_id: u16,
        ack_type: AckType,
        next_start: u16,
        next_count: u16,
    },
    /// PEERLIST_REQUEST — no payload.
    PeerlistRequest,
    /// PEERLIST_RESPONSE — payload: packed (ip:port) entries + flags byte.
    PeerlistResponse { peers: Vec<(SocketAddr, u8)> },
    /// PING — payload: beacon nonce (4 B) + node name (UTF-8). Doubles as
    /// LAN beacon when broadcast; a PONG proves the direct path works (N2).
    /// The nonce lets a node recognize (and ignore) its own broadcast echo.
    Ping { nonce: u32, name: String },
    /// PONG — optionally carries the sender's compact recent segment
    /// inventory. Empty payload remains compatible with older nodes.
    Pong {
        availability: Option<SegmentAvailability>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Datagram shorter than the fixed header.
    TruncatedHeader { len: usize },
    /// Magic bytes did not match `QST`.
    BadMagic { got: [u8; 3] },
    /// Unsupported protocol version.
    BadVersion { got: u8 },
    /// Unknown message type code.
    UnknownMessageType { code: u8 },
    /// Header's data length exceeds the datagram.
    TruncatedPayload { declared: u16, actual: usize },
    /// Node names and filenames must be valid UTF-8.
    InvalidUtf8,
    /// Unknown ACK flags byte.
    BadAckFlags { got: u8 },
    /// Progress ACK payload must be exactly 4 bytes.
    BadAckPayload { len: usize },
    /// Peerlist payload length must be a multiple of 7 bytes.
    BadPeerlistPayload { len: usize },
    /// Availability payload must be empty or exactly 10 bytes.
    BadAvailabilityPayload { len: usize },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::TruncatedHeader { len } => {
                write!(f, "datagram too short for header: {len} bytes")
            }
            ProtocolError::BadMagic { got } => {
                write!(f, "bad magic: {got:02x?} (expected QST)")
            }
            ProtocolError::BadVersion { got } => {
                write!(f, "unsupported protocol version: {got:#04x}")
            }
            ProtocolError::UnknownMessageType { code } => {
                write!(f, "unknown message type: {code:#04x}")
            }
            ProtocolError::TruncatedPayload { declared, actual } => {
                write!(
                    f,
                    "payload truncated: header says {declared} bytes, datagram has {actual}"
                )
            }
            ProtocolError::InvalidUtf8 => write!(f, "payload is not valid UTF-8"),
            ProtocolError::BadAckFlags { got } => {
                write!(f, "unknown ACK flags: {got:#04x}")
            }
            ProtocolError::BadAckPayload { len } => {
                write!(f, "progress ACK payload must be 4 bytes, got {len}")
            }
            ProtocolError::BadPeerlistPayload { len } => {
                write!(
                    f,
                    "peerlist payload must be a multiple of 7 bytes, got {len}"
                )
            }
            ProtocolError::BadAvailabilityPayload { len } => {
                write!(
                    f,
                    "availability payload must be empty or 10 bytes, got {len}"
                )
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Encode a message into a datagram buffer (header + payload).
pub fn encode(message: &Message) -> Vec<u8> {
    let (message_type, flags, transfer_id, packet_number, total_packets): (
        MessageType,
        u8,
        u16,
        u16,
        u16,
    ) = match message {
        Message::HandshakeRequest { .. } => (MessageType::HandshakeRequest, 0, 0, 0, 0),
        Message::HandshakeResponse { .. } => (MessageType::HandshakeResponse, 0, 0, 0, 0),
        Message::ManifestRequest => (MessageType::ManifestRequest, 0, 0, 0, 0),
        Message::ManifestResponse { .. } => (MessageType::ManifestResponse, 0, 0, 0, 0),
        Message::SegmentRequest { transfer_id, .. } => {
            (MessageType::SegmentRequest, 0, *transfer_id, 0, 0)
        }
        Message::SegmentContents {
            transfer_id,
            packet_number,
            total_packets,
            ..
        } => (
            MessageType::SegmentContents,
            0,
            *transfer_id,
            *packet_number,
            *total_packets,
        ),
        Message::SegmentNotFound {
            transfer_id,
            retryable,
            ..
        } => (
            MessageType::SegmentNotFound,
            if *retryable { SEGMENT_NOT_READY } else { 0 },
            *transfer_id,
            0,
            0,
        ),
        Message::Ack {
            transfer_id,
            ack_type,
            ..
        } => (MessageType::Ack, *ack_type as u8, *transfer_id, 0, 0),
        Message::PeerlistRequest => (MessageType::PeerlistRequest, 0, 0, 0, 0),
        Message::PeerlistResponse { .. } => (MessageType::PeerlistResponse, 0, 0, 0, 0),
        Message::Ping { .. } => (MessageType::Ping, 0, 0, 0, 0),
        Message::Pong { .. } => (MessageType::Pong, 0, 0, 0, 0),
    };

    let payload: Vec<u8> = match message {
        Message::HandshakeRequest { claimed, name } => {
            let mut p = endpoint_bytes(*claimed);
            p.extend_from_slice(name.as_bytes());
            p
        }
        Message::HandshakeResponse { observed, name } => {
            let mut p = endpoint_bytes(*observed);
            p.extend_from_slice(name.as_bytes());
            p
        }
        Message::ManifestRequest => Vec::new(),
        Message::ManifestResponse { data } => data.clone(),
        Message::SegmentRequest { filename, .. } => filename.as_bytes().to_vec(),
        Message::SegmentContents { data, .. } => data.clone(),
        Message::SegmentNotFound { availability, .. } => encode_availability(*availability),
        Message::Ack {
            ack_type,
            next_start,
            next_count,
            ..
        } => {
            if *ack_type == AckType::Progress {
                let mut p = Vec::with_capacity(4);
                p.extend_from_slice(&next_start.to_be_bytes());
                p.extend_from_slice(&next_count.to_be_bytes());
                p
            } else {
                Vec::new()
            }
        }
        Message::PeerlistRequest => Vec::new(),
        Message::PeerlistResponse { peers } => encode_peers(peers),
        Message::Ping { nonce, name } => {
            let mut p = nonce.to_be_bytes().to_vec();
            p.extend_from_slice(name.as_bytes());
            p
        }
        Message::Pong { availability } => encode_availability(*availability),
    };

    let mut buf = Vec::with_capacity(HEADER_SIZE + payload.len());
    buf.extend_from_slice(&MAGIC);
    buf.push(PROTOCOL_VERSION);
    buf.push(message_type as u8);
    buf.push(flags);
    buf.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    buf.extend_from_slice(&transfer_id.to_be_bytes());
    buf.extend_from_slice(&packet_number.to_be_bytes());
    buf.extend_from_slice(&total_packets.to_be_bytes());
    buf.extend_from_slice(&payload);
    buf
}

/// Decode a datagram into a message, validating magic, version and lengths.
pub fn decode(datagram: &[u8]) -> Result<Message, ProtocolError> {
    if datagram.len() < HEADER_SIZE {
        return Err(ProtocolError::TruncatedHeader {
            len: datagram.len(),
        });
    }

    let magic = [datagram[0], datagram[1], datagram[2]];
    if magic != MAGIC {
        return Err(ProtocolError::BadMagic { got: magic });
    }

    let version = datagram[3];
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::BadVersion { got: version });
    }

    let message_type = MessageType::from_u8(datagram[4])
        .ok_or(ProtocolError::UnknownMessageType { code: datagram[4] })?;
    let flags = datagram[5];
    let data_length = u16::from_be_bytes([datagram[6], datagram[7]]) as usize;
    let transfer_id = u16::from_be_bytes([datagram[8], datagram[9]]);
    let packet_number = u16::from_be_bytes([datagram[10], datagram[11]]);
    let total_packets = u16::from_be_bytes([datagram[12], datagram[13]]);

    let payload = &datagram[HEADER_SIZE..];
    if payload.len() < data_length {
        return Err(ProtocolError::TruncatedPayload {
            declared: data_length as u16,
            actual: payload.len(),
        });
    }
    let payload = &payload[..data_length];

    Ok(match message_type {
        MessageType::HandshakeRequest => {
            let (claimed, name) = split_endpoint_name(payload)?;
            Message::HandshakeRequest { claimed, name }
        }
        MessageType::HandshakeResponse => {
            let (observed, name) = split_endpoint_name(payload)?;
            Message::HandshakeResponse { observed, name }
        }
        MessageType::ManifestRequest => Message::ManifestRequest,
        MessageType::ManifestResponse => Message::ManifestResponse {
            data: payload.to_vec(),
        },
        MessageType::SegmentRequest => Message::SegmentRequest {
            transfer_id,
            filename: utf8_name(payload)?,
        },
        MessageType::SegmentContents => Message::SegmentContents {
            transfer_id,
            packet_number,
            total_packets,
            data: payload.to_vec(),
        },
        MessageType::SegmentNotFound => Message::SegmentNotFound {
            transfer_id,
            availability: decode_availability(payload)?,
            retryable: flags & SEGMENT_NOT_READY != 0,
        },
        MessageType::Ack => {
            let ack_type =
                AckType::from_u8(flags).ok_or(ProtocolError::BadAckFlags { got: flags })?;
            match ack_type {
                AckType::Complete => Message::Ack {
                    transfer_id,
                    ack_type,
                    next_start: 0,
                    next_count: 0,
                },
                AckType::Progress => {
                    if payload.len() != 4 {
                        return Err(ProtocolError::BadAckPayload { len: payload.len() });
                    }
                    Message::Ack {
                        transfer_id,
                        ack_type,
                        next_start: u16::from_be_bytes([payload[0], payload[1]]),
                        next_count: u16::from_be_bytes([payload[2], payload[3]]),
                    }
                }
            }
        }
        MessageType::PeerlistRequest => Message::PeerlistRequest,
        MessageType::PeerlistResponse => Message::PeerlistResponse {
            peers: decode_peers(payload)?,
        },
        MessageType::Ping => {
            if payload.len() < 4 {
                return Err(ProtocolError::TruncatedPayload {
                    declared: payload.len() as u16,
                    actual: 0,
                });
            }
            let nonce = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            Message::Ping {
                nonce,
                name: utf8_name(&payload[4..])?,
            }
        }
        MessageType::Pong => Message::Pong {
            availability: decode_availability(payload)?,
        },
    })
}

fn encode_availability(availability: Option<SegmentAvailability>) -> Vec<u8> {
    let Some(availability) = availability else {
        return Vec::new();
    };
    let mut payload = Vec::with_capacity(10);
    payload.extend_from_slice(&availability.newest.to_be_bytes());
    payload.extend_from_slice(&availability.mask.to_be_bytes());
    payload
}

fn decode_availability(payload: &[u8]) -> Result<Option<SegmentAvailability>, ProtocolError> {
    match payload.len() {
        0 => Ok(None),
        10 => Ok(Some(SegmentAvailability {
            newest: u64::from_be_bytes(payload[..8].try_into().unwrap()),
            mask: u16::from_be_bytes(payload[8..].try_into().unwrap()),
        })),
        len => Err(ProtocolError::BadAvailabilityPayload { len }),
    }
}

/// Encode a SocketAddr as 6 bytes (ipv4 octets + big-endian port);
/// non-IPv4 or port 0 becomes all zeros.
fn endpoint_bytes(addr: SocketAddr) -> Vec<u8> {
    let mut p = Vec::with_capacity(6);
    match addr {
        SocketAddr::V4(v4) if v4.port() != 0 => {
            p.extend_from_slice(&v4.ip().octets());
            p.extend_from_slice(&v4.port().to_be_bytes());
        }
        _ => p.extend_from_slice(&[0u8; 6]),
    }
    p
}

/// Split a 6-byte endpoint prefix from the payload; the rest is the name.
fn split_endpoint_name(payload: &[u8]) -> Result<(SocketAddr, String), ProtocolError> {
    if payload.len() < 6 {
        return Err(ProtocolError::TruncatedPayload {
            declared: payload.len() as u16,
            actual: 0,
        });
    }
    let ip = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
    let port = u16::from_be_bytes([payload[4], payload[5]]);
    let name = utf8_name(&payload[6..])?;
    Ok((SocketAddr::new(IpAddr::V4(ip), port), name))
}

/// Pack peer entries: 4-byte IPv4 octets + 2-byte big-endian port + 1 flag
/// byte, per entry.
fn encode_peers(peers: &[(SocketAddr, u8)]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(peers.len() * 7);
    for (peer, flags) in peers {
        if let SocketAddr::V4(v4) = peer {
            payload.extend_from_slice(&v4.ip().octets());
            payload.extend_from_slice(&v4.port().to_be_bytes());
            payload.push(*flags);
        }
    }
    payload
}

/// Decode packed peer entries; skips malformed ones (port 0, IPv6).
fn decode_peers(payload: &[u8]) -> Result<Vec<(SocketAddr, u8)>, ProtocolError> {
    if payload.len() % 7 != 0 {
        return Err(ProtocolError::BadPeerlistPayload { len: payload.len() });
    }
    let mut peers = Vec::new();
    for chunk in payload.chunks_exact(7) {
        let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);
        if port != 0 {
            peers.push((SocketAddr::new(IpAddr::V4(ip), port), chunk[6]));
        }
    }
    Ok(peers)
}

fn utf8_name(payload: &[u8]) -> Result<String, ProtocolError> {
    String::from_utf8(payload.to_vec()).map_err(|_| ProtocolError::InvalidUtf8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_handshake_request() {
        let msg = Message::HandshakeRequest {
            claimed: SocketAddr::from(([203, 0, 113, 7], 54444)),
            name: "peer-1".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_handshake_request_no_mapping() {
        let msg = Message::HandshakeRequest {
            claimed: SocketAddr::from(([0, 0, 0, 0], 0)),
            name: "peer-1".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_handshake_response() {
        let msg = Message::HandshakeResponse {
            observed: SocketAddr::from(([203, 0, 113, 7], 54321)),
            name: "master".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_ping_pong() {
        let ping = Message::Ping {
            nonce: 0xDEADBEEF,
            name: "peer-1".to_string(),
        };
        assert_eq!(decode(&encode(&ping)).unwrap(), ping);
        let legacy_pong = Message::Pong { availability: None };
        assert_eq!(decode(&encode(&legacy_pong)).unwrap(), legacy_pong);
        let pong = Message::Pong {
            availability: Some(SegmentAvailability {
                newest: 133_579,
                mask: 0b1011,
            }),
        };
        assert_eq!(decode(&encode(&pong)).unwrap(), pong);
    }

    #[test]
    fn roundtrip_manifest_response() {
        let msg = Message::ManifestResponse {
            data: b"#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.0,\nseg_0000.ts\n".to_vec(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_segment_request() {
        let msg = Message::SegmentRequest {
            transfer_id: 0xBEEF,
            filename: "seg_0042.ts".to_string(),
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_segment_contents() {
        let msg = Message::SegmentContents {
            transfer_id: 0xBEEF,
            packet_number: 7,
            total_packets: 60,
            data: vec![0x42; 1400],
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_segment_not_found() {
        let legacy = Message::SegmentNotFound {
            transfer_id: 0xBEEF,
            availability: None,
            retryable: false,
        };
        assert_eq!(decode(&encode(&legacy)).unwrap(), legacy);
        let msg = Message::SegmentNotFound {
            transfer_id: 0xBEEF,
            availability: Some(SegmentAvailability {
                newest: 133_579,
                mask: 0b1111,
            }),
            retryable: true,
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
        let datagram = encode(&msg);
        assert_eq!(datagram[5], SEGMENT_NOT_READY);
    }

    #[test]
    fn availability_mask_represents_newest_first() {
        let availability = SegmentAvailability {
            newest: 100,
            mask: 0b0101,
        };
        assert_eq!(availability.contains(100), Some(true));
        assert_eq!(availability.contains(99), Some(false));
        assert_eq!(availability.contains(98), Some(true));
        assert_eq!(availability.contains(84), None);
        assert_eq!(availability.contains(101), None);
    }

    #[test]
    fn rejects_invalid_availability_payload() {
        let mut datagram = encode(&Message::Pong { availability: None });
        datagram[6] = 0;
        datagram[7] = 1;
        datagram.push(0);
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadAvailabilityPayload { len: 1 })
        ));
    }

    #[test]
    fn roundtrip_ack_progress() {
        let msg = Message::Ack {
            transfer_id: 0xBEEF,
            ack_type: AckType::Progress,
            next_start: 6,
            next_count: 10,
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_ack_complete() {
        let msg = Message::Ack {
            transfer_id: 0xBEEF,
            ack_type: AckType::Complete,
            next_start: 0,
            next_count: 0,
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn roundtrip_peerlist_request() {
        let msg = Message::PeerlistRequest;
        let datagram = encode(&msg);
        assert_eq!(datagram.len(), HEADER_SIZE);
        assert_eq!(decode(&datagram).unwrap(), msg);
    }

    #[test]
    fn roundtrip_peerlist_response() {
        let msg = Message::PeerlistResponse {
            peers: vec![
                (
                    SocketAddr::from(([127, 0, 0, 1], 4444)),
                    PEER_UPNP_MAPPED | PEER_PARENT,
                ),
                (SocketAddr::from(([10, 0, 0, 2], 5555)), 0),
            ],
        };
        assert_eq!(decode(&encode(&msg)).unwrap(), msg);
    }

    #[test]
    fn peerlist_response_wire_format() {
        let msg = Message::PeerlistResponse {
            peers: vec![
                (
                    SocketAddr::from(([127, 0, 0, 1], 4444)),
                    PEER_UPNP_MAPPED | PEER_PARENT,
                ),
                (SocketAddr::from(([10, 0, 0, 2], 5555)), 0),
            ],
        };
        let expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x51, 0x00, 0x00, 0x0E, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x7F, 0x00, 0x00, 0x01, 0x11, 0x5C, 0x05, // 127.0.0.1:4444, upnp + parent
            0x0A, 0x00, 0x00, 0x02, 0x15, 0xB3, 0x00, // 10.0.0.2:5555
        ];
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    #[test]
    fn rejects_bad_peerlist_payload() {
        let msg = Message::PeerlistResponse {
            peers: vec![(SocketAddr::from(([127, 0, 0, 1], 4444)), 0)],
        };
        let mut datagram = encode(&msg);
        // Declare a 5-byte payload instead of 7.
        datagram[6] = 0x00;
        datagram[7] = 0x05;
        datagram.truncate(HEADER_SIZE + 5);
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadPeerlistPayload { len: 5 })
        ));
    }

    // ---- wire-format vectors matching PROTOCOL.pdf §5/§6 examples ----

    #[test]
    fn manifest_request_wire_format() {
        let expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, // magic QST
            0x03, // version
            0x20, // MANIFEST_REQUEST
            0x00, // flags
            0x00, 0x00, // data length
            0x00, 0x00, // transfer id
            0x00, 0x00, // packet #
            0x00, 0x00, // total
        ];
        assert_eq!(encode(&Message::ManifestRequest), expected);
        assert_eq!(decode(&expected).unwrap(), Message::ManifestRequest);
    }

    #[test]
    fn handshake_request_wire_format() {
        let msg = Message::HandshakeRequest {
            claimed: SocketAddr::from(([203, 0, 113, 7], 54444)),
            name: "peer-1".to_string(),
        };
        let mut expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x01, 0x00, 0x00, 0x0C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xCB, 0x00, 0x71, 0x07, 0xD4, 0xAC, // claimed 203.0.113.7:54444
        ];
        expected.extend_from_slice(b"peer-1");
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    #[test]
    fn ping_wire_format() {
        let msg = Message::Ping {
            nonce: 0xDEADBEEF,
            name: "peer-1".to_string(),
        };
        let mut expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x60, 0x00, 0x00, 0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xDE, 0xAD, 0xBE, 0xEF, // nonce
        ];
        expected.extend_from_slice(b"peer-1");
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    #[test]
    fn segment_request_wire_format() {
        let msg = Message::SegmentRequest {
            transfer_id: 0x01A7,
            filename: "seg_0042.ts".to_string(),
        };
        let mut expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x30, 0x00, 0x00, 0x0B, 0x01, 0xA7, 0x00, 0x00, 0x00, 0x00,
        ];
        expected.extend_from_slice(b"seg_0042.ts");
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    #[test]
    fn segment_contents_wire_format() {
        let msg = Message::SegmentContents {
            transfer_id: 0x01A7,
            packet_number: 1,
            total_packets: 60,
            data: vec![0xAB; 1400],
        };
        let mut expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x31, 0x00, 0x05, 0x78, 0x01, 0xA7, 0x00, 0x01, 0x00, 0x3C,
        ];
        expected.extend(vec![0xAB; 1400]);
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    #[test]
    fn ack_progress_wire_format() {
        let msg = Message::Ack {
            transfer_id: 0x01A7,
            ack_type: AckType::Progress,
            next_start: 6,
            next_count: 5,
        };
        let expected: Vec<u8> = vec![
            0x51, 0x53, 0x54, 0x03, 0x40, 0x00, 0x00, 0x04, 0x01, 0xA7, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x06, 0x00, 0x05,
        ];
        assert_eq!(encode(&msg), expected);
        assert_eq!(decode(&expected).unwrap(), msg);
    }

    // ---- malformed datagrams ----

    #[test]
    fn rejects_short_datagram() {
        assert!(matches!(
            decode(&[0x51, 0x53, 0x54]),
            Err(ProtocolError::TruncatedHeader { .. })
        ));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut datagram = encode(&Message::ManifestRequest);
        datagram[0] = 0x00;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadMagic { .. })
        ));
    }

    #[test]
    fn rejects_bad_version() {
        let mut datagram = encode(&Message::ManifestRequest);
        datagram[3] = 0x63;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadVersion { .. })
        ));
    }

    #[test]
    fn rejects_unknown_message_type() {
        let mut datagram = encode(&Message::ManifestRequest);
        datagram[4] = 0x99;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::UnknownMessageType { code: 0x99 })
        ));
    }

    #[test]
    fn rejects_truncated_payload() {
        let datagram = encode(&Message::SegmentRequest {
            transfer_id: 1,
            filename: "seg_0042.ts".to_string(),
        });
        let truncated = &datagram[..datagram.len() - 2];
        assert!(matches!(
            decode(truncated),
            Err(ProtocolError::TruncatedPayload { .. })
        ));
    }

    #[test]
    fn rejects_bad_ack_flags() {
        let mut datagram = encode(&Message::Ack {
            transfer_id: 1,
            ack_type: AckType::Complete,
            next_start: 0,
            next_count: 0,
        });
        datagram[5] = 0x02;
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadAckFlags { got: 0x02 })
        ));
    }

    #[test]
    fn rejects_bad_ack_payload() {
        let msg = Message::Ack {
            transfer_id: 1,
            ack_type: AckType::Progress,
            next_start: 6,
            next_count: 10,
        };
        let mut datagram = encode(&msg);
        // Declare a 5-byte payload instead of 4.
        datagram[6] = 0x00;
        datagram[7] = 0x05;
        datagram.push(0x00);
        assert!(matches!(
            decode(&datagram),
            Err(ProtocolError::BadAckPayload { len: 5 })
        ));
    }
}
