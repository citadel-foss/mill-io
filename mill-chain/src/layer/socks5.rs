//! SOCKS5 CONNECT (RFC 1928) with username/password auth (RFC 1929), as a mill-net layer.
//!
//! Hostnames go to the proxy as ATYP 3 and are never resolved locally. Tor's extended
//! reply codes (`socks-extensions.txt`) get their own errors.

use super::dns::ip_literal;
use mill_net::tcp::Layer;
use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

const VERSION: u8 = 5;
const AUTH_VERSION: u8 = 1;
const NO_AUTH: u8 = 0x00;
const USER_PASS: u8 = 0x02;
const NO_ACCEPTABLE: u8 = 0xFF;
const CMD_CONNECT: u8 = 1;
const ATYP_V4: u8 = 1;
const ATYP_DOMAIN: u8 = 3;
const ATYP_V6: u8 = 4;

/// Why a SOCKS5 handshake failed. Reply codes come from RFC 1928 and Tor's
/// `socks-extensions.txt`; calls see the [`Display`](fmt::Display) text inside
/// [`ChainError::Transport`](crate::ChainError::Transport).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Socks5Error {
    /// Reply 0x01: general SOCKS server failure.
    GeneralFailure,
    /// Reply 0x02: the proxy's ruleset does not allow the connection.
    NotAllowed,
    /// Reply 0x03: network unreachable.
    NetworkUnreachable,
    /// Reply 0x04: host unreachable; Tor also uses it when it cannot resolve the name.
    HostUnreachable,
    /// Reply 0x05: the target refused the connection.
    ConnectionRefused,
    /// Reply 0x06: TTL expired; Tor uses it when the connection attempt timed out.
    TtlExpired,
    /// Reply 0x07: the proxy does not support CONNECT.
    CommandNotSupported,
    /// Reply 0x08: the proxy does not support the address type sent.
    AddressTypeNotSupported,
    /// Tor reply 0xF0: the onion service descriptor cannot be found.
    OnionDescriptorNotFound,
    /// Tor reply 0xF1: the onion service descriptor is invalid.
    OnionDescriptorInvalid,
    /// Tor reply 0xF2: introduction to the onion service failed.
    OnionIntroFailed,
    /// Tor reply 0xF3: the rendezvous with the onion service failed.
    OnionRendezvousFailed,
    /// Tor reply 0xF4: the onion service needs client authorization.
    OnionMissingClientAuth,
    /// Tor reply 0xF5: the onion service rejected the client authorization.
    OnionWrongClientAuth,
    /// Tor reply 0xF6: the onion address is invalid.
    OnionBadAddress,
    /// Tor reply 0xF7: introduction to the onion service timed out.
    OnionIntroTimedOut,
    /// Any other nonzero reply code.
    Other(u8),
    /// The proxy accepted none of the authentication methods offered.
    NoAcceptableAuth,
    /// The proxy rejected the username and password.
    AuthRejected,
    /// The proxy's reply does not follow the protocol.
    Malformed(&'static str),
    /// The hostname is empty or longer than 255 bytes.
    BadHostname,
}

impl Socks5Error {
    /// The error for a CONNECT reply code; `None` for 0x00, which is success.
    pub fn from_reply(code: u8) -> Option<Self> {
        Some(match code {
            0x00 => return None,
            0x01 => Self::GeneralFailure,
            0x02 => Self::NotAllowed,
            0x03 => Self::NetworkUnreachable,
            0x04 => Self::HostUnreachable,
            0x05 => Self::ConnectionRefused,
            0x06 => Self::TtlExpired,
            0x07 => Self::CommandNotSupported,
            0x08 => Self::AddressTypeNotSupported,
            0xF0 => Self::OnionDescriptorNotFound,
            0xF1 => Self::OnionDescriptorInvalid,
            0xF2 => Self::OnionIntroFailed,
            0xF3 => Self::OnionRendezvousFailed,
            0xF4 => Self::OnionMissingClientAuth,
            0xF5 => Self::OnionWrongClientAuth,
            0xF6 => Self::OnionBadAddress,
            0xF7 => Self::OnionIntroTimedOut,
            other => Self::Other(other),
        })
    }

    /// The CONNECT reply code this error came from, if any.
    pub fn reply_code(&self) -> Option<u8> {
        Some(match self {
            Self::GeneralFailure => 0x01,
            Self::NotAllowed => 0x02,
            Self::NetworkUnreachable => 0x03,
            Self::HostUnreachable => 0x04,
            Self::ConnectionRefused => 0x05,
            Self::TtlExpired => 0x06,
            Self::CommandNotSupported => 0x07,
            Self::AddressTypeNotSupported => 0x08,
            Self::OnionDescriptorNotFound => 0xF0,
            Self::OnionDescriptorInvalid => 0xF1,
            Self::OnionIntroFailed => 0xF2,
            Self::OnionRendezvousFailed => 0xF3,
            Self::OnionMissingClientAuth => 0xF4,
            Self::OnionWrongClientAuth => 0xF5,
            Self::OnionBadAddress => 0xF6,
            Self::OnionIntroTimedOut => 0xF7,
            Self::Other(code) => *code,
            Self::NoAcceptableAuth
            | Self::AuthRejected
            | Self::Malformed(_)
            | Self::BadHostname => return None,
        })
    }
}

impl fmt::Display for Socks5Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self {
            Self::GeneralFailure => "general server failure",
            Self::NotAllowed => "connection not allowed by ruleset",
            Self::NetworkUnreachable => "network unreachable",
            Self::HostUnreachable => "host unreachable",
            Self::ConnectionRefused => "connection refused",
            Self::TtlExpired => "TTL expired",
            Self::CommandNotSupported => "command not supported",
            Self::AddressTypeNotSupported => "address type not supported",
            Self::OnionDescriptorNotFound => "onion service descriptor not found",
            Self::OnionDescriptorInvalid => "onion service descriptor is invalid",
            Self::OnionIntroFailed => "onion service introduction failed",
            Self::OnionRendezvousFailed => "onion service rendezvous failed",
            Self::OnionMissingClientAuth => "onion service needs client authorization",
            Self::OnionWrongClientAuth => "onion service rejected the client authorization",
            Self::OnionBadAddress => "invalid onion address",
            Self::OnionIntroTimedOut => "onion service introduction timed out",
            Self::Other(_) => "unknown reply",
            Self::NoAcceptableAuth => return write!(f, "SOCKS5: no acceptable auth method"),
            Self::AuthRejected => return write!(f, "SOCKS5: username/password rejected"),
            Self::Malformed(what) => return write!(f, "SOCKS5: malformed reply: {}", what),
            Self::BadHostname => return write!(f, "SOCKS5: hostname must be 1-255 bytes"),
        };
        match self.reply_code() {
            Some(code) => write!(f, "SOCKS5: {} (0x{:02x})", text, code),
            None => write!(f, "SOCKS5: {}", text),
        }
    }
}

impl std::error::Error for Socks5Error {}

impl From<Socks5Error> for io::Error {
    fn from(e: Socks5Error) -> Self {
        io::Error::new(io::ErrorKind::ConnectionRefused, e)
    }
}

/// A fresh username and password for one connection. Tor isolates streams by them.
pub(crate) fn isolation_credentials() -> (String, String) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let token = || {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(NEXT.fetch_add(1, Ordering::Relaxed));
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        hasher.write_u128(now);
        format!("{:016x}", hasher.finish())
    };
    (token(), token())
}

/// The CONNECT target as the request encodes it.
pub(crate) fn encode_address(host: &str, port: u16, out: &mut Vec<u8>) -> Result<(), Socks5Error> {
    match ip_literal(host) {
        Some(IpAddr::V4(ip)) => {
            out.push(ATYP_V4);
            out.extend_from_slice(&ip.octets());
        }
        Some(IpAddr::V6(ip)) => {
            out.push(ATYP_V6);
            out.extend_from_slice(&ip.octets());
        }
        None => {
            let len = u8::try_from(host.len()).map_err(|_| Socks5Error::BadHostname)?;
            if len == 0 {
                return Err(Socks5Error::BadHostname);
            }
            out.push(ATYP_DOMAIN);
            out.push(len);
            out.extend_from_slice(host.as_bytes());
        }
    }
    out.extend_from_slice(&port.to_be_bytes());
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Method,
    Auth,
    Reply,
    Ready,
}

/// Client side of one SOCKS5 CONNECT.
pub(crate) struct Socks5Layer {
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
    stage: Stage,
    buf: Vec<u8>,
}

impl Socks5Layer {
    /// Connects to `host:port` through the proxy, authenticating with `credentials` if set.
    pub(crate) fn new(host: String, port: u16, credentials: Option<(String, String)>) -> Self {
        Self {
            host,
            port,
            credentials,
            stage: Stage::Method,
            buf: Vec::new(),
        }
    }

    fn connect_request(&self, out: &mut Vec<u8>) -> Result<(), Socks5Error> {
        out.extend_from_slice(&[VERSION, CMD_CONNECT, 0]);
        encode_address(&self.host, self.port, out)
    }

    fn auth_request(&self, out: &mut Vec<u8>) -> Result<(), Socks5Error> {
        let Some((user, pass)) = &self.credentials else {
            return Err(Socks5Error::Malformed("chose an auth method not offered"));
        };
        let field = |s: &str| u8::try_from(s.len()).ok().filter(|&n| n > 0);
        let (Some(user_len), Some(pass_len)) = (field(user), field(pass)) else {
            return Err(Socks5Error::AuthRejected);
        };
        out.extend_from_slice(&[AUTH_VERSION, user_len]);
        out.extend_from_slice(user.as_bytes());
        out.push(pass_len);
        out.extend_from_slice(pass.as_bytes());
        Ok(())
    }

    /// Consumes one complete message from `buf`; false if more bytes are needed.
    fn step(&mut self, out: &mut Vec<u8>) -> Result<bool, Socks5Error> {
        match self.stage {
            Stage::Method => {
                let [version, method, ..] = self.buf[..] else {
                    return Ok(false);
                };
                if version != VERSION {
                    return Err(Socks5Error::Malformed("bad version in method reply"));
                }
                self.buf.drain(..2);
                match method {
                    NO_ACCEPTABLE => return Err(Socks5Error::NoAcceptableAuth),
                    USER_PASS => {
                        self.auth_request(out)?;
                        self.stage = Stage::Auth;
                    }
                    NO_AUTH if self.credentials.is_none() => {
                        self.connect_request(out)?;
                        self.stage = Stage::Reply;
                    }
                    _ => return Err(Socks5Error::Malformed("chose an auth method not offered")),
                }
            }
            Stage::Auth => {
                let [version, status, ..] = self.buf[..] else {
                    return Ok(false);
                };
                if version != AUTH_VERSION {
                    return Err(Socks5Error::Malformed("bad version in auth reply"));
                }
                if status != 0 {
                    return Err(Socks5Error::AuthRejected);
                }
                self.buf.drain(..2);
                self.connect_request(out)?;
                self.stage = Stage::Reply;
            }
            Stage::Reply => {
                let [version, reply, reserved, atyp, ..] = self.buf[..] else {
                    return Ok(false);
                };
                if version != VERSION {
                    return Err(Socks5Error::Malformed("bad version in CONNECT reply"));
                }
                if let Some(error) = Socks5Error::from_reply(reply) {
                    return Err(error);
                }
                if reserved != 0 {
                    return Err(Socks5Error::Malformed("nonzero reserved byte"));
                }
                let addr_len = match atyp {
                    ATYP_V4 => 4,
                    ATYP_V6 => 16,
                    ATYP_DOMAIN => match self.buf.get(4) {
                        Some(&len) => 1 + usize::from(len),
                        None => return Ok(false),
                    },
                    _ => return Err(Socks5Error::Malformed("unknown address type")),
                };
                let total = 4 + addr_len + 2;
                if self.buf.len() < total {
                    return Ok(false);
                }
                self.buf.drain(..total);
                self.stage = Stage::Ready;
            }
            Stage::Ready => return Ok(false),
        }
        Ok(true)
    }
}

impl Layer for Socks5Layer {
    fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        let method = if self.credentials.is_some() {
            USER_PASS
        } else {
            NO_AUTH
        };
        out.extend_from_slice(&[VERSION, 1, method]);
        Ok(())
    }

    fn decode(&mut self, input: &[u8], up: &mut Vec<u8>, out: &mut Vec<u8>) -> io::Result<()> {
        if self.stage == Stage::Ready {
            up.extend_from_slice(input);
            return Ok(());
        }
        self.buf.extend_from_slice(input);
        while self.step(out)? {}
        if self.stage == Stage::Ready {
            up.append(&mut self.buf);
        }
        Ok(())
    }

    fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        out.extend_from_slice(frame);
        Ok(())
    }

    fn is_ready(&self) -> bool {
        self.stage == Stage::Ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds() -> Option<(String, String)> {
        Some(("user".into(), "secret".into()))
    }

    fn started(layer: &mut Socks5Layer) -> Vec<u8> {
        let mut out = Vec::new();
        layer.start(&mut out).unwrap();
        out
    }

    /// Feeds `input` one byte at a time; returns what went up and what went out.
    fn feed_bytewise(layer: &mut Socks5Layer, input: &[u8]) -> io::Result<(Vec<u8>, Vec<u8>)> {
        let (mut up, mut out) = (Vec::new(), Vec::new());
        for byte in input {
            layer.decode(&[*byte], &mut up, &mut out)?;
        }
        Ok((up, out))
    }

    fn error_of(result: io::Result<impl fmt::Debug>) -> Socks5Error {
        let error = result.unwrap_err();
        *error
            .get_ref()
            .and_then(|e| e.downcast_ref::<Socks5Error>())
            .unwrap()
    }

    const SUCCESS_V4: [u8; 10] = [5, 0, 0, 1, 127, 0, 0, 1, 0x1f, 0x90];

    #[test]
    fn no_auth_connect_by_domain() {
        let mut layer = Socks5Layer::new("example.onion".into(), 50001, None);
        assert_eq!(started(&mut layer), [5, 1, 0]);
        let mut reply = vec![5, 0];
        reply.extend_from_slice(&SUCCESS_V4);
        reply.extend_from_slice(b"hello");
        let (up, out) = feed_bytewise(&mut layer, &reply).unwrap();
        let mut request = vec![5, 1, 0, 3, 13];
        request.extend_from_slice(b"example.onion");
        request.extend_from_slice(&50001u16.to_be_bytes());
        assert_eq!(out, request);
        assert_eq!(up, b"hello");
        assert!(layer.is_ready());

        let mut wire = Vec::new();
        layer.encode(b"ping", &mut wire).unwrap();
        assert_eq!(wire, b"ping");
        let (mut up, mut out) = (Vec::new(), Vec::new());
        layer.decode(b"pong", &mut up, &mut out).unwrap();
        assert_eq!((up.as_slice(), out.len()), (&b"pong"[..], 0));
    }

    #[test]
    fn user_pass_then_connect_in_one_chunk() {
        let mut layer = Socks5Layer::new("10.1.2.3".into(), 80, creds());
        assert_eq!(started(&mut layer), [5, 1, 2]);
        let (mut up, mut out) = (Vec::new(), Vec::new());
        layer.decode(&[5, 2, 1, 0], &mut up, &mut out).unwrap();
        let mut expected = vec![1, 4];
        expected.extend_from_slice(b"user");
        expected.push(6);
        expected.extend_from_slice(b"secret");
        expected.extend_from_slice(&[5, 1, 0, 1, 10, 1, 2, 3, 0, 80]);
        assert_eq!(out, expected);
        assert!(up.is_empty() && !layer.is_ready());
        layer.decode(&SUCCESS_V4, &mut up, &mut out).unwrap();
        assert!(layer.is_ready() && up.is_empty());
    }

    #[test]
    fn ipv6_targets_and_bound_addresses() {
        let mut layer = Socks5Layer::new("[2001:db8::1]".into(), 443, None);
        started(&mut layer);
        let mut reply = vec![5, 0, 5, 0, 0, 4];
        reply.extend_from_slice(&[0; 16]);
        reply.extend_from_slice(&[0, 0, b'x']);
        let (up, out) = feed_bytewise(&mut layer, &reply).unwrap();
        assert_eq!(&out[..4], [5, 1, 0, 4]);
        assert_eq!(
            out[4..20],
            "2001:db8::1"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .octets()
        );
        assert_eq!(out[20..], [1, 187]);
        assert_eq!(up, b"x");
    }

    #[test]
    fn domain_bound_address_is_skipped() {
        let mut layer = Socks5Layer::new("host".into(), 1, None);
        started(&mut layer);
        let mut reply = vec![5, 0, 5, 0, 0, 3, 3];
        reply.extend_from_slice(b"abc");
        reply.extend_from_slice(&[0, 1]);
        let (up, _) = feed_bytewise(&mut layer, &reply).unwrap();
        assert!(layer.is_ready() && up.is_empty());
    }

    #[test]
    fn every_reply_code_is_its_own_error() {
        let codes: Vec<u8> = (0x01..=0x08).chain(0xF0..=0xF7).collect();
        let mut texts = std::collections::HashSet::new();
        for code in codes.iter().copied() {
            let mut layer = Socks5Layer::new("h".into(), 1, None);
            started(&mut layer);
            let error = error_of(feed_bytewise(&mut layer, &[5, 0, 5, code, 0, 1]));
            assert_eq!(error, Socks5Error::from_reply(code).unwrap());
            assert_eq!(error.reply_code(), Some(code));
            assert_ne!(error, Socks5Error::Other(code));
            assert!(texts.insert(error.to_string()));
        }
        assert_eq!(Socks5Error::from_reply(0), None);
        assert_eq!(
            Socks5Error::from_reply(0x09),
            Some(Socks5Error::Other(0x09))
        );
        assert_eq!(Socks5Error::Other(0x09).reply_code(), Some(0x09));
        assert_eq!(Socks5Error::AuthRejected.reply_code(), None);
    }

    #[test]
    fn auth_failures() {
        let mut layer = Socks5Layer::new("h".into(), 1, None);
        started(&mut layer);
        assert_eq!(
            error_of(feed_bytewise(&mut layer, &[5, 0xFF])),
            Socks5Error::NoAcceptableAuth
        );

        let mut layer = Socks5Layer::new("h".into(), 1, creds());
        started(&mut layer);
        assert_eq!(
            error_of(feed_bytewise(&mut layer, &[5, 2, 1, 1])),
            Socks5Error::AuthRejected
        );

        let mut layer = Socks5Layer::new("h".into(), 1, creds());
        started(&mut layer);
        assert!(matches!(
            error_of(feed_bytewise(&mut layer, &[5, 0])),
            Socks5Error::Malformed(_)
        ));

        let mut layer = Socks5Layer::new("h".into(), 1, None);
        started(&mut layer);
        assert!(matches!(
            error_of(feed_bytewise(&mut layer, &[5, 2])),
            Socks5Error::Malformed(_)
        ));

        let mut layer = Socks5Layer::new("h".into(), 1, Some((String::new(), "p".into())));
        started(&mut layer);
        assert_eq!(
            error_of(feed_bytewise(&mut layer, &[5, 2])),
            Socks5Error::AuthRejected
        );
    }

    #[test]
    fn malformed_replies() {
        let cases: [(&[u8], bool); 6] = [
            (&[4, 0], false),
            (&[5, 2, 2, 0], true),
            (&[5, 0, 4, 0, 0, 1], false),
            (&[5, 0, 5, 0, 1, 1], false),
            (&[5, 0, 5, 0, 0, 9], false),
            (&[5, 9], false),
        ];
        for (reply, with_creds) in cases {
            let credentials = if with_creds { creds() } else { None };
            let mut layer = Socks5Layer::new("h".into(), 1, credentials);
            started(&mut layer);
            assert!(
                matches!(
                    error_of(feed_bytewise(&mut layer, reply)),
                    Socks5Error::Malformed(_)
                ),
                "{:?}",
                reply
            );
        }
    }

    #[test]
    fn incomplete_replies_wait_for_more() {
        let mut layer = Socks5Layer::new("h".into(), 1, None);
        started(&mut layer);
        let (up, out) = feed_bytewise(&mut layer, &[5, 0, 5, 0, 0, 3, 4, b'a']).unwrap();
        assert!(!layer.is_ready() && up.is_empty() && !out.is_empty());
        feed_bytewise(&mut layer, &[b'b', b'c', b'd', 0]).unwrap();
        assert!(!layer.is_ready());
        feed_bytewise(&mut layer, &[1]).unwrap();
        assert!(layer.is_ready());
    }

    #[test]
    fn hostnames_must_fit_one_length_byte() {
        let mut out = Vec::new();
        assert_eq!(
            encode_address("", 1, &mut out),
            Err(Socks5Error::BadHostname)
        );
        assert_eq!(
            encode_address(&"a".repeat(256), 1, &mut out),
            Err(Socks5Error::BadHostname)
        );
        assert!(out.is_empty());
        encode_address(&"a".repeat(255), 1, &mut out).unwrap();
        assert_eq!(out.len(), 2 + 255 + 2);

        let mut layer = Socks5Layer::new(String::new(), 1, None);
        started(&mut layer);
        assert_eq!(
            error_of(feed_bytewise(&mut layer, &[5, 0])),
            Socks5Error::BadHostname
        );
    }

    #[test]
    fn credentials_differ_per_call() {
        let (a, b) = (isolation_credentials(), isolation_credentials());
        assert_ne!(a, b);
        assert_ne!(a.0, a.1);
        assert!(a.0.len() == 16 && a.1.len() == 16);
    }

    #[test]
    fn errors_keep_their_text_through_io() {
        let error: io::Error = Socks5Error::OnionIntroTimedOut.into();
        assert_eq!(
            error.to_string(),
            "SOCKS5: onion service introduction timed out (0xf7)"
        );
        assert_eq!(
            Socks5Error::Other(0x42).to_string(),
            "SOCKS5: unknown reply (0x42)"
        );
        assert_eq!(
            Socks5Error::NoAcceptableAuth.to_string(),
            "SOCKS5: no acceptable auth method"
        );
    }
}
