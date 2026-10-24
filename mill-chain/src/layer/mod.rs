//! How a backend connection is made: plain TCP, through a SOCKS5 proxy such as Tor, and
//! with or without TLS on top.

mod dns;
mod socks5;
#[cfg(feature = "tls")]
mod tls;

use crate::ChainError;
use mill_net::errors::Result as NetResult;
use mill_net::tcp::{NetworkHandler, TcpClient};
pub use socks5::Socks5Error;
use socks5::Socks5Layer;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Handshake deadline through a proxy; Tor circuits to onion services are slow to build.
const HANDSHAKE_TIMEOUT_PROXIED: Duration = Duration::from_secs(120);
/// Handshake deadline for direct TLS.
#[cfg(feature = "tls")]
const HANDSHAKE_TIMEOUT_DIRECT: Duration = Duration::from_secs(30);

/// How to reach a backend. The default is a direct TCP connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransportConfig {
    /// Connect through this SOCKS5 proxy. The backend's hostname is then sent to the proxy
    /// and never resolved locally.
    pub socks5: Option<Socks5Config>,
    /// Speak TLS to the backend, over the proxy if one is set. Needs the `tls` feature.
    pub tls: Option<TlsConfig>,
}

/// A SOCKS5 proxy, such as Tor's `SocksPort`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Socks5Config {
    /// The proxy as `host:port`, for example `127.0.0.1:9050`, `10.21.22.10:9050` or
    /// `tor:9050`; an IPv6 host goes in brackets. A hostname here is resolved locally.
    pub proxy: String,
    /// Send a fresh random username and password on every connection, so Tor (with its
    /// default `IsolateSOCKSAuth`) puts each connection on its own circuit.
    pub isolate: bool,
}

/// TLS to a backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// The name to send (SNI) and verify; `None` uses the backend's host.
    pub server_name: Option<String>,
    /// How the server's certificate is checked.
    pub verify: TlsVerify,
}

/// How a server's TLS certificate is checked. Rejections reach calls as
/// [`ChainError::Transport`] texts starting `handshake failed: TLS:`, distinct for a name
/// mismatch, an expired certificate, an unknown issuer and a pin mismatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsVerify {
    /// Chain to the Mozilla roots in `webpki-roots`, with name and validity checks. The
    /// name is not checked for `.onion` hosts, whose address authenticates the server.
    WebPki,
    /// As [`WebPki`](Self::WebPki), but chain to the PEM certificates in this file.
    CaFile(PathBuf),
    /// Trust exactly the certificate whose DER encoding has this SHA-256, whatever its
    /// name, issuer or dates, as with a self-signed Electrum server certificate.
    PinSha256([u8; 32]),
    /// Accept any certificate, still checking handshake signatures against it. Only with
    /// the `testing` feature.
    #[cfg(feature = "testing")]
    InsecureForTests,
}

/// Splits `host:port`, keeping brackets around an IPv6 host.
fn split_host_port(s: &str) -> Option<(String, u16)> {
    let (host, port) = s.rsplit_once(':')?;
    let port = port.parse().ok()?;
    let bracketed = host.starts_with('[') && host.ends_with(']');
    if host.is_empty() || (host.contains(':') && !bracketed) {
        return None;
    }
    Some((host.to_string(), port))
}

/// Where the next TCP connection goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Hop {
    Connect(SocketAddr),
    /// A hostname to resolve on the compute pool first.
    Resolve(String, u16),
}

struct Proxy {
    host: String,
    port: u16,
    isolate: bool,
}

/// Builds each connection to one backend: the hop, then the layers above TCP.
pub(crate) struct Dialer {
    host: String,
    port: u16,
    proxy: Option<Proxy>,
    #[cfg(feature = "tls")]
    tls: Option<tls::TlsSetup>,
    lookups: Arc<AtomicU64>,
}

impl Dialer {
    /// Plain TCP to a known address.
    pub(crate) fn direct(addr: SocketAddr) -> Self {
        Self {
            host: addr.ip().to_string(),
            port: addr.port(),
            proxy: None,
            #[cfg(feature = "tls")]
            tls: None,
            lookups: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Connects to `host:port` as `transport` says. Fails on a malformed proxy, host or
    /// TLS config (such as an unreadable CA file), and on TLS without the `tls` feature.
    pub(crate) fn new(
        host: &str,
        port: u16,
        transport: &TransportConfig,
    ) -> Result<Self, ChainError> {
        let proxy = match &transport.socks5 {
            None => None,
            Some(socks5) => {
                let (proxy_host, proxy_port) = split_host_port(&socks5.proxy).ok_or_else(|| {
                    ChainError::Transport(format!(
                        "invalid SOCKS5 proxy {:?}: expected host:port",
                        socks5.proxy
                    ))
                })?;
                socks5::encode_address(host, port, &mut Vec::new())
                    .map_err(|e| ChainError::Transport(format!("{:?}: {}", host, e)))?;
                Some(Proxy {
                    host: proxy_host,
                    port: proxy_port,
                    isolate: socks5.isolate,
                })
            }
        };
        if host.is_empty() {
            return Err(ChainError::Transport("empty backend host".into()));
        }
        #[cfg(feature = "tls")]
        let tls = match &transport.tls {
            Some(config) => Some(tls::TlsSetup::new(config, host)?),
            None => None,
        };
        #[cfg(not(feature = "tls"))]
        if transport.tls.is_some() {
            return Err(ChainError::Unsupported(
                "TLS needs mill-chain's `tls` feature",
            ));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            proxy,
            #[cfg(feature = "tls")]
            tls,
            lookups: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Where to open the TCP connection: the proxy if one is set, else the backend.
    pub(crate) fn hop(&self) -> Hop {
        let (host, port) = match &self.proxy {
            Some(proxy) => (&proxy.host, proxy.port),
            None => (&self.host, self.port),
        };
        match dns::ip_literal(host) {
            Some(ip) => Hop::Connect(SocketAddr::new(ip, port)),
            None => Hop::Resolve(host.clone(), port),
        }
    }

    /// Resolves a [`Hop::Resolve`] name. Blocks; run it on the compute pool.
    pub(crate) fn resolver(&self) -> impl Fn(&str, u16) -> io::Result<Vec<SocketAddr>> + Send {
        let lookups = Arc::clone(&self.lookups);
        move |host, port| dns::resolve(host, port, &lookups)
    }

    /// Hostnames resolved locally so far.
    pub(crate) fn dns_lookups(&self) -> u64 {
        self.lookups.load(Ordering::SeqCst)
    }

    /// How long the layers may take from TCP connect to ready; `None` without layers.
    pub(crate) fn handshake_timeout(&self) -> Option<Duration> {
        if self.proxy.is_some() {
            return Some(HANDSHAKE_TIMEOUT_PROXIED);
        }
        #[cfg(feature = "tls")]
        if self.tls.is_some() {
            return Some(HANDSHAKE_TIMEOUT_DIRECT);
        }
        None
    }

    /// Adds this connection's layers to `client`, bottom first.
    pub(crate) fn add_layers<H: NetworkHandler>(
        &self,
        mut client: TcpClient<H>,
    ) -> NetResult<TcpClient<H>> {
        if let Some(proxy) = &self.proxy {
            let credentials = proxy.isolate.then(socks5::isolation_credentials);
            client =
                client.with_layer(Socks5Layer::new(self.host.clone(), self.port, credentials))?;
        }
        #[cfg(feature = "tls")]
        if let Some(tls) = &self.tls {
            client = client.with_layer(tls.layer()?)?;
        }
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn via(proxy: &str) -> TransportConfig {
        TransportConfig {
            socks5: Some(Socks5Config {
                proxy: proxy.to_string(),
                isolate: false,
            }),
            tls: None,
        }
    }

    fn dial(host: &str, port: u16, transport: &TransportConfig) -> Result<Dialer, ChainError> {
        Dialer::new(host, port, transport)
    }

    #[test]
    fn proxy_addresses_parse() {
        assert_eq!(
            split_host_port("10.21.22.10:9050"),
            Some(("10.21.22.10".into(), 9050))
        );
        assert_eq!(split_host_port("tor:9050"), Some(("tor".into(), 9050)));
        assert_eq!(split_host_port("[::1]:9050"), Some(("[::1]".into(), 9050)));
        for bad in ["9050", ":9050", "::1:9050", "tor:", "tor:70000", "tor"] {
            assert_eq!(split_host_port(bad), None, "{}", bad);
        }
    }

    #[test]
    fn hops_go_to_the_proxy_when_set() {
        let dialer = dial("example.onion", 50001, &via("10.0.0.5:9050")).unwrap();
        assert_eq!(dialer.hop(), Hop::Connect("10.0.0.5:9050".parse().unwrap()));
        assert_eq!(dialer.handshake_timeout(), Some(HANDSHAKE_TIMEOUT_PROXIED));

        let dialer = dial("example.onion", 50001, &via("tor:9050")).unwrap();
        assert_eq!(dialer.hop(), Hop::Resolve("tor".into(), 9050));

        let dialer = dial("node.example", 8332, &TransportConfig::default()).unwrap();
        assert_eq!(dialer.hop(), Hop::Resolve("node.example".into(), 8332));
        assert_eq!(dialer.handshake_timeout(), None);

        let dialer = Dialer::direct("[::1]:3".parse().unwrap());
        assert_eq!(dialer.hop(), Hop::Connect("[::1]:3".parse().unwrap()));
        assert_eq!(dialer.dns_lookups(), 0);
    }

    #[test]
    fn bad_targets_are_refused_up_front() {
        assert!(dial("h", 1, &via("no-port")).is_err());
        assert!(dial(&"a".repeat(256), 1, &via("127.0.0.1:9050")).is_err());
        assert!(dial("", 1, &TransportConfig::default()).is_err());
    }

    #[test]
    fn tls_needs_its_feature() {
        let transport = TransportConfig {
            socks5: None,
            tls: Some(TlsConfig {
                server_name: None,
                verify: TlsVerify::PinSha256([0; 32]),
            }),
        };
        let dialer = dial("127.0.0.1", 1, &transport);
        #[cfg(feature = "tls")]
        assert_eq!(
            dialer.map(|d| d.handshake_timeout()),
            Ok(Some(HANDSHAKE_TIMEOUT_DIRECT))
        );
        #[cfg(not(feature = "tls"))]
        assert!(matches!(dialer, Err(ChainError::Unsupported(_))));
    }
}
