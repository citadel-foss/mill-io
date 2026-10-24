//! Name resolution for direct connections, run on the loop's compute pool so a slow
//! resolver never stalls the reactor.

use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};

/// `host` as an IP address, accepting IPv6 in brackets; `None` for a hostname.
pub(crate) fn ip_literal(host: &str) -> Option<IpAddr> {
    let bare = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    bare.parse().ok()
}

/// Resolves `host` with the system resolver, counting the lookup in `lookups`. Blocks.
pub(crate) fn resolve(host: &str, port: u16, lookups: &AtomicU64) -> io::Result<Vec<SocketAddr>> {
    if let Some(ip) = ip_literal(host) {
        return Ok(vec![SocketAddr::new(ip, port)]);
    }
    lookups.fetch_add(1, Ordering::SeqCst);
    let addrs: Vec<SocketAddr> = (host, port).to_socket_addrs()?.collect();
    if addrs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} has no addresses", host),
        ));
    }
    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_skip_the_resolver() {
        let lookups = AtomicU64::new(0);
        assert_eq!(
            resolve("[::1]", 7, &lookups).unwrap(),
            ["[::1]:7".parse().unwrap()]
        );
        assert_eq!(
            resolve("10.0.0.1", 8, &lookups).unwrap(),
            ["10.0.0.1:8".parse().unwrap()]
        );
        assert_eq!(lookups.load(Ordering::SeqCst), 0);
        assert_eq!(ip_literal("::1"), Some("::1".parse().unwrap()));
        assert_eq!(ip_literal("[::1"), None);
        assert_eq!(ip_literal("example.com"), None);
    }

    #[test]
    fn hostnames_are_counted() {
        let lookups = AtomicU64::new(0);
        let addrs = resolve("localhost", 9, &lookups).unwrap();
        assert!(addrs.iter().all(|a| a.ip().is_loopback() && a.port() == 9));
        assert!(resolve("no-such-host.invalid", 9, &lookups).is_err());
        assert_eq!(lookups.load(Ordering::SeqCst), 2);
    }
}
