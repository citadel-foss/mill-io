use mio::net::TcpStream;
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::time::Duration;

/// Turns keep-alive on with `idle` before the first probe, or off for `None`.
pub(crate) fn set_keepalive(stream: &TcpStream, idle: Option<Duration>) -> io::Result<()> {
    let socket = SockRef::from(stream);
    match idle {
        Some(idle) => socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(idle)),
        None => socket.set_keepalive(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_is_applied() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();

        set_keepalive(&stream, Some(Duration::from_secs(42))).unwrap();
        let socket = SockRef::from(&stream);
        assert!(socket.keepalive().unwrap());
        #[cfg(not(windows))]
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(42)
        );

        set_keepalive(&stream, None).unwrap();
        assert!(!SockRef::from(&stream).keepalive().unwrap());
    }
}
