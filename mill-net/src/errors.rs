use crate::tcp::{ConnectionId, SendError};
use std::fmt;
use std::io;
use std::net::SocketAddr;

pub use mill_io::error::Result;

#[derive(Debug)]
pub enum NetworkError {
    Io(Box<dyn std::error::Error>),
    Accept(Box<dyn std::error::Error>),
    Connect(Box<dyn std::error::Error>),
    PoisonedLock(String),
    MaxConnectionsReached(SocketAddr),
    Configuration(String),
    HandlerError(String),
    EventLoopGone,
    ConnectionNotFound(ConnectionId),
    Send(SendError),
    Other(String),
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkError::Io(e) => write!(f, "IO Error: {}", e),
            NetworkError::Accept(e) => write!(f, "Accept Error: {}", e),
            NetworkError::Connect(e) => write!(f, "Connect Error: {}", e),
            NetworkError::PoisonedLock(msg) => write!(f, "Lock Poisoned: {}", msg),
            NetworkError::MaxConnectionsReached(addr) => {
                write!(f, "Max connections reached, rejecting {}", addr)
            }
            NetworkError::Configuration(msg) => write!(f, "Configuration Error: {}", msg),
            NetworkError::HandlerError(msg) => write!(f, "Handler Error: {}", msg),
            NetworkError::EventLoopGone => write!(f, "EventLoop is gone"),
            NetworkError::ConnectionNotFound(conn_id) => {
                write!(f, "Connection not found: {:?}", conn_id)
            }
            NetworkError::Send(e) => write!(f, "Send Error: {}", e),
            NetworkError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for NetworkError {}

impl From<io::Error> for NetworkError {
    fn from(err: io::Error) -> Self {
        NetworkError::Io(Box::new(err))
    }
}

#[derive(Debug)]
pub enum NetworkEvent {
    ConnectionEstablished(ConnectionId, SocketAddr),
    ConnectionClosed(ConnectionId),
    Listening(SocketAddr),
}
