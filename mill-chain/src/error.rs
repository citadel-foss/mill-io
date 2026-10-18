//! The error every mill-chain call returns.

use std::fmt;

/// Why a chain query did not produce an answer.
///
/// **Fund-safety rule:** only [`ChainError::NotFound`] means the backend answered and the
/// thing does not exist. Every other variant means the question went unanswered, and
/// must never be read as "absent": a swap that treats a timeout as "transaction unknown"
/// can broadcast a stale state or give up a coin it still owns. Use
/// [`is_absence`](ChainError::is_absence) rather than matching on variants.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainError {
    /// The backend answered: the transaction, block or output does not exist.
    NotFound,
    /// The connection failed, dropped, or could not be written to.
    Transport(String),
    /// The call's deadline passed before an answer arrived.
    Timeout,
    /// The handle's cancel flag was set before an answer arrived.
    Cancelled,
    /// The client was shut down.
    Shutdown,
    /// The backend refused the credentials.
    Auth,
    /// The backend answered with a JSON-RPC error object.
    Server {
        /// The error object's `code`.
        code: i64,
        /// The error object's `message`.
        message: String,
    },
    /// The backend's reply did not have the expected shape.
    Protocol(String),
    /// The backend cannot answer this kind of query.
    Unsupported(&'static str),
    /// A blocking call was made on the client's own event loop thread, where waiting
    /// would block the reply it waits for.
    WouldDeadlock,
}

impl ChainError {
    /// True only for [`NotFound`](Self::NotFound): every other error means the answer is
    /// unknown, not that the thing is absent.
    pub fn is_absence(&self) -> bool {
        matches!(self, ChainError::NotFound)
    }
}

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChainError::NotFound => write!(f, "not found"),
            ChainError::Transport(reason) => write!(f, "transport error: {}", reason),
            ChainError::Timeout => write!(f, "deadline exceeded"),
            ChainError::Cancelled => write!(f, "cancelled"),
            ChainError::Shutdown => write!(f, "client is shut down"),
            ChainError::Auth => write!(f, "authentication failed"),
            ChainError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
            ChainError::Protocol(reason) => write!(f, "protocol error: {}", reason),
            ChainError::Unsupported(what) => write!(f, "unsupported by this backend: {}", what),
            ChainError::WouldDeadlock => {
                write!(f, "blocking call on the event loop thread would deadlock")
            }
        }
    }
}

impl std::error::Error for ChainError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn every_variant() -> Vec<ChainError> {
        vec![
            ChainError::NotFound,
            ChainError::Transport("reset".into()),
            ChainError::Timeout,
            ChainError::Cancelled,
            ChainError::Shutdown,
            ChainError::Auth,
            ChainError::Server {
                code: -5,
                message: "No such mempool or blockchain transaction".into(),
            },
            ChainError::Protocol("bad".into()),
            ChainError::Unsupported("block_at_height"),
            ChainError::WouldDeadlock,
        ]
    }

    #[test]
    fn only_not_found_is_absence() {
        for error in every_variant() {
            assert_eq!(
                error.is_absence(),
                error == ChainError::NotFound,
                "{:?}",
                error
            );
        }
    }

    #[test]
    fn display_names_each_variant() {
        let shown: Vec<String> = every_variant().iter().map(|e| e.to_string()).collect();
        assert_eq!(
            shown,
            [
                "not found",
                "transport error: reset",
                "deadline exceeded",
                "cancelled",
                "client is shut down",
                "authentication failed",
                "server error -5: No such mempool or blockchain transaction",
                "protocol error: bad",
                "unsupported by this backend: block_at_height",
                "blocking call on the event loop thread would deadlock",
            ]
        );
    }

    #[test]
    fn is_a_std_error() {
        let error: Box<dyn std::error::Error + Send + Sync> = Box::new(ChainError::Timeout);
        assert!(error.source().is_none());
        assert_eq!(error.to_string(), "deadline exceeded");
    }
}
