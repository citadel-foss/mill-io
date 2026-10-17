// TODO: add custom error module and use it here
use lock_freedom::map::{Insertion, Map, Preview, Removed};
use mio::{Events, Interest, Poll, Token};
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use crate::{
    error::Result,
    handler::{EventHandler, HandlerEntry, SharedHandler},
};

type Registry = Arc<Map<Token, HandlerEntry>>;

/// The token of the loop's internal waker. [`PollHandle::register`] rejects it.
pub const WAKER_TOKEN: Token = Token(0);

/// The first token [`PollHandle::alloc_token`] hands out. Allocated tokens use the
/// upper half of the token space so they miss small hand-picked ones.
pub const FIRST_ALLOCATED_TOKEN: Token = Token(usize::MAX / 2 + 1);

pub struct PollHandle {
    poller: Arc<RwLock<mio::Poll>>,
    mio_registry: mio::Registry,
    registery: Registry,
    /// Handlers by token, so `reregister` can rebuild an entry. Not on the dispatch path.
    handlers: Map<Token, SharedHandler>,
    waker: Arc<mio::Waker>,
    next_token: AtomicUsize,
}

impl PollHandle {
    pub fn new() -> Result<Self> {
        let poll = Poll::new()?;
        let mio_registry = poll.registry().try_clone()?;
        let waker = mio::Waker::new(&mio_registry, WAKER_TOKEN)?;
        let poller = Arc::new(RwLock::new(poll));
        let registery: Registry = Arc::new(Map::new());
        Ok(PollHandle {
            poller,
            mio_registry,
            registery,
            handlers: Map::new(),
            waker: Arc::new(waker),
            next_token: AtomicUsize::new(FIRST_ALLOCATED_TOKEN.0),
        })
    }

    /// Returns a token that is not [`WAKER_TOKEN`] and never returned before.
    ///
    /// # Panics
    ///
    /// If the upper half of the token space is used up.
    pub fn alloc_token(&self) -> Token {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        // Past `usize::MAX` the counter wraps to 0, so a value below the range means it is spent.
        assert!(
            token >= FIRST_ALLOCATED_TOKEN.0,
            "mill-io: allocatable token space exhausted"
        );
        Token(token)
    }

    pub fn register<H, S>(
        &self,
        src: &mut S,
        token: Token,
        interest: Interest,
        handler: H,
    ) -> Result<()>
    where
        H: EventHandler + Send + Sync + 'static,
        S: mio::event::Source + ?Sized,
    {
        reject_waker_token(token)?;

        let handler = SharedHandler(Arc::new(handler));
        let handler_entry = HandlerEntry::new(handler.clone(), interest);

        // The entry must exist before the OS registration: a source that is
        // already ready produces its only edge-triggered event right away, and
        // a dispatch that finds no entry drops it for good.
        let previous_handler = self.handlers.insert(token, handler);
        let previous_entry = self.registery.insert(token, handler_entry);

        if let Err(e) = src.register(&self.mio_registry, token, interest) {
            restore(&self.handlers, token, previous_handler);
            restore(&self.registery, token, previous_entry);
            return Err(Box::new(e));
        }
        Ok(())
    }

    /// Changes the interests of a source registered under `token`, keeping its handler.
    pub fn reregister<S>(&self, src: &mut S, token: Token, interest: Interest) -> Result<()>
    where
        S: mio::event::Source + ?Sized,
    {
        reject_waker_token(token)?;

        let handler = match self.handlers.get(&token) {
            Some(entry) => entry.val().clone(),
            None => return Err(Box::new(not_registered(token))),
        };

        // Store the new interest before telling the OS: a source that is already
        // ready raises its only edge-triggered event at once, and a dispatch that
        // still sees the old interest filters it out. The swap is one step and
        // only happens if the entry is still present, so a dispatch never sees a
        // missing entry and a racing deregister is not undone.
        let insertion =
            self.registery
                .insert_with(token, |_, generated, stored| match (stored, generated) {
                    (None, _) => Preview::Discard,
                    (Some(_), Some(_)) => Preview::Keep,
                    (Some(_), None) => Preview::New(HandlerEntry::new(handler.clone(), interest)),
                });
        let previous = match insertion {
            Insertion::Failed(_) => return Err(Box::new(not_registered(token))),
            Insertion::Updated(previous) => Some(previous),
            Insertion::Created => None,
        };

        if let Err(e) = src.reregister(&self.mio_registry, token, interest) {
            if let Some(previous) = previous {
                self.registery
                    .reinsert_with(previous, |_, stored| stored.is_some());
            }
            return Err(Box::new(e));
        }
        Ok(())
    }

    pub fn deregister<S>(&self, source: &mut S, token: Token) -> Result<()>
    where
        S: mio::event::Source + ?Sized,
    {
        self.mio_registry.deregister(source)?;

        self.registery.remove(&token);
        self.handlers.remove(&token);

        Ok(())
    }

    pub fn poll(&self, events: &mut Events, timeout: Option<std::time::Duration>) -> Result<usize> {
        let mut poller = self
            .poller
            .write()
            .map_err(|_| "Failed to acquire poller write lock")?;
        poller.poll(events, timeout)?;
        Ok(events.iter().count())
    }

    pub fn wake(&self) -> Result<()> {
        Ok(self.waker.wake()?)
    }

    pub fn get_registery(&self) -> Registry {
        self.registery.clone()
    }
}

/// Undoes an `insert` into `map`: puts back what it replaced, or removes the key.
fn restore<V>(map: &Map<Token, V>, token: Token, previous: Option<Removed<Token, V>>) {
    match previous {
        Some(previous) => {
            map.reinsert(previous);
        }
        None => {
            map.remove(&token);
        }
    }
}

fn reject_waker_token(token: Token) -> Result<()> {
    if token == WAKER_TOKEN {
        return Err(Box::new(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Token(0) is reserved for the event loop's waker",
        )));
    }
    Ok(())
}

fn not_registered(token: Token) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("{token:?} is not registered with this event loop"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use mio::event::{Event, Source};
    use mio::Events;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    struct TestSource;
    impl Source for TestSource {
        fn register(
            &mut self,
            _registry: &mio::Registry,
            _token: Token,
            _interests: Interest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn reregister(
            &mut self,
            _registry: &mio::Registry,
            _token: Token,
            _interests: Interest,
        ) -> std::io::Result<()> {
            Ok(())
        }

        fn deregister(&mut self, _registry: &mio::Registry) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl TestSource {
        fn new() -> Self {
            TestSource
        }
    }

    #[test]
    fn test_poll() {
        let poller = PollHandle::new().unwrap();
        let mut events = Events::with_capacity(1024);
        poller
            .poll(&mut events, Some(Duration::from_secs(1)))
            .unwrap();
    }

    #[test]
    fn test_wake() {
        let poller = PollHandle::new().unwrap();
        assert!(poller.wake().is_ok());
    }

    #[test]
    fn test_register_unregister() {
        let poller = PollHandle::new().unwrap();
        let mut source = TestSource::new();
        let token = Token(1);

        struct TestHandler {
            called: Arc<AtomicBool>,
        }

        impl EventHandler for TestHandler {
            fn handle_event(&self, _event: &Event) {
                self.called.store(true, Ordering::SeqCst);
            }
        }

        let handler = TestHandler {
            called: Arc::new(AtomicBool::new(false)),
        };

        assert!(
            poller
                .register(&mut source, token, Interest::READABLE, handler)
                .is_ok(),
            "Failed to register source"
        );

        assert!(
            poller.registery.iter().any(|t| t.0 == token),
            "Token not found in registry"
        );

        assert!(
            poller.deregister(&mut source, token).is_ok(),
            "Failed to unregister source"
        );

        assert!(
            !poller.registery.iter().any(|t| t.0 == token),
            "Token should have been removed from registry"
        );
    }

    #[test]
    fn test_multiple_handlers() {
        let poller = PollHandle::new().unwrap();
        let mut src1 = TestSource::new();
        let mut src2 = TestSource::new();

        struct NoopHandler;
        impl EventHandler for NoopHandler {
            fn handle_event(&self, _event: &Event) {}
        }

        assert!(
            poller
                .register(&mut src1, Token(1), Interest::READABLE, NoopHandler)
                .is_ok(),
            "Failed to register src1"
        );
        assert!(
            poller
                .register(&mut src2, Token(2), Interest::WRITABLE, NoopHandler)
                .is_ok(),
            "Failed to register src2"
        );

        assert_eq!(poller.registery.iter().count(), 2);
        assert!(
            poller.registery.iter().any(|t| t.0 == Token(1)),
            "Failed to find src1"
        );
        assert!(
            poller.registery.iter().any(|t| t.0 == Token(2)),
            "Failed to find src2"
        );
    }
}
