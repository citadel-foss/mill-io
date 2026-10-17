use mio::{event::Event, Interest};
use std::sync::Arc;

pub trait EventHandler {
    fn handle_event(&self, event: &Event);
}
pub struct HandlerEntry {
    pub handler: Box<dyn EventHandler + Send + Sync>,
    pub interest: Interest,
}

impl HandlerEntry {
    pub fn new<H>(handler: H, interest: Interest) -> Self
    where
        H: EventHandler + Send + Sync + 'static,
    {
        HandlerEntry {
            handler: Box::new(handler),
            interest,
        }
    }
}

/// Lets `reregister` rebuild a registry entry around the same handler.
#[derive(Clone)]
pub(crate) struct SharedHandler(pub(crate) Arc<dyn EventHandler + Send + Sync>);

impl EventHandler for SharedHandler {
    fn handle_event(&self, event: &Event) {
        self.0.handle_event(event)
    }
}
