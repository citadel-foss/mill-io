//! Stream layers: byte transforms between the socket and the handler, such as SOCKS5 or TLS.
//!
//! A connection has a stack of layers, bottom (next to the socket) first:
//!
//! ```text
//! socket <-> layer 0 <-> layer 1 <-> ... <-> handler (on_data / ConnSender::send)
//! ```
//!
//! - **Handshake.** Layer 0 is started when the TCP connection is up (on accept for a
//!   server, on connect for a client). Each layer above is started once the one below
//!   reports [`Layer::is_ready`]. `on_connect` fires only when every layer is ready.
//! - **Inbound.** Bytes read from the socket go up through [`Layer::decode`]. What the top
//!   layer passes up reaches `on_data`.
//! - **Outbound.** A frame sent before the stack is ready is held whole, then encoded
//!   once `on_connect` is due. Bytes a layer emits on its own (handshake messages,
//!   replies) are encoded by the layers below it only.
//! - **Errors.** An error from any method closes the connection and is reported through
//!   `on_error`. So does the peer closing or failing before the handshake finished
//!   (`UnexpectedEof` or the read error); with no `on_connect` yet, there is no
//!   `on_disconnect`.
//!
//! Layer methods run under the connection's locks, so they must not block or call back
//! into the connection. Connections without layers skip all of this.

use std::io;

/// One stage of a connection's stream transform. See the [module docs](self).
pub trait Layer: Send + 'static {
    /// Called once, when the layer below is ready. Bytes appended to `out` go to the peer.
    fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
        let _ = out;
        Ok(())
    }

    /// Takes bytes from the peer. Appends bytes for the layer above to `up` (only once
    /// [`is_ready`](Self::is_ready) is true after this call) and bytes to send back to `out`.
    fn decode(&mut self, input: &[u8], up: &mut Vec<u8>, out: &mut Vec<u8>) -> io::Result<()>;

    /// Turns a frame from above into bytes for the layer below. Only called once ready.
    fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()>;

    /// True once the handshake is done and the layer passes data through.
    fn is_ready(&self) -> bool {
        true
    }
}

/// A connection's layers, bottom first.
pub(crate) struct LayerStack {
    layers: Vec<Box<dyn Layer>>,
    /// Layers `0..started` have had `start` called.
    started: usize,
    /// `on_connect` was already reported as due.
    announced: bool,
}

impl LayerStack {
    pub(crate) fn new(layers: Vec<Box<dyn Layer>>) -> Self {
        Self {
            layers,
            started: 0,
            announced: false,
        }
    }

    pub(crate) fn push(&mut self, layer: Box<dyn Layer>) {
        self.layers.push(layer);
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.started == self.layers.len() && self.layers.iter().all(|layer| layer.is_ready())
    }

    /// True once, the first time every layer is ready.
    pub(crate) fn take_ready(&mut self) -> bool {
        if !self.announced && self.is_ready() {
            self.announced = true;
            true
        } else {
            false
        }
    }

    /// Starts every layer whose lower neighbours are ready; their output goes to `wire`.
    pub(crate) fn advance(&mut self, wire: &mut Vec<u8>) -> io::Result<()> {
        while self.started < self.layers.len() {
            let below_ready = self.layers[..self.started]
                .iter()
                .all(|layer| layer.is_ready());
            if !below_ready {
                break;
            }
            let index = self.started;
            let mut out = Vec::new();
            self.layers[index].start(&mut out)?;
            self.started += 1;
            self.encode_below(index, out, wire)?;
        }
        Ok(())
    }

    /// Feeds bytes read from the socket. Bytes for the handler go to `app`, bytes for the
    /// peer to `wire`.
    pub(crate) fn decode(
        &mut self,
        input: &[u8],
        app: &mut Vec<u8>,
        wire: &mut Vec<u8>,
    ) -> io::Result<()> {
        let mut data = input.to_vec();
        let mut index = 0;
        while index < self.started && !data.is_empty() {
            let mut up = Vec::new();
            let mut out = Vec::new();
            self.layers[index].decode(&data, &mut up, &mut out)?;
            self.encode_below(index, out, wire)?;
            if !up.is_empty() && !self.layers[index].is_ready() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "layer passed data up before its handshake finished",
                ));
            }
            self.advance(wire)?;
            data = up;
            index += 1;
        }
        if index == self.layers.len() {
            app.extend_from_slice(&data);
        }
        Ok(())
    }

    /// Encodes a frame from the handler through every layer.
    pub(crate) fn encode(&mut self, frame: &[u8], wire: &mut Vec<u8>) -> io::Result<()> {
        self.encode_below(self.layers.len(), frame.to_vec(), wire)
    }

    /// Encodes `bytes` emitted by layer `top` through the layers below it.
    fn encode_below(
        &mut self,
        top: usize,
        mut bytes: Vec<u8>,
        wire: &mut Vec<u8>,
    ) -> io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        for layer in self.layers[..top].iter_mut().rev() {
            let mut out = Vec::new();
            layer.encode(&bytes, &mut out)?;
            bytes = out;
        }
        wire.extend_from_slice(&bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Adds `tag` to every byte, after a one-byte hello that must be answered.
    struct Shift {
        tag: u8,
        greeted: bool,
    }

    impl Layer for Shift {
        fn start(&mut self, out: &mut Vec<u8>) -> io::Result<()> {
            out.push(self.tag);
            Ok(())
        }

        fn decode(&mut self, input: &[u8], up: &mut Vec<u8>, _out: &mut Vec<u8>) -> io::Result<()> {
            let mut input = input;
            if !self.greeted {
                self.greeted = true;
                input = &input[1..];
            }
            up.extend(input.iter().map(|b| b.wrapping_sub(self.tag)));
            Ok(())
        }

        fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
            out.extend(frame.iter().map(|b| b.wrapping_add(self.tag)));
            Ok(())
        }

        fn is_ready(&self) -> bool {
            self.greeted
        }
    }

    fn shift(tag: u8) -> Box<dyn Layer> {
        Box::new(Shift {
            tag,
            greeted: false,
        })
    }

    #[test]
    fn stacked_handshakes_run_bottom_up() {
        let mut stack = LayerStack::new(vec![shift(1), shift(10)]);
        let mut wire = Vec::new();
        stack.advance(&mut wire).unwrap();
        assert_eq!(wire, [1], "only the bottom layer starts");
        assert!(!stack.is_ready());

        let mut app = Vec::new();
        wire.clear();
        // The peer's bottom hello, then its top hello encoded by its bottom layer, then data.
        stack
            .decode(&[1, 11, 1 + 10 + 5], &mut app, &mut wire)
            .unwrap();
        assert_eq!(wire, [11], "the top hello goes through the bottom layer");
        assert_eq!(app, [5]);
        assert!(stack.take_ready());
        assert!(!stack.take_ready());

        wire.clear();
        stack.encode(&[7], &mut wire).unwrap();
        assert_eq!(wire, [18]);
    }

    #[test]
    fn data_before_the_handshake_is_an_error() {
        struct Eager;
        impl Layer for Eager {
            fn decode(
                &mut self,
                input: &[u8],
                up: &mut Vec<u8>,
                _: &mut Vec<u8>,
            ) -> io::Result<()> {
                up.extend_from_slice(input);
                Ok(())
            }
            fn encode(&mut self, frame: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
                out.extend_from_slice(frame);
                Ok(())
            }
            fn is_ready(&self) -> bool {
                false
            }
        }
        let mut stack = LayerStack::new(vec![Box::new(Eager)]);
        stack.advance(&mut Vec::new()).unwrap();
        let err = stack
            .decode(b"x", &mut Vec::new(), &mut Vec::new())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
