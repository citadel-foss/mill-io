use mill_io::error::Result;
use mill_io::EventLoop;
use mill_net::errors::{NetworkError, NetworkEvent};
use mill_net::tcp::{config::TcpServerConfig, traits::*, ConnectionId, ServerContext, TcpServer};
use std::sync::Arc;

/// a simple handler that echoes data back to the client.
#[derive(Clone, Default)]
struct EchoHandler;

/// implement the `NetworkHandler` trait to define application logic.
impl NetworkHandler for EchoHandler {
    /// Called when a non-error network event occurs
    fn on_event(&self, _ctx: &ServerContext, event: NetworkEvent) -> Result<()> {
        match event {
            NetworkEvent::ConnectionEstablished(conn_id, addr) => {
                println!("[INFO] New client connected: {:?} from {}", conn_id, addr);
            }
            NetworkEvent::ConnectionClosed(conn_id) => {
                println!("[INFO] Client disconnected: {:?}", conn_id);
            }
            NetworkEvent::Listening(addr) => {
                println!("[INFO] Server listening on {}", addr);
            }
        }
        Ok(())
    }

    /// called when a new client connects.
    fn on_connect(&self, _ctx: &ServerContext, _conn_id: ConnectionId) -> Result<()> {
        // We handle logging in on_event now, but we could do other setup here
        Ok(())
    }

    /// called when data is received from a client.
    fn on_data(&self, ctx: &ServerContext, conn_id: ConnectionId, data: &[u8]) -> Result<()> {
        let message = String::from_utf8_lossy(data);
        println!(
            "[INFO] Received {} bytes from {:?}: {}",
            data.len(),
            conn_id,
            message.trim_end()
        );

        // echo the received data back to the sender.
        ctx.send_to(conn_id, data)?;

        Ok(())
    }

    /// called when a client disconnects.
    fn on_disconnect(&self, _ctx: &ServerContext, _conn_id: ConnectionId) -> Result<()> {
        // We handle logging in on_event now, but we could do cleanup here
        Ok(())
    }

    /// Called on errors
    fn on_error(&self, _ctx: &ServerContext, conn_id: Option<ConnectionId>, error: NetworkError) {
        if let Some(id) = conn_id {
            eprintln!("[ERROR] Error on connection {:?}: {}", id, error);
        } else {
            eprintln!("[ERROR] Server error: {}", error);
        }
    }
}

fn main() -> Result<()> {
    // create an event loop with default settings.
    let event_loop = Arc::new(EventLoop::default());

    // configure the tcp server.
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:8080".parse().unwrap())
        .build();

    // create an instance of our handler.
    let handler = EchoHandler;

    // create the tcp server, wrapped in an Arc for shared ownership.
    let server = Arc::new(TcpServer::new(config.clone(), handler)?);

    // start the server. this registers the listener with the event loop.
    server.start(&event_loop, event_loop.alloc_token())?;

    println!("[INFO] Echo server listening on {}", config.address);

    // run the event loop. this will block until the loop is stopped.
    event_loop.run()?;

    Ok(())
}
