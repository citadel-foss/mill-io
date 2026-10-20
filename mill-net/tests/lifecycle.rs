use mill_io::EventLoop;
use mill_net::errors::Result;
use mill_net::tcp::config::TcpServerConfig;
use mill_net::tcp::{
    ConnectionId, NetworkHandler, ServerContext, TcpClient, TcpClientConfig, TcpServer,
};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod common;

use common::{pool_and_direct, spawn_loop, stop_loop, wait_until};

#[derive(Clone, Default)]
struct Echo {
    connected: Arc<Mutex<Vec<ConnectionId>>>,
    disconnected: Arc<Mutex<Vec<ConnectionId>>>,
}

impl NetworkHandler for Echo {
    fn on_connect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.connected.lock().push(conn);
        Ok(())
    }

    fn on_data(&self, ctx: &ServerContext, conn: ConnectionId, data: &[u8]) -> Result<()> {
        if data == b"close" {
            return ctx.close_connection(conn);
        }
        ctx.send_to(conn, data)
    }

    fn on_disconnect(&self, _ctx: &ServerContext, conn: ConnectionId) -> Result<()> {
        self.disconnected.lock().push(conn);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct Client {
    connects: Arc<AtomicUsize>,
    disconnects: Arc<AtomicUsize>,
    received: Arc<Mutex<Vec<u8>>>,
}

impl NetworkHandler for Client {
    fn on_connect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_data(&self, _ctx: &ServerContext, _conn: ConnectionId, data: &[u8]) -> Result<()> {
        self.received.lock().extend_from_slice(data);
        Ok(())
    }

    fn on_disconnect(&self, _ctx: &ServerContext, _conn: ConnectionId) -> Result<()> {
        self.disconnects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn echo_server(event_loop: &Arc<EventLoop>) -> (Arc<TcpServer<Echo>>, Echo) {
    let echo = Echo::default();
    let config = TcpServerConfig::builder()
        .address("127.0.0.1:0".parse().unwrap())
        .build();
    let server = Arc::new(TcpServer::new(config, echo.clone()).unwrap());
    Arc::clone(&server)
        .start(event_loop, event_loop.alloc_token())
        .unwrap();
    (server, echo)
}

fn connected_client(
    event_loop: &Arc<EventLoop>,
    server: &TcpServer<Echo>,
) -> (TcpClient<Client>, Client) {
    let handler = Client::default();
    let mut client = TcpClient::connect(
        server.local_addr().unwrap(),
        TcpClientConfig::default(),
        handler.clone(),
    )
    .unwrap();
    client.start(event_loop).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || handler
            .connects
            .load(Ordering::SeqCst)
            == 1),
        "client did not connect"
    );
    (client, handler)
}

fn round_trip(client: &TcpClient<Client>, handler: &Client, message: &[u8]) -> bool {
    handler.received.lock().clear();
    client.send(message).unwrap();
    wait_until(Duration::from_secs(5), || {
        handler.received.lock().as_slice() == message
    })
}

#[test]
fn client_disconnect_deregisters_its_own_token() {
    for (mode, event_loop) in pool_and_direct() {
        let (server, echo) = echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);
        // B is accepted first, so it holds the server's lowest connection number.
        let (b, b_handler) = connected_client(&event_loop, &server);
        assert!(wait_until(Duration::from_secs(5), || echo
            .connected
            .lock()
            .len()
            == 1));
        let (a, a_handler) = connected_client(&event_loop, &server);
        assert!(wait_until(Duration::from_secs(5), || echo
            .connected
            .lock()
            .len()
            == 2));
        assert!(round_trip(&a, &a_handler, b"a before"), "{mode}");
        assert!(round_trip(&b, &b_handler, b"b before"), "{mode}");

        // The server closes A, so A's client tears down on EOF.
        a.send(b"close").unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || {
                a_handler.disconnects.load(Ordering::SeqCst) == 1
                    && echo.disconnected.lock().len() == 1
            }),
            "{mode}: A's close was not seen on both ends"
        );
        for i in 0..20 {
            let message = format!("b after server close {i}");
            assert!(
                round_trip(&b, &b_handler, message.as_bytes()),
                "{mode}: B lost its connection after A was closed (round {i})"
            );
        }

        // D disconnects itself.
        let (d, d_handler) = connected_client(&event_loop, &server);
        d.disconnect().unwrap();
        assert_eq!(d_handler.disconnects.load(Ordering::SeqCst), 1, "{mode}");
        assert!(
            wait_until(Duration::from_secs(5), || echo.disconnected.lock().len()
                == 2),
            "{mode}: server did not see D leave"
        );
        for i in 0..20 {
            let message = format!("b after disconnect {i}");
            assert!(
                round_trip(&b, &b_handler, message.as_bytes()),
                "{mode}: B lost its connection after D disconnected (round {i})"
            );
        }
        assert_eq!(server.connection_count(), 1, "{mode}");

        let (c, c_handler) = connected_client(&event_loop, &server);
        assert!(
            round_trip(&c, &c_handler, b"c"),
            "{mode}: server stopped accepting"
        );
        assert_eq!(b_handler.disconnects.load(Ordering::SeqCst), 0, "{mode}");
        assert_eq!(echo.disconnected.lock().len(), 2, "{mode}");

        stop_loop(&event_loop, runner);
        drop((a, b, c, d, server));
    }
}

#[test]
fn two_servers_on_one_loop_do_not_share_connection_ids() {
    for (mode, event_loop) in pool_and_direct() {
        let (first, first_echo) = echo_server(&event_loop);
        let (second, second_echo) = echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);

        let mut clients = Vec::new();
        for server in [&first, &second, &first, &second] {
            clients.push(connected_client(&event_loop, server));
        }
        for (i, (client, handler)) in clients.iter().enumerate() {
            let message = format!("client {i}");
            assert!(
                round_trip(client, handler, message.as_bytes()),
                "{mode}: client {i}"
            );
        }

        let mut ids: Vec<ConnectionId> = first_echo.connected.lock().clone();
        ids.extend(second_echo.connected.lock().iter().copied());
        ids.extend(clients.iter().map(|(client, _)| client.id()));
        let total = ids.len();
        ids.sort();
        ids.dedup();
        assert_eq!(
            ids.len(),
            total,
            "{mode}: connection ids repeat on one loop"
        );

        clients[0].0.disconnect().unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || first.connection_count() == 1),
            "{mode}"
        );
        assert_eq!(second.connection_count(), 2, "{mode}");
        for (i, (client, handler)) in clients.iter().enumerate().skip(1) {
            assert!(
                round_trip(client, handler, b"still here"),
                "{mode}: client {i}"
            );
        }

        stop_loop(&event_loop, runner);
        drop((clients, first, second));
    }
}
