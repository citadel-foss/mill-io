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

/// `cargo test` runs a file's tests as threads of one process, and the fd count is per
/// process, so these tests take turns.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

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
    let _serial = serial();
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
    let _serial = serial();
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

#[cfg(unix)]
fn open_fds() -> usize {
    std::fs::read_dir("/dev/fd").unwrap().count()
}

#[cfg(unix)]
#[test]
fn ten_thousand_cycles_leave_nothing_behind() {
    let _serial = serial();
    const CYCLES: usize = 10_000;
    const BATCH: usize = 100;

    for (mode, event_loop) in pool_and_direct() {
        let weak_loop = Arc::downgrade(&event_loop);
        let (server, echo) = echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);

        let warmup = connected_client(&event_loop, &server);
        assert!(round_trip(&warmup.0, &warmup.1, b"warm"), "{mode}");
        drop(warmup);
        assert!(wait_until(Duration::from_secs(5), || server
            .connection_count()
            == 0));

        let fds = open_fds();
        let timers = event_loop.pending_timers();

        for batch in 0..CYCLES / BATCH {
            let handler = Client::default();
            let clients: Vec<_> = (0..BATCH)
                .map(|_| {
                    let mut client = TcpClient::connect(
                        server.local_addr().unwrap(),
                        TcpClientConfig::default(),
                        handler.clone(),
                    )
                    .unwrap();
                    client.start(&event_loop).unwrap();
                    client.send(b"ping").unwrap();
                    client
                })
                .collect();
            assert!(
                wait_until(Duration::from_secs(10), || {
                    handler.received.lock().len() == BATCH * 4
                }),
                "{mode}: batch {batch}: {} of {} echoed bytes",
                handler.received.lock().len(),
                BATCH * 4
            );
            // The server closes first, so TIME_WAIT lands on its side. Client-side
            // TIME_WAITs from 20k cycles would use up macOS's 16k ephemeral ports.
            for client in &clients {
                client.send(b"close").unwrap();
            }
            assert!(
                wait_until(Duration::from_secs(10), || {
                    server.connection_count() == 0
                        && handler.disconnects.load(Ordering::SeqCst) == BATCH
                }),
                "{mode}: batch {batch}: {} connections left, {} of {BATCH} clients closed",
                server.connection_count(),
                handler.disconnects.load(Ordering::SeqCst)
            );
            for client in &clients[..BATCH / 2] {
                client.disconnect().unwrap();
            }
            drop(clients);
            assert_eq!(handler.disconnects.load(Ordering::SeqCst), BATCH, "{mode}");
        }

        let after = open_fds();
        assert!(
            after.abs_diff(fds) <= 2,
            "{mode}: {fds} fds before, {after} after"
        );
        assert_eq!(event_loop.pending_timers(), timers, "{mode}: timers left");
        assert_eq!(
            echo.disconnected.lock().len(),
            CYCLES + 1,
            "{mode}: server disconnects"
        );

        server.stop().unwrap();
        stop_loop(&event_loop, runner);
        drop((server, echo, event_loop));
        assert!(
            weak_loop.upgrade().is_none(),
            "{mode}: something still holds the event loop"
        );
    }
}

#[test]
fn server_stop_closes_live_connections() {
    let _serial = serial();
    const LIVE: usize = 100;

    for (mode, event_loop) in pool_and_direct() {
        let (server, echo) = echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);

        let handler = Client::default();
        let clients: Vec<_> = (0..LIVE)
            .map(|_| {
                let mut client = TcpClient::connect(
                    server.local_addr().unwrap(),
                    TcpClientConfig::default(),
                    handler.clone(),
                )
                .unwrap();
                client.start(&event_loop).unwrap();
                client
            })
            .collect();
        assert!(
            wait_until(Duration::from_secs(10), || echo.connected.lock().len()
                == LIVE),
            "{mode}: {} of {LIVE} accepted",
            echo.connected.lock().len()
        );

        server.stop().unwrap();
        assert_eq!(server.connection_count(), 0, "{mode}");
        assert_eq!(
            echo.disconnected.lock().len(),
            LIVE,
            "{mode}: server disconnects"
        );
        assert!(
            wait_until(Duration::from_secs(10), || {
                handler.disconnects.load(Ordering::SeqCst) == LIVE
            }),
            "{mode}: {} of {LIVE} clients saw the close",
            handler.disconnects.load(Ordering::SeqCst)
        );
        server.stop().unwrap();
        assert_eq!(echo.disconnected.lock().len(), LIVE, "{mode}: second stop");

        let late = Client::default();
        let late_client = TcpClient::connect(
            server.local_addr().unwrap(),
            TcpClientConfig::default(),
            late.clone(),
        );
        if let Ok(mut late_client) = late_client {
            late_client.start(&event_loop).unwrap();
            assert!(
                !wait_until(Duration::from_millis(300), || late
                    .connects
                    .load(Ordering::SeqCst)
                    > 0),
                "{mode}: a stopped server accepted a connection"
            );
        }
        assert!(Arc::clone(&server)
            .start(&event_loop, event_loop.alloc_token())
            .is_err());

        stop_loop(&event_loop, runner);
        drop((clients, server));
    }
}

#[test]
fn dropping_a_client_closes_its_connection() {
    let _serial = serial();
    for (mode, event_loop) in pool_and_direct() {
        let (server, echo) = echo_server(&event_loop);
        let runner = spawn_loop(&event_loop);
        let (client, handler) = connected_client(&event_loop, &server);
        let sender = client.sender();
        assert!(wait_until(Duration::from_secs(5), || server
            .connection_count()
            == 1));

        drop(client);
        assert_eq!(handler.disconnects.load(Ordering::SeqCst), 1, "{mode}");
        assert!(
            wait_until(Duration::from_secs(5), || server.connection_count() == 0),
            "{mode}: server still sees the dropped client"
        );
        assert_eq!(echo.disconnected.lock().len(), 1, "{mode}");
        assert!(sender.send(b"x").is_err(), "{mode}");

        stop_loop(&event_loop, runner);
        drop(server);
    }
}

#[test]
fn stalled_connection_closed_by_a_caller_timer() {
    let _serial = serial();
    use mill_net::testing::{ChaosConfig, ChaosProxy};
    use std::time::Instant;

    for (mode, event_loop) in pool_and_direct() {
        let (server, _echo) = echo_server(&event_loop);
        let proxy = ChaosProxy::start(
            server.local_addr().unwrap(),
            ChaosConfig {
                stall_after: Some(10),
                ..ChaosConfig::default()
            },
        )
        .unwrap();
        let runner = spawn_loop(&event_loop);
        let timers = event_loop.pending_timers();

        let handler = Client::default();
        let mut client =
            TcpClient::connect(proxy.addr(), TcpClientConfig::default(), handler.clone()).unwrap();
        client.start(&event_loop).unwrap();
        assert!(wait_until(Duration::from_secs(5), || handler
            .connects
            .load(Ordering::SeqCst)
            == 1));

        client.send(&[1; 100]).unwrap();
        let sender = client.sender();
        let started = Instant::now();
        event_loop.schedule_once(Duration::from_millis(300), move || sender.close());

        assert!(
            wait_until(Duration::from_secs(5), || handler
                .disconnects
                .load(Ordering::SeqCst)
                == 1),
            "{mode}: the deadline did not close the stalled connection"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(2),
            "{mode}: closed after {elapsed:?}"
        );
        assert!(handler.received.lock().len() <= 10, "{mode}");
        assert!(wait_until(Duration::from_secs(1), || event_loop
            .pending_timers()
            == timers));

        stop_loop(&event_loop, runner);
        drop((client, proxy, server));
    }
}

#[test]
fn server_keeps_running_after_its_handle_drops() {
    let _serial = serial();
    for (mode, event_loop) in pool_and_direct() {
        let echo = Echo::default();
        let config = TcpServerConfig::builder()
            .address("127.0.0.1:0".parse().unwrap())
            .build();
        let server = Arc::new(TcpServer::new(config, echo).unwrap());
        let addr = server.local_addr().unwrap();
        // As the crate docs and examples do: `start` takes the only handle.
        server.start(&event_loop, event_loop.alloc_token()).unwrap();
        let runner = spawn_loop(&event_loop);

        let handler = Client::default();
        let mut client =
            TcpClient::connect(addr, TcpClientConfig::default(), handler.clone()).unwrap();
        client.start(&event_loop).unwrap();
        assert!(
            wait_until(Duration::from_secs(5), || handler
                .connects
                .load(Ordering::SeqCst)
                == 1),
            "{mode}: the server stopped when its handle was dropped"
        );
        assert!(round_trip(&client, &handler, b"still serving"), "{mode}");

        stop_loop(&event_loop, runner);
        drop(client);
    }
}
