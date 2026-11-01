//! A `Handle` does not keep its loop alive. A UDP handler and a repeating timer both
//! capture one; when the loop is dropped their state is freed, and a late call through
//! the handle reports `LoopGone`. The loop is set up by hand here, without
//! `#[mill_io::main]`, so the example controls exactly when it is dropped.
//!
//! Run with `cargo run -p mill-io --example handle_no_cycle`. It exits after about 50 ms.

use mill_io::{EventHandler, EventLoop, Handle, JoinError, LoopGone};
use mio::{event::Event, net::UdpSocket, Interest};
use std::{
    error::Error,
    net,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

/// Prints when the loop frees the state that owns it.
struct Freed(&'static str);

impl Drop for Freed {
    fn drop(&mut self) {
        println!("freed: {}", self.0);
    }
}

struct Echo {
    socket: Arc<Mutex<UdpSocket>>,
    handle: Handle,
    _freed: Freed,
}

impl EventHandler for Echo {
    fn handle_event(&self, _: &Event) {
        let socket = self.socket.lock().unwrap();
        let mut buf = [0; 64];
        while let Ok((len, from)) = socket.recv_from(&mut buf) {
            let reply = buf[..len].to_ascii_uppercase();
            let _ = socket.send_to(&reply, from);
            let _ = self.handle.post(move || {
                println!("echoed {:?}", String::from_utf8_lossy(&reply));
            });
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let event_loop = EventLoop::builder()
        .io_workers(1)
        .compute_workers(1)
        .build()?;
    let handle = event_loop.handle();

    let socket = Arc::new(Mutex::new(UdpSocket::bind("127.0.0.1:0".parse()?)?));
    let echo_addr = socket.lock().unwrap().local_addr()?;
    event_loop.register(
        &mut *socket.lock().unwrap(),
        handle.alloc_token()?,
        Interest::READABLE,
        Echo {
            socket: Arc::clone(&socket),
            handle: handle.clone(),
            _freed: Freed("the echo handler and its Handle"),
        },
    )?;
    drop(socket);

    let ticks = Arc::new(AtomicUsize::new(0));
    let (timer_handle, timer_ticks) = (handle.clone(), Arc::clone(&ticks));
    let freed = Freed("the tick timer and its Handle");
    handle.schedule_repeating(Duration::from_millis(10), move || {
        let _ = &freed;
        let n = timer_ticks.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = timer_handle.post(move || println!("tick {n}"));
    })?;

    let client = net::UdpSocket::bind("127.0.0.1:0")?;
    client.set_read_timeout(Some(Duration::from_secs(5)))?;
    client.send_to(b"no cycle", echo_addr)?;

    thread::scope(|scope| {
        let runner = scope.spawn(|| event_loop.run().map_err(|e| e.to_string()));
        let mut reply = [0; 64];
        let reply = client
            .recv(&mut reply)
            .map(|len| String::from_utf8_lossy(&reply[..len]).into_owned());
        println!("client got {reply:?}");
        while ticks.load(Ordering::SeqCst) < 3 {
            thread::sleep(Duration::from_millis(5));
        }
        handle.stop();
        runner.join().expect("the reactor thread panicked")
    })?;
    println!("loop stopped after {} ticks", ticks.load(Ordering::SeqCst));

    println!("dropping the loop");
    drop(event_loop);

    let late = handle.post(|| unreachable!("the loop is gone"));
    println!("post through the handle afterwards: {late:?}");
    assert_eq!(late, Err(LoopGone));
    println!(
        "spawn through the handle afterwards: {:?}",
        handle.spawn(|| 1).join()
    );
    assert_eq!(handle.spawn(|| 1).join(), Err(JoinError::LoopStopped));
    Ok(())
}
