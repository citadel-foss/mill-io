//! Driving the event loop from other threads: `post()` work onto the reactor thread,
//! check `is_reactor_thread()`, register a UDP socket under an `alloc_token()` token and
//! flip its interest with `reregister`, then `stop()` the loop from another thread.
//! It also shows that a `stop()` made before `run()` is kept, not lost.
//!
//! Run with `cargo run -p mill-io --example post_and_stop`.

use mill_io::{error::Result, EventHandler, EventLoop};
use mio::{event::Event, net::UdpSocket, Interest, Token};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

const PRODUCERS: usize = 3;
const POSTS_PER_PRODUCER: usize = 4;

struct UppercaseEcho {
    event_loop: Weak<EventLoop>,
    socket: Arc<Mutex<UdpSocket>>,
    token: Token,
    replies: Mutex<Vec<(Vec<u8>, SocketAddr)>>,
}

impl UppercaseEcho {
    fn switch_interest(&self, socket: &mut UdpSocket, interest: Interest) {
        let Some(event_loop) = self.event_loop.upgrade() else {
            return;
        };
        if let Err(e) = event_loop.reregister(socket, self.token, interest) {
            eprintln!("reregister failed: {e}");
        }
    }

    fn receive(&self, socket: &mut UdpSocket) {
        let mut buf = [0u8; 1500];
        let mut replies = self.replies.lock().unwrap();
        loop {
            match socket.recv_from(&mut buf) {
                Ok((n, peer)) => replies.push((buf[..n].to_ascii_uppercase(), peer)),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    eprintln!("recv failed: {e}");
                    break;
                }
            }
        }
        if !replies.is_empty() {
            self.switch_interest(socket, Interest::WRITABLE);
        }
    }

    fn reply(&self, socket: &mut UdpSocket) {
        let mut replies = self.replies.lock().unwrap();
        while let Some((reply, peer)) = replies.first() {
            match socket.send_to(reply, *peer) {
                Ok(_) => {
                    replies.remove(0);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return,
                Err(e) => {
                    eprintln!("send failed: {e}");
                    replies.remove(0);
                }
            }
        }
        self.switch_interest(socket, Interest::READABLE);
    }
}

impl EventHandler for UppercaseEcho {
    fn handle_event(&self, event: &Event) {
        let on_reactor = self
            .event_loop
            .upgrade()
            .is_some_and(|event_loop| event_loop.is_reactor_thread());
        println!(
            "udp event: readable={} writable={} (handler on reactor thread: {on_reactor})",
            event.is_readable(),
            event.is_writable()
        );
        let mut socket = self.socket.lock().unwrap();
        if event.is_readable() {
            self.receive(&mut socket);
        }
        if event.is_writable() {
            self.reply(&mut socket);
        }
    }
}

fn main() -> Result<()> {
    let event_loop = Arc::new(EventLoop::new(2, 256, 100)?);

    event_loop.stop();
    event_loop.stop();
    let early = Instant::now();
    event_loop.run()?;
    println!(
        "run() after two early stop() calls returned in {:?}; they merged into that one run",
        early.elapsed()
    );

    let posted = Arc::new(AtomicUsize::new(0));
    let on_loop = Arc::clone(&event_loop);
    event_loop.post(move || {
        println!(
            "posted before run(), ran on the reactor thread: {}",
            on_loop.is_reactor_thread()
        );
    })?;

    let socket = Arc::new(Mutex::new(UdpSocket::bind("127.0.0.1:0".parse()?)?));
    let echo_addr = socket.lock().unwrap().local_addr()?;
    let token = event_loop.alloc_token();
    event_loop.register(
        &mut *socket.lock().unwrap(),
        token,
        Interest::READABLE,
        UppercaseEcho {
            event_loop: Arc::downgrade(&event_loop),
            socket: Arc::clone(&socket),
            token,
            replies: Mutex::new(Vec::new()),
        },
    )?;
    println!("udp echo on {echo_addr} registered under {token:?}");

    let producers: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let event_loop = Arc::clone(&event_loop);
            let posted = Arc::clone(&posted);
            thread::spawn(move || {
                assert!(!event_loop.is_reactor_thread());
                for job in 0..POSTS_PER_PRODUCER {
                    let on_loop = Arc::clone(&event_loop);
                    let posted = Arc::clone(&posted);
                    event_loop
                        .post(move || {
                            assert!(on_loop.is_reactor_thread());
                            posted.fetch_add(1, Ordering::SeqCst);
                            println!("producer {producer} job {job} ran on the reactor thread");
                        })
                        .expect("wake the loop");
                }
            })
        })
        .collect();

    let stopper = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || -> io::Result<Vec<String>> {
            for producer in producers {
                producer.join().expect("producer panicked");
            }
            let client = std::net::UdpSocket::bind("127.0.0.1:0")?;
            client.set_read_timeout(Some(Duration::from_secs(5)))?;
            let mut answers = Vec::new();
            for word in ["hello", "event loop"] {
                client.send_to(word.as_bytes(), echo_addr)?;
                let mut buf = [0u8; 1500];
                let (n, _) = client.recv_from(&mut buf)?;
                answers.push(String::from_utf8_lossy(&buf[..n]).into_owned());
            }
            println!("stopping the loop from another thread");
            event_loop.stop();
            Ok(answers)
        })
    };

    event_loop.run()?;
    let answers = stopper.join().expect("stopper panicked")?;
    event_loop.deregister(&mut *socket.lock().unwrap(), token)?;

    println!("udp answers: {answers:?}");
    println!(
        "posted jobs run: {} of {}",
        posted.load(Ordering::SeqCst),
        PRODUCERS * POSTS_PER_PRODUCER
    );
    assert_eq!(answers, ["HELLO", "EVENT LOOP"]);
    assert_eq!(
        posted.load(Ordering::SeqCst),
        PRODUCERS * POSTS_PER_PRODUCER
    );
    Ok(())
}
