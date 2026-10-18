//! A writer thread sends 100k sequence numbers, one 8-byte `write` each, into a
//! Unix socket pair; each write raises a new edge, so the reactor sees a steady
//! stream of readable events on one token (writes between two polls coalesce,
//! so fewer than 100k; the test prints the count). The handler reads, then
//! takes the state lock to append, like a connection handler: two events of the
//! token running at once can append out of order.
#![cfg(unix)]

use mill_io::{EventHandler, EventLoop};
use mio::{event::Event, net::UnixStream, Interest, Token};
use std::{
    io::{ErrorKind, Read, Write},
    os::unix::net::UnixStream as StdUnixStream,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const RECORDS: u64 = 100_000;
const RECORD_LEN: usize = 8;

fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Default)]
struct Received {
    partial: Vec<u8>,
    /// Records seen so far, which is also the next expected sequence number.
    count: u64,
    /// The first record that broke the order, as (expected, got).
    first_violation: Option<(u64, u64)>,
}

struct SequenceHandler {
    stream: Arc<Mutex<UnixStream>>,
    received: Arc<Mutex<Received>>,
    events: Arc<AtomicU64>,
    records: Arc<AtomicU64>,
}

impl EventHandler for SequenceHandler {
    fn handle_event(&self, event: &Event) {
        if !event.is_readable() {
            return;
        }
        self.events.fetch_add(1, Ordering::Relaxed);
        let mut buf = [0u8; 4096];
        loop {
            let n = match self.stream.lock().unwrap().read(&mut buf) {
                Ok(0) => return,
                Ok(n) => n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return,
                Err(e) => panic!("read failed: {e}"),
            };

            let mut received = self.received.lock().unwrap();
            received.partial.extend_from_slice(&buf[..n]);
            let whole = received.partial.len() / RECORD_LEN * RECORD_LEN;
            let bytes: Vec<u8> = received.partial.drain(..whole).collect();
            for record in bytes.chunks_exact(RECORD_LEN) {
                let got = u64::from_le_bytes(record.try_into().unwrap());
                let expected = received.count;
                if got != expected && received.first_violation.is_none() {
                    received.first_violation = Some((expected, got));
                }
                received.count += 1;
            }
            self.records.store(received.count, Ordering::SeqCst);
        }
    }
}

#[test]
fn pool_mode_preserves_per_token_order() {
    let event_loop = Arc::new(EventLoop::new(4, 1024, 10).unwrap());

    let (reader, mut writer) = StdUnixStream::pair().unwrap();
    reader.set_nonblocking(true).unwrap();
    let stream = Arc::new(Mutex::new(UnixStream::from_std(reader)));
    let received = Arc::new(Mutex::new(Received::default()));
    let events = Arc::new(AtomicU64::new(0));
    let records = Arc::new(AtomicU64::new(0));

    event_loop
        .register(
            &mut *stream.lock().unwrap(),
            Token(7),
            Interest::READABLE,
            SequenceHandler {
                stream: Arc::clone(&stream),
                received: Arc::clone(&received),
                events: Arc::clone(&events),
                records: Arc::clone(&records),
            },
        )
        .unwrap();

    let runner = {
        let event_loop = Arc::clone(&event_loop);
        thread::spawn(move || event_loop.run().map_err(|e| e.to_string()))
    };

    let writer_thread = thread::spawn(move || {
        for seq in 0..RECORDS {
            writer.write_all(&seq.to_le_bytes()).unwrap();
        }
        writer
    });

    let done = wait_until(Duration::from_secs(60), || {
        records.load(Ordering::SeqCst) >= RECORDS
    });
    let _writer = writer_thread.join().unwrap();
    event_loop.stop();
    assert_eq!(runner.join().unwrap(), Ok(()));

    let received = received.lock().unwrap();
    let events = events.load(Ordering::Relaxed);
    eprintln!("{RECORDS} records arrived in {events} readable events");
    assert!(
        done,
        "only {} of {RECORDS} records arrived within 60 s",
        received.count
    );
    assert_eq!(received.count, RECORDS);
    assert!(
        received.partial.is_empty(),
        "a partial record was left over"
    );
    assert_eq!(
        received.first_violation, None,
        "records arrived out of order (expected, got)"
    );
    assert!(events > 1, "the writer produced a single readiness event");
}
