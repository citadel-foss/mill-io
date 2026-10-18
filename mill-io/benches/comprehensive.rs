use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use mill_io::{EventHandler, EventLoop, ObjectPool, TaskPriority};
use mio::{
    event::Event,
    net::{TcpListener, TcpStream},
    Interest, Token,
};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Barrier,
    },
    thread,
    time::{Duration, Instant},
};

fn bench_threadpool_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("threadpool_latency");

    for pool_size in [1, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(pool_size),
            &pool_size,
            |b, &size| {
                let event_loop = EventLoop::new(size, 1024, 100).unwrap();

                b.iter(|| {
                    let done = Arc::new(AtomicBool::new(false));
                    let d = done.clone();
                    let start = Instant::now();

                    event_loop.spawn_compute(move || {
                        d.store(true, Ordering::Release);
                    });

                    while !done.load(Ordering::Acquire) {
                        thread::yield_now();
                    }

                    black_box(start.elapsed());
                });
            },
        );
    }
    group.finish();
}

fn bench_compute_priority_ordering(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_priority");

    group.bench_function("priority_enforcement", |b| {
        b.iter(|| {
            let event_loop = EventLoop::new(1, 1024, 100).unwrap();
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let barrier = Arc::new(Barrier::new(2));

            let b1 = barrier.clone();
            event_loop.spawn_compute_with_priority(
                move || {
                    b1.wait();
                    thread::sleep(Duration::from_millis(10));
                },
                TaskPriority::Low,
            );

            barrier.wait();
            let priorities = [
                TaskPriority::Low,
                TaskPriority::High,
                TaskPriority::Critical,
                TaskPriority::Normal,
            ];

            // A rendezvous between the tasks would need one compute worker per task.
            let (done_tx, done_rx) = mpsc::channel();

            for (i, &priority) in priorities.iter().enumerate() {
                let o = order.clone();
                let done = done_tx.clone();
                event_loop.spawn_compute_with_priority(
                    move || {
                        o.lock().unwrap().push((i, priority));
                        let _ = done.send(());
                    },
                    priority,
                );
            }

            drop(done_tx);
            for _ in 0..priorities.len() {
                done_rx.recv().unwrap();
            }
        });
    });

    group.finish();
}

fn bench_compute_pool_metrics_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute_metrics");

    group.bench_function("metrics_collection", |b| {
        let event_loop = EventLoop::default();

        b.iter(|| {
            for _ in 0..1000 {
                event_loop.spawn_compute(|| {
                    black_box(2 + 2);
                });
            }

            let metrics = event_loop.get_compute_metrics();
            black_box(metrics.tasks_submitted());
            black_box(metrics.queue_depth_high());
        });
    });

    group.finish();
}

fn bench_object_pool_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool");
    group.throughput(Throughput::Elements(1000));

    group.bench_function("with_pool_sized", |b| {
        let pool = ObjectPool::new(1000, || vec![0u8; 8192]); // Match iteration count

        b.iter(|| {
            for _ in 0..1000 {
                let obj = pool.acquire();
                black_box(obj.as_ref());
            }
        });
    });

    group.bench_function("with_pool_small", |b| {
        let pool = ObjectPool::new(100, || vec![0u8; 8192]);

        b.iter(|| {
            for _ in 0..1000 {
                let obj = pool.acquire();
                black_box(obj.as_ref());
            }
        });
    });

    group.bench_function("without_pool", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let obj = vec![0u8; 8192];
                black_box(&obj);
            }
        });
    });

    group.bench_function("with_pool_reuse", |b| {
        let pool = ObjectPool::new(10, || vec![0u8; 8192]);

        b.iter(|| {
            for _ in 0..1000 {
                let mut obj = pool.acquire();
                obj.as_mut()[0] = 42;
                black_box(obj.as_ref());
            }
        });
    });

    group.finish();
}

fn bench_object_pool_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool_contention");

    for thread_count in [1, 2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::from_parameter(thread_count),
            &thread_count,
            |b, &threads| {
                let pool = Arc::new(ObjectPool::new(50, || vec![0u8; 8192]));

                b.iter(|| {
                    let barrier = Arc::new(Barrier::new(threads + 1));
                    let mut handles = vec![];

                    for _ in 0..threads {
                        let p = pool.clone();
                        let b = barrier.clone();

                        handles.push(thread::spawn(move || {
                            b.wait();
                            for _ in 0..500 {
                                let obj = p.acquire();
                                black_box(obj.as_ref());
                            }
                        }));
                    }

                    barrier.wait();
                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_event_dispatch_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_dispatch");

    group.bench_function("tcp_events", |b| {
        b.iter(|| {
            let event_loop = Arc::new(EventLoop::default());
            let counter = Arc::new(AtomicUsize::new(0));

            // Create listener
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut listener = TcpListener::bind(addr).unwrap();
            let listener_addr = listener.local_addr().unwrap();

            struct CountHandler {
                counter: Arc<AtomicUsize>,
            }

            impl EventHandler for CountHandler {
                fn handle_event(&self, _: &Event) {
                    self.counter.fetch_add(1, Ordering::Relaxed);
                }
            }

            event_loop
                .register(
                    &mut listener,
                    Token(1),
                    Interest::READABLE,
                    CountHandler {
                        counter: counter.clone(),
                    },
                )
                .unwrap();

            let el = event_loop.clone();
            let handle = thread::spawn(move || {
                let _ = el.run();
            });

            // Generate events
            for _ in 0..100 {
                let _ = TcpStream::connect(listener_addr);
            }

            event_loop.stop();
            let _ = handle.join();

            black_box(counter.load(Ordering::Relaxed));
        });
    });

    group.finish();
}

fn bench_registration_deregistration(c: &mut Criterion) {
    let mut group = c.benchmark_group("registration");

    group.bench_function("register_deregister_cycle", |b| {
        let event_loop = EventLoop::default();

        struct NoOpHandler;
        impl EventHandler for NoOpHandler {
            fn handle_event(&self, _: &Event) {}
        }

        b.iter(|| {
            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let mut listener = TcpListener::bind(addr).unwrap();

            event_loop
                .register(&mut listener, Token(1), Interest::READABLE, NoOpHandler)
                .unwrap();

            event_loop.deregister(&mut listener, Token(1)).unwrap();
        });
    });

    group.finish();
}

fn bench_stress_max_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("stress_test");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);

    group.bench_function("rapid_task_submission", |b| {
        let event_loop = EventLoop::new(4, 1024, 100).unwrap();

        b.iter(|| {
            for _ in 0..1000 {
                event_loop.spawn_compute(|| {
                    black_box(42);
                });
            }
        });
    });

    group.finish();
}

criterion_group!(threadpool_benches, bench_threadpool_latency,);

criterion_group!(
    compute_benches,
    bench_compute_priority_ordering,
    bench_compute_pool_metrics_overhead,
);

criterion_group!(
    pool_benches,
    bench_object_pool_allocation,
    bench_object_pool_contention,
);

criterion_group!(
    reactor_benches,
    bench_event_dispatch_rate,
    bench_registration_deregistration,
);

criterion_group!(stress_benches, bench_stress_max_throughput,);

criterion_main!(
    threadpool_benches,
    compute_benches,
    pool_benches,
    reactor_benches,
    stress_benches,
);
