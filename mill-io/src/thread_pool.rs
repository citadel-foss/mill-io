#[cfg(feature = "unstable-mpmc")]
use std::sync::mpmc as channel;
#[cfg(not(feature = "unstable-mpmc"))]
use std::sync::mpsc as channel;

use parking_lot::{Condvar, Mutex};
use std::{
    cmp::Ordering as CmpOrdering,
    collections::BinaryHeap,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Barrier,
    },
    thread::{Builder, JoinHandle},
    time::Instant,
};

use crate::error::Result;

pub const DEFAULT_POOL_CAPACITY: usize = 4;

pub type Task = Box<dyn FnOnce() + Send + 'static>;

enum WorkerMessage {
    Task(Task),
    Terminate,
}

pub struct ThreadPool {
    workers: Vec<Worker>,
    senders: Vec<channel::Sender<WorkerMessage>>,
    next_worker: AtomicUsize,
    panics: Arc<AtomicU64>,
}

impl Default for ThreadPool {
    fn default() -> Self {
        let default_capacity = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(DEFAULT_POOL_CAPACITY);
        Self::new(default_capacity)
    }
}

impl ThreadPool {
    pub fn new(capacity: usize) -> Self {
        let mut workers = Vec::with_capacity(capacity);
        let mut senders = Vec::with_capacity(capacity);
        let panics = Arc::new(AtomicU64::new(0));

        for id in 0..capacity {
            let (sender, receiver) = channel::channel::<WorkerMessage>();
            workers.push(Worker::new(id, receiver, Arc::clone(&panics)));
            senders.push(sender);
        }

        Self {
            workers,
            senders,
            next_worker: AtomicUsize::new(0),
            panics,
        }
    }

    pub fn exec<F>(&self, task: F) -> Result<()>
    where
        F: FnOnce() + Send + 'static,
    {
        // Round-robin dispatch
        let index = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        Ok(self.senders[index].send(WorkerMessage::Task(Box::new(task)))?)
    }

    pub fn workers_len(&self) -> usize {
        self.workers.len()
    }

    /// Number of tasks that panicked since the pool was created.
    pub fn panics(&self) -> u64 {
        self.panics.load(Ordering::Relaxed)
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        for sender in &self.senders {
            let _ = sender.send(WorkerMessage::Terminate);
        }
        for worker in &mut self.workers {
            if let Some(t) = worker.take_thread() {
                let _ = t.join();
            }
        }
    }
}

struct Worker {
    #[allow(dead_code)]
    id: usize,
    thread: Option<JoinHandle<()>>,
}

impl Worker {
    pub fn new(
        id: usize,
        receiver: channel::Receiver<WorkerMessage>,
        panics: Arc<AtomicU64>,
    ) -> Self {
        let thread = Some(
            Builder::new()
                .name(format!("thread-pool-worker-{id}"))
                .spawn(move || {
                    while let Ok(message) = receiver.recv() {
                        match message {
                            WorkerMessage::Task(task) => {
                                // If this thread ended, its receiver would drop and
                                // every later `exec` routed here would fail.
                                if catch_unwind(AssertUnwindSafe(task)).is_err() {
                                    panics.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            WorkerMessage::Terminate => break,
                        }
                    }
                })
                .expect("Couldn't create the worker thread id={id}"),
        );

        Self { id, thread }
    }

    pub fn take_thread(&mut self) -> Option<JoinHandle<()>> {
        self.thread.take()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

#[derive(Debug, Default)]
pub struct ComputePoolMetrics {
    pub tasks_submitted: AtomicU64,
    pub tasks_completed: AtomicU64,
    pub tasks_failed: AtomicU64,
    pub active_workers: AtomicUsize,
    pub queue_depth_low: AtomicUsize,
    pub queue_depth_normal: AtomicUsize,
    pub queue_depth_high: AtomicUsize,
    pub queue_depth_critical: AtomicUsize,
    pub total_execution_time_ns: AtomicU64,
}

impl ComputePoolMetrics {
    pub fn tasks_submitted(&self) -> u64 {
        self.tasks_submitted.load(Ordering::Relaxed)
    }

    pub fn tasks_completed(&self) -> u64 {
        self.tasks_completed.load(Ordering::Relaxed)
    }

    pub fn tasks_failed(&self) -> u64 {
        self.tasks_failed.load(Ordering::Relaxed)
    }

    pub fn active_workers(&self) -> usize {
        self.active_workers.load(Ordering::Relaxed)
    }

    pub fn queue_depth_low(&self) -> usize {
        self.queue_depth_low.load(Ordering::Relaxed)
    }

    pub fn queue_depth_normal(&self) -> usize {
        self.queue_depth_normal.load(Ordering::Relaxed)
    }

    pub fn queue_depth_high(&self) -> usize {
        self.queue_depth_high.load(Ordering::Relaxed)
    }

    pub fn queue_depth_critical(&self) -> usize {
        self.queue_depth_critical.load(Ordering::Relaxed)
    }

    pub fn total_execution_time_ns(&self) -> u64 {
        self.total_execution_time_ns.load(Ordering::Relaxed)
    }
}

struct PriorityTask {
    task: Task,
    priority: TaskPriority,
    sequence: u64,
}

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl Eq for PriorityTask {}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        match self.priority.cmp(&other.priority) {
            CmpOrdering::Equal => other.sequence.cmp(&self.sequence),
            ord => ord,
        }
    }
}

struct ComputeSharedState {
    queue: Mutex<BinaryHeap<PriorityTask>>,
    condvar: Condvar,
    shutdown: AtomicBool,
}

pub struct ComputeThreadPool {
    workers: Vec<JoinHandle<()>>,
    state: Arc<ComputeSharedState>,
    sequence: AtomicU64,
    metrics: Arc<ComputePoolMetrics>,
}

impl Default for ComputeThreadPool {
    fn default() -> Self {
        let default_capacity = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(DEFAULT_POOL_CAPACITY);
        Self::new(default_capacity)
    }
}

impl ComputeThreadPool {
    pub fn new(capacity: usize) -> Self {
        let state = Arc::new(ComputeSharedState {
            queue: Mutex::new(BinaryHeap::new()),
            condvar: Condvar::new(),
            shutdown: AtomicBool::new(false),
        });
        let metrics = Arc::new(ComputePoolMetrics::default());

        let mut workers = Vec::with_capacity(capacity);
        // barrier to ensure all workers are started before returning
        let barrier = Arc::new(Barrier::new(capacity + 1));

        for id in 0..capacity {
            let state_clone = Arc::clone(&state);
            let barrier_clone = Arc::clone(&barrier);
            let metrics_clone = Arc::clone(&metrics);
            let thread = Builder::new()
                .name(format!("compute-worker-{id}"))
                .spawn(move || {
                    // wait for all workers to be ready
                    barrier_clone.wait();

                    loop {
                        let task = {
                            let mut queue = state_clone.queue.lock();

                            while queue.is_empty() && !state_clone.shutdown.load(Ordering::Relaxed)
                            {
                                state_clone.condvar.wait(&mut queue);
                            }

                            if state_clone.shutdown.load(Ordering::Relaxed) && queue.is_empty() {
                                break;
                            }

                            let t = queue.pop();
                            if let Some(ref pt) = t {
                                match pt.priority {
                                    TaskPriority::Low => metrics_clone
                                        .queue_depth_low
                                        .fetch_sub(1, Ordering::Relaxed),
                                    TaskPriority::Normal => metrics_clone
                                        .queue_depth_normal
                                        .fetch_sub(1, Ordering::Relaxed),
                                    TaskPriority::High => metrics_clone
                                        .queue_depth_high
                                        .fetch_sub(1, Ordering::Relaxed),
                                    TaskPriority::Critical => metrics_clone
                                        .queue_depth_critical
                                        .fetch_sub(1, Ordering::Relaxed),
                                };
                            }
                            t
                        };

                        if let Some(priority_task) = task {
                            metrics_clone.active_workers.fetch_add(1, Ordering::Relaxed);
                            let start = Instant::now();

                            let result = catch_unwind(AssertUnwindSafe(|| (priority_task.task)()));

                            let duration = start.elapsed();
                            metrics_clone
                                .total_execution_time_ns
                                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
                            metrics_clone.active_workers.fetch_sub(1, Ordering::Relaxed);

                            if result.is_ok() {
                                metrics_clone
                                    .tasks_completed
                                    .fetch_add(1, Ordering::Relaxed);
                            } else {
                                metrics_clone.tasks_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                })
                .expect("Failed to create compute worker thread");
            workers.push(thread);
        }

        // wait for all workers to start
        barrier.wait();

        Self {
            workers,
            state,
            sequence: AtomicU64::new(0),
            metrics,
        }
    }

    pub fn spawn<F>(&self, task: F, priority: TaskPriority)
    where
        F: FnOnce() + Send + 'static,
    {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let priority_task = PriorityTask {
            task: Box::new(task),
            priority,
            sequence,
        };

        {
            let mut queue = self.state.queue.lock();
            queue.push(priority_task);

            // All metric updates inside the lock so a reader never sees
            // tasks_submitted > sum(queue_depths) + completed + failed.
            self.metrics.tasks_submitted.fetch_add(1, Ordering::Relaxed);
            match priority {
                TaskPriority::Low => self.metrics.queue_depth_low.fetch_add(1, Ordering::Relaxed),
                TaskPriority::Normal => self
                    .metrics
                    .queue_depth_normal
                    .fetch_add(1, Ordering::Relaxed),
                TaskPriority::High => self
                    .metrics
                    .queue_depth_high
                    .fetch_add(1, Ordering::Relaxed),
                TaskPriority::Critical => self
                    .metrics
                    .queue_depth_critical
                    .fetch_add(1, Ordering::Relaxed),
            };
        }

        // Notify outside the lock so the woken worker doesn't immediately
        // block trying to acquire the lock we still hold.
        self.state.condvar.notify_one();
    }

    pub fn metrics(&self) -> Arc<ComputePoolMetrics> {
        self.metrics.clone()
    }
}

impl Drop for ComputeThreadPool {
    fn drop(&mut self) {
        {
            // Under the queue lock, so a worker between its `shutdown` check and
            // `wait` cannot miss this wake-up.
            let _queue = self.state.queue.lock();
            self.state.shutdown.store(true, Ordering::SeqCst);
            self.state.condvar.notify_all();
        }

        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        sync::{Arc, Barrier, Mutex},
        time::Duration,
    };

    use super::*;

    #[test]
    fn test_thread_pool_creation() {
        let pool = ThreadPool::new(4);
        assert_eq!(pool.workers_len(), 4);
    }

    #[test]
    fn test_task_execution() {
        let pool = ThreadPool::new(2);
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        pool.exec(move || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn test_multiple_tasks() {
        let pool = ThreadPool::new(4);
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..10 {
            let counter_clone = counter.clone();
            pool.exec(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }

        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_pool_cleanup() {
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let pool = ThreadPool::new(2);
            let counter_clone = counter.clone();

            pool.exec(move || {
                std::thread::sleep(Duration::from_millis(50));
                counter_clone.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    /// Helper: spawn a blocking task that occupies the single worker, queue
    /// several tasks with different priorities while it's blocked, then
    /// release and verify execution order.
    fn assert_priority_order(
        pool: &ComputeThreadPool,
        queued_priorities: &[TaskPriority],
        expected_order: &[usize],
    ) {
        let result = Arc::new(Mutex::new(Vec::new()));

        // Barrier ensures the blocker task has started before we queue more.
        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();

        // Second barrier: the blocker waits here until we've queued everything.
        let release = Arc::new(Barrier::new(2));
        let rel = release.clone();

        pool.spawn(
            move || {
                b.wait(); // signal "I'm running"
                rel.wait(); // wait until main says "go"
            },
            TaskPriority::Low,
        );

        // Worker is now running the blocker.
        barrier.wait();

        // Queue tasks while the worker is blocked.
        for (i, &pri) in queued_priorities.iter().enumerate() {
            let r = result.clone();
            pool.spawn(move || r.lock().unwrap().push(i), pri);
        }

        // Release the blocker so the worker drains the queue.
        release.wait();

        // Wait for all queued tasks to complete.
        let start = std::time::Instant::now();
        loop {
            if result.lock().unwrap().len() == queued_priorities.len() {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "Timed out waiting for tasks"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let res = result.lock().unwrap();
        assert_eq!(*res, expected_order);
    }

    #[test]
    fn test_compute_pool_priority_high_before_low() {
        let pool = ComputeThreadPool::new(1);
        // Queue: Low, High, Normal. Expected dequeue: High, Normal, Low.
        assert_priority_order(
            &pool,
            &[TaskPriority::Low, TaskPriority::High, TaskPriority::Normal],
            &[1, 2, 0],
        );
    }

    #[test]
    fn test_compute_pool_priority_critical_first() {
        let pool = ComputeThreadPool::new(1);
        // Critical should run before everything else.
        assert_priority_order(
            &pool,
            &[
                TaskPriority::Normal,
                TaskPriority::Low,
                TaskPriority::Critical,
                TaskPriority::High,
            ],
            &[2, 3, 0, 1],
        );
    }

    #[test]
    fn test_compute_pool_fifo_within_same_priority() {
        let pool = ComputeThreadPool::new(1);
        // All Normal: should execute in submission order (FIFO).
        assert_priority_order(
            &pool,
            &[
                TaskPriority::Normal,
                TaskPriority::Normal,
                TaskPriority::Normal,
            ],
            &[0, 1, 2],
        );
    }

    #[test]
    fn test_compute_pool_all_levels_ordered() {
        let pool = ComputeThreadPool::new(1);
        // One of each, submitted Low -> Normal -> High -> Critical.
        // Should dequeue Critical, High, Normal, Low.
        assert_priority_order(
            &pool,
            &[
                TaskPriority::Low,
                TaskPriority::Normal,
                TaskPriority::High,
                TaskPriority::Critical,
            ],
            &[3, 2, 1, 0],
        );
    }

    #[test]
    fn test_compute_pool_panic_is_caught() {
        let pool = ComputeThreadPool::new(1);
        let metrics = pool.metrics();

        let done = Arc::new(Barrier::new(2));
        let d = done.clone();

        // First task panics.
        pool.spawn(move || panic!("intentional panic"), TaskPriority::Normal);

        // Second task runs after the panic to prove the worker survived.
        pool.spawn(
            move || {
                d.wait();
            },
            TaskPriority::Normal,
        );

        done.wait();

        // The barrier opens inside the task; the worker counts it only after it returns.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while metrics.tasks_completed() < 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(metrics.tasks_failed(), 1);
        assert_eq!(metrics.tasks_completed(), 1);
    }

    #[test]
    fn test_compute_pool_fifo_with_mixed_duplicates() {
        let pool = ComputeThreadPool::new(1);
        // Two Highs and two Lows interleaved.
        // Should dequeue: High(0), High(2), Low(1), Low(3) (priority then FIFO).
        assert_priority_order(
            &pool,
            &[
                TaskPriority::High,
                TaskPriority::Low,
                TaskPriority::High,
                TaskPriority::Low,
            ],
            &[0, 2, 1, 3],
        );
    }

    #[test]
    fn test_compute_pool_panic_preserves_priority_order() {
        let pool = ComputeThreadPool::new(1);
        let result = Arc::new(Mutex::new(Vec::new()));

        let barrier = Arc::new(Barrier::new(2));
        let b = barrier.clone();
        let release = Arc::new(Barrier::new(2));
        let rel = release.clone();

        // Blocker task.
        pool.spawn(
            move || {
                b.wait();
                rel.wait();
            },
            TaskPriority::Low,
        );
        barrier.wait();

        // Queue: panic(High), then Normal, then Low.
        pool.spawn(move || panic!("boom"), TaskPriority::High);

        let r1 = result.clone();
        pool.spawn(
            move || r1.lock().unwrap().push("normal"),
            TaskPriority::Normal,
        );

        let r2 = result.clone();
        pool.spawn(move || r2.lock().unwrap().push("low"), TaskPriority::Low);

        release.wait();

        let start = std::time::Instant::now();
        loop {
            if result.lock().unwrap().len() == 2 {
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "Timed out waiting for tasks"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        // The panic task ran first (High), then Normal, then Low.
        let res = result.lock().unwrap();
        assert_eq!(*res, vec!["normal", "low"]);
    }

    #[test]
    fn test_compute_pool_shutdown_drains_queue() {
        let result = Arc::new(Mutex::new(Vec::new()));

        {
            let pool = ComputeThreadPool::new(1);

            let barrier = Arc::new(Barrier::new(2));
            let b = barrier.clone();
            let release = Arc::new(Barrier::new(2));
            let rel = release.clone();

            pool.spawn(
                move || {
                    b.wait();
                    rel.wait();
                },
                TaskPriority::Low,
            );
            barrier.wait();

            // Queue tasks while worker is blocked.
            for i in 0..5 {
                let r = result.clone();
                pool.spawn(move || r.lock().unwrap().push(i), TaskPriority::Normal);
            }

            // Release the blocker, then drop the pool.
            // Drop should wait for all queued tasks to complete.
            release.wait();
        }

        let res = result.lock().unwrap();
        assert_eq!(res.len(), 5);
        // All Normal, so FIFO order.
        assert_eq!(*res, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_compute_pool_multi_worker_picks_highest_first() {
        // 2 workers. Block both, then queue Low + Critical.
        let pool = ComputeThreadPool::new(2);
        let order = Arc::new(Mutex::new(Vec::new()));

        let started = Arc::new(Barrier::new(3)); // 2 workers + main
        let release_first = Arc::new(Barrier::new(2));
        let release_second = Arc::new(Barrier::new(2));

        for release in [release_first.clone(), release_second.clone()] {
            let s = started.clone();
            pool.spawn(
                move || {
                    s.wait();
                    release.wait();
                },
                TaskPriority::Low,
            );
        }
        started.wait();

        let o1 = order.clone();
        pool.spawn(move || o1.lock().unwrap().push("low"), TaskPriority::Low);
        let o2 = order.clone();
        pool.spawn(
            move || o2.lock().unwrap().push("critical"),
            TaskPriority::Critical,
        );

        // Free one worker only: with both free, the two tasks run concurrently
        // and the order they record in is not the order they were popped in.
        release_first.wait();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while order.lock().unwrap().len() < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        release_second.wait();

        assert_eq!(*order.lock().unwrap(), vec!["critical", "low"]);
    }

    #[test]
    fn test_compute_pool_drop_never_hangs() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for _ in 0..2000 {
                drop(ComputeThreadPool::new(4));
            }
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(60)).is_ok(),
            "dropping a ComputeThreadPool hung"
        );
    }

    #[test]
    fn test_compute_pool_metrics() {
        let pool = ComputeThreadPool::new(2);
        let metrics = pool.metrics();

        // Barrier: 2 workers + main thread.
        let barrier = Arc::new(Barrier::new(3));
        let b1 = barrier.clone();
        let b2 = barrier.clone();

        // Occupy both workers so subsequent tasks stay queued.
        pool.spawn(
            move || {
                b1.wait();
            },
            TaskPriority::Normal,
        );
        pool.spawn(
            move || {
                b2.wait();
            },
            TaskPriority::Normal,
        );

        // Give workers time to pick up tasks and increment active_workers.
        std::thread::sleep(Duration::from_millis(50));

        pool.spawn(|| {}, TaskPriority::Low);
        pool.spawn(|| {}, TaskPriority::High);
        pool.spawn(|| {}, TaskPriority::Critical);

        assert_eq!(metrics.tasks_submitted(), 5);
        assert_eq!(metrics.active_workers(), 2);
        assert_eq!(metrics.queue_depth_low(), 1);
        assert_eq!(metrics.queue_depth_high(), 1);
        assert_eq!(metrics.queue_depth_critical(), 1);
        // The two Normal tasks were already popped, so depth is 0.
        assert_eq!(metrics.queue_depth_normal(), 0);

        // Release workers.
        barrier.wait();

        let start = std::time::Instant::now();
        while metrics.tasks_completed() < 5 {
            assert!(
                start.elapsed() < Duration::from_secs(2),
                "Timed out waiting for tasks to complete"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(metrics.tasks_completed(), 5);
        assert_eq!(metrics.active_workers(), 0);
        assert_eq!(metrics.queue_depth_low(), 0);
        assert_eq!(metrics.queue_depth_high(), 0);
        assert_eq!(metrics.queue_depth_critical(), 0);
        assert!(metrics.total_execution_time_ns() > 0);
    }
}
