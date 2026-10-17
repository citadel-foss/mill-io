use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Unique identifier for a scheduled timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(pub(crate) u64);

// 4-level hierarchical timer wheel.
//
// Each level has a fixed number of slots. A timer is placed in the lowest
// level that can represent its delay. When a lower level wraps, timers
// cascade down from the next level, getting re-sorted into finer slots.
//
// Level 0: 256 slots, 1 ms/slot   (256 ms range)
// Level 1:  64 slots, 256 ms/slot (~16 s range)
// Level 2:  64 slots, ~16 s/slot  (~17 min range)
// Level 3:  64 slots, ~17 min/slot (~18 hr range)
//
// Timers beyond ~18 hours are stored in an overflow list.

const NUM_LEVELS: usize = 4;
const LEVEL_BITS: [u32; NUM_LEVELS] = [8, 6, 6, 6];
const LEVEL_SIZE: [usize; NUM_LEVELS] = [256, 64, 64, 64];

// Bit shifts and masks for extracting the slot index at each level
// from an absolute tick value.
const SHIFTS: [u32; NUM_LEVELS] = [0, 8, 14, 20];
const MASKS: [u64; NUM_LEVELS] = [0xFF, 0x3F, 0x3F, 0x3F];

// Total addressable ticks: 2^26 = 67,108,864 (~18.6 hours at 1ms/tick)
const MAX_RANGE_TICKS: u64 = 1 << 26;

const TICK: Duration = Duration::from_millis(1);

fn duration_to_ticks(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

enum TimerKind {
    Once(Option<Box<dyn FnOnce() + Send>>),
    Repeating(Box<dyn Fn() + Send + Sync>, Duration),
}

struct TimerEntry {
    id: TimerId,
    deadline_tick: u64,
    kind: TimerKind,
}

struct WheelInner {
    levels: [Vec<Vec<u64>>; NUM_LEVELS],
    current_tick: u64,
    start_time: Instant,
    /// All live timers keyed by raw ID. Cancelled timers are removed from here;
    /// stale IDs left in wheel slots are skipped on fire.
    timers: HashMap<u64, TimerEntry>,
    /// Timers whose deadline exceeds the wheel's addressable range.
    overflow: Vec<u64>,
    /// Smallest deadline tick currently sitting in `overflow`. Lets `tick`
    /// skip the O(n) overflow rescan until the cursor is close enough that at
    /// least one overflow timer can move into the wheel. None means empty.
    overflow_min_deadline: Option<u64>,
    timer_count: usize,
    /// Cached earliest deadline tick. None means dirty and needs a full
    /// recompute on the next time_until_next call. Kept up to date on
    /// inserts (O(1) min-update) and lazily invalidated on cancels/expirations.
    earliest_deadline: Option<u64>,
    /// Repeating timers whose callback is currently running (removed from
    /// `timers` for the duration). Lets `cancel` see a timer that is mid-fire.
    in_flight: HashSet<u64>,
    /// In-flight repeating timers that were cancelled during their callback
    /// window, so the reschedule step knows to drop them instead of re-arming.
    canceled_in_flight: HashSet<u64>,
}

impl WheelInner {
    fn new() -> Self {
        let levels = [
            vec![Vec::new(); LEVEL_SIZE[0]],
            vec![Vec::new(); LEVEL_SIZE[1]],
            vec![Vec::new(); LEVEL_SIZE[2]],
            vec![Vec::new(); LEVEL_SIZE[3]],
        ];

        WheelInner {
            levels,
            current_tick: 0,
            start_time: Instant::now(),
            timers: HashMap::new(),
            overflow: Vec::new(),
            overflow_min_deadline: None,
            timer_count: 0,
            earliest_deadline: None,
            in_flight: HashSet::new(),
            canceled_in_flight: HashSet::new(),
        }
    }

    fn instant_to_tick(&self, instant: Instant) -> u64 {
        duration_to_ticks(instant.saturating_duration_since(self.start_time))
    }

    fn deadline_after(now_tick: u64, delay: Duration) -> u64 {
        now_tick.saturating_add(duration_to_ticks(delay))
    }

    /// Place a timer ID into the correct wheel slot based on its deadline.
    fn insert_into_wheel(&mut self, id: u64, deadline_tick: u64) {
        let delta = deadline_tick.saturating_sub(self.current_tick);

        if delta >= MAX_RANGE_TICKS {
            self.overflow.push(id);
            self.overflow_min_deadline = Some(
                self.overflow_min_deadline
                    .map_or(deadline_tick, |d| d.min(deadline_tick)),
            );
            return;
        }

        let level = self.level_for_delta(delta);
        let slot = (deadline_tick >> SHIFTS[level]) & MASKS[level];
        self.levels[level][slot as usize].push(id);
    }

    /// Find the lowest wheel level whose span covers `delta` ticks.
    fn level_for_delta(&self, delta: u64) -> usize {
        let mut cumulative_bits = 0u32;
        for (level, &bits) in LEVEL_BITS.iter().enumerate() {
            cumulative_bits += bits;
            if delta < (1u64 << cumulative_bits) {
                return level;
            }
        }
        NUM_LEVELS - 1
    }

    /// Move timers from a higher level slot down into lower levels.
    /// Called when a lower level's cursor wraps around.
    fn cascade(&mut self, level: usize) {
        let slot = (self.current_tick >> SHIFTS[level]) & MASKS[level];
        let ids = std::mem::take(&mut self.levels[level][slot as usize]);
        for id in ids {
            if self.timers.contains_key(&id) {
                let deadline = self.timers[&id].deadline_tick;
                self.insert_into_wheel(id, deadline);
            }
        }
    }

    /// Move overflow timers into the wheel if they are now within range.
    fn reinsert_overflow(&mut self) {
        let overflow = std::mem::take(&mut self.overflow);
        // insert_into_wheel rebuilds this for any timer still out of range.
        self.overflow_min_deadline = None;
        for id in overflow {
            if let Some(entry) = self.timers.get(&id) {
                let deadline = entry.deadline_tick;
                self.insert_into_wheel(id, deadline);
            }
        }
    }

    /// Advance the wheel to `now` and return all expired timer entries.
    fn advance_to(&mut self, now: Instant) -> Vec<TimerEntry> {
        let target_tick = self.instant_to_tick(now);
        let mut expired = Vec::new();

        // If the time jump is larger than the wheel can represent (e.g. after
        // a long suspend), fire only the timers that are actually due and keep
        // the rest, instead of iterating tick-by-tick.
        if target_tick > self.current_tick.saturating_add(MAX_RANGE_TICKS) {
            self.drain_due_into(target_tick, &mut expired);
            return expired;
        }

        while self.current_tick < target_tick {
            let slot = (self.current_tick & MASKS[0]) as usize;

            // Fire all timers in the current level-0 slot.
            let ids = std::mem::take(&mut self.levels[0][slot]);
            for id in ids {
                if let Some(entry) = self.timers.remove(&id) {
                    self.timer_count -= 1;
                    expired.push(entry);
                }
            }

            self.current_tick += 1;

            // Cascade from higher levels when lower levels wrap.
            let mut shift_acc = LEVEL_BITS[0];
            for (level, &bits) in LEVEL_BITS.iter().enumerate().skip(1) {
                if (self.current_tick & ((1u64 << shift_acc) - 1)) == 0 {
                    self.cascade(level);
                } else {
                    break;
                }
                shift_acc += bits;
            }
        }

        // Only rescan overflow once the cursor is close enough that at least
        // one overflow timer can now fit in the wheel's addressable range.
        if self
            .overflow_min_deadline
            .is_some_and(|d| d.saturating_sub(self.current_tick) < MAX_RANGE_TICKS)
        {
            self.reinsert_overflow();
        }

        // Timers were removed; invalidate the cached earliest so the next
        // time_until_next call recomputes from the remaining set.
        if !expired.is_empty() {
            self.earliest_deadline = None;
        }

        expired
    }

    /// Handle a time jump beyond the wheel's range: fire only the timers due at
    /// or before `target_tick`, and reinsert the rest relative to the new tick.
    /// Avoids prematurely expiring far-future (overflow) timers.
    fn drain_due_into(&mut self, target_tick: u64, expired: &mut Vec<TimerEntry>) {
        // Pull every live timer id out of the wheel slots and overflow.
        let mut ids = Vec::new();
        for level in &mut self.levels {
            for slot in level.iter_mut() {
                ids.append(slot);
            }
        }
        ids.append(&mut self.overflow);
        self.overflow_min_deadline = None;

        // Advance the cursor first so survivors are reinserted into the right
        // slots relative to the new current time.
        self.current_tick = target_tick;

        for id in ids {
            let deadline = match self.timers.get(&id) {
                Some(entry) => entry.deadline_tick,
                None => continue, // stale id left by a cancel
            };
            if deadline <= target_tick {
                if let Some(entry) = self.timers.remove(&id) {
                    self.timer_count -= 1;
                    expired.push(entry);
                }
            } else {
                self.insert_into_wheel(id, deadline);
            }
        }

        self.earliest_deadline = None;
    }

    /// O(1) update when a timer is inserted. If the cache is valid, take the
    /// min. If it is dirty (None) leave it dirty so the next time_until_next
    /// recomputes — unless this is the only timer, in which case its deadline
    /// is trivially the earliest. Called after `timer_count` is incremented.
    fn note_inserted(&mut self, deadline_tick: u64) {
        self.earliest_deadline = match self.earliest_deadline {
            Some(current) => Some(current.min(deadline_tick)),
            None if self.timer_count == 1 => Some(deadline_tick),
            None => None,
        };
    }

    /// Invalidate the cache when the earliest timer might have been removed.
    fn note_removed(&mut self, removed_deadline: u64) {
        if self.earliest_deadline == Some(removed_deadline) {
            self.earliest_deadline = None;
        }
    }

    /// Full O(n) recompute of the earliest deadline and cache it.
    fn recompute_earliest(&mut self) -> Option<u64> {
        let earliest = self.timers.values().map(|e| e.deadline_tick).min();
        self.earliest_deadline = earliest;
        earliest
    }

    /// Duration until the nearest pending timer, or None if empty.
    /// O(1) when the cached earliest is valid; O(n) recompute only
    /// when the cache was invalidated by a cancel or expiration.
    fn time_until_next(&mut self) -> Option<Duration> {
        if self.timer_count == 0 {
            return None;
        }

        let earliest = match self.earliest_deadline {
            Some(e) => e,
            None => self.recompute_earliest()?,
        };

        let now = Instant::now();
        let now_tick = self.instant_to_tick(now);

        if earliest < now_tick {
            Some(Duration::ZERO)
        } else if earliest == now_tick {
            // A timer due at tick T fires only once tick T + 1 has started;
            // returning zero here would make the reactor spin on poll(0).
            let next_tick_start = self
                .start_time
                .checked_add(Duration::from_millis(now_tick.saturating_add(1)));
            Some(next_tick_start.map_or(TICK, |t| t.saturating_duration_since(now)))
        } else {
            Some(Duration::from_millis(earliest - now_tick))
        }
    }
}

/// Hierarchical timer wheel for O(1) timer scheduling and cancellation.
///
/// Timers are placed into one of four wheel levels depending on how far
/// in the future they fire. As time advances, higher-level timers cascade
/// down into finer-grained levels until they land in level 0 and fire.
///
/// Thread-safe: all methods can be called from any thread.
pub struct TimerWheel {
    inner: Mutex<WheelInner>,
    next_id: AtomicU64,
}

impl TimerWheel {
    pub fn new() -> Self {
        TimerWheel {
            inner: Mutex::new(WheelInner::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Schedule a one-shot timer that fires once after `delay`.
    /// A delay too large to represent, up to `Duration::MAX`, saturates: the
    /// timer never fires but can still be cancelled.
    pub fn schedule_once<F>(&self, delay: Duration, callback: F) -> TimerId
    where
        F: FnOnce() + Send + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timer_id = TimerId(id);

        let mut inner = self.inner.lock();
        let now_tick = inner.instant_to_tick(Instant::now());
        let deadline_tick = WheelInner::deadline_after(now_tick, delay);

        let entry = TimerEntry {
            id: timer_id,
            deadline_tick,
            kind: TimerKind::Once(Some(Box::new(callback))),
        };

        inner.timers.insert(id, entry);
        inner.timer_count += 1;
        inner.insert_into_wheel(id, deadline_tick);
        inner.note_inserted(deadline_tick);

        timer_id
    }

    /// Schedule a repeating timer that fires every `interval`.
    /// The first firing happens after one `interval` elapses.
    /// Intervals below one tick (1 ms), including zero, are clamped to one
    /// tick; huge ones saturate as in [`schedule_once`](Self::schedule_once).
    pub fn schedule_repeating<F>(&self, interval: Duration, callback: F) -> TimerId
    where
        F: Fn() + Send + Sync + 'static,
    {
        let interval = interval.max(TICK);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let timer_id = TimerId(id);

        let mut inner = self.inner.lock();
        let now_tick = inner.instant_to_tick(Instant::now());
        let deadline_tick = WheelInner::deadline_after(now_tick, interval);

        let entry = TimerEntry {
            id: timer_id,
            deadline_tick,
            kind: TimerKind::Repeating(Box::new(callback), interval),
        };

        inner.timers.insert(id, entry);
        inner.timer_count += 1;
        inner.insert_into_wheel(id, deadline_tick);
        inner.note_inserted(deadline_tick);

        timer_id
    }

    /// Cancel a pending timer. Returns true if the timer existed and was removed.
    pub fn cancel(&self, timer_id: TimerId) -> bool {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.timers.remove(&timer_id.0) {
            let deadline = entry.deadline_tick;
            inner.timer_count -= 1;
            inner.note_removed(deadline);
            true
        } else if inner.in_flight.contains(&timer_id.0) {
            // A repeating timer mid-callback is temporarily absent from
            // `timers`; record the cancel so it is not re-armed afterwards.
            inner.canceled_in_flight.insert(timer_id.0);
            true
        } else {
            false
        }
    }

    /// Advance the wheel to the current time, firing all expired timers.
    ///
    /// Repeating timers are automatically re-scheduled after firing.
    /// Callbacks run outside the wheel lock so they can safely schedule
    /// or cancel other timers.
    pub fn tick(&self) {
        let now = Instant::now();

        let expired = {
            let mut inner = self.inner.lock();
            inner.advance_to(now)
        };

        if expired.is_empty() {
            return;
        }

        // Mark repeating timers as in-flight so a cancel() landing while their
        // callback runs (including a self-cancel) is not silently lost.
        {
            let mut inner = self.inner.lock();
            for entry in &expired {
                if matches!(entry.kind, TimerKind::Repeating(..)) {
                    inner.in_flight.insert(entry.id.0);
                }
            }
        }

        // Fire callbacks without holding the lock.
        let mut to_reschedule = Vec::new();

        for mut entry in expired {
            match &mut entry.kind {
                TimerKind::Once(cb) => {
                    if let Some(callback) = cb.take() {
                        callback();
                    }
                }
                TimerKind::Repeating(cb, _) => {
                    cb();
                    to_reschedule.push(entry);
                }
            }
        }

        if !to_reschedule.is_empty() {
            let mut inner = self.inner.lock();
            let now_tick = inner.instant_to_tick(Instant::now());

            for entry in to_reschedule {
                let id = entry.id.0;
                inner.in_flight.remove(&id);
                if inner.canceled_in_flight.remove(&id) {
                    // Cancelled while its callback was running; drop it.
                    continue;
                }
                if let TimerKind::Repeating(_, interval) = &entry.kind {
                    let next_deadline = WheelInner::deadline_after(now_tick, *interval);

                    let new_entry = TimerEntry {
                        id: entry.id,
                        deadline_tick: next_deadline,
                        kind: entry.kind,
                    };

                    inner.timers.insert(id, new_entry);
                    inner.timer_count += 1;
                    inner.insert_into_wheel(id, next_deadline);
                    inner.note_inserted(next_deadline);
                }
            }
        }
    }

    /// Duration until the nearest pending timer, or None if no timers exist.
    /// Used by the reactor to shorten the poll timeout when a timer is due soon.
    ///
    /// Returns zero only when a `tick()` made now would fire that timer.
    pub fn time_until_next(&self) -> Option<Duration> {
        self.inner.lock().time_until_next()
    }

    /// Number of currently scheduled timers.
    pub fn pending_count(&self) -> usize {
        self.inner.lock().timer_count
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_schedule_once_fires() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        wheel.schedule_once(Duration::from_millis(5), move || {
            fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(wheel.pending_count(), 1);

        std::thread::sleep(Duration::from_millis(10));
        wheel.tick();

        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_cancel_timer() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        let id = wheel.schedule_once(Duration::from_millis(50), move || {
            fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        assert!(wheel.cancel(id));
        assert_eq!(wheel.pending_count(), 0);

        std::thread::sleep(Duration::from_millis(60));
        wheel.tick();

        assert_eq!(fired.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_cancel_returns_false_for_unknown() {
        let wheel = TimerWheel::new();
        assert!(!wheel.cancel(TimerId(999)));
    }

    #[test]
    fn test_repeating_timer() {
        let wheel = TimerWheel::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        wheel.schedule_repeating(Duration::from_millis(10), move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(15));
            wheel.tick();
        }

        let final_count = count.load(Ordering::SeqCst);
        assert!(
            final_count >= 3,
            "Expected at least 3 firings, got {final_count}"
        );
        assert_eq!(
            wheel.pending_count(),
            1,
            "Repeating timer should remain scheduled"
        );
    }

    #[test]
    fn test_cancel_repeating_timer() {
        let wheel = TimerWheel::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count_clone = count.clone();

        let id = wheel.schedule_repeating(Duration::from_millis(10), move || {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(15));
        wheel.tick();
        let after_first = count.load(Ordering::SeqCst);
        assert!(after_first >= 1);

        wheel.cancel(id);

        std::thread::sleep(Duration::from_millis(30));
        wheel.tick();
        assert_eq!(count.load(Ordering::SeqCst), after_first);
    }

    // Regression: a repeating timer that cancels itself from inside its own
    // callback must not be rescheduled. It is removed from `timers` while the
    // callback runs, so cancel has to recognize the in-flight id.
    #[test]
    fn test_repeating_timer_self_cancel_from_callback() {
        let wheel = Arc::new(TimerWheel::new());
        let count = Arc::new(AtomicUsize::new(0));
        let id_slot: Arc<Mutex<Option<TimerId>>> = Arc::new(Mutex::new(None));

        let w = wheel.clone();
        let c = count.clone();
        let slot = id_slot.clone();
        let id = wheel.schedule_repeating(Duration::from_millis(5), move || {
            c.fetch_add(1, Ordering::SeqCst);
            if let Some(tid) = *slot.lock() {
                w.cancel(tid);
            }
        });
        *id_slot.lock() = Some(id);

        std::thread::sleep(Duration::from_millis(10));
        wheel.tick(); // fires once; callback cancels itself
        std::thread::sleep(Duration::from_millis(10));
        wheel.tick(); // must not fire again

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(wheel.pending_count(), 0);
    }

    // Regression: after the earliest timer is cancelled (dirtying the cache),
    // inserting a *later* timer must not heal the cache to that later deadline
    // and hide a remaining earlier one. time_until_next must reflect the real
    // nearest deadline.
    #[test]
    fn test_insert_after_cancel_does_not_hide_earlier_timer() {
        let wheel = TimerWheel::new();

        let a = wheel.schedule_once(Duration::from_millis(100), || {});
        wheel.schedule_once(Duration::from_millis(200), || {}); // b, the real nearest after cancel

        wheel.cancel(a); // dirties the earliest-deadline cache
        wheel.schedule_once(Duration::from_millis(500), || {}); // c, later than b

        // Should report ~200ms (b), never ~500ms (c).
        let next = wheel.time_until_next().expect("timers pending");
        assert!(
            next < Duration::from_millis(350),
            "time_until_next returned {next:?}, expected < 350ms (the 200ms timer)"
        );
    }

    #[test]
    fn test_timer_does_not_fire_early() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        // Use a large deadline so no amount of CI load can cause an overshoot.
        wheel.schedule_once(Duration::from_secs(10), move || {
            fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 0);

        std::thread::sleep(Duration::from_millis(50));
        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        assert_eq!(wheel.pending_count(), 1);
    }

    #[test]
    fn test_many_timers() {
        let wheel = TimerWheel::new();
        let count = Arc::new(AtomicUsize::new(0));

        for i in 0..1000 {
            let count_clone = count.clone();
            wheel.schedule_once(Duration::from_millis(5 + (i % 20)), move || {
                count_clone.fetch_add(1, Ordering::SeqCst);
            });
        }

        assert_eq!(wheel.pending_count(), 1000);

        std::thread::sleep(Duration::from_millis(30));
        wheel.tick();

        assert_eq!(count.load(Ordering::SeqCst), 1000);
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_level_assignment() {
        let wheel = TimerWheel::new();
        let inner = wheel.inner.lock();

        assert_eq!(inner.level_for_delta(0), 0);
        assert_eq!(inner.level_for_delta(255), 0);
        assert_eq!(inner.level_for_delta(256), 1);
        assert_eq!(inner.level_for_delta(16383), 1);
        assert_eq!(inner.level_for_delta(16384), 2);
        assert_eq!(inner.level_for_delta(1048575), 2);
        assert_eq!(inner.level_for_delta(1048576), 3);
    }

    #[test]
    fn test_time_until_next() {
        let wheel = TimerWheel::new();
        assert!(wheel.time_until_next().is_none());

        wheel.schedule_once(Duration::from_millis(100), || {});
        let until = wheel.time_until_next().unwrap();
        assert!(until.as_millis() <= 100);
        assert!(until.as_millis() >= 90);
    }

    #[test]
    fn test_higher_level_timer_does_not_fire_early() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        // 10s is well into level 1 (>= 256ms) and impossible to overshoot.
        wheel.schedule_once(Duration::from_secs(10), move || {
            fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        assert_eq!(wheel.pending_count(), 1);
    }

    #[test]
    fn test_higher_level_timer_fires_after_deadline() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let fired_clone = fired.clone();

        // 300ms falls into level 1 (>= 256ms). Sleep well past the deadline.
        wheel.schedule_once(Duration::from_millis(300), move || {
            fired_clone.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(400));
        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_schedule_and_tick() {
        let wheel = Arc::new(TimerWheel::new());
        let count = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let wheel = wheel.clone();
            let count = count.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..250 {
                    let count = count.clone();
                    wheel.schedule_once(Duration::from_millis(5), move || {
                        count.fetch_add(1, Ordering::SeqCst);
                    });
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        std::thread::sleep(Duration::from_millis(20));
        wheel.tick();

        assert_eq!(count.load(Ordering::SeqCst), 1000);
    }

    #[test]
    fn test_zero_duration_fires_on_next_tick() {
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let f = fired.clone();

        wheel.schedule_once(Duration::ZERO, move || {
            f.fetch_add(1, Ordering::SeqCst);
        });

        // Even Duration::ZERO needs a tick() call to fire.
        assert_eq!(fired.load(Ordering::SeqCst), 0);

        std::thread::sleep(Duration::from_millis(1));
        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_tick_on_empty_wheel() {
        let wheel = TimerWheel::new();
        // Must not panic.
        wheel.tick();
        wheel.tick();
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_one_shot_fires_exactly_once() {
        let wheel = TimerWheel::new();
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        wheel.schedule_once(Duration::from_millis(5), move || {
            c.fetch_add(1, Ordering::SeqCst);
        });

        std::thread::sleep(Duration::from_millis(10));
        wheel.tick();
        wheel.tick();
        wheel.tick();

        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_cancel_after_fire_returns_false() {
        let wheel = TimerWheel::new();
        let id = wheel.schedule_once(Duration::from_millis(1), || {});

        std::thread::sleep(Duration::from_millis(5));
        wheel.tick();

        assert!(!wheel.cancel(id));
    }

    #[test]
    fn test_callback_schedules_new_timer() {
        // Proves the lock is not held during callback execution.
        let wheel = Arc::new(TimerWheel::new());
        let inner_fired = Arc::new(AtomicUsize::new(0));

        let w = wheel.clone();
        let f = inner_fired.clone();
        wheel.schedule_once(Duration::from_millis(1), move || {
            w.schedule_once(Duration::from_millis(1), move || {
                f.fetch_add(1, Ordering::SeqCst);
            });
        });

        std::thread::sleep(Duration::from_millis(5));
        wheel.tick();
        // The inner timer was just scheduled; it hasn't fired yet.
        assert_eq!(inner_fired.load(Ordering::SeqCst), 0);
        assert_eq!(wheel.pending_count(), 1);

        std::thread::sleep(Duration::from_millis(5));
        wheel.tick();
        assert_eq!(inner_fired.load(Ordering::SeqCst), 1);
        assert_eq!(wheel.pending_count(), 0);
    }

    #[test]
    fn test_callback_cancels_another_timer() {
        let wheel = Arc::new(TimerWheel::new());
        let victim_fired = Arc::new(AtomicUsize::new(0));
        let vf = victim_fired.clone();

        let victim_id = wheel.schedule_once(Duration::from_millis(50), move || {
            vf.fetch_add(1, Ordering::SeqCst);
        });

        let w = wheel.clone();
        wheel.schedule_once(Duration::from_millis(1), move || {
            w.cancel(victim_id);
        });

        std::thread::sleep(Duration::from_millis(5));
        wheel.tick();

        // Victim was cancelled by the first callback.
        std::thread::sleep(Duration::from_millis(60));
        wheel.tick();
        assert_eq!(victim_fired.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_time_until_next_returns_zero_for_overdue() {
        let wheel = TimerWheel::new();
        wheel.schedule_once(Duration::from_millis(1), || {});

        std::thread::sleep(Duration::from_millis(5));

        let until = wheel.time_until_next().unwrap();
        assert_eq!(until, Duration::ZERO);
    }

    #[test]
    fn test_mixed_oneshot_and_repeating() {
        let wheel = TimerWheel::new();
        let once_count = Arc::new(AtomicUsize::new(0));
        let repeat_count = Arc::new(AtomicUsize::new(0));

        let oc = once_count.clone();
        wheel.schedule_once(Duration::from_millis(5), move || {
            oc.fetch_add(1, Ordering::SeqCst);
        });

        let rc = repeat_count.clone();
        wheel.schedule_repeating(Duration::from_millis(10), move || {
            rc.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(wheel.pending_count(), 2);

        // After enough time, the one-shot fires once and the repeater fires multiple times.
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(15));
            wheel.tick();
        }

        assert_eq!(once_count.load(Ordering::SeqCst), 1);
        assert!(repeat_count.load(Ordering::SeqCst) >= 3);
        // One-shot is gone, repeater remains.
        assert_eq!(wheel.pending_count(), 1);
    }

    #[test]
    fn test_overflow_timer() {
        // A timer beyond the wheel's ~18hr range goes into overflow.
        let wheel = TimerWheel::new();
        let fired = Arc::new(AtomicUsize::new(0));
        let f = fired.clone();

        // MAX_RANGE_TICKS = 2^26 = 67,108,864 ms. Schedule beyond that.
        wheel.schedule_once(Duration::from_millis(MAX_RANGE_TICKS + 1000), move || {
            f.fetch_add(1, Ordering::SeqCst);
        });

        assert_eq!(wheel.pending_count(), 1);

        // Ticking now should not fire it.
        wheel.tick();
        assert_eq!(fired.load(Ordering::SeqCst), 0);
        assert_eq!(wheel.pending_count(), 1);
    }

    #[test]
    fn test_pending_count_tracks_lifecycle() {
        let wheel = TimerWheel::new();
        assert_eq!(wheel.pending_count(), 0);

        let id1 = wheel.schedule_once(Duration::from_millis(5), || {});
        assert_eq!(wheel.pending_count(), 1);

        let id2 = wheel.schedule_once(Duration::from_millis(5), || {});
        assert_eq!(wheel.pending_count(), 2);

        wheel.cancel(id1);
        assert_eq!(wheel.pending_count(), 1);

        std::thread::sleep(Duration::from_millis(10));
        wheel.tick();
        assert_eq!(wheel.pending_count(), 0);

        // Cancelling already-fired timer doesn't affect count.
        wheel.cancel(id2);
        assert_eq!(wheel.pending_count(), 0);
    }
}
