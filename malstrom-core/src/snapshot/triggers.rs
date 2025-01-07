use std::time::{Duration, Instant};

pub trait SnapshotTrigger: 'static {
    fn should_trigger(&mut self) -> bool;
}
impl<F> SnapshotTrigger for F
where
    F: FnMut() -> bool + 'static,
{
    fn should_trigger(&mut self) -> bool {
        self()
    }
}

/// A simple trigger which fires at constant interval based on system time
pub struct IntervalSnapshots {
    interval: Duration,
    last_trigger: Instant,
}

impl IntervalSnapshots {
    /// Create a new trigger of the given interval
    pub fn new(interval: Duration) -> Self {
        IntervalSnapshots {
            interval,
            last_trigger: Instant::now(),
        }
    }
}
impl SnapshotTrigger for IntervalSnapshots {
    fn should_trigger(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_trigger) > self.interval {
            self.last_trigger = now;
            true
        } else {
            false
        }
    }
}

pub struct NoSnapshots;
impl SnapshotTrigger for NoSnapshots {
    #[inline(always)]
    fn should_trigger(&mut self) -> bool {
        false
    }
}
