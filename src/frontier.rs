use std::ops::Add;

use crate::channels::watch;
use thiserror::Error;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    pub fn new(val: u64) -> Self {
        Self(val)
    }
}

impl From<Timestamp> for u64 {
    fn from(val: Timestamp) -> Self {
        val.0
    }
}
impl From<u64> for Timestamp {
    fn from(num: u64) -> Self {
        Self(num)
    }
}

impl Add for Timestamp {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

#[derive(Error, Debug)]
pub enum FrontierError {
    #[error("Attempted to set a desired time lower than actual")]
    DesiredLessThanActual,
}

#[derive(Clone, Debug)]
pub struct Probe {
    inner: watch::Receiver<Timestamp>,
}

impl Probe {
    pub fn new(recv: watch::Receiver<Timestamp>) -> Self {
        Self { inner: recv }
    }

    pub fn read(&self) -> Timestamp {
        self.inner.read()
    }
}

#[derive(Clone, Debug)]
pub struct Frontier {
    desired: Timestamp,
    actual: watch::Sender<Timestamp>,
    upstreams: Vec<Probe>,
}

impl Frontier {
    pub fn add_upstream_probe(&mut self, probe: Probe) {
        self.upstreams.push(probe)
    }

    pub fn get_actual(&self) -> Timestamp {
        self.actual.read()
    }

    pub fn advance_to(&mut self, desired: Timestamp) {
        self.desired = desired;
        self.update();
    }
    fn update(&mut self) {
        match self.upstreams.iter().map(|x| x.read()).min() {
            Some(m) => self.actual.write(m.min(self.desired)),
            None => self.actual.write(self.desired),
        }
    }

    pub fn get_probe(&self) -> Probe {
        Probe::new(self.actual.subscribe())
    }
}

impl Default for Frontier {
    fn default() -> Self {
        let (tx, _) = watch::channel(Timestamp::default());
        Self {
            desired: Timestamp::default(),
            actual: tx,
            upstreams: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct FrontierHandle<'g> {
    frontier: &'g mut Frontier,
}

impl<'g> FrontierHandle<'g> {
    pub fn new(frontier: &'g mut Frontier) -> Self {
        FrontierHandle { frontier }
    }

    pub fn advance_to(&mut self, desired: Timestamp) {
        self.frontier.advance_to(desired)
    }
    pub fn get_actual(&self) -> Timestamp {
        self.frontier.get_actual()
    }
}
