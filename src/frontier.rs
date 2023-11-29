use bincode::{Decode, Encode};
use thiserror::Error;

use crate::channels::watch;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    pub fn new(val: u64) -> Self {
        Self(val)
    }
}

impl Into<u64> for Timestamp {
    fn into(self) -> u64 {
        self.0
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
    pub fn add_upstream_probe(&mut self, probe: Probe) -> () {
        self.upstreams.push(probe)
    }

    pub fn get_actual(&self) -> Timestamp {
        self.actual.read()
    }

    pub fn advance_to(&mut self, desired: Timestamp) -> Result<(), FrontierError> {
        match desired < self.actual.read() {
            true => Err(FrontierError::DesiredLessThanActual),
            false => {
                self.desired = desired;
                self.update();
                Ok(())
            }
        }
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

    pub fn advance_to(&mut self, desired: Timestamp) -> Result<(), FrontierError> {
        self.frontier.advance_to(desired)
    }
    pub fn get_actual(&self) -> Timestamp {
        self.frontier.get_actual()
    }

    /// Get the minimum frontier of the upstream operators
    /// This is essentially the same as asking "What is the oldest
    /// message I still need to be able to handle"
    ///
    /// If there are no upstreams, this will be Timestamp::MAX
    pub fn get_upstream_actual(&self) -> Timestamp {
        self.frontier
            .upstreams
            .iter()
            .map(|x| x.read())
            .min()
            .unwrap_or(Timestamp::MAX)
    }
}
