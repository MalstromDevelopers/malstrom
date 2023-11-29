use thiserror::Error;

use crate::channels::watch;

#[derive(Error, Debug)]
pub enum FrontierError {
    #[error("Attempted to set a desired time lower than actual")]
    DesiredLessThanActual,
}

#[derive(Clone, Debug)]
pub struct Probe {
    inner: watch::Receiver<u64>,
}

impl Probe {
    pub fn new(recv: watch::Receiver<u64>) -> Self {
        Self { inner: recv }
    }

    pub fn read(&self) -> u64 {
        self.inner.read()
    }
}

#[derive(Clone, Debug)]
pub struct Frontier {
    desired: u64,
    actual: watch::Sender<u64>,
    upstreams: Vec<Probe>,
}

impl Frontier {
    pub fn add_upstream_probe(&mut self, probe: Probe) -> () {
        self.upstreams.push(probe)
    }

    pub fn get_actual(&self) -> u64 {
        self.actual.read()
    }

    pub fn advance_to(&mut self, desired: u64) -> Result<(), FrontierError> {
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
        let (tx, _) = watch::channel(0);
        Self {
            desired: 0,
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

    pub fn advance_to(&mut self, desired: u64) -> Result<(), FrontierError> {
        self.frontier.advance_to(desired)
    }
    pub fn get_actual(&self) -> u64 {
        self.frontier.get_actual()
    }

    /// Get the minimum frontier of the upstream operators
    /// This is essentially the same as asking "What is the oldest
    /// message I still need to be able to handle"
    ///
    /// If there are no upstreams, this will be u64::MAX
    pub fn get_upstream_actual(&self) -> u64 {
        self.frontier
            .upstreams
            .iter()
            .map(|x| x.read())
            .min()
            .unwrap_or(u64::MAX)
    }
}
