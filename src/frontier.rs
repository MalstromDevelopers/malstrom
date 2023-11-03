use thiserror::Error;

use crate::watch;

#[derive(Error, Debug)]
pub enum FrontierError {
    #[error("Attempted to set a desired time lower than actual")]
    DesiredLessThanActual,
    #[error("Can not advance as others actual frontiers are larger")]
    CanNotAdvance
}

#[derive(Clone)]
pub struct Probe {
    inner: crate::watch::Receiver<u64>
}

impl Probe {
    pub fn new(recv: crate::watch::Receiver<u64>) -> Self {
        Self { inner: recv }
    }

    pub fn read(&self) -> u64 {
        self.inner.read()
    }
}

#[derive(Clone, Debug)]
pub struct Frontier {
    desired: u64,
    actual: watch::Sender<u64>
}

impl Frontier {

    pub fn get_actual(&self) -> u64 {
        self.actual.read()
    }

    pub fn set_desired(&mut self, desired: u64) -> () {
        self.desired = desired
    }

    pub fn try_fulfill(&mut self, others: &Vec<u64>) -> () {
        let upstream_min = others.iter().min();
        let desired = &self.desired;
        println!("Trying to fullfills {desired} {upstream_min:?}");

        match upstream_min {
            Some(m) => self.actual.send(*m.min(&self.desired)),
            None => self.actual.send(self.desired)
        };
    }

    pub fn get_probe(&self) -> Probe {
        Probe::new(self.actual.subscribe())
    }
}

impl Default for Frontier {
    
    fn default() -> Self {
        let (tx, _) = watch::channel(0);
        Self { desired: 0, actual: tx }
    }
}

#[derive(Debug)]
pub struct FrontierHandle<'g> {
    frontier: &'g mut Frontier,
}

impl <'g>FrontierHandle<'g> {

    pub fn new(frontier: &'g mut Frontier)  -> Self {
        FrontierHandle { frontier }
    }

    pub fn advance_to(&mut self, desired: u64) -> Result<(), FrontierError> {
        match desired < self.frontier.actual.read() {
           true => Err(FrontierError::DesiredLessThanActual),
           false => {self.frontier.desired = desired; Ok(())}
        }
    }
    pub fn advance_by(&mut self, desired: u64) -> Result<(), FrontierError> {
        let new_desired = self.frontier.desired + desired;
        match new_desired < self.frontier.actual.read() {
           true => Err(FrontierError::DesiredLessThanActual),
           false => {self.frontier.desired = new_desired; Ok(())}
        }
    }

    pub fn get_actual(&self) -> u64 {
        self.frontier.actual.read()
    }

}