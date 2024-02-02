use std::collections::{HashMap, HashSet};

use crate::{OperatorId, WorkerId};

pub mod selective_broadcast;
pub mod watch;

#[derive(Clone)]
pub struct ShutdownAck<K> {
    worker_id: WorkerId,
    whitelist: HashSet<K>,
    hold: HashSet<K>
}

#[derive(Clone)]
pub struct Redistribute<K> {
    key: K,
    key_state: HashMap<OperatorId, Vec<u8>>,
    next: Option<K>
}

#[derive(Clone)]
pub struct Acquire<K> {
    key: K,
    key_state: HashMap<OperatorId, Vec<u8>>,
}


#[derive(Clone)]
pub enum Message<K, T, P> {
    /// normal data
    Data(T),
    // ABS Barrier
    Barrier(P),
    /// Snapshot Load
    Load(P),
    /// Signals worker to begin shutdown
    Shutdown(WorkerId),
    /// Worker Shutdown Acknowledgement
    ShutdownAck(ShutdownAck<K>),
    /// Signals last message for key (used in shutdown procedure)
    Final(K),
    /// Shutdown procedure packaged state
    Redistribute(Redistribute<K>),
    /// Contains state to be acquired by another worker
    Acquire(Acquire<K>)
}
