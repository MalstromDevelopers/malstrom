use std::{hash::Hash, marker::PhantomData, rc::Rc, sync::Mutex};

use bincode::config::Configuration;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    snapshot::PersistenceBackend,
    stream::operator::OperatorContext,
    Data, DataMessage, Key, Message, OperatorId, WorkerId,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::distributed::{
    DistData, DistKey, NetworkMessage, PhaseDistributor, ScalableMessage, Version,
};
