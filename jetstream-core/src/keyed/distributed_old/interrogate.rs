use std::{rc::Rc, sync::Mutex};

use indexmap::{IndexMap, IndexSet};

use crate::{keyed::WorkerPartitioner, DataMessage, Key, RescaleMessage, ShutdownMarker, WorkerId};

use super::{
    collect::CollectDistributor,
    dist_trait::DistributorKind,
    messages::{Interrogate, LocalOutgoingMessage, OutgoingMessage, VersionedMessage},
    Version,
};

#[derive(Debug)]
pub(super) struct InterrogateDistributor<K> {
    worker_id: WorkerId,
    whitelist: Rc<Mutex<IndexSet<K>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    running_interrogate: Option<Interrogate<K>>,
}

impl<K> InterrogateDistributor<K>
where
    K: Key,
{
    pub(super) fn new(
        worker_id: WorkerId,
        worker_set: IndexSet<WorkerId>,
        version: Version,
        trigger: RescaleMessage,
    ) -> Self {
        let old_worker_set = worker_set;
        let new_worker_set: IndexSet<WorkerId> = match trigger {
            RescaleMessage::ScaleRemoveWorker(x) => {
                old_worker_set.difference(&x).map(|y| y.clone()).collect()
            }
            RescaleMessage::ScaleAddWorker(x) => {
                old_worker_set.union(&x).map(|y| y.clone()).collect()
            }
        };
        Self {
            worker_id,
            whitelist: Rc::new(Mutex::new(IndexSet::new())),
            old_worker_set,
            new_worker_set,
            version: version,
            running_interrogate: None,
        }
    }

    pub(super) fn handle_msg<V, T>(
        &self,
        msg: DataMessage<K, V, T>,
        msg_version: Option<Version>,
        partitioner: &dyn WorkerPartitioner<K>,
    ) -> OutgoingMessage<K, V, T> {
        if msg_version.map(|v| v > self.version).unwrap_or(false) {
            return OutgoingMessage::Local(LocalOutgoingMessage::Data(msg));
        }
        let old_target = partitioner(&msg.key, &self.old_worker_set);
        let new_target = partitioner(&msg.key, &self.new_worker_set);

        match (*old_target == self.worker_id, *new_target == self.worker_id) {
            (true, true) => OutgoingMessage::Local(LocalOutgoingMessage::Data(msg)),
            (true, false) => {
                self.whitelist.lock().unwrap().insert(msg.key.clone());
                OutgoingMessage::Local(LocalOutgoingMessage::Data(msg))
            }
            (false, _) => OutgoingMessage::Remote(*new_target,
                VersionedMessage::new( self.version.clone(), msg),
            ),
        }
    }

    pub(super) fn try_into_collect<V, T>(self) -> Result<CollectDistributor<K, V, T>, Self> {
        if let Some(interrogate) = self.running_interrogate {
            match interrogate.try_unwrap() {
                Ok(whitelist) => {
                    // interrogate is done
                    let collector = CollectDistributor::new(
                        self.worker_id,
                        whitelist,
                        self.old_worker_set,
                        self.new_worker_set,
                        self.version,
                    );
                    Ok(collector)
                }
                // still running
                Err(e) => Err(Self {
                    running_interrogate: Some(e),
                    ..self
                }),
            }
        } else {
            Err(self)
        }
    }

    pub(super) fn run<V, T>(
        &mut self,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> OutgoingMessage<K, V, T> {
        match &self.running_interrogate {
            Some(_) => OutgoingMessage::None,
            // no interrogate running, so start a new one
            None => {
                // returns true if the key does not require redistribution
                let worker_set = self.new_worker_set.clone();
                let wid = self.worker_id.clone();
                let tester = move |k: &_| *partitioner(k, &worker_set) != wid;
                let interrogate = Interrogate::new(self.whitelist.clone(), Rc::new(tester));

                self.running_interrogate = Some(interrogate.clone());
                OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(interrogate))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        keyed::distributed::messages::{DoneMessage, RemoteMessage, VersionedMessage},
        time::NoTime,
        DataMessage, NoData,
    };

    use super::*;

    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    fn make_upscale_distributor<K: Key>() -> InterrogateDistributor<K> {
        InterrogateDistributor::new(
            0,
            IndexSet::from([0]),
            0,
            RescaleMessage::ScaleAddWorker(IndexSet::from([1])),
        )
    }

    /// It should create the new worker sets correctly on up and downscales
    #[test]
    fn create_new_worker_set() {
        let dist: InterrogateDistributor<usize> = InterrogateDistributor::new(
            0,
            IndexSet::from([0]),
            0,
            RescaleMessage::ScaleAddWorker(IndexSet::from([1])),
        );
        assert_eq!(dist.new_worker_set, IndexSet::from([0, 1]));

        let dist: InterrogateDistributor<usize> = InterrogateDistributor::new(
            0,
            IndexSet::from([0, 1]),
            0,
            RescaleMessage::ScaleRemoveWorker(IndexSet::from([1])),
        );
        assert_eq!(dist.new_worker_set, IndexSet::from([0]));
    }

    #[test]
    /// Should send an interrogate message on the first time, that run is called
    fn sends_first_interrogate() {
        let mut dist = make_upscale_distributor::<usize>();
        let result: OutgoingMessage<usize, NoData, NoTime> = dist.run(Rc::new(partiton_index));
        match result {
            OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(_)) => (),
            _ => panic!("Wrong answer buddy"),
        }
    }

    #[test]
    /// Should not do anything if, the interrogate is still underway
    fn noop_if_interrogate_is_running() {
        let mut dist = make_upscale_distributor::<usize>();
        let _msg: OutgoingMessage<usize, NoData, NoTime> = dist.run(Rc::new(partiton_index));

        // nothing should happen when we run while holding onto the interrogator
        let result: OutgoingMessage<usize, NoData, NoTime> = dist.run(Rc::new(partiton_index));
        match result {
            OutgoingMessage::None => (),
            _ => panic!("Wrong message returned"),
        }
    }

    #[test]
    /// Should create the collect distributor when the interrogate is done
    fn creates_collector() {
        let mut dist = make_upscale_distributor::<usize>();
        let interrogate: OutgoingMessage<usize, NoData, NoTime> = dist.run(Rc::new(partiton_index));
        let mut msg = match interrogate {
            OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(x)) => x,
            _ => panic!("Wrong message"),
        };
        msg.add_keys(&[42, 77]);
        drop(msg);

        // since there is no reference to the interrogate we should
        // now get a collector
        let dist = dist.try_into_collect::<NoData, i32>();
        assert!(matches!(dist, Ok(CollectDistributor)));
    }

    #[test]
    /// Handle Rule 1.1
    /// • Rule 1.1: If (F(K) == Local) && (F'(K) != Local)
    /// • add the key K to the set whitelist
    /// • pass the message downstream
    fn handle_data_rule_1_1() {
        let dist = make_upscale_distributor::<usize>();
        let msg = DataMessage {
            key: 1,
            value: 42,
            timestamp: NoTime,
        };
        let out = dist.handle_msg(msg, None, &partiton_index);
        assert!(matches!(
            out,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(_))
        ));
        assert!(dist.whitelist.lock().unwrap().contains(&1));
    }

    #[test]
    /// Handle Rule 1.2
    /// • Rule 1.2: If (F(K) == Local) && (F'(K) == Local)
    /// • pass the message downstream
    fn handle_data_rule_1_2() {
        let dist = make_upscale_distributor::<usize>();
        let msg = DataMessage {
            key: 0,
            value: 42,
            timestamp: NoTime,
        };
        let out = dist.handle_msg(msg, None, &partiton_index);
        assert!(matches!(
            out,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(_))
        ));
    }

    #[test]
    /// Handle Rule 2
    /// • Rule 2:  If (F(K) != Local)
    /// • send the message to the worker determined by F
    fn handle_data_rule_2() {
        let dist = make_upscale_distributor::<usize>();
        let msg = DataMessage {
            key: 0,
            value: 42,
            timestamp: NoTime,
        };
        let out = dist.handle_msg(msg, None, &|_x, _y| &1);
        assert!(matches!(
            out,
            OutgoingMessage::Remote(1, VersionedMessage {
                version: 1,
                message: _
            })
        ));
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let dist = make_upscale_distributor::<usize>();

        let input = DataMessage {
            key: 1,
            value: 42,
            timestamp: NoTime,
        };
        let result = dist.handle_msg(input, Some(1), &partiton_index);
        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime
            }))
        ));
    }

    /// Should not add keys to whitelist if they do not need to be redistributed
    #[test]
    fn only_collects_necessary_keys() {
        let mut dist = make_upscale_distributor::<usize>();
        let interrogate: OutgoingMessage<usize, NoData, NoTime> = dist.run(Rc::new(partiton_index));
        let mut msg = match interrogate {
            OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(m)) => m,
            _ => panic!("Wrong message"),
        };

        msg.add_keys(&[0, 1, 24, 77]);
        assert_eq!(
            dist.whitelist.lock().unwrap().clone(),
            IndexSet::from([1, 77])
        );
    }
}
