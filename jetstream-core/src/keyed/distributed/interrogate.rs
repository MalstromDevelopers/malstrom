use std::{rc::Rc, sync::Mutex};

use indexmap::{IndexMap, IndexSet};

use crate::{keyed::WorkerPartitioner, DataMessage, Key, ShutdownMarker, WorkerId};

use super::{
    collect::CollectDistributor,
    dist_trait::{Distributor, DistributorKind},
    messages::{
        IncomingMessage, Interrogate, LocalOutgoingMessage, OutgoingMessage, RescaleMessage,
        TargetedMessage,
    },
    Version,
};

#[derive(Debug)]
pub(super) struct InterrogateDistributor<K> {
    worker_id: WorkerId,
    whitelist: Rc<Mutex<IndexSet<K>>>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    remote_versions: IndexMap<WorkerId, Version>,
    // if we receive another scale instruction during the interrogation,
    // there is no real good way for us to handle that, so we will
    // queue it up to be handled after collect is done
    queued_rescales: Vec<RescaleMessage>,
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
        remote_versions: IndexMap<WorkerId, Version>,
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
            remote_versions,
            queued_rescales: Vec::new(),
            running_interrogate: None,
        }
    }

    fn handle_data<V, T>(
        &mut self,
        msg: DataMessage<K, V, T>,
        version: Option<Version>,
        partitioner: impl WorkerPartitioner<K>,
    ) -> OutgoingMessage<K, V, T> {
        if let Some(v) = version {
            if v > self.version {
                return OutgoingMessage::Local(LocalOutgoingMessage::Data(msg));
            }
        }
        let old_target = partitioner(&msg.key, &self.old_worker_set);
        let new_target = partitioner(&msg.key, &self.new_worker_set);

        match (*old_target == self.worker_id, *new_target == self.worker_id) {
            (true, true) => OutgoingMessage::Local(LocalOutgoingMessage::Data(msg)),
            (true, false) => {
                self.whitelist.lock().unwrap().insert(msg.key.clone());
                OutgoingMessage::Local(LocalOutgoingMessage::Data(msg))
            }
            (false, _) => OutgoingMessage::Remote(super::messages::RemoteMessage::Data(
                TargetedMessage::new(*new_target, self.version.clone(), msg),
            )),
        }
    }

    fn handle_msg<V, T>(
        mut self,
        msg: IncomingMessage<K, V, T>,
        partitioner: impl WorkerPartitioner<K>,
    ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>) {
        match msg {
            IncomingMessage::Data(msg, version) => {
                let out = self.handle_data(msg, version, partitioner);
                (DistributorKind::<K, V, T>::Interrogate(self), out)
            }
            IncomingMessage::Rescale(trigger) => {
                self.queued_rescales.push(trigger);
                (
                    DistributorKind::<K, V, T>::Interrogate(self),
                    OutgoingMessage::None,
                )
            }
            IncomingMessage::Done(d) => {
                self.remote_versions.insert(d.worker_id, d.version);
                (
                    DistributorKind::<K, V, T>::Interrogate(self),
                    OutgoingMessage::None,
                )
            }
        }
    }

    fn run<V, T>(
        mut self,
        partitioner: impl WorkerPartitioner<K>
    ) -> (DistributorKind<K, V, T>, OutgoingMessage<K, V, T>) {
        match self.running_interrogate {
            Some(x) => {
                self.running_interrogate = match x.try_unwrap() {
                    Ok(whitelist) => {
                        // interrogate is done
                        let collector = CollectDistributor::new(
                            self.worker_id,
                            whitelist,
                            self.old_worker_set,
                            self.new_worker_set,
                            self.version,
                            self.remote_versions,
                            self.queued_rescales,
                        );

                        return (DistributorKind::Collect(collector), OutgoingMessage::None);
                    }
                    // still running
                    Err(e) => Some(e),
                };
                (DistributorKind::Interrogate(self), OutgoingMessage::None)
            }
            // no interrogate running, so start a new one
            None => {
                // returns true if the key does not require redistribution
                let worker_set = self.new_worker_set.clone();
                let wid = self.worker_id.clone();
                let tester = move |k: &_| *partitioner(k, &worker_set) != wid;
                let interrogate = Interrogate::new(self.whitelist.clone(), tester);

                self.running_interrogate = Some(interrogate.clone());
                (
                    DistributorKind::Interrogate(self),
                    OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(interrogate)),
                )
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        keyed::distributed::messages::{DoneMessage, RemoteMessage, TargetedMessage},
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
            IndexMap::new(),
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
            IndexMap::new(),
            RescaleMessage::ScaleAddWorker(IndexSet::from([1])),
        );
        assert_eq!(dist.new_worker_set, IndexSet::from([0, 1]));

        let dist: InterrogateDistributor<usize> = InterrogateDistributor::new(
            0,
            IndexSet::from([0, 1]),
            0,
            IndexMap::new(),
            RescaleMessage::ScaleRemoveWorker(IndexSet::from([1])),
        );
        assert_eq!(dist.new_worker_set, IndexSet::from([0]));
    }

    #[test]
    /// Should send an interrogate message on the first time, that run is called
    fn sends_first_interrogate() {
        let dist = make_upscale_distributor::<usize>();
        let result: (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);
        match result {
            (
                DistributorKind::Interrogate(_),
                OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(_)),
            ) => (),
            _ => panic!("Wrong answer buddy"),
        }
    }

    #[test]
    /// Should not do anything if, the interrogate is still underway
    fn noop_if_interrogate_is_running() {
        let dist = make_upscale_distributor::<usize>();
        let (dist, _msg): (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);

        let dist = match dist {
            DistributorKind::Interrogate(x) => x,
            _ => panic!(),
        };

        // nothing should happen when we run while holding onto the interrogator
        let result: (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);
        match result.1 {
            OutgoingMessage::None => (),
            _ => panic!("Wrong message returned"),
        }
    }

    #[test]
    /// Should create the collect distributor when the interrogate is done
    fn creates_collector() {
        let dist = make_upscale_distributor::<usize>();
        let (dist, interrogate): (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);
        let mut msg = match interrogate {
            OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(x)) => x,
            _ => panic!("Wrong message"),
        };
        msg.add_keys(&[42, 77]);
        drop(msg);

        let dist = match dist {
            DistributorKind::Interrogate(x) => x,
            _ => panic!("Wrong distributor"),
        };

        // since there is no reference to the interrogate we should
        // now get a collector
        let (dist, interrogate): (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);

        match (dist, interrogate) {
            (DistributorKind::Collect(_), OutgoingMessage::None) => (),
            _ => panic!("Did not get a a collector"),
        }
    }

    #[test]
    /// Handle Rule 1.1
    /// • Rule 1.1: If (F(K) == Local) && (F'(K) != Local)
    /// • add the key K to the set whitelist
    /// • pass the message downstream
    fn handle_data_rule_1_1() {
        let dist = make_upscale_distributor::<usize>();
        let msg: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let (dist, out) = dist.handle_msg(msg, partiton_index);
        let dist = match dist {
            DistributorKind::Interrogate(x) => x,
            _ => panic!(),
        };
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
        let msg: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let (dist, out) = dist.handle_msg(msg, partiton_index);
        let _dist = match dist {
            DistributorKind::Interrogate(x) => x,
            _ => panic!(),
        };
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
        let msg: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
            crate::DataMessage {
                key: 0,
                value: 42,
                timestamp: NoTime,
            },
            None,
        );
        let (dist, out) = dist.handle_msg(msg, |_x, _y| &1);
        let _dist = match dist {
            DistributorKind::Interrogate(x) => x,
            _ => panic!(),
        };
        assert!(matches!(
            out,
            OutgoingMessage::Remote(RemoteMessage::Data(TargetedMessage {
                target: 1,
                version: 1,
                message: _
            }))
        ));
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let dist = make_upscale_distributor::<usize>();

        let input: IncomingMessage<usize, i32, NoTime> = IncomingMessage::Data(
            crate::DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime,
            },
            Some(1),
        );
        let (new_dist, result) = dist.handle_msg(input, partiton_index);
        match new_dist {
            DistributorKind::Interrogate(x) => x,
            x => panic!("Wrong distributor returned {x:?}"),
        };

        assert!(matches!(
            result,
            OutgoingMessage::Local(LocalOutgoingMessage::Data(DataMessage {
                key: 1,
                value: 42,
                timestamp: NoTime
            }))
        ));
    }

    #[test]
    /// Should update the corresponding worker version in its map
    fn handle_done_message() {
        let dist = make_upscale_distributor::<usize>();
        let input: IncomingMessage<usize, NoData, usize> =
            IncomingMessage::Done(DoneMessage::new(1, 42));
        // since there are no other
        let (dist, _) = dist.handle_msg(input, partiton_index);
        match dist {
            DistributorKind::Interrogate(d) => assert_eq!(d.remote_versions.get(&1).unwrap(), &42),
            _ => panic!("Wrong distributor returned"),
        }
    }

    /// Should not add keys to whitelist if they do not need to be redistributed
    #[test]
    fn only_collects_necessary_keys() {
        let dist = make_upscale_distributor::<usize>();
        let (dist, interrogate): (
            DistributorKind<usize, NoData, NoTime>,
            OutgoingMessage<usize, NoData, NoTime>,
        ) = dist.run(partiton_index);
        let (dist, mut msg) = match (dist, interrogate) {
            (
                DistributorKind::Interrogate(d),
                OutgoingMessage::Local(LocalOutgoingMessage::Interrogate(m)),
            ) => (d, m),
            _ => panic!("Wrong message"),
        };

        msg.add_keys(&[0, 1, 24, 77]);
        assert_eq!(
            dist.whitelist.lock().unwrap().clone(),
            IndexSet::from([1, 77])
        );
    }
}
