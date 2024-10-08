use std::{collections::VecDeque, marker::PhantomData, rc::Rc};

use indexmap::IndexSet;

use crate::{
    channels::selective_broadcast::{Receiver, Sender},
    keyed::types::{DistKey, WorkerPartitioner},
    snapshot::Barrier,
    stream::OperatorContext,
    types::{DataMessage, MaybeData, Message, RescaleMessage, ShutdownMarker, Timestamp, WorkerId},
};

use super::{
    collect_dist::CollectDistributor,
    icadd_operator::{DistributorKind, TargetedMessage},
    versioner::VersionedMessage,
    Interrogate, Version,
};

#[derive(Debug)]
pub(super) struct InterrogateDistributor<K, V, T> {
    worker_id: WorkerId,
    whitelist: IndexSet<K>,
    old_worker_set: IndexSet<WorkerId>,
    new_worker_set: IndexSet<WorkerId>,
    version: Version,
    running_interrogate: Option<Interrogate<K>>,
    phantom: PhantomData<(V, T)>,
    // these are control messages we can not handle while rescaling
    // so we will buffer them, waiting for the normal dist to deal with them
    barrier: Option<Barrier>,
    // barrier comes first, then other stuff
    queued_rescales: VecDeque<RescaleMessage>,
    shutdown_marker: Option<ShutdownMarker>,
}

impl<K, V, T> InterrogateDistributor<K, V, T>
where
    K: DistKey,
    V: MaybeData,
    T: Timestamp,
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
                old_worker_set.difference(&x).copied().collect()
            }
            RescaleMessage::ScaleAddWorker(x) => old_worker_set.union(&x).copied().collect(),
        };
        Self {
            worker_id,
            whitelist: IndexSet::new(),
            old_worker_set,
            new_worker_set,
            version,
            running_interrogate: None,
            phantom: PhantomData,
            barrier: None,
            queued_rescales: VecDeque::new(),
            shutdown_marker: None,
        }
    }

    pub(super) fn new_from_collect(
        worker_id: WorkerId,
        worker_set: IndexSet<WorkerId>,
        version: Version,
        trigger: RescaleMessage,
        queued_rescales: VecDeque<RescaleMessage>,
        shutdown_marker: Option<ShutdownMarker>,
    ) -> Self {
        let mut this = Self::new(worker_id, worker_set, version, trigger);
        this.queued_rescales = queued_rescales;
        this.shutdown_marker = shutdown_marker;
        this
    }

    pub(super) fn handle_data(
        &mut self,
        msg: DataMessage<K, VersionedMessage<V>, T>,
        partitioner: &dyn WorkerPartitioner<K>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
    ) {
        if msg.value.version > self.version {
            output.send(Message::Data(DataMessage::new(
                msg.key,
                TargetedMessage::new(msg.value, None),
                msg.timestamp,
            )));
            return;
        }
        let old_target = partitioner(&msg.key, &self.old_worker_set);
        let new_target = partitioner(&msg.key, &self.new_worker_set);

        let vmsg = VersionedMessage {
            inner: msg.value.inner,
            version: self.version,
            sender: self.worker_id,
        };
        match (*old_target == self.worker_id, *new_target == self.worker_id) {
            // send locally
            (true, true) => output.send(Message::Data(DataMessage::new(
                msg.key,
                TargetedMessage {
                    inner: vmsg,
                    target: None,
                },
                msg.timestamp,
            ))),
            (true, false) => {
                self.whitelist.insert(msg.key.clone());
                // send locally
                output.send(Message::Data(DataMessage::new(
                    msg.key,
                    TargetedMessage {
                        inner: vmsg,
                        target: None,
                    },
                    msg.timestamp,
                )));
            }
            (false, _) => {
                // send to new target
                output.send(Message::Data(DataMessage::new(
                    msg.key,
                    TargetedMessage::new(vmsg, Some(*old_target)),
                    msg.timestamp,
                )));
            }
        }
    }

    #[allow(clippy::result_large_err)]
    pub(super) fn try_into_collect(
        self,
        ctx: &mut OperatorContext,
    ) -> Result<CollectDistributor<K, V, T>, Self> {
        if let Some(interrogate) = self.running_interrogate {
            match interrogate.try_unwrap() {
                Ok(whitelist) => {
                    // interrogate is done
                    let collector = CollectDistributor::new(
                        whitelist.union(&self.whitelist).cloned().collect(),
                        self.old_worker_set,
                        self.new_worker_set,
                        self.version,
                        ctx,
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

    pub(super) fn run(
        mut self,
        input: &mut Receiver<K, VersionedMessage<V>, T>,
        output: &mut Sender<K, TargetedMessage<V>, T>,
        ctx: &mut OperatorContext,
        partitioner: Rc<dyn WorkerPartitioner<K>>,
    ) -> DistributorKind<K, V, T> {
        match &self.running_interrogate {
            Some(_) => (),
            // no interrogate running, so start a new one
            None => {
                // returns true if the key does not require redistribution
                let worker_set = self.new_worker_set.clone();
                let wid = self.worker_id;

                let pclone = partitioner.clone();
                let tester = move |k: &_| *pclone(k, &worker_set) != wid;
                let interrogate = Interrogate::new(Rc::new(tester));
                self.running_interrogate = Some(interrogate.clone());
                output.send(Message::Interrogate(interrogate));
            }
        };
        if self.barrier.is_none() {
            if let Some(msg) = input.recv() {
                match msg {
                    Message::Data(d) => self.handle_data(d, partitioner.as_ref(), output),
                    Message::Epoch(e) => output.send(Message::Epoch(e)),
                    Message::AbsBarrier(b) => {
                        self.barrier.replace(b);
                    }
                    Message::Rescale(r) => self.queued_rescales.push_back(r),
                    Message::ShutdownMarker(s) => {
                        self.shutdown_marker.replace(s);
                    }
                    Message::Interrogate(_) => (),
                    Message::Collect(_) => (),
                    Message::Acquire(_) => (),
                    Message::DropKey(_) => (),
                }
            }
        }
        match self.try_into_collect(ctx) {
            Ok(x) => DistributorKind::Collect(x),
            Err(x) => DistributorKind::Interrogate(x),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        keyed::distributed::icadd_operator::make_icadd_with_dist, testing::OperatorTester,
        types::NoData,
    };

    use super::*;

    // a partitioner that just uses the key as a wrapping index
    fn partiton_index<'a>(i: &usize, s: &'a IndexSet<WorkerId>) -> &'a WorkerId {
        s.get_index(i % s.len()).unwrap()
    }

    /// Get a tester for a transition from workerset {0} to  {0, 1}
    fn get_upscale_tester(
    ) -> OperatorTester<usize, VersionedMessage<i32>, i32, usize, TargetedMessage<i32>, i32, ()>
    {
        OperatorTester::built_by(
            |ctx| {
                make_icadd_with_dist(
                    Rc::new(partiton_index),
                    DistributorKind::Interrogate(InterrogateDistributor::new(
                        ctx.worker_id,
                        IndexSet::from([0]),
                        0,
                        RescaleMessage::ScaleAddWorker(IndexSet::from([1])),
                    )),
                )
            },
            0,
            5,
            0..2,
        )
    }
    /// Get a tester for a transition from workerset {0, 1} to  {0}
    fn get_downscale_tester(
    ) -> OperatorTester<usize, VersionedMessage<i32>, i32, usize, TargetedMessage<i32>, i32, ()>
    {
        OperatorTester::built_by(
            |ctx| {
                make_icadd_with_dist(
                    Rc::new(partiton_index),
                    DistributorKind::Interrogate(InterrogateDistributor::new(
                        ctx.worker_id,
                        ctx.get_worker_ids().into_iter().collect(),
                        0,
                        RescaleMessage::ScaleRemoveWorker(IndexSet::from([1])),
                    )),
                )
            },
            0,
            5,
            0..2,
        )
    }

    /// It should create the new worker sets correctly on up and downscales
    #[test]
    fn create_new_worker_set() {
        let dist: InterrogateDistributor<usize, NoData, i32> = InterrogateDistributor::new(
            0,
            IndexSet::from([0]),
            0,
            RescaleMessage::ScaleAddWorker(IndexSet::from([1])),
        );
        assert_eq!(dist.new_worker_set, IndexSet::from([0, 1]));

        let dist: InterrogateDistributor<usize, NoData, i32> = InterrogateDistributor::new(
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
        let mut dist = get_upscale_tester();
        dist.step();
        let out = dist.recv_local().unwrap();
        assert!(matches!(out, Message::Interrogate(_)));
    }

    #[test]
    /// Should not do anything if, the interrogate is still underway
    fn noop_if_interrogate_is_running() {
        let mut dist = get_upscale_tester();
        dist.step();
        let _interrogator = dist.recv_local().unwrap();

        // nothing should happen when we run while holding onto the interrogator
        dist.step();
        assert!(dist.recv_local().is_none());
    }

    #[test]
    /// Should create the collect distributor when the interrogate is done
    fn creates_collector() {
        let mut dist = get_upscale_tester();
        dist.step();
        let msg = dist.recv_local().unwrap();
        let mut interrogator = match msg {
            Message::Interrogate(x) => x,
            _ => panic!("Wrong message"),
        };
        interrogator.add_keys(&[42, 77]);
        drop(interrogator);

        // since there is no reference to the interrogate we should
        // now get a collector
        dist.step();
        dist.step();
        let out = dist.recv_local().unwrap();
        assert!(matches!(out, Message::Collect(_)));
    }

    #[test]
    /// Handle Rule 1.1
    /// • Rule 1.1: If (F(K) == Local) && (F'(K) != Local)
    /// • add the key K to the set whitelist
    /// • pass the message downstream
    fn handle_data_rule_1_1() {
        let mut dist = get_upscale_tester();
        // this message would go to remote under F'(K) since the key is 1
        let msg = DataMessage {
            key: 1,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 0,
            },
            timestamp: 500,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        let _interrogator = dist.recv_local();
        dist.step();
        let out = dist.recv_local().unwrap();
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 1,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 0,
                        sender: 0
                    },
                    target: None
                },
                timestamp: 500
            })
        ), "{out:?}");
    }

    #[test]
    /// Handle Rule 1.2
    /// • Rule 1.2: If (F(K) == Local) && (F'(K) == Local)
    /// • pass the message downstream
    fn handle_data_rule_1_2() {
        let mut dist = get_upscale_tester();
        // this message would go to local under F'(K) since the key is 0
        let msg = DataMessage {
            key: 0,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 0,
            },
            timestamp: 500,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        let _interrogator = dist.recv_local();
        dist.step();
        let out = dist.recv_local().unwrap();
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 0,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 0,
                        sender: 0
                    },
                    target: None
                },
                timestamp: 500
            })
        ));
    }

    #[test]
    /// Handle Rule 2
    /// • Rule 2:  If (F(K) != Local)
    /// • send the message to the worker determined by F
    fn handle_data_rule_2() {
        let mut dist = get_downscale_tester();
        // this message would go to remote under F(K) since the key is 1
        let msg = DataMessage {
            key: 1,
            value: VersionedMessage {
                inner: 42,
                version: 0,
                sender: 0,
            },
            timestamp: 500,
        };
        dist.send_local(Message::Data(msg));
        dist.step();

        // it will first create an interrogator
        let _interrogator = dist.recv_local().unwrap();

        let out = dist.recv_local().unwrap();
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 1,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 0,
                        sender: 0
                    },
                    target: Some(1)
                },
                timestamp: 500
            })
        ), "{out:?}");
    }

    #[test]
    /// Check data is ALWAYS forwarded locally, if the version is higher
    fn forward_local_higher_version() {
        let mut dist = get_downscale_tester();
        // this message would go to remote under F(K) since the key is 1
        let msg = DataMessage {
            key: 1,
            value: VersionedMessage {
                inner: 42,
                version: 1,
                sender: 0,
            },
            timestamp: 500,
        };
        dist.send_local(Message::Data(msg));
        dist.step();
        let _interrogator = dist.recv_local();
        let out = dist.recv_local().unwrap();
        assert!(matches!(
            out,
            Message::Data(DataMessage {
                key: 1,
                value: TargetedMessage {
                    inner: VersionedMessage {
                        inner: 42,
                        version: 1,
                        sender: 0
                    },
                    target: None
                },
                timestamp: 500
            })
        ), "{out:?}");
    }
}
