use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    net::{SocketAddr, IpAddr},
    ops::Range, io, cmp::Ordering, time::Duration,
};

use crossbeam::channel::{Receiver, Sender};
use dns_lookup::lookup_host;

use crate::{
    frontier::FrontierHandle,
    stream::jetstream::{dist_rand, Data, JetStream},
    stream::operator::OperatorBuilder
};

fn hash<T>(obj: T) -> u64
where
    T: Hash,
{
    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

fn rendezvous_hash<T: Hash>(value: T, range: Range<usize>) -> Option<usize> {
    if range.len() == 0 {
        return None;
    }

    let ranks = Vec::with_capacity(range.len());
    for i in range {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        i.hash(&mut hasher);
        ranks.push((hasher.finish(), i));
    }
    let x: usize = ranks
        .iter()
        .scan((u64::MAX, 0), |acc, (h, i)| {
            if h < &acc.0 {
                acc.0 = h;
                acc.1 = i;
                Some(i)
            } else {
                Some(acc.1)
            }
        })
        .collect();
    Some(x)
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiscoveryError {
    #[error("Error looking up service in DNS {0}")]
    DNSLookupError(#[from] io::Error),
}
struct NetworkExchanger {
    remotes: Vec<IpAddr>,
    target_remote_count: usize,
    k8s_service_url: String,
}

impl NetworkExchanger {

    fn wait_for_remotes(&self) -> () {
        while self.remotes.len() < self.target_remote_count {
            let mut hosts = lookup_host(&self.k8s_service_url).expect("Error discovering K8S service hosts");
            match hosts.len().cmp(&self.target_remote_count) {
                Ordering::Equal => std::mem::swap(&mut self.remotes, &mut hosts),
                Ordering::Less => {std::thread::sleep(Duration::from_millis(200)); continue;},
                Ordering::Greater => {panic!("Too many hosts")}
            }
        }
    }

    fn exchange<T: Data>(
        &self,
        inputs: Vec<Receiver<T>>,
        outputs: Vec<Sender<T>>,
        _frontier_handle: &mut FrontierHandle,
    ) -> () {
        self.wait_for_remotes();
        let input_values = inputs.iter().filter_map(|o| o.try_recv().ok());
    
        let targets = input_values.map(|x| (rendezvous_hash(x, self.remotes.len() + 1).unwrap(), x));
        let (locals, remotes) = targets.partition(|x| x.0 == self.remotes.len());
        dist_rand(locals, &outputs);


    }

}


impl<O> NetworkExchange<O> for JetStream<O>
where
    O: Data,
{
    fn network_exchange<T: Data>(self, mut mapper: impl FnMut(O) -> T + 'static) -> JetStream<T> {
        let operator = OperatorBuilder::new(exchange);
        self.then(operator)
    }
}
