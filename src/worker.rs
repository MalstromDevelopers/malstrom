use crate::channels::selective_broadcast::{Receiver, Sender, self};
use crate::stream::jetstream::{JetStream, JetStreamBuilder, DataUnion, Data};
use crate::frontier::Probe;
use crate::stream::operator::StandardOperator;
pub struct Worker {
    streams: Vec<JetStream>,
    probes: Vec<Probe>
}
impl Worker {
    pub fn new() -> Worker {
        Worker {
            streams: Vec::new(),
            probes: Vec::new()
        }
    }

    pub fn add_stream(&mut self, stream: JetStream) {
        self.probes.push(stream.get_probe());
        self.streams.push(stream);
    }

    pub fn get_frontier(&self) -> Option<u64> {
        self.probes.last().and_then(|x| Some(x.read()))
    }

    pub fn step(&mut self) {
        for (i, stream) in self.streams.iter_mut().enumerate().rev() {
            let upstream_frontiers = self.probes[..i].iter().map(|x| x.read()).collect();
            stream.step(&upstream_frontiers);
            while stream.has_queued_work() {
                stream.step(&upstream_frontiers);
            }
        }
    }

    pub fn union_n<const N: usize, Output: Data>(&mut self, streams: [JetStreamBuilder<Output>; N]) -> JetStreamBuilder<Output>{

        // this is the operator which reveives the union stream
        let mut unioned = StandardOperator::from(
            |input: &mut Receiver<Output>, output: &mut Sender<Output>| {
                if let Some(x) = input.recv() {
                    output.send(x)
                }
            },
        );

        for mut input_stream in streams.into_iter() {
            selective_broadcast::link(input_stream.get_output_mut(), unioned.get_input_mut());
            self.add_stream(input_stream.build());
        }
        JetStreamBuilder::from_operator(unioned)
    }

}
