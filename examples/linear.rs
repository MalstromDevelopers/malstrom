/// This is an example of linear flow control
use crossbeam::channel::{Receiver, Sender};
use jetstream::{
    filter::Filter,
    map::Map,
    stream::{dist_rand, JetStream, Nothing, StandardOperator},
    worker::{self, Worker},
};

pub fn main() {
    // this source generates 5 numbers every time it is
    // activated
    let source: StandardOperator<Nothing, Vec<i64>> = StandardOperator::new(|_, outputs, _frontier_handle| {
        let data: Vec<i64> = (0..5).collect();
        dist_rand(vec![data].into_iter(), outputs)
    });

    // the next operator will flatten this vec
    // and produce 5 output messages for every one input message
    let flatten = StandardOperator::from(
        |inputs: &Vec<Receiver<Vec<i64>>>, outputs: &Vec<Sender<i64>>| {
            let data = inputs.iter().filter_map(|r| r.try_recv().ok()).flatten();
            dist_rand(data, outputs)
        },
    );

    // this operator just reads one record from each of its inputs, when it gets activated
    let print =
        StandardOperator::from(|inputs: &Vec<Receiver<i64>>, _outputs: &Vec<Sender<()>>| {
            let input_size: usize = inputs.iter().map(|x| x.len()).sum();
            println!("outstanding messages: {input_size}");
            for msg in inputs.iter().filter_map(|x| x.try_recv().ok()) {
                println!("{msg}");
            }
        });
    let stream = JetStream::from_operator(source).then(flatten).then(print);

    // We will now step this stream 50 times
    // On each step the stream will schedule all operators bottom-up **at most once**.
    //
    // if we scheduled every operator exactly once per step, the number of outstanding
    // messages for print to process would grow with each step, as print processes fewer
    // messages per step than our source produces
    //
    // To avoid this, jetstream schedules each operator at most once, starting downstream
    // and stopping a round of scheduling, if an operator still has remaining input after
    // being scheduled.
    // The result is, that we see the count of outstanding messages for print never exceeding 5.
    let mut worker = Worker::new();
    worker.add_stream(stream.finalize());
    for _ in 0..50 {
        worker.step()
    }

    // almost the same stream, but written more concisely
    let _ = JetStream::new()
        .source(|_| Some((0..5).collect::<Vec<i64>>()))
        .map(|mut x| x.pop())
        .filter(|x| x.is_some())
        .map(|x| println!("{x:?}"));
}
