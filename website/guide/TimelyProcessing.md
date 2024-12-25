# Timely Processing

When working with unbounded datastreams we will often want to perform some operation over a finite timeframe. This could be an aggregation, for example we track vehicle movements and want to know the average speed over intervals of five minutes, or it could be other operations: Imagine we try to detect some events, the more datapoints we use the higher the certainty, but we want to put an upper limit on the time needed for detection.

For both these use cases we need _time_. A naive approach would be to use the system clock, meaning to get intervals of five minutes (these are called "windows") we would wait until five minutes had elapsed in real world time.
This works, it is usually called a "session window", but comes with a very big issue: Using system time inherently ties the result of our operation to the throughput of stream; if we process more messages per second, we end up with more messages per window.
In normal operation, this might not be an issue, but when replaying historic data or trying to catch up on a datastream we have fallen behind on, this is a big problem. For one the increased window size can present technical issues, like out-of-memory errors, and on the other hand, using a system clock means relying on a side effect and therefore makes our computation non-deterministic. Non-determinism opens the gates to all kinds of hard to troubleshoot and headache-inducing problems. But don't despair, we can do better.

## Event Time

Event time is a crucial concept in stream processing, referring to the timestamp associated with each data point that indicates when the event actually occurred. When working with streaming data you will find that almost all data somehow has a time attached, either explicitly as a timestamp or implicitly through some other attribute.

Malstrom is extremely flexible in what can be used as event time: You can use actual timestamps, but really any type works as long as it supports ordering.

## Epochs

For some operations, like windowing, it is crucial to know, when a certain timestamp will not appear anymore, i.e. when time has advanced past this timestamp.

If our inputs were strictly ordered, meaning timestamps came in an always ascending order,
this would be easy, as we would just need to observe the timestamps attached to our data.
Unfortunately, the real world often does not grant us this simplicity: In real world applications
data can be out of order by a few seconds, many hours sometimes even days or weeks.

To still be able to reason about time and close windowing aggregations, Malstrom allows you to
generate a special message type called an **Epoch**.
An Epoch is a special marker message, which acts like a promise: It contains a specific timestamp and any data messages following it, **must** have a timestamp greater than that of the epoch.
This in turn means, any operator that an Epoch of time _T_ reaches, can safely assume, that it will
never see any more messages with a timestamp <= _T_

Let's see how timestamps and epochs can be created on a datastream. For this example we will use the [fake](https://github.com/cksac/fake-rs) crate to generate fake financial transaction data. We will then use the event time to get a weekly balance for each account.
This is something you would usually do with a [window](Windows), but we will use the `stateful_op` operator for demonstration purposes here. For a more detailed explanation on `statful_op` see [[CustomOperators]].

First install [fake](https://docs.rs/fake/latest/fake/) and [chrono](https://docs.rs/chrono/latest/chrono/): `cargo add chrono fake -F chrono`

```rust
use fake::{Dummy, Fake, Faker};
use chrono::DateTime;
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, SingleThreadRuntimeFlavor};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};
use malstrom::keyed::rendezvous_select;

type AccountId = usize;
/// Fake transaction to track
#[derive(Debug, Dummy)]
pub struct Transaction {
    #[dummy(faker = "0..100")]
    account_id: AccountId,
    #[dummy(faker = "-1000.0..1000.0")]
	amount: f32,
	# TODO
	transaction_time: NaiveDateTime
}

fn main() {
	SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(flavor: SingleThreadRuntimeFlavor) -> WorkerBuilder {
	let data = std::iter::from_fn(|| Faker.fake::<Transaction>()).take(1000);
	let mut latest_timestamp = DateTime::MIN_UTC;
	let worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence);
	
	let stream, _ = worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(data))
		)
		.key_distribute("key-account", |msg| msg.value.account_id, rendezvous_select)
		.assign_timestamps("assign-time", |msg| msg.value.transaction_time)
		.generate_epochs("end-of-week", move |msg, _| {
			// issue an epoch everytime we advance a week
			if (msg.timestamp - latest_timestamp).num_weeks() > 0 {
				// emit the start of the week as an epoch
				let start_of_week = NaiveDate::from_isoywd_opt(
					msg.timestamp.year(), msg.timestamp.week(), Weekday::Mon
				).unwrap();
				Some(start_of_week);
			}
		});
	stream
		.stateful_op(TransactionCounter)
		.inspect("print" |msg, _| {
			println!(
			"Account {}: {} in week {}",
			msg.key,
			msg.value,
			msg.timestamp
			)
		})
		.finish();
	worker
}

struct TransactionCounter;

impl StatefulOp<AccountId, Transaction, NaiveDateTime, f32> for TransactionCounter {

	fn on_data(
		msg: DataMessage<AccountId, Transaction, NaiveDateTime>,
		weekly_balance: f32,
		output: &mut Output<usize, f32, NaiveDateTime>) {
			// update the balance
			weelky_balance + msg.value.amount
	}

	/// At the end of every week emit and reset the balance
    fn on_epoch(
        &mut self,
        epoch: &NaiveDateTime,
        balances: &mut IndexMap<AccountId, f32>,
        output: &mut Output<AccountId, f32, NaiveDateTime>,
    ) -> () {
	    // draining resets all accounts since the state will default to 0.0
	    for (account, balance) in balances.drain(..) {
		    output.send(
			    Message::Data(DataMessage::new(account, balance, epoch.clone()))
		    )
	    }
    }
}
```

Running the example above, you will see we get a printout of all weekly balances every time our datastream advances one week.

## Out-of-orderness

The code example above assumes all data to be strictly ordered. In the real world however, this is rarely the case, especially when dealing with multiple datasources.

Dealing with out-of-order records we fundamentally have to strategies:

1. Ignoring them entirely
2. Compromising on latency and waiting a limited amount of time for late records

Strategy 1 is what the example code is actually doing. If you look closely, the `generate_epochs` operator returns two streams, one stream of on-time messages and one stream of late messages. Late messages here are those where their timestamp is smaller or equal to the last emitted epoch.

Strategy 2 is usually more useful for any practical applications. Malstrom comes with a prebuilt utility for this:

```rust
use fake::{Dummy, Fake, Faker};
use chrono::{NaiveDateTime, TimeDelta};
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, SingleThreadRuntimeFlavor};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};
use malstrom::keyed::rendezvous_select;
use malstrom::timely::BoundedOutOfOrderness;

type AccountId = usize;
/// Fake transaction to track
#[derive(Debug, Dummy)]
pub struct Transaction {
    #[dummy(faker = "0..100")]
    account_id: AccountId,
    #[dummy(faker = "-1000.0..1000.0")]
	amount: f32,
	# TODO
	transaction_time: NaiveDateTime
}

fn main() {
	SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(flavor: SingleThreadRuntimeFlavor) -> WorkerBuilder {
	let data = std::iter::from_fn(|| Faker.fake::<Transaction>()).take(1000);
	let mut latest_timestamp = DateTime::MIN_UTC;
	let worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence);
	
	let _, late = worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(data))
		)
		.key_distribute("key-account", |msg| msg.value.account_id, rendezvous_select)
		.assign_timestamps("assign-time", |msg| msg.value.transaction_time)
		.generate_epochs("end-of-week", BoundedOutOfOrderness::new(TimeDelta::days(1)));
	late
		.inspect("print" |msg, _| println!("Late message: {msg:?}"))
		.finish();
	worker
}
```


Using bounded of orderness yields us two streams where in one the records are guaranteed to be at most one day out of order from another. The other stream (the "late" stream) has unlimited out-of-orderness and has **no epochs** issued.
How you handle late messages depends on your applications needs.

## Epochs on unioned streams

Malstrom allows us to join multiple timestamped streams together, as long as they have the same timestamp type. Do understand how epochs are handled on joined streams, we must introduce a new term: "Frontier".
An operators frontier is the largest epoch timestamp he has received this far. Any operator can safely assume it will never see a record with a timestamp less than or equal to its frontier.

If one a stream union we were to just pass epochs as-is, we would risk increasing a downstream operator's frontier too far if one side of the join is further ahead in time. We would then not be allowed anymore to send the messages of the other join side downstream.
To avoid this, the `union` operator will "merge" epochs when it encounters them. The merge logic depends on the timestamp type, but usually means using the minimum frontier of all union input streams.

For example if we have a union of two streams an one stream sends `Epoch(10)` and the other stream sends `Epoch(125)`, the downstream operators will observe a message `Epoch(10)`.

## Performance considerations

Using timestamps is very close to free in terms of performance. Message size increases by the size of the timestamp however this is expected to be negligible for most applications.

Epochs travel the computation graph like any other messages, therefore issuing lots of epochs can have an adverse effect on performance in the same way a larger overall data volume would have. It is generally advisable to place the `generate_epochs` operator close to the operation where epochs are needed.

Placing `generate_epochs` after a `key_distribute` rather than before it may also improve performance, since then epochs do not need to cross network boundaries.

