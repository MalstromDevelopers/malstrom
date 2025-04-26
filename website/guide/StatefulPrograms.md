# Stateful Programs

>"Stateless is useless!"
>	– my boss

Almost any streaming program but the simplest of ETL pipelines must utilise program state.
The state may be very simple, like the total count of messages received, or very complex like the current relations in a graph.

Let's see how we can make our program stateful.

<<< @../../malstrom-core/examples/stateful_programs.rs

This program will print the running sum of all numbers from 0 to 100 added up.
Let's dissect the `stateful_map` operator.

```rust
.stateful_map("sum", |_key, value, state: i32| {
	let state = state + msg.value;
	(state.clone(), Some(state))
})
```

- `"sum"` is the name of the operator
- `_key` is a reference to the key of the message
- `msg` is the value of the message to be processed
- `state` is the last emitted state of this operator or the default value for state type, if the last emitted state is `None`

We return a tuple of two values here:

1. The value of the message being passed downstream
2. Our new operator state

The emitted state may be `Some` or `None`. If we return `Some`, we will receive the state as an
input again on the next invocation, if we return `None` the state is dropped and the next invocation
will receive the default state value.

## Keyed State

In Malstrom all state is keyed. This means our state is partitioned by the same properties as the
datastream being processed. For every invocation of the stateful function in `stateful_map` the
input state is the state for the key of
the message. In the example above we used only a single key, giving us a single state, but this is
usually not what you would do in a real application.

Let's look at an example with multiple keys:

<<< @../../malstrom-core/examples/stateful_program_multiple_keys.rs

Now instead of getting a running sum of all numbers, we gut running sums of all even and odd numbers.
This is because our state is keyed by parity of the numbers.

For more information about keying see [Keyed Streams](./KeyedStreams.md).

## Persistent State

At runtime Malstrom keeps all state in memory, but what if we want to make our program resilient
against machine failures or pause execution for some time and resume where we left off?
This is where "persistent" state comes in. Malstrom uses snapshotting for persistence, which means
periodically saving the program state to a permanent location. If there is a failure, the program
can resume from the last snapshot instead of starting with a blank state.

### Persistence Backends

Persistence backends are interfaces to persistent storage where snapshots can be saved.
Currently Malstrom comes with two different backends to choose from:

- **NoPersistenceBackend**: As the name implies, this is a no-op backend which **does not** persist
  state. On program restarts all state is lost. This is useful for tests, stateless programs or
  programs which do not need to recover state on restarts.
- **SlateDbBackend**: This backend is available with the feature `slatedb`.
  It uses [SlateDB](https://slatedb.io/) with [Object Store](https://docs.rs/object_store/latest/object_store/)
  as a backend which can save snapshots to either local disk or a cloud store like S3, GCS or Azure Blob.

Let's see how we can make our program state persistent:

First make sure you have SlateDB and ObjectStore installed: `cargo add malstrom -F slatedb` and `cargo add object_store`

<<< @../../malstrom-core/examples/slatedb_backend.rs

Just like this, we have made our programs state persistent. Let's review the changes we introduced:

- `.snapshots(Duration::from_secs(10))`: This tells Malstrom to take a state snapshot every 10 seconds.
  This means, in the worst case, our program will have to re-process 10 seconds of data on restarts. 
- `SlateDbBackend::new(...)`: 
  This is our persistence backend. For this example we save snapshots to a temporary directory.

Unfortunately right now we have too little data, the program will finish before even taking the first snapshot. Let's take more data and introduce some failures:

```rust
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, RuntimeFlavor};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};
use malstrom::distributed::rendezvous_select;
use malstrom::snapshot::{SlateDbBackend, IntervalSnapshots};
use object_store::{Path, local::LocalFileSystem};
use std::{sync::Arc, time::Duratinon};

fn main() {
	loop {
		match SingleThreadRuntime::new(build_dataflow).execute() {
			Ok(_) => return;
			Err(_) => continue; // restart
		}
	}
	
}

fn build_dataflow<F: RuntimeFlavor>(flavor: F) -> WorkerBuilder {
	let storage = LocalFileSystem::new_with_prefix(Path("/tmp"));
	let worker = WorkerBuilder::new(
		flavor,
		IntervalSnapshots::new(Duration::from_secs(10)),
		SlateDbBackend::new(Arc::new(storage))
	);
	let start_time = Instant::now();
	let fail_interval = Duration::from_secs(15);
	worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(
				SingleIteratorSource::new((0.=i32::MAX))
			)
		)
		.stateful_map("sum", |msg, state: i64| {
			let state = state + (msg.value as i64);
			(state.clone(), Some(state))
		})
		.inspect("print", |x, _ctx_| println!("{x}"))
		.inspect("fail-random" |_msg, _ctx| {
			if Instant::now().duration_since(start_time) > fail_interval {
				panic!("Oh no!")
			}
		})
		.finish();
	worker
}
```

Our program will now "fail" and restart every 15 seconds. You may observe some duplicate outputs,
but the running total calculated remains correct, i.e. every integer is added **exactly once**.

### Exactly Once

Malstroms [checkpointing system](ABS Algorithm) guarantees all datapoints
**affect the program state exactly once**. Notably this means, you will always get the correct end
results, but **in failure cases** messages may be processed and emitted more than once.
While this sounds like a weak compromise, it is actually the strongest guarantee any stream
processing system offers to date (to the best of the author's knowledge).
You can however still make your outputs exactly-once _observable_ by using
[idempotent](https://en.wikipedia.org/wiki/Idempotence) output operations or
[atomic commits](https://en.wikipedia.org/wiki/Atomic_commit).
