# Joining and Splitting Streams

We saw already that a stream has one input and one output, however this does not prevent us from consuming multiple datasources or writing to multiple sinks.

Let's extend the example from the beginning to take multiple inputs:

```rust
// main.rs
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, SingleThreadRuntimeFlavor};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};

fn main() {
	SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(flavor: SingleThreadRuntimeFlavor) -> WorkerBuilder {
	let worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence);
	let numbers = worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(0.=100))
		);
	let more_numbers = worker
		.new_stream()
		.source(
			"other-iter-source",
			StatelessSource::new(SingleIteratorSource::new(0.=100))
		);

	numbers
	.union("union-nums", more_numbers)
	.sink("std-out-sink", StatelessSink::new(StdOutSink::default()))
	.finish();
	worker
}
```

If you run this example, you'll see we get each number twice. The `union` operator takes messages from two streams and fuses them into one. Note that this is different from a `zip` operation: `union` does not necessarily alternate between the left and right stream.

For multiple outputs we can split our stream just as easily:

```rust
// main.rs
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, SingleThreadRuntimeFlavor};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};

fn main() {
	SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(flavor: SingleThreadRuntimeFlavor) -> WorkerBuilder {
	let worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence);
	let [numbers, more_numbers] = worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(0.=100))
		).const_cloned();
	
	numbers
	.sink("std-out-sink", StatelessSink::new(StdOutSink::default()))
	.finish();
	
	more_numbers
	.sink("more-std-out-sink", StatelessSink::new(StdOutSink::default()))
	.finish();
	
	worker
}
```

Here the `const_cloned` operator will clone each message into a fixed number of output streams. There is also the `cloned` operator, which allows determining the number of output streams at runtime.

If we want to select into which output stream a message goes, we can use the `const_split` and `split` operators:

```rust
// main.rs
use malstrom::runtime::{WorkerBuilder, SingleThreadRuntime, SingleThreadRuntimeFlavor};
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::operators::*;
use malstrom::source::{SingleIteratorSource, StatelessSource};

fn main() {
	SingleThreadRuntime::new(build_dataflow).execute().unwrap()
}

fn build_dataflow(flavor: SingleThreadRuntimeFlavor) -> WorkerBuilder {
	let worker = WorkerBuilder::new(flavor, NoSnapshots, NoPersistence);
	let [even_nums, all_nums] = worker
		.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(SingleIteratorSource::new(0.=100))
		)
		.const_split(|msg, outputs| {
			if msg.value.is_even() {
				*outputs = [true, true];
			} else {
				*outputs = [false, t];
			}
		})
	
	even_nums
	.sink("even- std-out-sink", StatelessSink::new(StdOutSink::default()))
	.finish();
	
	all_nums
	.sink("all-std-out-sink", StatelessSink::new(StdOutSink::default()))
	.finish();
	
	worker
}
```

The `split` and `const_split` operators take a function as a parameter which determines where messages are routed. The function receives a mutable slice of booleans representing the split outputs. By default all slice values are `false`. Setting a value to `true` will send the message to that output, e.g. with two outputs `[true, false]` sends a message only to the first (left) output, `[true, true]` to both and `[false, false]` to neither.
The message will be cloned as often as necessary.
