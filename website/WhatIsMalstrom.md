# What is Malstrom?

Malstrom is a distributed, stateful stream processing framework written in Rust.
In usage it is similar to [Apache Flink](https://flink.apache.org/) or [bytewax](https://bytewax.io), although implemented fundamentally differently.
Malstrom's goal is to offer best-in-class usability, reliability and performance, enabling everyone to build fast parallel systems with unparalleled up-time.

---
**Distributed**: Malstrom can run on many machines in parallel, sharing the processing workload and enabling [[zero-downtime scaling]] to fit any demand.
[[Kubernetes]] is supported as a first-class deployment environment, others can be added through a public trait interface.

**Stateful**: Processing jobs can hold arbitrary state, which is snapshotted regularly to [persistent storage](Stateful%20Programs.md) like disk or S3. In case of failure or restarts, the job resumes from the last snapshot. Malstroms utilizes the [[ABS Algorithm]], ensuring every message affects the state **exactly once**.

**Usability**: Malstrom provides a straight-forward dataflow API, which can [be extended](CustomOperators) when needed. A simple [threading model](ThreadingModel) means no async, no complex lifetimes, no `Send` or `Sync` needed. Data only needs to be serialisable when explicitly send to other processes.

**Reliability**: Using the world's safest programming language makes building highly-reliable stream processors a breeze. In any case [[zero-downtime scaling]] and [[zero-downtime upgrades]] (TBD) allow for awesome uptime.

# Code Example

```rust
use malstrom::operators::*;
use malstrom::runtime::MultiThreadRuntime;
use malstrom::snapshot::{NoPersistence, NoSnapshots};
use malstrom::source::{SingleIteratorSource, StatelessSource};
use malstrom::sink::{StatelessSink, StdOutSink};

fn main() {
	MultiThreadRuntime::new(
	|runtime| {
		let worker = WorkerBuilder::new(runtime, NoSnapshots, NoPersistence);
		worker.new_stream()
		.source(
			"iter-source",
			StatelessSource::new(
				SingleIteratorSource::new(["Look", "ma'", "I'm" "streaming"])
				)
			)
		.map("upper", |x| x.to_uppercase())
		.sink("stdout", StatelessSink::new(StdOutSink::default()))
		.finish();
		worker
	}
	).unwrap().execute()
}

>>> LOOK MA' I'M STREAMING
```

