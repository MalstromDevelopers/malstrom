# Malstrom Compared to Other Frameworks

Below is a best-effort attempt to compare Malstrom with other stream processing frameworks.
We tried being as fair and objective as possible, but obviously we are very biased.
Please also note, that the feature sets of both Malstrom and other frameworks can change quickly.
If you see something outdated, wrong or misrepresented, please do not hesitate to open an issue on
Github.

How does Malstrom compare to...

## Flink

Compared Version: `1.20.0`/`2.0.0`

[Flink](https://flink.apache.org/) is the de-facto industry standard of stream processing
and for good reason, it is extremely powerful.

### Similarities

Both Flink and Malstrom

- can run on a single machine or cluster
- use the dataflow programming model
- are capable of persistent stateful computations
- allow exactly-once processing
- use the ABS algorithm for snapshotting
- can store snapshots to cloud storage
- use a very similar event time system

Malstrom draws a lot of inspiration from the many things Flink does well, while at the same time
aiming to be more lightweight and easier to use.

### Differences

Malstrom's API language is Rust, Flink's API languages are Java, SQL, ~~Scala~~[^flinkscala] and
Python[^flinkpython].

[^flinkscala]: Scala support was removed in Flink 2.0
[^flinkpython]: The Python API does not have feature parity with the Java API

Flink uses a task-parallel model to parallelize computation, while Malstrom uses a data-parallel model.
The task-parallel model is more flexible in terms of graph layout, while the data-parallel model is potentially
more efficient and easier to understand. For more details see
[this blogpost](https://bytewax.io/blog/data-parallel-task-parallel-and-agent-actor-architectures) by the excellent people from [bytewax](https://bytewax.io).

Aside from this:
- Flink runs on the JVM, Malstrom compiles to native code.
- Flink supports larger than memory state, while Malstrom does not (yet!).
- Flink favors higher level APIs while Malstrom exposes both high and low level APIs.
- Flink clusters can not rescale without downtime, Malstrom can.
- Malstrom can be extended with custom runtimes and snapshot storage

## Bytewax

Bytewax is a stream processing system for Python built on a Rust core using Timely Dataflow.
Bytewax is an excellent choice for Python based streaming systems.

### Similarities

Both Bytewax and Malstrom

- use the dataflow programming model
- use a data-parallel architecture
- support exactly-once processing
- support snapshotting
- both have high- and low-level APIs

### Differences

Bytewax's API language is Python, Malstrom's API language is Rust.

While both have an event-time system, Malstrom exposes more direct control over timestamps and
epochs to the user.

Malstrom clusters can rescale without downtime, Bytewax clusters can not.

## Fluvio

Fluvio is a stream processing framework written in Rust which aims to be an alternative to both
Kafka and Flink.

### Similarities

- both Fluvio and Malstrom are written in Rust
- both support stateful streaming programs
- both have an event time system

### Differences

- Fluvio can also act as a message broker, while Malstrom focuses on processing
- Malstrom can be used as a library, while Fluvio requires a running cluster
- Fluvio uses WASM to compile custom operators, while Malstrom compiles to a native binary
- Malstrom gives more control over its event time system
- Malstrom gives more control over persistent state and checkpoints

Overall Fluvio is much more mature and arguably offers higher level APIs than Malstrom, while
Malstrom focuses more on lower level APIs and extensibility.

## Arroyo

### Similarities

- both Arroyo and Malstrom are distributed stateful stream processing engines
- both support checkpointing state to object storage
- both can run on individual machines or on multiple nodes, both can run on Kubernetes

### Differences

- Arroyo's API language is SQL (with Rust or Python UDFs) while Malstrom's API language is Rust
- Malstrom offers a full event time system, Arroyo offers limited event time semantics via windows
- Malstrom jobs can be rescaled without downtime


