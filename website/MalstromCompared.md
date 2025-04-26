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
more efficient and easier to understand. For more details see [this blogpost](https://bytewax.io/blog/data-parallel-task-parallel-and-agent-actor-architectures) by the excellent people from [bytewax](bytewax.io).

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

### Differences

Fluvio does not have an event-time system.

## Seastreamer

## RisingWave

## Timely Dataflow

## Arroyo

## denormalized

