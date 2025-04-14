# Implementing Custom Data Sources

Sources are how data records enter your computation. Usually this means connecting to some external
system like a message broker or database.

At the moment Malstrom supports two types of sources:

- Kafka compatible message brokers via the [`malstrom-kafka`](docs.rs/malstrom-kafka) crate. You can find more
information about that [here](./Kafka.md).
- Any type [`IntoIterator`](https://doc.rust-lang.org/std/iter/trait.IntoIterator.html) via the
built-in `SingleIteratorSource`

`SingleIteratorSource` is mainly useful for testing, as it can only produce records on a single
worker, regardless of the overall job parallelism.

We aim to support more sources in the future, in the meantime though, this guide will teach you
how to implement your own source.
Make sure to read about [Keying](./KeyedStreams.md) and [Event Time](./TimelyProcessing.md)
first, as these concepts are important to fully understand how sources work.

## Custom Stateless Source

As an example we will implement a source reading from files on the local filesystem.
Sources can be stateless, meaning they retain no information when the job restarts or is rescaled,
or stateful.
A stateless source is a bit simpler to implement, so that is what we will start with.

For our end result, we will want a source which takes as parameters a list of filepaths, and
then emits lines from the files as records into our stream:

```rs
StatelessSource::new(FileSource::new(
    vec!["/some/path.txt".to_string(), "/some/other/path.txt".to_string()]
));
```

### Parts and Partitions

Sources in Malstrom are always partitioned, meaning they consist of multiple inner sources whith
each of them emitting unique records. This enables Malstrom jobs to parallelize data reading.
It is however totally valid for a source to only have a single partition, i.e. only be read by
a single worker at a time. This is the case for `SingleIteratorSource`.

Parts can be thought of as the names of partitions. Each `Part` maps to one partition. The records
emitted by any source have the `Part` of the partition which emitted them as their key.

### The StatelessSourceImpl Trait

To turn any type into a source, we need to implement the `StatelessSourceImpl` trait on it.
Example:

<<< @../../malstrom-core/examples/file_source_stateless.rs#source_impl

Here we first define a struct `FileSource`, which holds the list of files we whish to read.
On `FileSource` we then implement `StatelessSourceImpl<String, usize>` meaning our source emits
records with a value of type `String`, a line from the file, and a timestamp of type `usize`, the
line number in the file.

`type Part = String;` means the names of our parts, and in turn keys of the records we emit, are
strings.

For this trait we must implement two functions: `list_parts` which tells Malstrom how many and
what partitions we have. Here we simply return our file list, since every file will become a
partition.

`build_part` gets called for every `Part` returned by `list_parts` to instantiate the partition.
This methods gets called on the worker where the partition will be located and may get called
again when the job is rescaled.

We are however still lacking the implementation of the actual partition itself:

### The StatelessSourcePartition Trait

For our partitions we want to open and read the file at the partition's respective filepath and then
emit every line and line number as data.

To do this, we implement Malstrom's StatelessSourcePartition trait:

<<< @../../malstrom-core/examples/file_source_stateless.rs#partition_impl

The trait consists of two methods:

- `.poll` is called by the worker whenever it is ready to accept a new record. It can either return
`Some((value, timestamp))` to emit a record or `None` if no data is available currently. Note that
the implementation of `poll` must not block until data becomes available, it should instead return
`None` immediatly.

- `.is_finished` tells the worker, as the name implies, whether this source is finished. Returning
`true` indicates no more records being available

### Full Code

`StatelessSourceImpl` and `StatelessSourcePartition` are all you need to implement for any
(stateless) source. Our file source can then be used like this on a stream:

<<< @../../malstrom-core/examples/file_source_stateless.rs#usage

::: details Full code
<<< @../../malstrom-core/examples/file_source_stateless.rs
:::

## Custom Stateful Source

Currently the source we built does not retain any state persistently. This means, if the job
is restarted or rescaled, we will start reading files from the beginning again.

It would be much nicer if instead we continued reading, where we left off.

### The StatefulSourceImpl Trait

To achieve this we swap out the implementation of `StatelessSourceImpl` for `StatefulSourceImpl`.

::: tip
Actually you could also implement both, making the source stateless/stateful at the discretion
of the user.
:::

<<< @../../malstrom-core/examples/file_source_stateful.rs#source_impl{17,26}

To make this work, we need to define one more associated type, `PartitionState` which is the type
of the state we wish to retain. We want to keep the current line number of the file we are reading,
therefore we set `PartitionState` to `usize`.

Additionally now `build_part` takes an extra parameter, the partition's state, to create the
partition. This is optional, since the state may not exist yet, for example when we start the job
for the first time.

Similarly we also need to swap `StatelessSourcePartition` for `StatefulSourcePartition`.

### The StatefulSourcePartition Trait

Conceptually we need to make two changes to our partition:

1. Keep track of which line in a file we last read
2. Tell Malstrom about this information so it can persist or move the state for us

We will introduce an additional attribute `next_line` to check which line we would need to read
on the next call to `poll`.

<<< @../../malstrom-core/examples/file_source_stateful.rs#partition_impl{6,26,30,42-48}

The method `snapshot` is called by Malstrom whenever it takes a persisted snapshot of the job state.
The state returned here will be given to `build_part` when the job starts from a snapshot.

::: warning
It is very easy to introduce off-by-one errors when implementing `snapshot`.
Think carefully about the state being persisted and how it is used in `build_part`.
:::

The other method we added, `collect`, looks very similar, though the signature is slightly different.
It takes `self` by value rather than by reference, this is because Malstrom will call this method
to destruct the partition, yielding its current state. This happens when the partition is moved to
another worker on rescaling. On that other worker the returned state will then again be given to
`build_part` to instantiate a partition there.

### Full Code

Similar to the stateless version, our stateful implementation can be used with a stream by wrapping
it in `StatefulSource`

<<< @../../malstrom-core/examples/file_source_stateful.rs#usage

::: details Full code
<<< @../../malstrom-core/examples/file_source_stateful.rs
:::
