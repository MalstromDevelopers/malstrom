# Implementing Custom Sinks

Sinks are how data leaves a Malstrom computation. This usually means sending or writing data
somewhere like a message broker, database or file.

Currently Malstrom supports three types of sinks

- Kafka compatible message brokers via the [`malstrom-kafka`](docs.rs/malstrom-kafka) crate. You can find more
information about that [here](./Kafka.md).
- Printing records to stdout via the built-in `StdOutSink`
- Collecting records in a vector via the built-in `VecSink`

We aim to support more types of sinks in the future, until then you can implement any sink yourself.
This guide will teach you how to do that.
Make sure to read about [Keying](./KeyedStreams.md) and [Event Time](./TimelyProcessing.md)
first, as these concepts are important to fully understand how sinks work.

## Custom Stateless Sink

If you read the guide on [Custom Sources](./CustomSources.md) before, the following will feel very
familiar.

As an example we will implement a sink which writes records to a specific file on the local
filesystem.

Meaning if we have the keys "foo", "bar" and "baz" our directory should look like this:

```
/the/directory
  |_ foo.txt
  |_ bar.txt
  |_ baz.txt
```

and we would like to construct the sink in Rust like this:

```rs
StatelessSink::new(FileSink::new("/path/to/the/directory".to_string()))
```

### The StatelessSinkImpl Trait

To turn any type into a source, we need to implement the `StatelessSinkImpl` trait on it.
Example:

<<< @../../malstrom-core/examples/file_sink_stateless.rs#sink_impl

Let's see what is going on here:

The implementation `impl<T> StatelessSinkImpl<String, String, T> for FileSink` means we implement
`FileSink` as a sink for any datastream where the records have a `Key` of type `String`
(the first type parameter) and a value of type `String` (the second type parameter).
The generic `T` indicates the records may have a timestamp of any type.

For a stateless sink, we only need to implement a single method: `fn sink(&mut self, msg: DataMessage<String, String, T>)`
This method gets called for every record arriving at the sink and we can do with that record as we
whish.

And that's it. That is a fully functioning sink!

### Full Code

You can find the fully functional, runnable example code below:

::: details Full code
<<< @../../malstrom-core/examples/file_sink_stateless.rs
:::

## Custom Stateful Sinks

The sink we implemented above is not able to retain any state across job restarts or rescalings.
Generally data sinks rarely need to be stateful. This is also the reason the example used in
this guide may seem a bit contrived, because it is!

We will use the same sink as above, but this time adding line numbers to the files we write.
Note that you could achieve the same with a `stateful_map` before a stateless sink.

### The StatefulSinkImpl Trait

Instead of `StatelessSinkImpl` we must implement `StatefulSinkImpl` for our sink type:

<<< @../../malstrom-core/examples/file_sink_stateful.rs#sink_impl

There is a lot more going on here than in the stateless version.
First we must understand that stateful sinks in Malstrom are explicitely partitioned. This means,
each sink is really composed of many smaller sinks, it's partitions.

In our case we will create one partition per file we write, as you will see below. Each partition
belongs to a `Part`. You can think of the `Part` as the name of the partition.
Here we use the filepath of the file the partition is writing to as the `Part`.

Each partition also has a state, the `PartitionState`. In our example we will keep the next line
number to be used in a file as the partition's state, which is of type `usize`.

For the `StatefulSinkImpl` trait we must implement two methods:

- `assign_part`: This function determines the mapping between a record's key and the partition it
will go to. This function **must be stable and deterministic**. In this example we simply use the
key to construct a filepath, which is our target partition's `Part`
- `build_part`: This function constructs partitions and is called everytime we encounter a key for
which we do not yet have a partition, when the partition needs to be recreated because its state
was moved or when we are restoring the sink from a snapshot.

Next we need to implement the partition itself

### The StatefulSinkPartition Trait

The trait `StatefulSinkPartition` is what our sinks partitions must implement. Let's look at the
implementation for our example:

<<< @../../malstrom-core/examples/file_sink_stateful.rs#partition

What is happening in this code?

- In the `new` method, we open or create the file we want to write to and set our internal state,
  the next line number to use, to either the state passed in or a default of `0` for the first line.
- `type PartitionState = usize` as above declares the persistent state of our partition to be of
  type `usize`
- The method `sink` is called for every record reaching this sink. For every record we first write
  write the line number followed by space, then the record and then a newline. Finally we increment
  the line number by one.
- `snapshot` gets called by Malstrom when it takes a snapshot for persistence. In this method we must
  return the state we whish to persist, in our case the next line number to be used.
- `collect` gets called by Malstrom when it moves the partition to another worker. Note how `collect`
  takes `self` by value and not by reference, as the partition will be dropped here and recreated
  on another worker using the returned state.

::: warning
It is very easy to introduce off-by-one errors when implementing `snapshot` and `collect`.
Think carefully about the state being persisted and how it is used in `build_part`.
:::

### Full code

You can find the full code and usage below.

::: details Full code
<<< @../../malstrom-core/examples/file_sink_stateful.rs
:::