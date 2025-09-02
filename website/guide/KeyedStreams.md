# Keyed Streams

When we want to process streams of data in a distributed manner, i.e. on multiple threads or multiple machines, we must somehow determine where each message shall go to be processed. We could just randomly send messages to our processors, but very often this is not a particularly useful thing to do.
Instead of using random distribution, we can split our data by some attribute or characteristic, we call this attribute the "key" of the message.
For example if we are tracking financial transactions, we may want to distribute them by the receiving account, ensuring each transaction for an account goes to the same processor which can then accurately keep track of the account balance.

## Keying in Malstrom

Let's see how keying is performed in Malstrom. We will take a simple stream of numbers and key them by whether they are even or not.

<<< @../../malstrom-core/examples/keyed_streams.rs

In the output we will see, that all even numbers where processed at one worker, and all other numbers at the other.
The distribution happens in the `key_distribute` operator. Let's examine it more closely:

`key_distribute("key-odd-even", |x| (x.value & 1) == 0, rendezvous_select)`

- `"key-odd-even"` is the name of the operator
- `|x| (x.value & 1) == 0` This is the keying function, it is invoked on every message and is used to determine its key, in this case either `true` of `false`
- `rendezvous_select` This is the distributor function and is used to determine the receiving worker for each key. In this case we used a distributor function from the Malstrom library, but you may also write your own. `rendezvous_select` uses a [Rendezvous Hash](https://en.wikipedia.org/wiki/Rendezvous_hashing) for distribution to optimise [rescaling](zero-downtime scaling) operations.

Important: The keying and distributor functions **must be deterministic and stable**, unstable functions could lead to messages getting routed to the wrong processor, which won't crash your program, but even worse, give wrong results.

## Keyed Operators

Besides and distributing messages, the `key_distribute` operator also changes the type of our stream itself. Before keying, the type of our stream was `Malstrom<NoKey, i32, usize>` and it changed to `Malstrom<bool, i32, usize>`.
Stateful operators are usually only implemented for keyed streams to enable correct [rescaling](zero-downtime scaling), saving and loading of [persistent state](Persistent State).

## Caveats and Performance Considerations

### Unstable Functions

Correct keying is the "key" (pun intended) to correct stateful programs. As said before when writing keying or distribution functions you must ensure they are deterministic and stable.
Non-deterministic functions or functions where the output changes between program versions are a bad idea an will give you headaches. Keep in mind, that Rust's `std::hash::Hasher` does not guarantee stability across versions.

### Keying is not free

There is some performance overhead to keying your stream.
- invocation of the keying function
- invocation of the distribution function
- the size of the key

However all of these are most likely to be negligible for most programs. One rather large overhead though can be the distribution of messages across networks. This is a necessary evil we must accept to have correct distributed programs. Therefore it is a good idea to reduce message volume and size before keying when possible, for example via filtering.
In some cases you may not even need key your stream, for example the [Kafka Source](./Kafka.md) already outputs a keyed stream.

### Skewed Keyspace

When keying our streams we usually aim for an evenly distributed keyspace, meaning each key has about the same message volume and in turn each worker receives about the same amount of data. Unfortunately in the real world we must often deal with "skewed" keyspaces, meaning a few keys receive vastly more data than others. This can then lead to some of our processors being overloaded, while others are underutilised.
If you know you are dealing with a skewed keyspace, it may be worthwhile to write a custom distribution function. For example if you know the key `baz` while have many more messages than the others, your function could look like this:

```rust
fn distribute(key: &str, workers: Vec<WorkerId>) -> WorkerId {
	if key == "baz" {
		workers[0] // <-- this worker only gets "baz" messages
	} else {
		rendezvous_select(key, workers[1:])
	}
}
```

### Rescaling

Another consideration is how your distribution function behaves when the amount of workers changes. This is because when we allocate a new worker or deallocate an existing one, we must move some program state either to or from that worker.
Why is this important? Consider this naive distributing function, where we take an integer key and use the mod to determine the receiving worker: 

```rust
|key: i32, workers: Vec<WorkerId>| workers[workers.len() % key]
```

This looks straight forward, but if we change the amount of workers (i.e. the size of the `Vec`) our output changes for **most of the total keyspace**. This in turn means most of our total application state must be redistributed, even if we only add or remove one worker, an expensive operation. Malstrom's built-in `rendezvous_select` function is designed with this in mind and minimises state redistribution.


