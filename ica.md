#

@ DAMION 
THIS ACTUALLY WORKS, BUT ONLY WHEN WE COMBINE THE
WHITELIST/HOLD APPROACH WITH THE REGIONS
Bottom-up? Top-down? All at once?
Each key_by is a new region. We go region by region, building whitelist/hold for
every region.

Instead of regions, do per operator, like megaphone, lol

```rust
.region_start::<(topic, partition)>()
    .source(KafkaSource)
    .key_by::<(topic, partition), String>() // all state this holds must be keyed by (topic, partition)
.region_end::<(topic, partition)>()
.region_start::<String>()
    .stateful_map(...) // all state this holds must be keyed by String
    .exchange_by_key::<String, String>() // logically distinct strings, see below
.region_end()
.region_start()
    .stateful_map()
.region_end()
```

Whitelist/Hold is in the .region_end
An empty Whitelist/Hold redirects **ALL** traffic

# The ICA rescaling algorithm

I: Interrogate
C: Collect
A: Acquire

Rescalable region
K: KeyType
- region_start<K>
- operator
- opertator
- ...
- region_end<K>
- -region_start<K'>
- ...

## Phase 1: Interrogate
The region start does not know which keys exist in the region,
since keys may be arbitrarily created or destroyed.

The region_start sends the message `Interrogate`.
This message has a method to register keys `add_keys(&[K])`.

- Each operator must call `add_keys` with all keys for which it holds state
- Each operator must pass along the `Interrogate` message
- Operators which may still create new keys during the rescale phase (i.e. sources),
  MUST hold onto a copy or reference of the `Interrogate` message.
  Whenever the operator create a new key, it must call `add_keys` again, with this
  key.

The region_start has knowledge about all keys with which `add_keys` has been called.
**Implementation**: `add_keys` could be implemented via a shared HashSet or channel.

Result: region_start now knows about all keys in the region, and will be informed
whenever a key is added

## Phase 2: Collect

The region_start chooses a key `K` which for the new scale will not be located in its
region anymore.

It sends the message `Collect(K)`. `Collect(K)` has a method `add_state(operator_id, state)`.

- Each operator must call `add_state` with its own operator_id and its state for `K`
- Each operator must pass along the `Collect` message
- Operators which may create keys (i.e. sources) MUST NOT emit the key `K` anymore

| NOTE: It is important to understand the reasoning for this last rule: `K` must not be emitted
anymore, because, and only for this reason, there must not be anything affecting the state of an
operator after it has been collected. It follows therefore, that operators can actually break this
rule, as long as they can guarantee, that continuing to emit `K` will not affect any local state.
We will see applications of this below.

**Implementation:** A HashMap<OperatorId, State>
**Edge Case:** Operators with multiple input stream may see multiple instance of `Collect`
**Solution:** Operators should call `add_state` on each instance with the newest available state.
Older states should be overwritten. The region should reference count `Collect`, to determine
when all instances have been processed.

Result: region_start now has the entire region state for a key `K`.

## Phase 3: Acquire

The region_start takes the collected state for key `K` and sends it to the region_start at new
worker via a deterministic distribution function.
Upon receiving the state, the region_start at the new worker sends a message `Acquire(K)` which
has a method `take_state(operator_id)`.
Upon receiving the `Acquire` each operator may take and apply its state.

**Edge Case:** Operators with multiple input stream may see multiple instance of `Acquire`
**Solution:** Operators should call `take_state` on each instance. The first call should remove
the state from `Acquire` such that following invocations will return `None`. The operator then
MUST not apply `None` as its state (obviously).

## General Edge Cases

The phases repeat until all keys are drained.
**Edge case:** A source in the region may keep continously creating new keys. In this case the rescale
would never finish.
**Solution:**: ???

**Edge case:** A `key_by` function, which produces keys from incoming messages via a function, may not be
able to guarantee, that it can cease producing the key `K`
**Solution:** The `key_by` function IS the region_start. It holds messages which would produce `K` in a queue
until it has obtained `Acquire(K)`. I then sends `Acquire(K)` and then the held messages.
This maintains causal order between messages and state.

## Example

The following example is in Rust-esque pseudo code:

Dataflow
```rust
let stream = Jetstream::new()
    .key_by()
    .source(KafkaSource::new())
```
The first `key_by` is attached to an empty stream. We call this a "virtual key_by", since it does not actually get any
input messages to key. Its sole purpose is creating a region for redistributable state.
Real JetStream code hides this fact from you. In reality you would merely write
`let stream = JetStream::new().stateful_source(KafkaSource::new())`

The actual keys of this region, must be useful for redistribution. In the case of a KafkaSource a useful
key would be the `(topic, partition)` tuple, since the task of reading a given partition in a given topic
can be sensibly moved to a different worker.

However, (topic, partition) is not necessarily a useful keying for users.
Let's assume we get a stream of chat messages from Kafka, where each chat message is a tuple of
`(sender, message)`.
We want to store a history per sender as state, therefore we will re-key our stream:

```rust
let stream = stream
    .key_by::<(topic, partition), String>(|msg| msg.0)
    .stateful_map(...)
```

This is simple code on the surface, but we have actually done something fairly complex here:
We have opened up a new region for redistributable state.
If we were to spell out the code in a more verbose form, it could look like this:

```rust
let stream: JetStream<NoKey, NoData> = Jetstream::new()
stream
.region_start()
.virtual_key_by::<NoKey, (topic, partition)>(|_| unimplemented!())
.source(KafkaSource::new())
.key_by::<(topic, partition), String>(|msg| msg.0)
.region_end()
.region_start()
.stateful_map(...)
.region_end()
```

Note how the second call to `key_by` is *before* the first region end. Why is this?
Recall our possible edge case from before:

| A `key_by` function, which produces keys from incoming messages via a function, may not be
able to guarantee, that it can cease producing the key `K`

By structuring our regions this way, we have circumvented this problem:
When the first region wants to rescale, it will ask all operators
to cease emitting messages of a certain `(topic, partition)` combination via the `Collect` message.

For the Kafka input this is simple: It merely stops reading that partition.
For the key_by **this is even simpler**! Since it creates keys of type `String`, it can not possibly
create any more keys of that `(topic, partition)` combination.

If you are very attentive, you may yet spot another edge case.
Suppose we have a function

```rust
.key_by::<String, String>()
```

Which takes String keys and outputs String keys. This does at first glance seem like it breaks our assertion from the
previous paragraph, but these two String types, while being the same data type, are actually **logically different**.
To illustrate this, let's call them `StringA` and `StringB`

```rust
.key_by::<StringA, StringB>()
```

All operators follwing this `key_by` partition their state by type `StringB`, however the only operator
following `key_by` *in the same region* is the `region_end` itself.
Since none of the ICA messages crosses region boundaries, `key_by` need not stop emitting any keys, since there
is guaranteed to never be state for these keys in the region.

## Dealing with Distribution operators

JetStream provides a function called `distribute_by_key` to exchange data messages between workers.
The operator `distribute_by_key` emits any messages it receives from other workers downstream.

You may wonder how this function does not break our rescaling algorithm, as it has an obvious issue;
While the KafkaSource can choose to stop reading a specific `topic`, the `distribute_by_key` function
can not so easily stop receiving specific keys, as it does not know the key of a message, before
receiving it.
We could in theory cease receiving messages altogether, but this would result in blocking large parts
of the execution and could result in unacceptable latency.
Maybe we could coordinate between workers, to synchronize the rescaling process and delay emitting
the `Collect(K)` message downstream, until all other workers have promised us to not send `K` anymore,
but that sounds awfully complicated.

There is a much simpler solution. Recall our case from earlier; a `key_by` with identical input and output types:

```rust
.key_by::<String, String>()
```

Remember how these are actually distinct *logical types*: `StringA` and `StringB` and we solved our issue by
placing `region_end` directly after the `key_by` such that no state *in the region* could possibly be keyed on
`StringB`.
Guess what, we will do it again! This means our code (in very verbose form) is actually

```rust
.region_start()
.key_by(|x| do_something(x))
.region_end()
.region_start()
.distribute_by_key(|key| partition_somehow(key))
.region_end()
.region_start()
.some_other_logic(...)
....
```

In the real world the `key_by().distribute_by_key()` is so common, that in real JetStream code it can be written simply as
`stream.key_distribute()`.


## The actual procedure

- start all the regions at the same time
- apply "shutdown pressure" from the top