# TODO: The shutdown message should probably be produced at the root
# TODO: Scale Up/Down decision probably too

# IDEA:

Before the tree roots currently sits the snapshot controller.
We can extend this to be a lifecycle controller.
This component instructs the cluster to do (and when to do) the following things:
- snapshot
- load
- scale_down
- scale_up
- shutdown

This extends the message type "every" operator needs to handle to

- Data
- Barrier
- Load
- ScaleDown
- ScaleUp
- Shutdown

This is fairly complex and most of these do not matter to many operators
but the operators could just do e.g.

```rust
match msg {
    Data(d) => do_something(d),
    x => output.send(x)
}
```

Practically this design would mean, every DAG has a single root,
the lifecycle controller, with all other "user-roots" just being splits
of this stream.

# IDEA:

Maybe it would be clearer if instead of handling rescaling and distribution in
the key_on region we instead do a "distributed" region.

E.g.
```rust
stream
.distribute_by_key(Fn(&K) -> u64)
// everything in here is rescalable
.local()
// everything here is not rescalable
.build()
```

We can also adjust the `build` on the distributed region to automatically insert the local.
```rust
stream
.distribute_by_key(Fn(&K) -> u64)
// everything in here is rescalable
.build() // same as .local().build()
```

# Not reimplementing statefulmap

Statefulmap could be implemented for any Stream<K, V>.
To make this work with the distributed Stream we could turn "Stream"
into a trait, which has the ".then" and ".build" methods

# Scaling down

When the key_on ~decides to rescale it~ receives ScaleDown

- starts recording all keys it observes into whitelist immediately
- sends a message `Shutdown<WorkerId>` into its outputs

The message `Shutdown(WorkerId)` indicates to all operators on the worker with WorkerId,
that they should produce a corresponding `ShutdownAck(state_keys: Vec<K>)` containing all keys,
for which they hold state.

When the key_on receives the `ShutdownAck` messages, it adds those keys to its whitelist.
After receiving all ShutdownAck messages, key_on now has a complete view of all keys on the worker
(recorded + acknowledged keys).

Any other keys it receives, (which are neither recorded nor acknowledged) are distributed to other workers according
to the new distribution function.

Key_on now chooses one key from `whitelist`, adds it to `hold` and sends a `Final(K, HashMap(OperatorId, State))` message
for this key.

Upon receiving `Final` each operator packs its state for `K` and adds it to the HashMap under its own OperatorId.
It then forwards the Final Message.

When key_on as gotten back all `Final` messages, it removes the key `K` from `hold` and produces the message
`Acquire(K, HashMap(OperatorId, State))`

Upon receiving the Acquire Message, the exchange operator switches to the new distribution function, and routes
the Acquire Message according to this new function.

This repeats until `whitelist` is completely drained.

When whitelist is drained, the workers inputs are shut down.
When all remaining queues are drained, the worker can be terminated.

# Scaling Up

Upon receiving instruction to scale up, key_on creates function which determines, whether a given
key can stay at the worker or not:

```rust
ScaleUp {
    worker_id: WorkerId,
    can_stay: Fn(&K) -> bool
}
```

See that other document

# Shutting Down

A shutdown message is introduce into every tree root.
This shutdown message instructs all inputs to stop producing data.

Upon seeing this message, an operator shall advance its timestamp to Timestamp::MAX as soon as possible.
**An operator, whos frontier == Timestamp::MAX will never be scheduled again.