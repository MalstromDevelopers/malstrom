# TTL Map Operator

The TTL (Time-to-Live) Map Operator is a stateful operator in Malstrom that provides automatic state cleanup based on time-to-live values. This operator is particularly useful when you need to maintain state that should expire after a certain period of time.

## How It Works

The TTL Map Operator uses an `ExpireMap` to store state with expiration times. When an epoch reaches the operator, all state values whose expiry time is less than or equal to the epoch value are automatically removed from state.

The operator applies a transforming function to every message, giving the function access to the state belonging to that message's key. The function can either return a new state or `None` to indicate that the state for that key need not be retained.

## Key Features

1. **Automatic State Cleanup**: State is automatically cleaned up when epochs advance
2. **Keyed State**: State is partitioned by key, just like other stateful operators in Malstrom
3. **Flexible State Types**: Any state type can be used as long as it implements `Default`, `Serialize`, and `DeserializeOwned` traits
4. **Expiration Control**: You control the expiration time for each state entry

## Basic Usage

Here's a simple example that demonstrates how to use the TTL Map Operator:

```rust
use malstrom::operators::TtlMap;
use expiremap::ExpireMap;

stream
    .key_local("key-local", |x| x.some_key)
    .ttl_map(
        "ttl-operator",
        |key, value, timestamp, mut state: ExpireMap<String, i32, usize>| {
            // Get existing value or default to 0
            let current = state.get(&"counter".to_string()).unwrap_or(&0);
            let new_value = current + value;

            // Update state with new value and TTL
            state.insert("counter".to_string(), new_value, timestamp + 10);

            // Return the new value and updated state
            (new_value, Some(state))
        }
    );
```

## Example: Running Total with TTL

This example shows how to calculate a running total that resets when the TTL expires:

<<< @../../malstrom-core/examples/ttl_map_example.rs#build_running_total_dataflow

## Example: Sliding Window Concatenation

This example demonstrates how to maintain a sliding window of recent values:

<<< @../../malstrom-core/examples/ttl_map_example.rs#build_sliding_window_dataflow

## State Expiration

The key feature of the TTL Map Operator is its automatic state cleanup. When an epoch reaches the operator, the `on_epoch` method is called, which:

1. Iterates through all state entries
2. Removes entries whose expiration time is less than or equal to the current epoch
3. Keeps only non-empty state maps

This ensures that your state doesn't grow indefinitely and stale data is automatically cleaned up.

## When to Use TTL Map

Consider using the TTL Map Operator when:

- You need to maintain state that should expire after a certain time
- You want automatic cleanup of stale state
- You're implementing sliding window operations
- You need to track recent activity with automatic expiration
- You're building caches with time-based invalidation

## Comparison with Stateful Map

The TTL Map Operator is similar to the regular `stateful_map` operator but with automatic state cleanup. If you don't need TTL-based expiration, the regular `stateful_map` might be more appropriate.