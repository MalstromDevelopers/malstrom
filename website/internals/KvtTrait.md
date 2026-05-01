# Understanding the Kvt Trait

The `Kvt` trait is an abstraction that significantly reduces generic parameter verbosity in Malstrom's operator API.

## Problem: Too Many Generic Parameters

In a streaming framework like Malstrom, operators need to work with multiple type parameters:
- **Key**: The key type for keyed streams
- **Value**: The data type being processed
- **Timestamp**: The timestamp type for event time processing

Without the `Kvt` trait, operator signatures might look like this:

```rust
fn operator<KIn, VIn, TIn, KOut, VOut, TOut, Func>(self, name: impl Into<String>, func: Func)
    -> StreamBuilder<KIn, VIn, TIn, KOut, VOut, TOut, Func>
```

This requires 6 generic parameters(!) just to express the message in and out types,
making the API verbose and hard to read.

## Solution: The Kvt Trait

The `Kvt` trait bundles these three type parameters into a single trait:

```rust
pub trait Kvt: Clone + 'static {
    type Key: MaybeKey;
    type Value: MaybeData;
    type Timestamp: MaybeTime;
}
```

This allows operator signatures to be much cleaner:

```rust
fn operator<In: Kvt, Out: Kvt, Func>(self, name: impl Into<String>, func: Func)
    -> StreamBuilder<Out>
```

## Implementations

### Tuple

The most common way to use `Kvt` is with tuple types `(K, V, T)`:

```rust
impl<K, V, T> Kvt for (K, V, T)
where
    K: MaybeKey,
    V: MaybeData,
    T: MaybeTime,
{
    type Key = K;
    type Value = V;
    type Timestamp = T;
}
```

This blanket implementation means any tuple of types that implement the appropriate marker traits automatically implements `Kvt`.

### Unit

For streams without keys, data, or timestamps, the unit type `()` implements `Kvt`:

```rust
impl Kvt for () {
    type Key = NoKey;
    type Value = NoData;
    type Timestamp = NoTime;
}
```

### Type Extraction

Operators use associated type access to extract the component types:
- `In::Key` - extracts the key type
- `In::Value` - extracts the value type
- `In::Timestamp` - extracts the timestamp type

## Examples

### Map Operator

```rust
fn map<In: Kvt, T: Data, Mapper>(self, name: impl Into<String>, mapper: Mapper)
    -> StreamBuilder<(In::Key, T, In::Timestamp)>
```

Instead of:
```rust
fn map<K, V, T, NewV, Mapper>(self, name: impl Into<String>, mapper: Mapper)
    -> StreamBuilder<(K, NewV, T)>
```

### Filter Operator

```rust
fn filter<In: Kvt, FilterFunc>(self, name: impl Into<String>, filter: FilterFunc)
    -> StreamBuilder<(In::Key, In::Value, In::Timestamp)>
```

## Benefits

1. **Reduced Verbosity**: Operators typically need only 1-2 generic parameters instead of 3-6
2. **Type Safety**: Full type safety is maintained through associated types
3. **Readability**: Operator signatures are much easier to read and understand


## Marker Traits

The `Kvt` trait uses several marker traits to constrain the associated types:

- `MaybeKey`: Marker for types that can be used as keys (or `NoKey`)
- `MaybeData`: Marker for types that can be used as data (or `NoData`)
- `MaybeTime`: Marker for types that can be used as timestamps (or `NoTime`)

These marker traits have blanket implementations for appropriate types, making them very flexible.
