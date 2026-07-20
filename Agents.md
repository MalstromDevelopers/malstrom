# Structure

Prefer locality of behaviour over separation of concern

Prefer making small files. Aim for one public type/function + associated types/functions.
Do not separate out closely related things.
Example: one public function + its return type + its error type should be all in the same file
Example: One struct + its impl + the return and error types of all functions in the impl should be in the same file

For functions and types always create doc comments. Document parameters, but not the return type
Remember you can use [<type name>] in Rust to create links to other types where useful.
Doc comments should be brief.

Ideally each function which could error should have its own error type.

## Order

In files public types and functions come first.
Impl blocks come directly after the type they are for

# Libraries to use

- thiserror for error types
- tracing for logging

# Visibility

Use the lowest visibility possible i.e. in order of preference

- private
- pub(super)
- pub(crate)
- pub (only for public api)
