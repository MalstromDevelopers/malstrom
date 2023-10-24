from typing import Callable, TypeVar

# Timestamp Type
T = TypeVar("T")
# Data type
D = TypeVar("D")
# State Type
P = TypeVar("P")

def persistent_source(
        scope,
        state_backend,
        logic: Callable
) -> Stream[tuple[D, P]]:
    ...


def sample_logic(state: P, advance_to: Callable[[T], None]) -> tuple[D, P]:
    # do something with the state

    # optionally advance the time
    advance_to(...)

    # return some data and the new state
    return (..., ...)


# persistent_sink
# A sink which tracks its deliveries in a state to ensure
# at least once semantics across restarts
def persistent_sink(self: Stream, state_backend, logic) -> None:
    ...