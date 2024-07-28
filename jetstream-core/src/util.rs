/// Panics with an error message explaining the "Early Data Error"
/// Call this if the operator encounters data before having received
/// a state load message
pub(crate) fn panic_early_data<T>() -> T {
    panic!(
        "
        Early Data Error:
        An operator encountered data to process before the initial
        state load message had reached it.
        This is a bug.
        Sources in JetStream MUST NOT produce data before the initial
        state load message has passed them, therefore it should be impossible
        for data to reach any operator before the state loads.

        If you implemented a source or exchange operator yourself, check it does
        not produce data before the state load message.
        If you believe this to be a bug in JetStream itself, please report
        it to the maintainers.
    "
    )
}
