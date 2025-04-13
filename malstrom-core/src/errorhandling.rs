//! Global error handling mechanisms. Very TODO!

/// Let Malstrom handle fatal errors in the process.
/// TODO: [catch_unwind](https://doc.rust-lang.org/stable/std/panic/fn.catch_unwind.html) may be
/// the better way to do this
pub trait MalstromFatal<T, E>: Sized + sealed::Sealed {
    /// Abort the computation as gracefully as possible due to a fatal non-recoverable error.
    fn malstrom_fatal(self) -> T;
}

impl<T, E> MalstromFatal<T, E> for Result<T, E>
where
    E: std::fmt::Debug + std::error::Error + Send + Sync + 'static,
{
    fn malstrom_fatal(self) -> T {
        match self {
            Ok(x) => x,
            Err(e) => {
                let report = eyre::Report::new(e);
                panic!("{report:?}")
            }
        }
    }
}

mod sealed {
    pub trait Sealed {}

    impl<T, E> Sealed for Result<T, E> {}
}
