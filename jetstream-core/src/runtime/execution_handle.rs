/// The ExecutionHandle is the very final result of build a JetStream worker and runtime.
/// This type is, what is ulitmately presented to the user to start the execution of all
/// dataflows.
pub trait ExecutionHandle {
    fn execute(self) -> Result<(), ExecutionError>;

    fn step() {
        panic!("Remove this function. It is only here to allow me to compile for now")
    }
}

/// Error to be returned if the Execution could not be started or failed.
#[derive(thiserror::Error, Debug)]
#[error(transparent)]
pub struct ExecutionError(#[from] Box<dyn std::error::Error>);
