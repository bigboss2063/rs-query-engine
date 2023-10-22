use std::io;

use arrow::error::ArrowError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    NoSuchField,
    ArrowError(ArrowError),
    IOError(io::Error),
    NoSuchTable(String),
    PlanError(String),
    LogicalPlanError(String),
    IntervalError(String),
}

impl From<ArrowError> for Error {
    fn from(arrow_error: ArrowError) -> Self {
        Error::ArrowError(arrow_error)
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IOError(value)
    }
}
