use crate::catalog::schema::Schema;
use crate::error::Result;
use arrow::array::RecordBatch;
use std::fmt::Debug;

/// Implement this trait to implement each data source type, such as memory, csv or Parquet
pub trait Table: Debug {
    /// Return the schema for the underlying data source
    fn schema(&self) -> &Schema;

    /// Scan the data source, selecting the specified columns
    fn scan(&self, projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>>;
}
