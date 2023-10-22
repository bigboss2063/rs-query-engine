use crate::datatype::schema::Schema;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::fmt::Debug;
use std::sync::Arc;

pub type TableRef = Arc<dyn Table>;

/// Implement this trait to implement each data source type, such as memory, csv or Parquet
pub trait Table: Debug {
    /// Return the schema for the underlying data source
    fn schema(&self) -> &Schema;

    /// Scan the data source, selecting the specified columns
    fn scan(&self, projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>>;

    /// Returns the type of data source
    fn source_type(&self) -> String;
}
