pub mod binary;
pub mod column;
pub mod literal;

use crate::datatype::column_array::ColumnArray;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub type PhysicalExprRef = Arc<dyn PhysicalExpr>;

pub trait PhysicalExpr {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray>;
}
