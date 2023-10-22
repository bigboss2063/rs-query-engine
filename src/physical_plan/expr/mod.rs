mod binary;
mod column;
mod literal;

use crate::error::Result;
use crate::physical_plan::column_vector::ColumnVector;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

type PhysicalExprRef = Arc<dyn PhysicalExpr>;

pub trait PhysicalExpr {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnVector>;
}
