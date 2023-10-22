use crate::datatype::column_array::ColumnArray;
use crate::datatype::scalar::Scalar;
use crate::physical_plan::expr::PhysicalExpr;
use arrow::record_batch::RecordBatch;
use std::any::Any;

pub struct LiteralExpr {
    literal: Scalar,
}

impl PhysicalExpr for LiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> crate::error::Result<ColumnArray> {
        Ok(ColumnArray::Literal(self.literal.clone(), input.num_rows()))
    }
}
