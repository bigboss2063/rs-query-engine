use crate::catalog::scalar::Scalar;
use crate::physical_plan::column_vector::ColumnVector;
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

    fn evaluate(&self, input: &RecordBatch) -> crate::error::Result<ColumnVector> {
        Ok(ColumnVector::Literal(self.literal.clone(), input.num_rows()))
    }
}
