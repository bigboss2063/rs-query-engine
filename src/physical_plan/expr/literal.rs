use crate::datatype::column_array::ColumnArray;
use crate::datatype::scalar::Scalar;
use crate::physical_plan::expr::{PhysicalExpr, PhysicalExprRef};
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub struct LiteralExpr {
    pub literal: Scalar,
}

impl LiteralExpr {
    pub fn new(literal: Scalar) -> PhysicalExprRef {
        Arc::new(Self { literal })
    }
}

impl PhysicalExpr for LiteralExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> crate::error::Result<ColumnArray> {
        Ok(ColumnArray::Literal(self.literal.clone(), input.num_rows()))
    }
}
