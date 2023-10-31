use crate::datatype::column_array::ColumnArray;
use crate::datatype::field::Field;
use crate::datatype::scalar::Scalar;
use crate::error::Result;
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

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        Ok(ColumnArray::Literal(self.literal.clone(), input.num_rows()))
    }

    fn to_field(&self, _input: &RecordBatch) -> Result<Field> {
        let field = self.literal.to_field();

        Ok(Field::new(
            &self.literal.to_string(),
            field.data_type().clone(),
            false,
        ))
    }
}
