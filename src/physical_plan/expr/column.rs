use crate::datatype::column_array::ColumnArray;
use crate::datatype::field::Field;
use crate::error::Result;
use crate::physical_plan::expr::{PhysicalExpr, PhysicalExprRef};
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

#[derive(Clone)]
pub struct ColumnExpr {
    pub index: usize,
}

impl ColumnExpr {
    pub fn new(index: usize) -> PhysicalExprRef {
        Arc::new(Self { index })
    }
}

impl PhysicalExpr for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        let column = input.column(self.index).clone();
        Ok(ColumnArray::Array(column))
    }

    fn to_field(&self, input: &RecordBatch) -> Result<Field> {
        Ok(Field::from(input.schema().field(self.index).clone()))
    }
}
