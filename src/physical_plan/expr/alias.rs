use super::{PhysicalExpr, PhysicalExprRef};
use crate::datatype::column_array::ColumnArray;
use crate::datatype::field::Field;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub struct Alias {
    name: String,
    index: usize,
}

impl Alias {
    pub fn new(name: String, index: usize) -> PhysicalExprRef {
        Arc::new(Self { name, index })
    }
}

impl PhysicalExpr for Alias {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        let column = input.column(self.index).clone();
        Ok(ColumnArray::Array(column))
    }

    fn to_field(&self, input: &RecordBatch) -> Result<Field> {
        let field = input.schema().field(self.index).clone();
        Ok(Field::new(&self.name, field.data_type().clone(), false))
    }
}
