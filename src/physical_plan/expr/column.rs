use crate::datatype::column_array::ColumnArray;
use crate::error::Result;
use crate::physical_plan::expr::PhysicalExpr;
use arrow::record_batch::RecordBatch;
use std::any::Any;

pub struct ColumnExpr {
    pub index: usize,
}

impl PhysicalExpr for ColumnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        let column = input.column(self.index).clone();
        Ok(ColumnArray::Array(column))
    }
}
