use super::{PhysicalExpr, PhysicalExprRef};
use crate::datatype::column_array::ColumnArray;
use crate::datatype::field::Field;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::any::Any;
use std::sync::Arc;

pub struct AliasExpr {
    name: String,
    expr: PhysicalExprRef,
}

impl AliasExpr {
    pub fn new(name: String, expr: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(Self { name, expr })
    }
}

impl PhysicalExpr for AliasExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        let column = self.expr.evaluate(input)?.to_array();
        Ok(ColumnArray::Array(column))
    }

    fn to_field(&self, input: &RecordBatch) -> Result<Field> {
        let column = self.expr.evaluate(input)?.to_array();
        Ok(Field::new(&self.name, column.data_type().clone(), false))
    }
}
