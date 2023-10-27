use super::{AggrOperator, AggrOperatorRef};
use crate::datatype::scalar::Scalar;
use crate::datatype::{field::Field, schema::Schema};
use crate::error::Result;
use crate::physical_plan::expr::column::ColumnExpr;
use crate::physical_plan::expr::PhysicalExpr;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

pub struct Count {
    count: usize,
    column: ColumnExpr,
}

impl Count {
    pub fn new(column: ColumnExpr) -> AggrOperatorRef {
        Box::new(Self { count: 0, column })
    }
}

impl AggrOperator for Count {
    fn to_field(&self, schema: &Schema) -> Result<Field> {
        let field = schema.field(self.column.index);
        Ok(Field::new(
            format!("COUNT({})", field.name()).as_str(),
            DataType::UInt64,
            false,
        ))
    }

    fn update_batch(&mut self, record_batch: &RecordBatch) -> Result<()> {
        let column = self.column.evaluate(record_batch)?.to_array();
        self.count += column.len() - column.null_count();
        Ok(())
    }

    fn update(&mut self, record_batch: &RecordBatch, i: usize) -> Result<()> {
        let column = self.column.evaluate(record_batch)?.to_array();
        if !column.is_null(i) {
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<Scalar> {
        Ok(Scalar::UInt64(Some(self.count as u64)))
    }

    fn clear(&mut self) -> Result<()> {
        self.count = 0;
        Ok(())
    }
}
