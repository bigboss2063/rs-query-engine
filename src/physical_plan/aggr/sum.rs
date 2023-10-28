use crate::datatype::field::Field;
use crate::datatype::scalar::Scalar;
use crate::datatype::schema::Schema;
use crate::error::Result;
use crate::physical_plan::aggr::{AggrOperator, AggrOperatorRef};
use crate::physical_plan::expr::column::ColumnExpr;
use crate::physical_plan::expr::PhysicalExpr;
use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;

pub struct Sum {
    sum: Scalar,
    column: ColumnExpr,
}

impl Sum {
    pub fn new(data_type: DataType, column: ColumnExpr) -> AggrOperatorRef {
        let scalar_value = match data_type {
            DataType::Int64 => Scalar::Int64(Some(0i64)),
            DataType::UInt64 => Scalar::UInt64(Some(0u64)),
            DataType::Float64 => Scalar::Float64(Some(0f64)),
            _ => unimplemented!(),
        };

        Box::new(Self {
            sum: scalar_value,
            column,
        })
    }
}

macro_rules! update_batch {
    ($COLUMN: expr, $DT: ty, $SCALARTYPE: ident, $SELF: expr) => {{
        let column = $COLUMN
            .as_any()
            .downcast_ref::<PrimitiveArray<$DT>>()
            .unwrap();
        if let Scalar::$SCALARTYPE(Some(mut sum)) = $SELF.sum {
            for val in column.into_iter().flatten() {
                sum += val;
            }
            $SELF.sum = Scalar::$SCALARTYPE(Some(sum));
        }
    }};
}

macro_rules! update {
    ($COLUMN: expr, $DT: ty, $SCALARTYPE: ident, $SELF: expr, $IDX: expr) => {{
        let column = $COLUMN
            .as_any()
            .downcast_ref::<PrimitiveArray<$DT>>()
            .unwrap();
        if !column.is_null($IDX) {
            if let Scalar::$SCALARTYPE(Some(mut sum)) = $SELF.sum {
                sum += column.value($IDX);
                $SELF.sum = Scalar::$SCALARTYPE(Some(sum));
            }
        }
    }};
}

impl AggrOperator for Sum {
    fn to_field(&self, schema: &Schema) -> Result<Field> {
        let field = schema.field(self.column.index);
        Ok(Field::new(
            format!("SUM({})", field.name()).as_str(),
            field.data_type().clone(),
            false,
        ))
    }

    fn update_batch(&mut self, record_batch: &RecordBatch) -> Result<()> {
        let column = self.column.evaluate(record_batch)?.to_array();

        match column.data_type() {
            DataType::Int64 => update_batch!(column, Int64Type, Int64, self),
            DataType::UInt64 => update_batch!(column, UInt64Type, UInt64, self),
            DataType::Float64 => update_batch!(column, Float64Type, Float64, self),
            _ => unimplemented!(),
        }

        Ok(())
    }

    fn update(&mut self, record_batch: &RecordBatch, i: usize) -> Result<()> {
        let column = self.column.evaluate(record_batch)?.to_array();

        match column.data_type() {
            DataType::Int64 => update!(column, Int64Type, Int64, self, i),
            DataType::UInt64 => update!(column, UInt64Type, UInt64, self, i),
            DataType::Float64 => update!(column, Float64Type, Float64, self, i),
            _ => unimplemented!(),
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<Scalar> {
        Ok(self.sum.clone())
    }

    fn clear(&mut self) -> Result<()> {
        match self.sum {
            Scalar::Int64(_) => self.sum = Scalar::Int64(Some(0i64)),
            Scalar::UInt64(_) => self.sum = Scalar::UInt64(Some(0u64)),
            Scalar::Float64(_) => self.sum = Scalar::Float64(Some(0f64)),
            _ => unimplemented!(),
        }

        Ok(())
    }
}
