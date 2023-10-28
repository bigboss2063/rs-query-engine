use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::{DataType, Float64Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};

use crate::{
    datatype::{field::Field, scalar::Scalar, schema::Schema},
    error::Result,
    physical_plan::expr::{column::ColumnExpr, PhysicalExpr},
};

use super::{AggrOperator, AggrOperatorRef};

pub struct Avg {
    count: i64,
    sum: Scalar,
    column: ColumnExpr,
}

impl Avg {
    pub fn new(data_type: DataType, column: ColumnExpr) -> AggrOperatorRef {
        let scalar_value = match data_type {
            DataType::Int64 => Scalar::Int64(Some(0i64)),
            DataType::UInt64 => Scalar::UInt64(Some(0u64)),
            DataType::Float64 => Scalar::Float64(Some(0f64)),
            _ => unimplemented!(),
        };

        Box::new(Self {
            count: 0,
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
                $SELF.count += 1;
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
                $SELF.count += 1;
            }
        }
    }};
}

macro_rules! avg {
    ($COUNT: expr, $SUM: expr) => {
        ($SUM as f64 / $COUNT as f64)
    };
}

impl AggrOperator for Avg {
    fn to_field(&self, schema: &Schema) -> Result<Field> {
        let field = schema.field(self.column.index);
        Ok(Field::new(
            format!("AVG({})", field.name()).as_str(),
            DataType::Float64,
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
        let avg = match self.sum {
            Scalar::Int64(Some(sum)) => avg!(self.count, sum),
            Scalar::UInt64(Some(sum)) => avg!(self.count, sum),
            Scalar::Float64(Some(sum)) => avg!(self.count, sum),
            _ => unimplemented!(),
        };

        Ok(Scalar::Float64(Some(avg)))
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
