use super::{AggrOperator, AggrOperatorRef};
use crate::{
    datatype::{field::Field, scalar::Scalar, schema::Schema},
    error::Result,
    physical_plan::expr::{column::ColumnExpr, PhysicalExpr},
};
use arrow::{
    array::{Array, PrimitiveArray},
    datatypes::{DataType, Float64Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};

pub struct Min {
    min: Scalar,
    column: ColumnExpr,
}

impl Min {
    pub fn new(data_type: DataType, column: ColumnExpr) -> AggrOperatorRef {
        let scalar_value = match data_type {
            DataType::Int64 => Scalar::Int64(Some(i64::MAX)),
            DataType::UInt64 => Scalar::UInt64(Some(u64::MAX)),
            DataType::Float64 => Scalar::Float64(Some(f64::MAX)),
            _ => unimplemented!(),
        };
        Box::new(Self {
            min: scalar_value,
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
        for val in column.into_iter().flatten() {
            if let Scalar::$SCALARTYPE(Some(cur_min)) = $SELF.min {
                if val < cur_min {
                    $SELF.min = Scalar::$SCALARTYPE(Some(val))
                }
            }
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
            if let Scalar::$SCALARTYPE(Some(cur_min)) = $SELF.min {
                let val = column.value($IDX);
                if val < cur_min {
                    $SELF.min = Scalar::$SCALARTYPE(Some(val))
                }
            }
        }
    }};
}

impl AggrOperator for Min {
    fn to_field(&self, schema: &Schema) -> Result<Field> {
        let field = schema.field(self.column.index);
        Ok(Field::new(
            format!("MIN({})", field.name()).as_str(),
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
        todo!()
    }

    fn clear(&mut self) -> Result<()> {
        todo!()
    }
}
