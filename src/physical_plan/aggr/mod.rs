use arrow::array::{BooleanArray, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Int64Type, UInt64Type};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::vec;

use crate::datatype::scalar::Scalar;
use crate::datatype::{field::Field, schema::Schema};
use crate::error::Result;
use crate::util::concat_batches;

use super::expr::PhysicalExprRef;
use super::physical_plan::{PhysicalPlan, PhysicalPlanRef};

pub mod avg;
pub mod count;
pub mod max;
pub mod min;
pub mod sum;

pub trait AggrOperator {
    fn to_field(&self, schema: &Schema) -> Result<Field>;

    fn update_batch(&mut self, record_batch: &RecordBatch) -> Result<()>;

    fn update(&mut self, record_batch: &RecordBatch, i: usize) -> Result<()>;

    fn evaluate(&self) -> Result<Scalar>;

    fn clear(&mut self) -> Result<()>;
}

pub type AggrOperatorRef = Box<dyn AggrOperator>;

pub struct Aggregation {
    input: PhysicalPlanRef,
    group_expr: Option<PhysicalExprRef>,
    aggr_expr: Mutex<Vec<AggrOperatorRef>>,
    schema: Schema,
}

impl Aggregation {
    pub fn new(
        input: PhysicalPlanRef,
        group_expr: Option<PhysicalExprRef>,
        aggr_expr: Vec<AggrOperatorRef>,
        schema: Schema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            input,
            group_expr,
            aggr_expr: Mutex::new(aggr_expr),
            schema,
        })
    }
}

macro_rules! group_by_datatype {
    ($COLUMN: expr, $GROUP_BY_DT: ty, $AGGR_OPS: expr, $BATCH: expr, $SCHEMA: expr, $SCALAR_TYPE: ident) => {{
        let mut group_idxs = HashMap::<$GROUP_BY_DT, Vec<usize>>::new();

        for (idx, val) in $COLUMN.iter().enumerate() {
            if let Some(val) = val {
                if let Some(idxs) = group_idxs.get_mut(&val) {
                    idxs.push(idx);
                } else {
                    group_idxs.insert(val, vec![idx]);
                }
            }
        }

        let mut batches = vec![];

        for (val, group_idx) in group_idxs.into_iter() {
            for idx in group_idx.iter() {
                for aggr_op in $AGGR_OPS.iter_mut() {
                    aggr_op.update(&$BATCH, *idx)?;
                }
            }

            let mut arrays = vec![];

            arrays.push(Scalar::$SCALAR_TYPE(Some(val)).to_array(1));

            for aggr_op in $AGGR_OPS.iter() {
                let array = aggr_op.evaluate()?.to_array(1);
                arrays.push(array);
            }

            for aggr_op in $AGGR_OPS.iter_mut() {
                aggr_op.clear()?;
            }

            let batch = RecordBatch::try_new($SCHEMA, arrays)?;
            batches.push(batch);
        }

        let batch = concat_batches(&$SCHEMA, batches.as_slice())?;

        Ok(vec![batch])
    }};
}

impl PhysicalPlan for Aggregation {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let mut fields = vec![];

        let batches = self.input.execute()?;
        let batch = concat_batches(&Arc::new(self.schema().clone().into()), batches.as_slice())?;

        if let Some(group_expr) = &self.group_expr {
            fields.push(group_expr.to_field(&batch)?);
        }

        // Generates Schema based on aggregation operations
        let mut aggr_ops = self.aggr_expr.lock().unwrap();
        for aggr_op in aggr_ops.iter() {
            fields.push(aggr_op.to_field(self.schema())?)
        }

        let schema = Schema::new(fields);

        if self.group_expr.is_none() {
            // Since `self.aggr_expr` is wrapped in a Mutex,
            // we can obtain a mutable reference of `self.aggr_expr` after locking it
            for batch in &batches {
                for aggr_op in aggr_ops.iter_mut() {
                    aggr_op.update_batch(batch)?;
                }
            }

            let mut arrays = vec![];

            // Since no grouping is needed,
            // directly execute aggr_expr to get the result and convert to Array
            for aggr_op in aggr_ops.iter() {
                arrays.push(aggr_op.evaluate()?.to_array(1));
            }

            let record_batch = RecordBatch::try_new(Arc::new(schema.clone().into()), arrays)?;

            Ok(vec![record_batch])
        } else {
            if let Some(group_expr) = &self.group_expr {
                let column = group_expr.evaluate(&batch)?.to_array();

                match column.data_type() {
                    DataType::Int64 => {
                        let column = column
                            .as_any()
                            .downcast_ref::<PrimitiveArray<Int64Type>>()
                            .unwrap();
                        return group_by_datatype!(
                            column,
                            i64,
                            aggr_ops,
                            batch,
                            Arc::new(schema.clone().into()),
                            Int64
                        );
                    }
                    DataType::UInt64 => {
                        let column = column
                            .as_any()
                            .downcast_ref::<PrimitiveArray<UInt64Type>>()
                            .unwrap();
                        return group_by_datatype!(
                            column,
                            u64,
                            aggr_ops,
                            batch,
                            Arc::new(schema.clone().into()),
                            UInt64
                        );
                    }
                    DataType::Boolean => {
                        let column = column.as_any().downcast_ref::<BooleanArray>().unwrap();
                        return group_by_datatype!(
                            column,
                            bool,
                            aggr_ops,
                            batch,
                            Arc::new(schema.clone().into()),
                            Boolean
                        );
                    }
                    DataType::Utf8 => {
                        let mut group_idxs = HashMap::<&str, Vec<usize>>::new();
                        let column = column.as_any().downcast_ref::<StringArray>().unwrap();
                        for (idx, val) in column.iter().enumerate() {
                            if let Some(val) = val {
                                if let Some(idxs) = group_idxs.get_mut(&val) {
                                    idxs.push(idx);
                                } else {
                                    group_idxs.insert(val, vec![idx]);
                                }
                            }
                        }

                        let mut batches = vec![];

                        for (val, group_idx) in group_idxs.into_iter() {
                            for idx in group_idx.iter() {
                                for aggr_op in aggr_ops.iter_mut() {
                                    aggr_op.update(&batch, *idx)?;
                                }
                            }

                            let mut arrays = vec![];

                            arrays.push(Scalar::Utf8(Some(val.to_string())).to_array(1));

                            for aggr_op in aggr_ops.iter() {
                                let array = aggr_op.evaluate()?.to_array(1);
                                arrays.push(array);
                            }

                            for aggr_op in aggr_ops.iter_mut() {
                                aggr_op.clear()?;
                            }

                            let batch =
                                RecordBatch::try_new(Arc::new(schema.clone().into()), arrays)?;
                            batches.push(batch);
                        }

                        let batch =
                            concat_batches(&Arc::new(schema.clone().into()), batches.as_slice())?;

                        return Ok(vec![batch]);
                    }
                    _ => unimplemented!(),
                }
            }

            Ok(vec![])
        }
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {

    use arrow::util::pretty;

    use crate::{
        datasource::csv_table::CSVTable,
        error::Result,
        logical_plan::logical_expr::Operator,
        physical_plan::{
            expr::{binary::BinaryExpr, column::ColumnExpr, literal::LiteralExpr},
            scan::Scan,
        },
    };

    use super::{avg::Avg, count::Count, max::Max, min::Min, sum::Sum, *};

    #[test]
    fn test_aggregation() -> Result<()> {
        let source = CSVTable::try_create_table("data/test.csv")?;

        let scan = Scan::new(source.clone(), None);

        let group_expr = BinaryExpr::new(
            ColumnExpr::new(2),
            Operator::LtEq,
            LiteralExpr::new(Scalar::Int64(Some(24))),
        );

        let column = ColumnExpr::new(3);
        let column = column.as_any().downcast_ref::<ColumnExpr>().unwrap();

        let max = Max::new(DataType::Float64, column.clone());
        let min = Min::new(DataType::Float64, column.clone());
        let count = Count::new(column.clone());
        let avg = Avg::new(DataType::Float64, column.clone());
        let sum = Sum::new(DataType::Float64, column.clone());

        let aggregation = Aggregation::new(
            scan,
            Some(group_expr),
            vec![max, min, count, avg, sum],
            source.schema().clone(),
        );

        let batch = aggregation.execute()?;

        pretty::print_batches(&batch)?;

        Ok(())
    }
}
