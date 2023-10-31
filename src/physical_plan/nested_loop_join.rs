use std::sync::Arc;
use std::vec;

use super::expr::PhysicalExpr;
use super::{
    expr::column::ColumnExpr,
    physical_plan::{PhysicalPlan, PhysicalPlanRef},
};
use crate::error::Result;
use crate::util::concat_batches;
use crate::{datatype::schema::Schema, error};

use arrow::array::{Array, Int64Builder, PrimitiveArray};
use arrow::compute;
use arrow::datatypes::Int64Type;
use arrow::record_batch::RecordBatch;

pub struct NestedLoopJoin {
    left: PhysicalPlanRef,
    right: PhysicalPlanRef,
    on: Vec<(ColumnExpr, ColumnExpr)>,
    schema: Schema,
}

impl NestedLoopJoin {
    pub fn new(
        left: PhysicalPlanRef,
        right: PhysicalPlanRef,
        on: Vec<(ColumnExpr, ColumnExpr)>,
        schema: Schema,
    ) -> PhysicalPlanRef {
        Arc::new(Self {
            left,
            right,
            on,
            schema,
        })
    }
}

impl PhysicalPlan for NestedLoopJoin {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        if self.on.is_empty() {
            return Err(error::Error::PhysicalPlanError(
                "`on` cannot be empty when executing nested loop join".to_string(),
            ));
        }

        let outer_table = self.left.execute()?;
        let outer_table =
            concat_batches(&Arc::new(self.left.schema().clone().into()), &outer_table)?;

        let inner_table = self.right.execute()?;
        let inner_table =
            concat_batches(&Arc::new(self.right.schema().clone().into()), &inner_table)?;

        let mut outer_pos = Int64Builder::new(0);
        let mut inner_pos = Int64Builder::new(0);

        let mut left_flags = Vec::<Vec<bool>>::new();
        let mut right_flags = Vec::<Vec<bool>>::new();

        for (i, (left_col, right_col)) in self.on.iter().enumerate() {
            let left_col = left_col.evaluate(&outer_table)?.to_array();
            let right_col = right_col.evaluate(&inner_table)?.to_array();

            left_flags.push(vec![false; left_col.len()]);
            right_flags.push(vec![false; right_col.len()]);

            if left_col.data_type() != right_col.data_type() {
                return Err(error::Error::PhysicalPlanError(
                    "Left and right types of on should match".to_string(),
                ));
            }

            let left_col = left_col
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap();
            let right_col = right_col
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap();

            for (left_pos, left_val) in left_col.iter().enumerate() {
                for (right_pos, right_val) in right_col.iter().enumerate() {
                    match (left_val, right_val) {
                        (Some(left_val), Some(right_val)) => {
                            if left_val == right_val {
                                left_flags[i][left_pos] = true;
                                right_flags[i][right_pos] = true;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        let left_column_len = left_flags[0].len();

        for left_pos in 0..left_column_len {
            let mut flag = true;
            for left_flag in left_flags.iter() {
                if !left_flag[left_pos] {
                    flag = false;
                    break;
                }
            }

            if flag {
                outer_pos.append_value(left_pos as i64)?;
            }
        }

        let right_column_len = right_flags[0].len();

        for right_pos in 0..right_column_len {
            let mut flag = false;
            for right_flag in right_flags.iter() {
                flag = right_flag[right_pos];
            }

            if flag {
                inner_pos.append_value(right_pos as i64)?;
            }
        }

        let outer_pos = outer_pos.finish();
        let inner_pos = inner_pos.finish();

        let mut columns = vec![];

        for i in 0..self.left.schema().fields().len() {
            columns.push(compute::take(outer_table.column(i), &outer_pos, None)?);
        }

        for i in 0..self.right.schema().fields().len() {
            columns.push(compute::take(inner_table.column(i), &inner_pos, None)?);
        }

        let batch = RecordBatch::try_new(Arc::new(self.schema().clone().into()), columns)?;

        Ok(vec![batch])
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.left.clone(), self.right.clone()])
    }
}

#[cfg(test)]
mod tests {

    use arrow::util::pretty;

    use super::*;
    use crate::{datasource::csv_table::CSVTable, error::Result, physical_plan::scan::Scan};

    #[test]
    fn test_nested_loop_join() -> Result<()> {
        let test_source = CSVTable::try_create_table("data/test.csv")?;
        let salary_source = CSVTable::try_create_table("data/salary.csv")?;

        let test_source_scan = Scan::new(test_source.clone(), None);
        let salary_source_scan = Scan::new(salary_source.clone(), None);

        let id_column = ColumnExpr::new(0)
            .as_any()
            .downcast_ref::<ColumnExpr>()
            .unwrap()
            .clone();

        let fields = vec![
            test_source.schema().fields().clone(),
            salary_source.schema().fields().clone(),
        ];
        let schema = Schema::new(
            fields
                .iter()
                .flatten()
                .map(|field| field.clone())
                .collect::<Vec<_>>(),
        );

        let nested_loop_join = NestedLoopJoin::new(
            test_source_scan.clone(),
            salary_source_scan.clone(),
            vec![(id_column.clone(), id_column.clone())],
            schema.clone(),
        );

        let batch = nested_loop_join.execute()?;

        pretty::print_batches(&batch)?;

        Ok(())
    }
}
