use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use crate::datatype::column_array::ColumnArray;
use crate::datatype::schema::Schema;
use crate::physical_plan::expr::PhysicalExprRef;
use crate::physical_plan::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;

pub struct ProjectionExpr {
    input: PhysicalPlanRef,
    schema: Schema,
    expr: Vec<PhysicalExprRef>,
}

impl ProjectionExpr {
    pub fn new(input: PhysicalPlanRef, schema: Schema, expr: Vec<PhysicalExprRef>) -> PhysicalPlanRef {
        Arc::new(Self {
            input,
            schema,
            expr,
        })
    }
}

impl PhysicalPlan for ProjectionExpr {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;

        if self.schema.fields().is_empty() {
            Ok(input)
        } else {
            let batches = input
                .iter()
                .map(|record_batch| {
                    let columns = self
                        .expr
                        .iter()
                        .map(|expr| expr.evaluate(record_batch).unwrap())
                        .collect::<Vec<ColumnArray>>()
                        .iter()
                        .map(|column_array| {
                            column_array.clone().to_array()
                        })
                        .collect::<Vec<_>>();
                    RecordBatch::try_new(SchemaRef::from(self.schema.clone()), columns).unwrap()
                })
                .collect::<Vec<_>>();
            Ok(batches)
        }
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use super::*;

    use crate::datasource::csv_table::CSVTable;
    use crate::datatype::scalar::Scalar;
    use crate::datatype::schema::Schema;
    use crate::error::Result;
    use crate::logical_plan::logical_expr::Operator;
    use crate::physical_plan::expr::binary::BinaryExpr;
    use crate::physical_plan::expr::column::ColumnExpr;
    use crate::physical_plan::expr::literal::LiteralExpr;
    use crate::physical_plan::scan::Scan;

    #[test]
    fn projection_physical_plan() -> Result<()> {
        let table = CSVTable::try_create_table("data/test.csv")?;

        let scan = Scan::new(table.clone(), None);

        let schema = Schema::new(
            vec![
                table.schema().field(0).clone(),
                table.schema().field(3).clone(),
            ]
        );

        let add_expr = BinaryExpr::new(
            ColumnExpr::new(3),
            Operator::Add,
            LiteralExpr::new(Scalar::Float64(Some(1f64))),
        );

        let column_expr = ColumnExpr::new(0);

        let expr = vec![
            column_expr,
            add_expr,
        ];

        let projection = ProjectionExpr::new(scan, schema, expr);

        let record_batch = projection.execute()?;

        assert_eq!(record_batch.len(), 1);

        let record_batch = &record_batch[0];

        assert_eq!(record_batch.column(0), &(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef));
        assert_eq!(record_batch.column(1), &(Arc::new(Float64Array::from(vec![
            1.0, 101.0, 100.99, 100.98, 100.97,
        ])) as ArrayRef));

        Ok(())
    }
}

