use crate::datatype::schema::Schema;
use crate::error::Result;
use crate::physical_plan::{
    expr::PhysicalExprRef,
    physical_plan::{PhysicalPlan, PhysicalPlanRef},
};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder, StringArray,
    StringBuilder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::DataType;
use arrow::{
    array::{Array, BooleanArray},
    record_batch::RecordBatch,
};
use std::{sync::Arc, vec};

pub struct Selection {
    pub input: PhysicalPlanRef,
    pub expr: PhysicalExprRef,
}

impl Selection {
    pub fn new(input: PhysicalPlanRef, expr: PhysicalExprRef) -> PhysicalPlanRef {
        Arc::new(Self { input, expr })
    }
}

macro_rules! build_array_by_predicate {
    ($COLUMN: expr, $PREDICATE: expr, $ARRAY_TYPE: ty, $ARRAY_BUILDER: ty, $TYPE: ty) => {{
        let array = $COLUMN.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let mut builder = <$ARRAY_BUILDER>::new(array.len());
        let iter = $PREDICATE.iter().zip(array.iter());
        for (valid, val) in iter {
            match valid {
                Some(valid) => {
                    if valid {
                        builder.append_option(val)?;
                    }
                }
                None => builder.append_option(None::<$TYPE>)?,
            }
        }
        Arc::new(builder.finish())
    }};
}

impl PhysicalPlan for Selection {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn execute(&self) -> Result<Vec<RecordBatch>> {
        let input = self.input.execute()?;

        let predicates = input
            .iter()
            .map(|record_batch| self.expr.evaluate(record_batch).unwrap())
            .collect::<Vec<_>>()
            .iter()
            .map(|column_array| column_array.clone().to_array())
            .collect::<Vec<_>>();
        let predicates = predicates
            .iter()
            .map(|array| array.as_any().downcast_ref::<BooleanArray>().unwrap())
            .collect::<Vec<_>>();

        let mut batches = vec![];

        for (i, record_batch) in input.iter().enumerate() {
            let mut columns = vec![];
            for column in record_batch.columns() {
                let data_type = column.data_type();
                let array_ref: ArrayRef = match data_type {
                    DataType::Boolean => build_array_by_predicate!(
                        column,
                        &predicates[i],
                        BooleanArray,
                        BooleanBuilder,
                        bool
                    ),
                    DataType::Int64 => build_array_by_predicate!(
                        column,
                        &predicates[i],
                        Int64Array,
                        Int64Builder,
                        i64
                    ),
                    DataType::UInt64 => build_array_by_predicate!(
                        column,
                        &predicates[i],
                        UInt64Array,
                        UInt64Builder,
                        u64
                    ),
                    DataType::Float64 => build_array_by_predicate!(
                        column,
                        &predicates[i],
                        Float64Array,
                        Float64Builder,
                        f64
                    ),
                    DataType::Utf8 => build_array_by_predicate!(
                        column,
                        &predicates[i],
                        StringArray,
                        StringBuilder,
                        &str
                    ),
                    _ => unimplemented!(),
                };
                columns.push(array_ref);
            }
            let record_batch =
                RecordBatch::try_new(Arc::new(self.schema().clone().into()), columns)?;
            batches.push(record_batch);
        }

        Ok(batches)
    }

    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![self.input.clone()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::csv_table::CSVTable;
    use crate::datatype::scalar::Scalar;
    use crate::logical_plan::logical_expr::Operator;
    use crate::physical_plan::expr::binary::BinaryExpr;
    use crate::physical_plan::expr::column::ColumnExpr;
    use crate::physical_plan::expr::literal::LiteralExpr;
    use crate::physical_plan::projection::Projection;
    use crate::physical_plan::scan::Scan;
    use arrow::array::{ArrayRef, StringArray};
    use arrow::util;

    #[test]
    fn test_selection() -> Result<()> {
        let source = CSVTable::try_create_table("data/test.csv")?;
        let schema = Schema::new(vec![
            source.schema().field(1).clone(),
            source.schema().field(3).clone(),
        ]);

        let scan = Scan::new(source, None);

        let sub_expr = BinaryExpr::new(
            ColumnExpr::new(3),
            Operator::Sub,
            LiteralExpr::new(Scalar::Float64(Some(1f64))),
        );

        let expr = vec![ColumnExpr::new(1), sub_expr];

        let projection = Projection::new(scan, schema, expr);

        let expr = BinaryExpr::new(
            ColumnExpr::new(1),
            Operator::Gt,
            LiteralExpr::new(Scalar::Float64(Some(0f64))),
        );

        let selection = Selection::new(projection, expr);

        let res = selection.execute()?;

        assert_eq!(res.len(), 1);
        let batch = &res[0];

        util::pretty::print_batches(&res).unwrap();

        let name_excepted: ArrayRef = Arc::new(StringArray::from(vec![
            "Vincent Hu",
            "KamenRider",
            "nutswalker",
            "Brian",
        ]));

        let score_excepted: ArrayRef =
            Arc::new(Float64Array::from(vec![99f64, 98.99, 98.98, 98.97]));

        assert_eq!(batch.column(0), &name_excepted);
        assert_eq!(batch.column(1), &score_excepted);

        Ok(())
    }
}
