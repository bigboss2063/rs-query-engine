use crate::datatype::field::Field;
use crate::datatype::schema::Schema;
use crate::error::Error::LogicalPlanError;
use crate::error::Result;
use crate::logical_plan::logical_expr::{AggregateFuncExpr, LogicalExpr};
use crate::logical_plan::logical_plan::{
    Aggregation, Join, JoinType, LogicalPlan, Projection, Selection,
};
use std::sync::Arc;

#[derive(Clone)]
/// DataFrame provides an elegant interface to create logical plans
/// A DataFrame is just an abstraction around a logical query plan and has methods to perform transformations and actions.
/// The core idea is to apply some logical plans to the current dataframe in a chained manner for easy calling.
pub struct DataFrame {
    pub plan: LogicalPlan,
}

impl DataFrame {
    pub fn new(plan: LogicalPlan) -> Self {
        Self { plan }
    }

    pub fn project(self, exprs: Vec<LogicalExpr>) -> Result<Self> {
        let mut fields = vec![];
        for expr in &exprs {
            fields.push(expr.to_field(&self.plan)?);
        }

        let schema = Schema::new(fields);

        Ok(Self {
            plan: LogicalPlan::Projection(Projection {
                input: Arc::new(self.plan),
                exprs,
                schema,
            }),
        })
    }

    pub fn select(self, expr: LogicalExpr) -> Result<Self> {
        Ok(Self {
            plan: LogicalPlan::Selection(Selection {
                input: Arc::new(self.plan),
                expr,
            }),
        })
    }

    pub fn aggregate(
        self,
        group_expr: LogicalExpr,
        aggr_expr: Vec<AggregateFuncExpr>,
    ) -> Result<Self> {
        let mut fields = vec![group_expr.to_field(&self.plan)?];
        fields.append(
            &mut aggr_expr
                .iter()
                .map(|expr| expr.to_field(&self.plan).unwrap())
                .collect::<Vec<Field>>(),
        );
        let schema = Schema::new(fields);
        Ok(Self {
            plan: LogicalPlan::Aggregation(Aggregation {
                input: Arc::new(self.plan),
                group_expr,
                aggr_expr,
                schema,
            }),
        })
    }

    pub fn join(
        self,
        right: &LogicalPlan,
        join_type: JoinType,
        on: (Vec<String>, Vec<String>),
    ) -> Result<Self> {
        if on.0.len() != on.1.len() {
            return Err(LogicalPlanError(
                "The number of columns to be joined must be the same".to_string(),
            ));
        }

        let (left_keys, right_keys) = on;
        let on = left_keys
            .into_iter()
            .zip(right_keys.into_iter())
            .collect::<Vec<(String, String)>>();

        let join_schema = self.plan.schema().join(right.schema());

        Ok(Self {
            plan: LogicalPlan::Join(Join {
                left: Arc::new(self.plan.clone()),
                on,
                right: Arc::new(right.clone()),
                join_type,
                schema: join_schema,
            }),
        })
    }

    pub fn schema(&self) -> &Schema {
        self.plan.schema()
    }

    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::Catalog;
    use crate::datatype::scalar::Scalar;
    use crate::error::Result;
    use crate::logical_plan::logical_expr::{BinaryExpr, LogicalExpr, Operator};

    #[test]
    fn create_logical_plan_by_df() -> Result<()> {
        let mut catalog = Catalog::default();
        catalog.add_csv_table("test", "data/test.csv")?;

        let df = catalog
            .get_table_df("test")?
            .select(LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(LogicalExpr::Column("age".to_string())),
                op: Operator::GtEq,
                right: Box::new(LogicalExpr::Literal(Scalar::Int64(Some(24)))),
            }))?
            .project(vec![
                LogicalExpr::Column("name".to_string()),
                LogicalExpr::Column("score".to_string()),
            ])?;

        assert_eq!(
            "Projection:\
            \n  exprs: [Column(\"name\"), Column(\"score\")]\
            \n  input:\
            \n    Selection:\
            \n      expr: BinaryExpr(BinaryExpr { left: Column(\"age\"), op: GtEq, right: Literal(Int64(Some(24))) })\
            \n      input:\
            \n        Scan:\
            \n          source_type: \"CSV file\"\
            \n          projection: None\
            \n  schema: Schema { fields: [Field { field: Field { name: \"name\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None } }, Field { field: Field { name: \"score\", data_type: Float64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None } }] }\n",
            format!("{}", df.plan)
        );

        Ok(())
    }
}
