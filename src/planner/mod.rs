use crate::error::{Error::NoSuchColumn, Result};
use crate::logical_plan::logical_expr::AggregateFunc;
use crate::physical_plan::aggr::sum::Sum;
use crate::physical_plan::expr::alias::AliasExpr;
use crate::physical_plan::expr::column::ColumnExpr;
use crate::physical_plan::expr::literal::LiteralExpr;
use crate::{
    logical_plan::{logical_expr::LogicalExpr, logical_plan::LogicalPlan},
    physical_plan::expr::{binary::BinaryExpr, PhysicalExprRef},
};
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn create_physical_expr(
        input: &LogicalPlan,
        expr: &LogicalExpr,
    ) -> Result<PhysicalExprRef> {
        match expr {
            LogicalExpr::BinaryExpr(binary_expr) => {
                let left_expr = QueryPlanner::create_physical_expr(input, &binary_expr.left)?;
                let right_expr = QueryPlanner::create_physical_expr(input, &binary_expr.right)?;
                Ok(BinaryExpr::new(
                    left_expr,
                    binary_expr.op.clone(),
                    right_expr,
                ))
            }
            LogicalExpr::Literal(literal) => Ok(LiteralExpr::new(literal.clone())),
            LogicalExpr::Alias(alias) => Ok(AliasExpr::new(
                alias.name.clone(),
                QueryPlanner::create_physical_expr(input, &alias.expr)?,
            )),
            LogicalExpr::Column(column) => {
                for (i, field) in input.schema().fields().iter().enumerate() {
                    if field.name() == column {
                        return Ok(ColumnExpr::new(i));
                    }
                }
                Err(NoSuchColumn(format!("Column {} does not exist", column)))
            }
            LogicalExpr::AggregateFuncExpr(_) => todo!(), // Create aggregation operators directly when creating a physical aggregation plan
            _ => unimplemented!(),
        }
    }
}
