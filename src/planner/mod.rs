use crate::error::Error;
use crate::error::Result;
use crate::logical_plan::logical_expr::AggregateFunc::{AVG, COUNT, MAX, MIN, SUM};
use crate::physical_plan::aggr::avg::Avg;
use crate::physical_plan::aggr::count::Count;
use crate::physical_plan::aggr::max::Max;
use crate::physical_plan::aggr::min::Min;
use crate::physical_plan::aggr::sum::Sum;
use crate::physical_plan::aggr::Aggregation;
use crate::physical_plan::expr::alias::AliasExpr;
use crate::physical_plan::expr::column::ColumnExpr;
use crate::physical_plan::expr::literal::LiteralExpr;
use crate::physical_plan::expr::PhysicalExpr;
use crate::physical_plan::nested_loop_join::NestedLoopJoin;
use crate::physical_plan::physical_plan::PhysicalPlanRef;
use crate::physical_plan::projection::Projection;
use crate::physical_plan::scan::Scan;
use crate::physical_plan::selection::Selection;
use crate::{
    logical_plan::{logical_expr::LogicalExpr, logical_plan::LogicalPlan},
    physical_plan::expr::{binary::BinaryExpr, PhysicalExprRef},
};
pub struct QueryPlanner;

impl QueryPlanner {
    pub fn create_physical_plan(logical_plan: &LogicalPlan) -> Result<PhysicalPlanRef> {
        match logical_plan {
            LogicalPlan::Scan(scan) => {
                Ok(Scan::new(scan.data_source.clone(), scan.projection.clone()))
            }
            LogicalPlan::Projection(projection) => {
                let exprs = projection
                    .exprs
                    .iter()
                    .map(|expr| {
                        QueryPlanner::create_physical_expr(&projection.input, expr).unwrap()
                    })
                    .collect::<Vec<_>>();

                let input = QueryPlanner::create_physical_plan(&projection.input)?;
                Ok(Projection::new(input, projection.schema.clone(), exprs))
            }
            LogicalPlan::Selection(selection) => {
                let expr = QueryPlanner::create_physical_expr(&selection.input, &selection.expr)?;
                let input = QueryPlanner::create_physical_plan(&selection.input)?;
                Ok(Selection::new(input, expr))
            }
            LogicalPlan::Aggregation(aggreagtion) => {
                let field = aggreagtion.group_expr.to_field(&aggreagtion.input)?;

                let group_expr = QueryPlanner::create_physical_expr(
                    &aggreagtion.input,
                    &aggreagtion.group_expr,
                )?;

                let mut aggr_expr = vec![];

                for aggr_func_expr in aggreagtion.aggr_expr.iter() {
                    let column = QueryPlanner::create_physical_expr(
                        &aggreagtion.input,
                        &aggr_func_expr.expr,
                    )?;
                    let column = column.as_any().downcast_ref::<ColumnExpr>().unwrap();

                    match aggr_func_expr.func {
                        SUM => aggr_expr.push(Sum::new(field.data_type().clone(), column.clone())),
                        MIN => aggr_expr.push(Min::new(field.data_type().clone(), column.clone())),
                        MAX => aggr_expr.push(Max::new(field.data_type().clone(), column.clone())),
                        AVG => aggr_expr.push(Avg::new(field.data_type().clone(), column.clone())),
                        COUNT => aggr_expr.push(Count::new(column.clone())),
                    }
                }

                let input = QueryPlanner::create_physical_plan(&aggreagtion.input)?;

                Ok(Aggregation::new(input, Some(group_expr), aggr_expr))
            }
            LogicalPlan::Join(join) => {
                let left = QueryPlanner::create_physical_plan(join.left.as_ref())?;
                let right = QueryPlanner::create_physical_plan(join.right.as_ref())?;

                let mut on = vec![];

                for (left_col, right_col) in join.on.iter() {
                    let mut left_idx = -1;
                    let mut right_idx = -1;

                    for (i, field) in left.schema().fields().iter().enumerate() {
                        if field.name() == left_col {
                            left_idx = i as i64;
                        }
                    }

                    for (i, field) in right.schema().fields().iter().enumerate() {
                        if field.name() == left_col {
                            right_idx = i as i64;
                        }
                    }

                    if left_idx == -1 {
                        return Err(Error::NoSuchColumn(format!(
                            "Column {} does not exist",
                            left_col
                        )));
                    }

                    if right_idx == -1 {
                        return Err(Error::NoSuchColumn(format!(
                            "Column {} does not exist",
                            right_col
                        )));
                    }

                    on.push((
                        ColumnExpr::new(left_idx as usize)
                            .as_any()
                            .downcast_ref::<ColumnExpr>()
                            .unwrap()
                            .clone(),
                        ColumnExpr::new(right_idx as usize)
                            .as_any()
                            .downcast_ref::<ColumnExpr>()
                            .unwrap()
                            .clone(),
                    ))
                }

                Ok(NestedLoopJoin::new(left, right, on, join.schema.clone()))
            }
        }
    }

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
                Err(Error::NoSuchColumn(format!(
                    "Column {} does not exist",
                    column
                )))
            }
            LogicalExpr::AggregateFuncExpr(_) => todo!(), // Create aggregation operators directly when creating a physical aggregation plan
            _ => unimplemented!(),
        }
    }
}
