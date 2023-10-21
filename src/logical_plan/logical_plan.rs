use std::{sync::Arc, vec};
use crate::catalog::{schema::Schema, table::TableRef};

use super::logical_expr::{AggregateFuncExpr, LogicalExpr};

/// A logic plan is an intermediate representation generated during a query.
/// It is used to express how to execute a query to satisfy the conditions in a logical expression.
/// In the logical plan, the database query engine converts logical expressions to a series of logical operations.
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// The Scan logical plan represents fetching data from a DataSource with an optional projection.
    /// Scan is the only logical plan in our query engine that does not have another logical plan as an input.
    /// It is a leaf node in the query tree.
    Scan(Scan),
    /// The Projection logical plan applies a projection to its input.
    /// A projection is a list of expressions to be evaluated against the input data.
    Projection(Projection),
    /// The selection logical plan applies a filter expression to determine
    /// which rows should be selected (included) in its output.
    /// This is represented by the WHERE clause in SQL.
    Selection(Selection),
    /// Aggregate logical plan calculates aggregates of underlying data
    /// such as calculating minimum, maximum, averages, and sums of data.
    Aggregate(Aggregate),
    /// Join two logical plans on one or more join columns
    Join(Join),
}

impl LogicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            LogicalPlan::Scan(Scan { data_source, .. }) => data_source.schema(),
            LogicalPlan::Projection(Projection { schema, .. }) => schema,
            LogicalPlan::Selection(Selection { input, .. }) => input.schema(),
            LogicalPlan::Aggregate(Aggregate { schema, .. }) => schema,
            LogicalPlan::Join(Join { schema, .. }) => schema,
        }
    }

    pub fn children(&self) -> Vec<Arc<LogicalPlan>> {
        match self {
            LogicalPlan::Scan(_) => vec![], // Scan logical plan has no sublogical plan
            LogicalPlan::Projection(Projection { input, .. }) => vec![input.clone()],
            LogicalPlan::Selection(Selection { input, .. }) => vec![input.clone()],
            LogicalPlan::Aggregate(Aggregate { input, .. }) => vec![input.clone()],
            LogicalPlan::Join(Join { left, right, .. }) => vec![left.clone(), right.clone()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scan {
    pub data_source: TableRef,
    pub projection: Option<Vec<usize>>,
}

#[derive(Debug, Clone)]
pub struct Projection {
    pub input: Arc<LogicalPlan>,
    pub exprs: Vec<LogicalExpr>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct Selection {
    pub input: Arc<LogicalPlan>,
    pub expr: LogicalExpr,
}

#[derive(Debug, Clone)]
pub struct Aggregate {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<LogicalExpr>,
    pub aggr_expr: Vec<AggregateFuncExpr>,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub struct Join {
    pub left: Arc<LogicalPlan>,
    pub on: Vec<(String, String)>,
    pub right: Arc<LogicalPlan>,
    pub join_type: JoinType,
    pub schema: Schema,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}