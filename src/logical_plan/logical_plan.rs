use crate::datatype::schema::Schema;
use crate::datasource::table::TableRef;
use core::fmt::{Debug, Result};
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
    vec,
};

use super::logical_expr::{AggregateFuncExpr, LogicalExpr};

/// A logic plan is an intermediate representation generated during a query.
/// It is used to express how to execute a query to satisfy the conditions in a logical expr.
/// In the logical plan, the database query engine converts logical expressions to a series of logical operations.
#[derive(Clone)]
pub enum LogicalPlan {
    /// The Scan logical plan represents fetching data from a DataSource with an optional projection.
    /// Scan is the only logical plan in our query engine that does not have another logical plan as an input.
    /// It is a leaf node in the query tree.
    Scan(Scan),
    /// The Projection logical plan applies a projection to its input.
    /// A projection is a list of expressions to be evaluated against the input data.
    Projection(Projection),
    /// The selection logical plan applies a filter expr to determine
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

impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        Debug::fmt(&self, f)
    }
}

impl Debug for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        do_pretty_print(self, f, 0)
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
    CrossJoin,
}

/// Implement more friendly output for logical plan
fn do_pretty_print(plan: &LogicalPlan, f: &mut Formatter<'_>, depth: usize) -> Result {
    write!(f, "{}", "  ".repeat(depth))?;

    match plan {
        LogicalPlan::Scan(Scan {
            data_source,
            projection,
        }) => {
            writeln!(f, "Scan:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "source_type: {:?}", data_source.source_type())?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "projection: {:?}", projection)
        }
        LogicalPlan::Projection(Projection {
            exprs,
            input,
            schema,
        }) => {
            writeln!(f, "Projection:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "exprs: {:?}", exprs)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
        LogicalPlan::Selection(Selection { expr, input }) => {
            writeln!(f, "Selection:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "expr: {:?}", expr)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)
        }
        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
        }) => {
            writeln!(f, "Aggregate:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "input:")?;
            do_pretty_print(input.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "group_expr: {:?}", group_expr)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "aggr_expr: {:?}", aggr_expr)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            join_type,
            schema,
        }) => {
            writeln!(f, "Join:")?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "left:")?;
            do_pretty_print(left.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "right:")?;
            do_pretty_print(right.as_ref(), f, depth + 2)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "on: {:?}", on)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "join_type: {:?}", join_type)?;

            write!(f, "{}", "  ".repeat(depth + 1))?;
            writeln!(f, "schema: {:?}", schema)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datatype::scalar::Scalar;
    use crate::datasource::csv_table::CSVTable;
    use crate::error::Result;
    use crate::logical_plan::logical_expr::{AggregateFunc, BinaryExpr, Operator};

    use super::*;

    #[test]
    fn create_and_print_logical_plan() -> Result<()> {
        let table = CSVTable::try_create_table("data/test.csv")?;
        let schema = Schema::new(vec![]);

        let scan = LogicalPlan::Scan(super::Scan {
            data_source: table.clone(),
            projection: None,
        });

        assert_eq!(
            "Scan:\
            \n  source_type: \"CSV file\"\
            \n  projection: None\n",
            format!("{}", scan)
        );

        let scan = Arc::new(scan);

        let selection = LogicalPlan::Selection(Selection {
            input: scan.clone(),
            expr: LogicalExpr::BinaryExpr(BinaryExpr {
                left: Box::new(LogicalExpr::Column("age".to_string())),
                op: Operator::GtEq,
                right: Box::new(LogicalExpr::Literal(Scalar::Int64(Some(24)))),
            }),
        });

        assert_eq!(
            "Selection:\
            \n  expr: BinaryExpr(BinaryExpr { left: Column(\"age\"), op: GtEq, right: Literal(Int64(Some(24))) })\
            \n  input:\
            \n    Scan:\
            \n      source_type: \"CSV file\"\
            \n      projection: None\n",
            format!("{}", selection)
        );

        let projection = LogicalPlan::Projection(Projection {
            input: scan.clone(),
            exprs: vec![LogicalExpr::Column("age".to_string())],
            schema: schema.clone(),
        });

        assert_eq!(
            "Projection:\
            \n  exprs: [Column(\"age\")]\
            \n  input:\
            \n    Scan:\
            \n      source_type: \"CSV file\"\
            \n      projection: None\
            \n  schema: Schema { fields: [] }\n",
            format!("{}", projection)
        );

        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: scan.clone(),
            group_expr: vec![LogicalExpr::Column("age".to_string())],
            aggr_expr: vec![AggregateFuncExpr {
                func: AggregateFunc::MAX,
                expr: Box::new(LogicalExpr::Column("age".to_string())),
            }],
            schema: schema.clone(),
        });

        assert_eq!(
            "Aggregate:\
            \n  input:\
            \n    Scan:\
            \n      source_type: \"CSV file\"\
            \n      projection: None\
            \n  group_expr: [Column(\"age\")]\
            \n  aggr_expr: [AggregateFuncExpr { func: MAX, expr: Column(\"age\") }]\
            \n  schema: Schema { fields: [] }\n",
            format!("{}", aggregate)
        );

        let projection = Arc::new(projection);

        let join = LogicalPlan::Join(Join {
            left: scan.clone(),
            on: vec![],
            right: projection,
            join_type: JoinType::Inner,
            schema: schema.clone(),
        });

        assert_eq!(
            "Join:\
            \n  left:\
            \n    Scan:\
            \n      source_type: \"CSV file\"\
            \n      projection: None\
            \n  right:\
            \n    Projection:\
            \n      exprs: [Column(\"age\")]\
            \n      input:\
            \n        Scan:\
            \n          source_type: \"CSV file\"\
            \n          projection: None\
            \n      schema: Schema { fields: [] }\
            \n  on: []\
            \n  join_type: Inner\
            \n  schema: Schema { fields: [] }\n",
            format!("{}", join)
        );

        Ok(())
    }
}
