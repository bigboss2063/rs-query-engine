/// A logic plan is an intermediate representation generated during a query.
/// It is used to express how to execute a query to satisfy the conditions in a logical expression.
/// In the logical plan, the database query engine converts logical expressions to a series of logical operations.
#[derive(Clone)]
pub enum LogicalPlan {
    Scan(Scan),
    Projection(Projection),
    Selection(Selection),
    Aggregate(Aggregate),
}

impl LogicalPlan {}

#[derive(Clone)]
pub struct Scan {}

#[derive(Clone)]
pub struct Projection {}

#[derive(Clone)]
pub struct Selection {}

#[derive(Clone)]
pub struct Aggregate {}

#[derive(Clone)]
pub struct Join {}
