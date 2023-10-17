use crate::catalog::field::Field;
use crate::catalog::scalar::Scalar;
use crate::error::Result;
use crate::logical_plan::logical_plan::LogicalPlan;
use arrow::datatypes::DataType;

/// A logical expression is an abstract representation of a query condition or filter condition.
/// It usually consists of logical operators (such as AND, OR, NOT) and comparison operations.
#[derive(Debug, Clone)]
pub enum LogicalExpr {
    /// Binary expressions are simple expressions that accept two inputs.
    /// Include comparison, boolean and mathematical expressions.
    BinaryExpr(BinaryExpr),
    /// Literal expressions represent literal values.
    Literal(Scalar),
    /// Alias a logical expression
    Alias(Alias),
    /// The Column expression simply represents a reference to a named column.
    Column(String),
    /// Scalar function expressions perform a scalar function
    /// such as CONCAT, ABS, LENGTH on an input expression.
    ScalarFuncExpr(ScalarFuncExpr),
    /// Aggregate function expressions perform an aggregate function
    /// such as MIN, MAX, COUNT, SUM, or AVG on an input expression.
    AggregateFuncExpr(AggregateFuncExpr),
}

impl LogicalExpr {
    pub fn to_field(&self, input: &LogicalPlan) -> Result<Field> {
        match self {
            LogicalExpr::BinaryExpr(expr) => expr.to_field(input),
            LogicalExpr::Literal(scalar) => scalar.to_field(),
            LogicalExpr::Alias(alias) => {
                let field = alias.expr.to_field(input)?;
                Ok(Field::new(
                    &alias.name,
                    field.data_type().clone(),
                    field.is_nullable(),
                ))
            }
            LogicalExpr::Column(_) => todo!(),
            LogicalExpr::ScalarFuncExpr(_) => todo!(),
            LogicalExpr::AggregateFuncExpr(_) => todo!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub name: String,
    pub left: Box<LogicalExpr>,
    /// Comparison, logical or arithmetic operators.
    pub op: Operator,
    pub right: Box<LogicalExpr>,
}

impl BinaryExpr {
    fn to_field(&self, input: &LogicalPlan) -> Result<Field> {
        let left = self.left.to_field(input)?;
        let left = left.name();

        let right = match &*self.right {
            // If it is a literal value, get the string representation of the value.
            LogicalExpr::Literal(scalar) => scalar.to_string(),
            _ => self.right.to_field(input)?.name().clone(),
        };

        let operator = match self.op {
            Operator::Eq => "=",
            Operator::Neq => "!=",
            Operator::Lt => "<",
            Operator::LtEq => "<=",
            Operator::Gt => ">",
            Operator::GtEq => ">=",
            Operator::And => "and",
            Operator::Or => "or",
            Operator::Add => "+",
            Operator::Sub => "-",
            Operator::Mul => "*",
            Operator::Div => "/",
            Operator::Mod => "%",
        };

        Ok(Field::new(
            &format!("{} {} {}", left, right, operator),
            DataType::Boolean,
            true,
        ))
    }
}

#[derive(Debug, Clone)]
pub enum Operator {
    /// Equality (`=`) comparison
    Eq,
    /// Inequality (`!=`) comparison
    Neq,
    /// Greater than (`>`) comparison
    Gt,
    /// Greater than or equals (`>=`) comparison
    GtEq,
    /// Less than (`<`) comparison
    Lt,
    /// Less than or equals (`<=`) comparison
    LtEq,
    /// Logical AND (`==`)
    And,
    /// Logical OR (`||`)
    Or,
    /// Addition operator (`+`)
    Add,
    /// Subtract operator (`-`)
    Sub,
    /// Multiply operator (`*`)
    Mul,
    /// Divide operator (`/`)
    Div,
    /// Modulus operator (`%`)
    Mod,
}

#[derive(Clone, Debug)]
pub struct Alias {
    pub name: String,
    pub expr: Box<LogicalExpr>,
}

#[derive(Clone, Debug)]
/// Represents a series of operations on scalar values
pub struct ScalarFuncExpr {
    pub func: ScalarFunc,
    pub expr: Box<LogicalExpr>,
}

#[derive(Clone, Debug)]
pub enum ScalarFunc {
    CONCAT,
    SUBSTRING,
    ABS,
    SQRT,
}

#[derive(Clone, Debug)]
pub struct AggregateFuncExpr {
    pub func: AggregateFunc,
    pub expr: Box<LogicalExpr>,
}

#[derive(Clone, Debug)]
/// Represents a series of aggregation operations.
pub enum AggregateFunc {
    SUM,
    MIN,
    MAX,
    AVG,
    COUNT,
}
