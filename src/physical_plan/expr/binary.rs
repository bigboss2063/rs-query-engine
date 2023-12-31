use crate::datatype::column_array::ColumnArray;
use crate::datatype::field::Field;
use crate::error::{Error, Result};
use crate::logical_plan::logical_expr::Operator;
use crate::physical_plan::expr::{PhysicalExpr, PhysicalExprRef};
use arrow::{
    array::{BooleanArray, PrimitiveArray},
    compute::{
        add, and, divide, eq_dyn, gt_dyn, gt_eq_dyn, lt_dyn, lt_eq_dyn, modulus, multiply, neq_dyn,
        or, subtract,
    },
    datatypes::{DataType, Float64Type, Int64Type, UInt64Type},
    record_batch::RecordBatch,
};
use std::any::Any;
use std::sync::Arc;

use super::literal::LiteralExpr;

macro_rules! compare_op {
    ($OP:expr, $LEFT:expr, $RIGHT:expr) => {
        $OP($LEFT, $RIGHT)
            .map_err(|e| e.into())
            .map(|array| ColumnArray::Array(Arc::new(array)))
    };
}

macro_rules! binary_op {
    ($OP:expr, $LEFT_DATA_TYPE:expr, $RIGHT_DATA_TYPE:expr, $LEFT:expr, $RIGHT:expr, $OP_TYPE:expr) => {
        if $LEFT_DATA_TYPE != DataType::Boolean && $RIGHT_DATA_TYPE != DataType::Boolean {
            return Err(Error::IntervalError(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                $OP_TYPE, $LEFT_DATA_TYPE, $RIGHT_DATA_TYPE
            )));
        } else {
            let left = $LEFT.as_any().downcast_ref::<BooleanArray>().unwrap();
            let right = $RIGHT.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(ColumnArray::Array(Arc::new($OP(left, right)?)))
        }
    };
}

macro_rules! arithmetic_op {
    ($OP:expr, $LEFT_DATA_TYPE:expr, $LEFT:expr, $RIGHT:expr) => {
        match $LEFT_DATA_TYPE {
            DataType::Int64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Int64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnArray::Array(Arc::new(x)))
            }
            DataType::UInt64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<UInt64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<UInt64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnArray::Array(Arc::new(x)))
            }
            DataType::Float64 => {
                let left = $LEFT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let right = $RIGHT
                    .as_any()
                    .downcast_ref::<PrimitiveArray<Float64Type>>()
                    .unwrap();
                let x = $OP(left, right)?;
                Ok(ColumnArray::Array(Arc::new(x)))
            }
            _ => unimplemented!(),
        }
    };
}

pub struct BinaryExpr {
    pub left: PhysicalExprRef,
    pub op: Operator,
    pub right: PhysicalExprRef,
}

impl BinaryExpr {
    pub fn new(left: PhysicalExprRef, op: Operator, right: PhysicalExprRef) -> PhysicalExprRef {
        Arc::new(Self { left, op, right })
    }
}

impl PhysicalExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnArray> {
        let left_value = self.left.evaluate(input)?;
        let right_value = self.right.evaluate(input)?;

        let left_type = left_value.data_type();
        let right_type = right_value.data_type();

        if left_type != right_type {
            return Err(Error::IntervalError(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_type, right_type
            )));
        }

        let left_array = left_value.to_array();
        let right_array = right_value.to_array();

        match self.op {
            Operator::Eq => compare_op!(eq_dyn, &left_array, &right_array),
            Operator::Neq => compare_op!(neq_dyn, &left_array, &right_array),
            Operator::Gt => compare_op!(gt_dyn, &left_array, &right_array),
            Operator::GtEq => compare_op!(gt_eq_dyn, &left_array, &right_array),
            Operator::Lt => compare_op!(lt_dyn, &left_array, &right_array),
            Operator::LtEq => compare_op!(lt_eq_dyn, &left_array, &right_array),
            Operator::And => binary_op!(
                and,
                left_type,
                right_type,
                left_array,
                right_array,
                Operator::And
            ),
            Operator::Or => binary_op!(
                or,
                left_type,
                right_type,
                left_array,
                right_array,
                Operator::Or
            ),
            Operator::Add => arithmetic_op!(add, left_type, left_array, right_array),
            Operator::Sub => arithmetic_op!(subtract, left_type, left_array, right_array),
            Operator::Mul => arithmetic_op!(multiply, left_type, left_array, right_array),
            Operator::Div => arithmetic_op!(divide, left_type, left_array, right_array),
            Operator::Mod => arithmetic_op!(modulus, left_type, left_array, right_array),
        }
    }

    fn to_field(&self, input: &RecordBatch) -> Result<Field> {
        let left = self.left.to_field(input)?;
        let left_name = left.name();

        let right = &*self.right.as_any();

        let right_name = match right {
            right if right.is::<LiteralExpr>() => self.right.to_field(input)?.name().clone(),
            _ => self.right.to_field(input)?.name().clone(),
        };

        let (operator, data_type) = match self.op {
            Operator::Eq => ("=", DataType::Boolean),
            Operator::Neq => ("!=", DataType::Boolean),
            Operator::Lt => ("<", DataType::Boolean),
            Operator::LtEq => ("<=", DataType::Boolean),
            Operator::Gt => (">", DataType::Boolean),
            Operator::GtEq => (">=", DataType::Boolean),
            Operator::And => ("and", DataType::Boolean),
            Operator::Or => ("or", DataType::Boolean),
            Operator::Add => ("+", left.data_type().clone()),
            Operator::Sub => ("-", left.data_type().clone()),
            Operator::Mul => ("*", left.data_type().clone()),
            Operator::Div => ("/", left.data_type().clone()),
            Operator::Mod => ("%", left.data_type().clone()),
        };

        Ok(Field::new(
            &format!("{} {} {}", left_name, operator, right_name),
            data_type,
            true,
        ))
    }
}
