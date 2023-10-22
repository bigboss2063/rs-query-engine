use crate::error::{Error, Result};
use crate::logical_plan::logical_expr::Operator;
use crate::physical_plan::column_vector::ColumnVector;
use crate::physical_plan::expr::{PhysicalExpr, PhysicalExprRef};
use arrow::{
    array::{BooleanArray, PrimitiveArray},
    compute::{and, eq_dyn, gt_dyn, gt_eq_dyn, lt_dyn, lt_eq_dyn, neq_dyn, or, add, subtract, multiply, divide, modulus},
    datatypes::{DataType, Int64Type, UInt64Type, Float64Type},
    record_batch::RecordBatch,
};
use std::any::Any;
use std::sync::Arc;

macro_rules! compare_op {
    ($OP:expr, $LEFT:expr, $RIGHT:expr) => {
        $OP($LEFT, $RIGHT)
            .map_err(|e| e.into())
            .map(|array| ColumnVector::Array(Arc::new(array)))
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
            Ok(ColumnVector::Array(Arc::new($OP(left, right)?)))
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
                Ok(ColumnVector::Array(Arc::new(x)))
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
                Ok(ColumnVector::Array(Arc::new(x)))
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
                Ok(ColumnVector::Array(Arc::new(x)))
            }
            _ => unimplemented!(),
        }
    };
}

pub struct BinaryExpr {
    left: PhysicalExprRef,
    op: Operator,
    right: PhysicalExprRef,
}

impl PhysicalExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, input: &RecordBatch) -> Result<ColumnVector> {
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
            Operator::Add => binary_op!(
                and,
                left_type,
                right_type,
                left_array,
                right_array,
                Operator::Add
            ),
            Operator::Or => binary_op!(
                or,
                left_type,
                right_type,
                left_array,
                right_array,
                Operator::Or
            ),
            Operator::And => arithmetic_op!(add, left_type, left_array, right_array),
            Operator::Sub => arithmetic_op!(subtract, left_type, left_array, right_array),
            Operator::Mul => arithmetic_op!(multiply, left_type, left_array, right_array),
            Operator::Div => arithmetic_op!(divide, left_type, left_array, right_array),
            Operator::Mod => arithmetic_op!(modulus, left_type, left_array, right_array),
        }
    }
}
