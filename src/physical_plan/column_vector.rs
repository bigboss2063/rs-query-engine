use crate::catalog::scalar::Scalar;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;

pub enum ColumnVector {
    Array(ArrayRef),
    Literal(Scalar, usize),
}

impl ColumnVector {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnVector::Array(array_ref) => array_ref.data_type().clone(),
            ColumnVector::Literal(scalar, _) => scalar.to_field().data_type().clone(),
        }
    }

    pub fn to_array(self) -> ArrayRef {
        match self {
            ColumnVector::Array(array_ref) => array_ref,
            ColumnVector::Literal(scalar, size) => scalar.to_array(size),
        }
    }
}
