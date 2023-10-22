use crate::datatype::scalar::Scalar;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;

/// Encapsulating ColumnArray avoids having to use a specific Array type for each data type,
/// and provides a more convenient interface.
/// This abstraction also makes it possible to have an implementation for scalar values,
/// avoiding the need to create and populate a FieldVector with a literal value repeated for
/// every index in the column.
pub enum ColumnArray {
    Array(ArrayRef),
    Literal(Scalar, usize), // the second member represents how many rows this column has
}

impl ColumnArray {
    pub fn data_type(&self) -> DataType {
        match self {
            ColumnArray::Array(array_ref) => array_ref.data_type().clone(),
            ColumnArray::Literal(scalar, _) => scalar.to_field().data_type().clone(),
        }
    }

    pub fn to_array(self) -> ArrayRef {
        match self {
            ColumnArray::Array(array_ref) => array_ref,
            ColumnArray::Literal(scalar, size) => scalar.to_array(size),
        }
    }
}
