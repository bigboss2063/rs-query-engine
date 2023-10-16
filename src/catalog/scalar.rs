use arrow::{
    self,
    array::{
        new_null_array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
    },
    datatypes::DataType,
};
use std::{iter::repeat, sync::Arc, vec};

use super::fields::Field;

#[derive(Debug, Clone)]
/// Scalar values can be converted to array values
pub enum Scalar {
    /// represents `DataType::Null`, it can be cast to or from any other type
    Null,
    /// true or false
    Boolean(Option<bool>),
    // 64bit float
    Float64(Option<f64>),
    // signed 64bit int
    Int64(Option<i64>),
    // unsigned 64bit int
    UInt64(Option<u64>),
    // utf-8 encoded string
    Utf8(Option<String>),
}

/// Macro used to convert scalar values to array based on the scalar value type
macro_rules! scalar_to_array {
    ($Data_Type:ident, $ARRAY_TYPE:ident, $VALUE:expr, $Size:expr) => {{
        match $VALUE {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(value, $Size)),
            None => new_null_array(&DataType::$Data_Type, $Size),
        }
    }};
}

impl Scalar {
    /// Creates a Field corresponding to the scalar value type
    pub fn to_field(&self) -> Field {
        match self {
            Scalar::Null => Field::new("Null", DataType::Null, true),
            Scalar::Boolean(_) => Field::new("Boolean", DataType::Boolean, true),
            Scalar::Float64(_) => Field::new("Float64", DataType::Float64, true),
            Scalar::Int64(_) => Field::new("Int64", DataType::Int64, true),
            Scalar::UInt64(_) => Field::new("UInt64", DataType::UInt64, true),
            Scalar::Utf8(_) => Field::new("Utf8", DataType::Utf8, true),
        }
    }

    /// Convert scalar value to array
    pub fn to_array(self, size: usize) -> ArrayRef {
        match self {
            Scalar::Null => new_null_array(&DataType::Null, size),
            Scalar::Boolean(v) => Arc::new(BooleanArray::from(vec![v; size])),
            Scalar::Float64(v) => scalar_to_array!(Float64, Float64Array, v, size),
            Scalar::Int64(v) => scalar_to_array!(Int64, Int64Array, v, size),
            Scalar::UInt64(v) => scalar_to_array!(UInt64, UInt64Array, v, size),
            Scalar::Utf8(v) => match v {
                Some(str) => Arc::new(StringArray::from_iter_values(repeat(str).take(size))),
                None => new_null_array(&DataType::Utf8, size),
            },
        }
    }
}
