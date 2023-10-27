use arrow::record_batch::RecordBatch;

use crate::datatype::scalar::Scalar;
use crate::datatype::{field::Field, schema::Schema};
use crate::error::Result;

pub mod count;
mod max;

pub trait AggrOperator {
    fn to_field(&self, schema: &Schema) -> Result<Field>;

    fn update_batch(&mut self, record_batch: &RecordBatch) -> Result<()>;

    fn update(&mut self, record_batch: &RecordBatch, i: usize) -> Result<()>;

    fn evaluate(&self) -> Result<Scalar>;

    fn clear(&mut self) -> Result<()>;
}

pub type AggrOperatorRef = Box<dyn AggrOperator>;
