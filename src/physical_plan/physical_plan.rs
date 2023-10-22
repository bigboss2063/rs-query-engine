use crate::datatype::schema::Schema;
use crate::error::Result;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub type PhysicalPlanRef = Arc<dyn PhysicalPlan>;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;

    fn execute(&self) -> Result<Vec<RecordBatch>>;

    fn children(&self) -> Result<Vec<PhysicalPlanRef>>;
}
