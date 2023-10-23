use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use crate::datasource::table::TableRef;
use crate::datatype::schema::Schema;
use crate::physical_plan::physical_plan::{PhysicalPlan, PhysicalPlanRef};
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct Scan {
    source: TableRef,
    projection: Option<Vec<usize>>,
}

impl Scan {
    pub fn new(source: TableRef, projection: Option<Vec<usize>>) -> PhysicalPlanRef {
        Arc::new(Self { source, projection })
    }
}

impl PhysicalPlan for Scan {
    fn schema(&self) -> &Schema {
        self.source.schema()
    }

    /// Gets data from the specified data source
    fn execute(&self) -> Result<Vec<RecordBatch>> {
        self.source.scan(self.projection.clone())
    }

    /// Scan physical plan has no child nodes
    fn children(&self) -> Result<Vec<PhysicalPlanRef>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
    use crate::datasource::csv_table::CSVTable;
    use crate::error::Result;
    use crate::physical_plan::scan::Scan;

    #[test]
    fn scan_physical_plan() -> Result<()> {
        let table = CSVTable::try_create_table("data/test.csv")?;
        let scan = Scan::new(table, None);
        let res = scan.execute()?;

        assert_eq!(res.len(), 1);

        let record_batch = &res[0];
        assert_eq!(
            &(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef),
            record_batch.column(0)
        );
        assert_eq!(
            &(Arc::new(StringArray::from(vec![
                "bigboss2063",
                "Vincent Hu",
                "KamenRider",
                "nutswalker",
                "Brian",
            ])) as ArrayRef),
            record_batch.column(1)
        );
        assert_eq!(
            &(Arc::new(Int64Array::from(vec![24, 24, 18, 18, 26])) as ArrayRef),
            record_batch.column(2)
        );
        assert_eq!(
            &(Arc::new(Float64Array::from(vec![0.0, 100.0, 99.99, 99.98, 99.97])) as ArrayRef),
            record_batch.column(3)
        );
        Ok(())
    }
}