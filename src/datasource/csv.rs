use crate::catalog::schema::Schema;
use crate::catalog::table::{Table, TableRef};
use crate::error::Result;
use arrow::{csv, record_batch::RecordBatch};
use std::env;
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug)]
/// Stores schema and records read from a CSV file.
pub struct CSVTable {
    schema: Schema,
    batches: Vec<RecordBatch>,
}

impl Table for CSVTable {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan(&self, projection: Option<Vec<usize>>) -> Result<Vec<RecordBatch>> {
        // If projection is not empty, select the specified columns.
        if let Some(projection) = projection {
            let batches = self
                .batches
                .iter()
                .map(|record_batch| record_batch.project(projection.as_ref()).unwrap())
                .collect::<Vec<_>>();
            return Ok(batches);
        }
        // Otherwise return the entire batch.
        Ok(self.batches.clone())
    }
}

impl CSVTable {
    /// Creates a table from a CSV file.
    pub fn try_create_table(filename: &str) -> Result<TableRef> {
        let mut file = File::open(env::current_dir()?.join(Path::new(filename))).unwrap();

        // Uses Arrow's CSV Reader to get the table's Schema.
        let (arrow_schema, _) =
            arrow::csv::reader::infer_reader_schema(&mut file, b',', Some(3), true)?;
        // Converts to our own Schema format.
        let schema = Schema::from(&arrow_schema);

        file.rewind()?;

        let reader = csv::Reader::new(
            file,
            Arc::new(arrow_schema),
            true,
            Some(b','),
            1_000_000,
            None,
            None,
            None,
        );

        let mut batches = vec![];

        for record in reader.into_iter() {
            batches.push(record?);
        }

        Ok(Arc::new(Self { schema, batches }))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Float64Array, Int64Array, StringArray},
        datatypes::{self, DataType},
    };

    use super::*;

    #[test]
    fn test_infer_schema_from_csv() -> Result<()> {
        let table = CSVTable::try_create_table("data/test.csv")?;

        let schema = table.schema();
        let excepted_schema = Arc::new(datatypes::Schema::new(vec![
            datatypes::Field::new("id", DataType::Int64, false),
            datatypes::Field::new("name", DataType::Utf8, false),
            datatypes::Field::new("age", DataType::Int64, false),
            datatypes::Field::new("score", DataType::Float64, false),
        ]));

        assert_eq!(schema.fields().len(), excepted_schema.fields().len());

        let iter = schema.fields().iter().zip(excepted_schema.fields().iter());
        for (field, excepted_field) in iter {
            assert_eq!(field.name(), excepted_field.name());
            assert_eq!(field.data_type(), excepted_field.data_type());
            assert_eq!(field.is_nullable(), excepted_field.is_nullable());
        }

        Ok(())
    }

    #[test]
    fn test_read_from_csv() -> Result<()> {
        let table = CSVTable::try_create_table("data/test.csv")?;

        let batches = table.scan(None)?;

        assert_eq!(batches.len(), 1);

        let record_batch = &batches[0];
        assert_eq!(record_batch.num_rows(), 5);
        assert_eq!(record_batch.num_columns(), 4);

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
                "Brian"
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
