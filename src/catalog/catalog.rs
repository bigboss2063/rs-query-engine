use crate::catalog::table::Table;
use crate::datasource::csv::CSVTable;
use crate::error::{Error, Result};
use crate::logical_plan::data_frame::DataFrame;
use crate::logical_plan::logical_plan::{LogicalPlan, Scan};
use std::collections::HashMap;
use std::sync::Arc;

use super::table::TableRef;

#[derive(Default)]
/// Stores metadata for all tables, needs to implement Table trait
pub struct Catalog {
    tables: HashMap<String, Arc<dyn Table>>,
}

impl Catalog {
    pub fn add_csv_table(&mut self, table_name: &str, csv_file: &str) -> Result<()> {
        self.tables.insert(
            table_name.to_string(),
            CSVTable::try_create_table(csv_file)?,
        );
        Ok(())
    }

    pub fn get_table_by_name(&self, table_name: &str) -> Result<TableRef> {
        self.tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::NoSuchTable(format!("Table {} does not exist", table_name)))
    }

    pub fn get_table_df(&self, table_name: &str) -> Result<DataFrame> {
        let table = self
            .tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| Error::NoSuchTable(format!("No table named: {}", table_name)))?;
        let plan = LogicalPlan::Scan(Scan {
            data_source: table,
            projection: None,
        });
        Ok(DataFrame::new(plan))
    }
}
