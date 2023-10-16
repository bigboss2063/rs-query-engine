use crate::catalog::table::Table;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
/// Stores metadata for all tables, needs to implement Table trait
pub struct Catalog {
    tables: HashMap<String, Arc<dyn Table>>,
}