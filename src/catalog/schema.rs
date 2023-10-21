use crate::catalog::field::Field;
use crate::error::{Error, Result};
use arrow::datatypes;

#[derive(Debug, Clone)]
/// Schema holds the metadata for a relation
pub struct Schema {
    fields: Vec<Field>,
}

impl Schema {
    pub fn new_null_schema() -> Self {
        Self { fields: vec![] }
    }

    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn from(schema: &datatypes::Schema) -> Self {
        Self::new(
            schema
                .fields()
                .iter()
                .map(|field| Field {
                    field: field.clone(),
                })
                .collect(),
        )
    }

    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }

    pub fn field(&self, i: usize) -> &Field {
        &self.fields[i]
    }

    pub fn index_of(&self, name: &str) -> Result<usize> {
        for (i, field) in self.fields.iter().enumerate() {
            if field.name() == name {
                return Ok(i);
            }
        }
        Err(Error::NoSuchField)
    }

    pub fn find_field_by_name(&self, name: &str) -> Result<Field> {
        for field in self.fields.iter() {
            if field.name() == name {
                return Ok(field.clone());
            }
        }
        Err(Error::NoSuchField)
    }
}

impl From<Schema> for datatypes::Schema {
    fn from(schema: Schema) -> Self {
        datatypes::Schema::new(
            schema
                .fields
                .into_iter()
                .map(|f| f.field)
                .collect::<Vec<_>>(),
        )
    }
}

impl From<Schema> for datatypes::SchemaRef {
    fn from(schema: Schema) -> Self {
        datatypes::SchemaRef::new(schema.into())
    }
}
