use arrow::datatypes::{self, DataType};

#[derive(Debug, Clone)]
/// Wraps the Field struct from the Arrow
pub struct Field {
    pub field: datatypes::Field,
}

impl Field {
    pub fn new(name: &str, data_type: DataType, nullable: bool) -> Self {
        Self {
            field: datatypes::Field::new(name, data_type, nullable),
        }
    }

    pub fn from(field: datatypes::Field) -> Self {
        Self { field }
    }

    pub fn name(&self) -> &String {
        self.field.name()
    }

    pub fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }
}

impl From<Field> for datatypes::Field {
    fn from(f: Field) -> Self {
        f.field.clone()
    }
}
