use serde::{Deserialize, Serialize};

const SCHEMA: &str = include_str!("assets/schemas/table.json");

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Table {
    pub name: String,
    pub fields: Vec<Field>,
}

impl Table {
    pub fn new(name: String) -> Self {
        Self {
            name,
            fields: vec![],
        }
    }

    pub fn add_field(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn to_bytes(&self) -> crate::errors::Result<Vec<u8>> {
        let schema = apache_avro::schema::Schema::parse_str(SCHEMA)?;
        let value = apache_avro::to_value(self)?;
        let bytes = apache_avro::to_avro_datum(&schema, value)?;
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::errors::Result<Self> {
        let schema = apache_avro::schema::Schema::parse_str(SCHEMA)?;
        let mut data = bytes;
        let value = apache_avro::from_avro_datum(&schema, &mut data, None)?;
        let table = apache_avro::from_value::<Self>(&value)?;
        Ok(table)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Field {
    name: String,
    r#type: FieldType,
}

impl Field {
    pub fn new(name: String, r#type: FieldType) -> Self {
        Self { name, r#type }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum FieldType {
    String,
    Int,
    Float,
    Bool,
    Bytes,
}

#[cfg(test)]
mod tests {
    use crate::table::{Field, FieldType, Table, SCHEMA};
    use apache_avro::to_value;

    #[test]
    fn test_schema() {
        let schema = apache_avro::schema::Schema::parse_str(SCHEMA).expect("Invalid schema");

        let mut table = Table::new("Person".to_string());
        table.add_field(Field::new("name".to_string(), FieldType::String));
        table.add_field(Field::new("age".to_string(), FieldType::Int));
        table.add_field(Field::new("height".to_string(), FieldType::Float));
        table.add_field(Field::new("is_married".to_string(), FieldType::Bool));
        table.add_field(Field::new("photo".to_string(), FieldType::Bytes));

        let value = to_value(&table).expect("Failed to convert table to avro value");

        let bytes = apache_avro::to_avro_datum(&schema, value)
            .expect("Failed to convert avro value to avro datum");

        let mut data = &bytes[..];

        let deserialized_value = apache_avro::from_avro_datum(&schema, &mut data, None)
            .expect("Unable to deserialize table");
        let deserialized_table = apache_avro::from_value::<Table>(&deserialized_value)
            .expect("Unable to deserialize table");
        assert_eq!(table, deserialized_table);
    }
}
