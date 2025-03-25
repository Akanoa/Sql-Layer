use serde::{Deserialize, Serialize};

const SCHEMA: &str = include_str!("assets/schemas/table_metadata.json");

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub max_row_id: u64,
}

impl TableMetadata {
    pub fn new(name: String) -> Self {
        Self {
            name,
            max_row_id: 0,
        }
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
        let table_metadata = apache_avro::from_value::<TableMetadata>(&value)?;
        Ok(table_metadata)
    }

    pub fn increment_max_row_id(&mut self) {
        self.max_row_id += 1;
    }

    pub fn get_current_row_id(&self) -> u64 {
        self.max_row_id
    }
}

#[cfg(test)]
mod tests {
    use crate::table_metadata::TableMetadata;

    #[test]
    fn test_table_metadata() {
        let mut metadata = TableMetadata::new("Person".to_string());
        metadata.increment_max_row_id();
        let bytes = metadata.to_bytes().unwrap();
        let metadata = TableMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(metadata.name, "Person");
        assert_eq!(metadata.max_row_id, 1);
    }
}
