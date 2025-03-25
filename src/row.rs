use serde::{Deserialize, Serialize};
use serde_with::{serde_as, Bytes};

const SCHEMA: &str = include_str!("assets/schemas/row.json");

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub columns: Vec<Option<Column>>,
}

impl Row {
    pub fn new() -> Self {
        Self { columns: vec![] }
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(Some(column));
    }

    pub fn add_null_column(&mut self) {
        self.columns.push(None);
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct Column {
    pub column_string: Option<ColumnString>,
    pub column_int: Option<ColumnInt>,
    pub column_float: Option<ColumnFloat>,
    pub column_bool: Option<ColumnBool>,
    pub column_bytes: Option<ColumnBytes>,
}

impl Column {
    pub fn new_string(value: String) -> Self {
        Self {
            column_string: Some(ColumnString(value)),
            ..Default::default()
        }
    }

    pub fn new_int(value: i64) -> Self {
        Self {
            column_int: Some(ColumnInt(value)),
            ..Default::default()
        }
    }

    pub fn new_float(value: f64) -> Self {
        Self {
            column_float: Some(ColumnFloat(value)),
            ..Default::default()
        }
    }

    pub fn new_bool(value: bool) -> Self {
        Self {
            column_bool: Some(ColumnBool(value)),
            ..Default::default()
        }
    }

    pub fn new_bytes(value: Vec<u8>) -> Self {
        Self {
            column_bytes: Some(ColumnBytes(value)),
            ..Default::default()
        }
    }
    pub fn is_null(&self) -> bool {
        self.column_bool.is_none()
            && self.column_int.is_none()
            && self.column_float.is_none()
            && self.column_string.is_none()
            && self.column_bytes.is_none()
    }
}

impl Row {
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
        let row = apache_avro::from_value::<Row>(&value)?;
        Ok(row)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnString(pub String);
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnInt(pub i64);
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnFloat(pub f64);
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnBool(pub bool);
#[serde_as]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnBytes(#[serde_as(as = "Bytes")] pub Vec<u8>);

#[cfg(test)]
mod tests {
    use crate::row::{Column, Row, SCHEMA};
    #[test]
    fn test_row() {
        let schema = apache_avro::schema::Schema::parse_str(SCHEMA).expect("Invalid schema");

        let mut row = Row::new();
        row.add_column(Column::new_string("John".to_string()));
        row.add_column(Column::new_int(20));
        row.add_column(Column::new_float(20.5));
        row.add_column(Column::new_bool(true));
        row.add_column(Column::new_bytes(b"arbitrary data".to_vec()));

        let value = apache_avro::to_value(&row).expect("Failed to convert row to avro value");
        let bytes = apache_avro::to_avro_datum(&schema, value)
            .expect("Failed to convert avro value to avro datum");
        let mut data = &bytes[..];
        let deserialized_value = apache_avro::from_avro_datum(&schema, &mut data, None)
            .expect("Unable to deserialize row");
        let deserialized_row =
            apache_avro::from_value::<Row>(&deserialized_value).expect("Unable to deserialize row");
        assert_eq!(row, deserialized_row);
    }
}
