use crate::row::Row;

#[derive(Debug, PartialEq)]
struct Record {
    row_id: u64,
    columns: Vec<Column>,
}

#[derive(Debug, PartialEq)]
enum Column {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Null,
}

impl From<Row> for Record {
    fn from(value: Row) -> Self {
        let columns = value
            .columns
            .into_iter()
            .map(|column| match column {
                Some(column) => column.into(),
                None => Column::Null,
            })
            .collect();
        Record {
            row_id: value.row_id,
            columns,
        }
    }
}

impl From<crate::row::Column> for Column {
    fn from(value: crate::row::Column) -> Self {
        if let Some(column) = value.column_string {
            return Column::String(column.0);
        }
        if let Some(column) = value.column_int {
            return Column::Int(column.0);
        }

        if let Some(column) = value.column_float {
            return Column::Float(column.0);
        }

        if let Some(column) = value.column_bool {
            return Column::Bool(column.0);
        }

        if let Some(column) = value.column_bytes {
            return Column::Bytes(column.0);
        }

        Column::Null
    }
}

#[cfg(test)]
mod tests {
    use crate::record::{Column, Record};
    use crate::row::Row;

    #[test]
    fn test_convert_row_to_record() {
        let mut row = Row::new(1);
        row.add_column(crate::row::Column::new_string("John".to_string()));
        row.add_column(crate::row::Column::new_int(20));
        row.add_column(crate::row::Column::new_float(20.5));
        row.add_column(crate::row::Column::new_bool(true));
        row.add_column(crate::row::Column::new_bytes(b"arbitrary data".to_vec()));

        let record: Record = row.into();
        assert_eq!(record.columns.len(), 5);
        assert_eq!(
            record,
            Record {
                row_id: 1,
                columns: vec![
                    Column::String("John".to_string()),
                    Column::Int(20),
                    Column::Float(20.5),
                    Column::Bool(true),
                    Column::Bytes(b"arbitrary data".to_vec())
                ]
            }
        )
    }
}
