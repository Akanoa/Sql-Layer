use crate::row::Row;
use foundationdb_tuple::{TupleDepth, TuplePack, VersionstampOffset};
use std::io::Write;

#[derive(Debug, PartialEq, Clone)]
pub struct Record {
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Column {
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
        Record { columns }
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

#[derive(Debug)]
pub struct Columns<'a>(pub &'a Vec<&'a Column>);

impl<'a> Columns<'a> {
    pub fn new(columns: &'a Vec<&'a Column>) -> Self {
        Self(columns)
    }
}

impl TuplePack for Column {
    fn pack<W: Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<foundationdb_tuple::VersionstampOffset> {
        match self {
            Column::String(value) => value.pack(w, tuple_depth),
            Column::Int(value) => value.pack(w, tuple_depth),
            Column::Float(value) => value.pack(w, tuple_depth),
            Column::Bool(value) => value.pack(w, tuple_depth),
            Column::Bytes(value) => value.pack(w, tuple_depth),
            Column::Null => ().pack(w, tuple_depth),
        }
    }
}

impl TuplePack for Columns<'_> {
    fn pack<W: Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<VersionstampOffset> {
        self.0.pack(w, tuple_depth)
    }
}

impl From<&Record> for Row {
    fn from(value: &Record) -> Self {
        let mut row = Row::new();
        for column in &value.columns {
            if let Column::Null = column {
                row.add_null_column();
                continue;
            }
            row.add_column(column.into());
        }
        row
    }
}

impl From<&Column> for crate::row::Column {
    fn from(value: &Column) -> Self {
        match value {
            Column::String(value) => crate::row::Column::new_string(value.to_string()),
            Column::Int(value) => crate::row::Column::new_int(*value),
            Column::Float(value) => crate::row::Column::new_float(*value),
            Column::Bool(value) => crate::row::Column::new_bool(*value),
            Column::Bytes(value) => crate::row::Column::new_bytes(value.clone()),
            Column::Null => {
                unreachable!("Null column is not allowed in a record")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::record::{Column, Record};
    use crate::row::Row;

    #[test]
    fn test_convert_row_to_record() {
        let mut row = Row::new();
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
