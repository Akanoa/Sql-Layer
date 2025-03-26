use crate::errors::SqlLayerError;
use crate::record::Column;
use crate::record::{Columns, Record};
use crate::row::Row;
use crate::storage::Storage;
use crate::table;
use crate::table::{FieldType, Table};
use crate::table_metadata::TableMetadata;
use foundationdb::{FdbBindingError, RetryableTransaction};
use foundationdb_tuple::{pack, unpack, Subspace, TupleDepth, TuplePack, VersionstampOffset};
use std::io::Write;
use std::iter::zip;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataPrefix {
    Table = 1,
    TableMeta = 2,
    Row = 3,
    PrimaryKey = 4,
    Index = 5,
}

impl TuplePack for DataPrefix {
    fn pack<W: Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<VersionstampOffset> {
        (*self as u64).pack(w, tuple_depth)
    }
}

struct Database {
    root_subspace: Subspace,
    storage: Storage,
}

impl Database {
    fn new(root_subspace: Subspace, storage: Storage) -> Self {
        Self {
            root_subspace,
            storage,
        }
    }

    /// Creates a new table in the database.
    ///
    /// This method serializes the provided table into a byte array
    /// and stores it in the database using the `Subspace` associated
    /// with tables. The table's name is used to generate a unique key
    /// within the `root_subspace` for storage.
    ///
    /// # Arguments
    ///
    /// * `table` - A reference to the `Table` to be created and stored in the database.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Serialization of the table fails.
    /// - An error occurs during the storage operation (e.g., database write failure).
    async fn create_table(&self, table: &Table) -> crate::errors::Result<()> {
        let bytes = table.to_bytes()?;
        let key = self
            .root_subspace
            .subspace(&DataPrefix::Table)
            .pack(&table.name);
        self.storage.set(&key, &bytes).await?;
        Ok(())
    }

    /// Adds an index to a table in the database.
    ///
    /// This method retrieves the table corresponding to the given name from the database,
    /// adds the specified index to the table's metadata, and updates the table's information
    /// in the database.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to which the index should be added.
    /// * `index` - A reference to the `Index` that should be added to the table's metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table with the specified name does not exist.
    /// - The table update operation fails due to a database error.
    async fn add_index(&self, table_name: &str, index: &table::Index) -> crate::errors::Result<()> {
        self.storage
            .database
            .run(|trx, _| async move {
                let mut table = self
                    .get_table_internal(&trx, table_name)
                    .await?
                    .ok_or(SqlLayerError::TableNotFound(table_name.to_string()))?;
                table.add_index(index);
                self.update_table_internal(&trx, table).await?;
                Ok(())
            })
            .await?;
        Ok(())
    }

    ///
    /// Retrieves a table from the database by its name.
    ///
    /// This method fetches a serialized table using the provided name from the database.
    /// If the table is found, it deserializes the bytes into a `Table` instance and
    /// returns it. If the table does not exist, `None` is returned.
    ///
    /// # Arguments
    ///
    /// * `trx` - A reference to a `RetryableTransaction` used for the operation.
    /// * `table_name` - The name of the table to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Table))` if the table is found and successfully deserialized.
    /// * `Ok(None)` if the table does not exist in the database.
    /// * `Err` if an error occurs during the deserialization or retrieval process.
    async fn get_table_internal(
        &self,
        trx: &RetryableTransaction,
        table_name: &str,
    ) -> crate::errors::Result<Option<Table>> {
        let key = self
            .root_subspace
            .subspace(&DataPrefix::Table)
            .pack(&table_name);
        let bytes = trx.get(&key, false).await?;
        match bytes {
            Some(bytes) => Ok(Some(Table::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    async fn update_table_internal(
        &self,
        trx: &RetryableTransaction,
        table: Table,
    ) -> crate::errors::Result<()> {
        let key = self
            .root_subspace
            .subspace(&DataPrefix::Table)
            .pack(&table.name);
        let bytes = table.to_bytes()?;
        trx.set(&key, &bytes);
        Ok(())
    }

    async fn get_table(&self, table_name: &str) -> crate::errors::Result<Option<Table>> {
        let table = self
            .storage
            .database
            .run(|trx, _| async move { Ok(self.get_table_internal(&trx, table_name).await?) })
            .await?;
        Ok(table)
    }

    /// Retrieves the metadata of a table from the database by its name.
    ///
    /// This method fetches the serialized metadata of a table using the provided
    /// table name. If the metadata is not found, a new `TableMetadata` instance is
    /// created, serialized, and stored in the database before being returned.
    ///
    /// # Arguments
    ///
    /// * `trx` - A reference to a `RetryableTransaction` used to perform the operation.
    /// * `table_name` - The name of the table whose metadata needs to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Ok(TableMetadata)` containing the metadata if retrieval or creation succeeds.
    /// * `Err` if an error occurs during retrieval, creation, or serialization.
    async fn get_table_meta(
        &self,
        trx: &RetryableTransaction,
        table_name: &str,
    ) -> crate::errors::Result<TableMetadata> {
        let key = self
            .root_subspace
            .subspace(&DataPrefix::TableMeta)
            .pack(&table_name);
        let bytes = trx.get(&key, false).await?;
        match bytes {
            Some(bytes) => Ok(TableMetadata::from_bytes(&bytes)?),
            None => {
                let table_meta = TableMetadata::new(table_name.to_string());
                let bytes = table_meta.to_bytes()?;
                trx.set(&key, &bytes);
                Ok(table_meta)
            }
        }
    }

    /// Inserts a record into a specified table in the database.
    ///
    /// This method validates the provided record against the table's schema, ensuring that
    /// all required fields are present and match the expected data types. It then constructs
    /// a primary key based on the table's schema and stores the record in the database.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table where the record is to be inserted.
    /// * `record` - A reference to the `Record` that contains the data to be inserted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table does not exist.
    /// - The record is missing required fields or has fields that do not match the schema.
    /// - An error occurs during the storage operation, such as a database write failure.
    async fn insert(&self, table_name: &str, record: &Record) -> crate::errors::Result<()> {
        self.storage
            .database
            .run(|trx, _| async move {
                if let Some(table) = self.get_table_internal(&trx, table_name).await? {
                    // check column fit table fields
                    for (field, column) in zip(table.fields.iter(), record.columns.iter()) {
                        check_field_against_column(&field.r#type, column)?;
                    }

                    // build the primary key tuple
                    let mut pk = vec![];
                    for field in table.primary_key.iter() {
                        let i = table
                            .get_field_pos(field)
                            .ok_or(SqlLayerError::MissingColumn(field.to_string()))?;
                        let columns = record
                            .columns
                            .get(i)
                            .ok_or(SqlLayerError::MissingColumn(field.to_string()))?;
                        pk.push(columns);
                    }
                    let mut meta = self.get_table_meta(&trx, table_name).await?;
                    let row_id = meta.get_current_row_id() as i64;

                    // store the primary key
                    let pk = Columns::new(&pk);
                    // dbg!("Inserting record with primary key {:?}", &pk);
                    let subspace_pk = self
                        .root_subspace
                        .subspace(&DataPrefix::PrimaryKey)
                        .subspace(&table_name)
                        .pack(&pk);

                    trx.set(&subspace_pk, pack(&row_id).as_ref());

                    // store index
                    for index in &table.indexes {
                        let mut columns = vec![];
                        for field in index.fields() {
                            let i = table
                                .get_field_pos(field)
                                .ok_or(SqlLayerError::MissingColumn(field.to_string()))?;
                            let column = record
                                .columns
                                .get(i)
                                .ok_or(SqlLayerError::MissingColumn(field.to_string()))?;
                            columns.push(column);
                        }
                        let columns = Columns::new(&columns);
                        let subspace_index = self
                            .root_subspace
                            .subspace(&DataPrefix::Index)
                            .subspace(&table_name)
                            .pack(&columns);
                        println!(
                            "Inserting record with index {} {:?}",
                            index.name(),
                            &columns
                        );
                        trx.set(&subspace_index, pack(&row_id).as_ref());
                    }

                    // store the record
                    let row: Row = record.into();
                    let row_bytes = row.to_bytes()?;
                    let key = self
                        .root_subspace
                        .subspace(&DataPrefix::Row)
                        .subspace(&table_name)
                        .pack(&row_id);
                    trx.set(&key, &row_bytes);

                    // increment row_id
                    meta.increment_max_row_id();
                    let meta_bytes = meta.to_bytes()?;
                    trx.set(
                        &self
                            .root_subspace
                            .subspace(&DataPrefix::TableMeta)
                            .pack(&table_name),
                        &meta_bytes,
                    );
                }

                Ok(())
            })
            .await?;
        Ok(())
    }

    ///
    /// Fetches a record from the database based on the given primary key.
    ///
    /// # Parameters
    ///
    /// - `table_name`: The name of the table from which to fetch the record.
    /// - `pk`: A reference to the primary key of the record to retrieve.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(record))` containing the record if it exists, `Ok(None)` if the record
    /// does not exist, or an error if the operation fails.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The table does not exist.
    /// - The provided primary key does not match the schema.
    /// - There is an issue with the database read operation.
    // todo: use [get_mapped_ranges] instead
    async fn get_record_by_pk(
        &self,
        table_name: &str,
        pk: &Columns<'_>,
    ) -> crate::errors::Result<Option<Record>> {
        let kv = self
            .storage
            .database
            .run(|trx, _| async move {
                // build primary key subspace out of the primary key columns
                let subspace_pk = self
                    .root_subspace
                    .subspace(&DataPrefix::PrimaryKey)
                    .subspace(&table_name)
                    .pack(&pk);

                // get the row_id
                let kv = trx.get(&subspace_pk, false).await?;

                let Some(kv) = kv else {
                    return Ok(None);
                };

                let row_id = unpack::<i64>(&kv).map_err(FdbBindingError::PackError)?;

                // get the row
                let key = self
                    .root_subspace
                    .subspace(&DataPrefix::Row)
                    .subspace(&table_name)
                    .pack(&row_id);
                let row = trx.get(&key, false).await?;

                Ok(row)
            })
            .await?;

        let Some(kv) = kv else {
            return Ok(None);
        };

        let row = Row::from_bytes(&kv)?;
        let record = Record::from(row);
        Ok(Some(record))
    }
}

fn check_field_against_column(field: &FieldType, column: &Column) -> crate::errors::Result<()> {
    match (field, column) {
        (FieldType::Bool, Column::Bool(_)) => {}
        (FieldType::Int, Column::Int(_)) => {}
        (FieldType::String, Column::String(_)) => {}
        (FieldType::Float, Column::Float(_)) => {}
        (FieldType::Bytes, Column::Bytes(_)) => {}
        (expected, found) => {
            let expected = format!("{expected:?}");
            let found = format!("{found:?}");
            return Err(SqlLayerError::MismatchedColumnType(expected, found));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::Index;
    use crate::table;
    use table::{Field, FieldType};

    #[tokio::test]
    async fn test_database() {
        let _guard = fdb_testcontainer::get_db_once().await;
        let storage = Storage::new(_guard.clone());
        let database = Database::new(Subspace::all(), storage);
        let mut table = Table::new("Person".to_string(), vec!["name".to_string()]);
        table.add_field(Field::new("name".to_string(), FieldType::String));
        table.add_field(Field::new("age".to_string(), FieldType::Int));
        table.add_field(Field::new("height".to_string(), FieldType::Float));
        table.add_field(Field::new("is_married".to_string(), FieldType::Bool));
        table.add_field(Field::new("photo".to_string(), FieldType::Bytes));
        database
            .create_table(&table)
            .await
            .expect("Unable to create table");
        let found_table = database
            .get_table("Person")
            .await
            .expect("Unable to get table");
        assert_eq!(found_table, Some(table));
    }

    #[tokio::test]
    async fn test_insert_record() {
        let _guard = fdb_testcontainer::get_db_once().await;
        let storage = Storage::new(_guard.clone());
        let database = Database::new(Subspace::all(), storage);
        let mut table = Table::new("Person".to_string(), vec!["name".to_string()]);
        table.add_field(Field::new("name".to_string(), FieldType::String));
        table.add_field(Field::new("age".to_string(), FieldType::Int));
        table.add_field(Field::new("height".to_string(), FieldType::Float));
        table.add_field(Field::new("is_married".to_string(), FieldType::Bool));
        table.add_field(Field::new("photo".to_string(), FieldType::Bytes));
        database
            .create_table(&table)
            .await
            .expect("Unable to create table");
        let record = Record {
            columns: vec![
                Column::String("John".to_string()),
                Column::Int(20),
                Column::Float(20.5),
                Column::Bool(true),
                Column::Bytes(b"arbitrary data".to_vec()),
            ],
        };
        database
            .insert("Person", &record)
            .await
            .expect("Unable to insert record");
        let found_record = database
            .get_record_by_pk(
                "Person",
                &Columns(&vec![&Column::String("John".to_string())]),
            )
            .await
            .expect("Unable to get record");
        assert_eq!(found_record, Some(record));
    }

    #[tokio::test]
    async fn insert_multiple_records() {
        let _guard = fdb_testcontainer::get_db_once().await;
        let storage = Storage::new(_guard.clone());
        let database = Database::new(Subspace::all(), storage);
        let mut table = Table::new("Person".to_string(), vec!["name".to_string()]);
        table.add_field(Field::new("name".to_string(), FieldType::String));
        table.add_field(Field::new("age".to_string(), FieldType::Int));
        table.add_field(Field::new("height".to_string(), FieldType::Float));
        table.add_field(Field::new("is_married".to_string(), FieldType::Bool));
        table.add_field(Field::new("photo".to_string(), FieldType::Bytes));
        database
            .create_table(&table)
            .await
            .expect("Unable to create table");

        // populate database
        for i in 0..10 {
            let record = Record {
                columns: vec![
                    Column::String(format!("John {}", i)),
                    Column::Int(i),
                    Column::Float(i as f64),
                    Column::Bool(i % 2 == 0),
                    Column::Bytes(b"arbitrary data".to_vec()),
                ],
            };
            database
                .insert("Person", &record)
                .await
                .expect("Unable to insert record");
        }

        // check record are inserted
        for i in 0..10 {
            let expected = Record {
                columns: vec![
                    Column::String(format!("John {}", i)),
                    Column::Int(i),
                    Column::Float(i as f64),
                    Column::Bool(i % 2 == 0),
                    Column::Bytes(b"arbitrary data".to_vec()),
                ],
            };
            // check that the record is retrievable from pk
            let found = database
                .get_record_by_pk(
                    "Person",
                    &Columns(&vec![&Column::String(format!("John {}", i))]),
                )
                .await
                .expect("Unable to get record");
            assert_eq!(found, Some(expected));
        }
    }

    #[tokio::test]
    async fn test_retrieve_row_by_index() {
        let _guard = fdb_testcontainer::get_db_once().await;
        let storage = Storage::new(_guard.clone());
        let database = Database::new(Subspace::all(), storage);
        let mut table = Table::new("Person".to_string(), vec!["name".to_string()]);
        table.add_field(Field::new("name".to_string(), FieldType::String));
        table.add_field(Field::new("age".to_string(), FieldType::Int));
        table.add_field(Field::new("height".to_string(), FieldType::Float));
        table.add_field(Field::new("is_married".to_string(), FieldType::Bool));
        table.add_field(Field::new("photo".to_string(), FieldType::Bytes));
        database
            .create_table(&table)
            .await
            .expect("Unable to create table");

        let index = &Index::new("idx_age", vec!["age"]);
        database
            .add_index("Person", index)
            .await
            .expect("Unable to add index");

        let record = Record {
            columns: vec![
                Column::String("John".to_string()),
                Column::Int(20),
                Column::Float(20.5),
                Column::Bool(true),
                Column::Bytes(b"arbitrary data".to_vec()),
            ],
        };

        database
            .insert("Person", &record)
            .await
            .expect("Unable to insert record");
    }
}
