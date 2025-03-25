use crate::storage::Storage;
use crate::table::Table;
use foundationdb_tuple::{Subspace, TupleDepth, TuplePack, VersionstampOffset};
use std::io::Write;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataPrefix {
    Table = 1,
    TableMeta = 2,
    Row = 3,
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

    /// Retrieves a table from the database by its name.
    ///
    /// This method attempts to fetch the serialized representation of a table
    /// from the database using the provided table name. If the table exists,
    /// the serialized data is deserialized into a `Table` instance and returned.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to be retrieved.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(Table))` if the table is found and successfully deserialized.
    /// * `Ok(None)` if no table with the specified name exists.
    /// * `Err` if an error occurs during the retrieval or deserialization process.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The read operation from the database fails.
    /// - The deserialization of the retrieved data fails.
    async fn get_table(&self, table_name: &str) -> crate::errors::Result<Option<Table>> {
        let key = self
            .root_subspace
            .subspace(&DataPrefix::Table)
            .pack(&table_name);
        let bytes = self.storage.get(&key).await?;
        match bytes {
            Some(bytes) => Ok(Some(Table::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table;
    use table::{Field, FieldType};
    #[tokio::test]
    async fn test_database() {
        let _guard = fdb_testcontainer::get_db_once().await;
        let storage = Storage::new(_guard.clone());
        let database = Database::new(Subspace::all(), storage);
        let mut table = Table::new("Person".to_string());
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
}
