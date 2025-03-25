//! # Storage Module
//!
//! This module provides a `Storage` struct that acts as an abstraction layer for interacting with
//! a FoundationDB database. It supports basic CRUD (Create, Read, Update, Delete) operations and
//! is designed to work with asynchronous code using the Tokio runtime.
//!
//! The operations exposed by the `Storage` struct include:
//!
//! - `set`: Store a key-value pair in the database.
//! - `get`: Retrieve the value associated with a specific key.
//! - `delete`: Remove a key-value pair from the database.
//! - `flip_atomic_bool`: Perform an atomic operation to modify a boolean-like value at a given key.
//!
//! ## Notes
//!
//! - All methods in `Storage` return a `Result` to handle potential errors during database access.
//! - The module assumes that a properly configured FoundationDB instance is available for use.
//! - Correct usage of the `foundationdb` library is required to avoid database-related issues.
//!
//! ## Testing
//!
//! The module also includes unit tests to verify the correctness of its functionality. The tests
//! rely on the `fdb_testcontainer` crate, which sets up a test instance of FoundationDB.

use foundationdb::future::FdbValue;
use foundationdb::{Database, FdbBindingError, RangeOption};
use futures::Stream;
use futures_util::stream::StreamExt;
use futures_util::TryStreamExt;
use std::sync::Arc;

const MAX_SCAN_SIZE: usize = 20;

#[derive(Clone)]
pub struct Storage {
    database: Arc<Database>,
}

impl Storage {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Sets a key-value pair in the FoundationDB database.
    ///
    /// # Parameters
    ///
    /// * `key`: A byte slice representing the key to store in the database.
    /// * `value`: A byte slice representing the value associated with the key.
    ///
    /// # Returns
    ///
    /// Returns a `Result` which is `Ok(())` if the operation succeeds, or an error of type `crate::errors::Error`
    /// if the operation fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if the transaction to set the key-value
    /// pair in FoundationDB cannot be completed.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> crate::errors::Result<()> {
        self.database
            .run(|trx, _| async move {
                trx.set(key, value);
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Retrieves the value associated with the given key from the FoundationDB database.
    ///
    /// # Parameters
    ///
    /// * `key`: A byte slice representing the key to retrieve from the database.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing either:
    /// - `Ok(Some(Vec<u8>))`: If the key exists in the database, the value is returned as a `Vec<u8>`.
    /// - `Ok(None)`: If the key does not exist in the database.
    /// - `Err`: If there is an error during database communication.
    ///
    /// # Errors
    ///
    /// This method will return an error if the transaction to retrieve the value
    /// from the FoundationDB database cannot be completed.
    pub async fn get(&self, key: &[u8]) -> crate::errors::Result<Option<Vec<u8>>> {
        let value = self
            .database
            .run(|trx, _| async move { Ok(trx.get(key, true).await?) })
            .await?;
        let value = value.map(|v| v.to_vec());
        Ok(value)
    }

    /// Deletes a key-value pair from the FoundationDB database.
    ///
    /// # Parameters
    ///
    /// * `key`: A byte slice representing the key to be deleted from the database.
    ///
    /// # Returns
    ///
    /// Returns a `Result` which is `Ok(())` if the operation succeeds, or an error of type `crate::errors::Error`
    /// if the operation fails.
    ///
    /// # Errors
    ///
    /// This method will return an error if the transaction to delete the key-value
    /// pair from the FoundationDB database cannot be completed.
    pub async fn delete(&self, key: &[u8]) -> crate::errors::Result<()> {
        self.database
            .run(|trx, _| async move {
                trx.clear(key);
                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Scans a range of key-value pairs in the FoundationDB database.
    ///
    /// # Parameters
    ///
    /// * `start`: A byte slice representing the starting key of the range (inclusive).
    /// * `end`: A byte slice representing the ending key of the range (exclusive).
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing:
    /// - `Ok(Vec<(Vec<u8>, Vec<u8>)>)`: A vector of key-value pairs within the specified range.
    /// - `Err`: If there is an error during the range scan operation.
    ///
    /// # Errors
    ///
    /// This method will return an error if the transaction to scan the range
    /// of key-value pairs in FoundationDB cannot be completed.
    pub async fn scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> crate::errors::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let kvs = self
            .database
            .run(|trx, _| async move {
                let range = RangeOption::from((start, end));
                let stream = trx.get_ranges_keyvalues(range, false);
                collect_stream(stream).await
            })
            .await?;

        Ok(kvs)
    }

    /// Performs a full key-value scan over a specified range in the FoundationDB database.
    ///
    /// The scan uses a streaming approach to retrieve large amounts of data without loading
    /// everything into memory at once. Key-value pairs are retrieved incrementally, starting from
    /// the given `start` key and continuing up to (but not including) the specified `end` key.
    ///
    /// # Parameters
    ///
    /// * `start`: A byte slice representing the starting key of the range (inclusive).
    /// * `end`: A byte slice representing the ending key of the range (exclusive).
    ///
    /// # Returns
    ///
    /// Returns an asynchronous stream that yields `Result` values:
    /// - `Ok<(Vec<u8>, Vec<u8>)>`: A key-value pair as `Vec<u8>`, representing a record in the database.
    /// - `Err`: If there is an error during the full scan operation.
    ///
    /// The stream will terminate once all key-value pairs in the specified range have been iterated over.
    ///
    /// # Errors
    ///
    /// This method will return an error for any issues that happen during transactions or communication
    /// with the FoundationDB database. Streaming will stop immediately upon encountering the first error.
    pub async fn full_scan(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> impl Stream<Item = crate::errors::Result<(Vec<u8>, Vec<u8>)>> {
        let mut start = start.to_vec();
        async_stream::try_stream! {
            loop {
                let kvs = self.scan(&start, end).await?;
                if kvs.is_empty() {
                    break;
                }
                if let Some(kv) = kvs.last() {
                    start = kv.0.to_vec();
                    start.push(0xff);
                }
                for kv in kvs {
                    yield kv;
                }
            }
        }
    }
}

/// Collects key-value pairs from a FoundationDB stream.
///
/// # Parameters
///
/// * `stream`: A stream of `foundationdb::FdbResult<FdbValue>` that represents the results
///   of a range scan query or a similar operation in FoundationDB.
///
/// # Returns
///
/// Returns a `Result` containing:
/// - `Ok(Vec<(Vec<u8>, Vec<u8>)>)`: A vector of tuples, where each tuple contains the key and value as `Vec<u8>`,
///   if the stream is successfully processed.
/// - `Err(FdbBindingError)`: If an error occurs while collecting the data from the stream.
///
/// # Errors
///
/// This function will return an error if the input stream yields any errors when processed.
async fn collect_stream<S>(stream: S) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbBindingError>
where
    S: futures_util::Stream<Item = foundationdb::FdbResult<FdbValue>>,
{
    let records = stream
        .map(|x| match x {
            Ok(value) => {
                let data = (value.key().to_vec(), value.value().to_vec());
                Ok(data)
            }
            Err(err) => Err(err),
        })
        .take(20)
        .try_collect::<Vec<(Vec<u8>, Vec<u8>)>>()
        .await?;
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fdb_testcontainer::get_db_once;
    use foundationdb_tuple::pack;

    #[tokio::test]
    async fn test_database() {
        let _guard = get_db_once().await;
        let storage = Storage::new(_guard.clone());

        storage
            .set(b"key", b"value")
            .await
            .expect("Unable to set key");
        let result = storage.get(b"key").await.expect("Unable to get key");
        assert_eq!(result, Some(b"value".to_vec()));
        storage.delete(b"key").await.expect("Unable to delete key");
        let result = storage
            .get(b"key")
            .await
            .expect("Unable to get key after delete");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_scan() {
        let _guard = get_db_once().await;
        let storage = Storage::new(_guard.clone());

        storage
            .set(b"key1", b"value1")
            .await
            .expect("Unable to set key1");
        storage
            .set(b"key2", b"value2")
            .await
            .expect("Unable to set key2");
        storage
            .set(b"key3", b"value3")
            .await
            .expect("Unable to set key3");
        let result = storage
            .scan(b"key1", b"key3")
            .await
            .expect("Unable to scan");
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_bigger_than_max_scan() {
        let _guard = get_db_once().await;
        let storage = Storage::new(_guard.clone());

        for i in 0..MAX_SCAN_SIZE + 1 {
            let key = pack(&("key", &i));
            storage
                .set(&key, format!("value{}", i).as_bytes())
                .await
                .expect("Unable to set key");
        }

        let start = pack(&("key", &0));
        let end = pack(&("key", &100));

        let result = storage.scan(&start, &end).await.expect("Unable to scan");
        assert_eq!(result.len(), MAX_SCAN_SIZE);
    }

    #[tokio::test]
    async fn test_full_scan() {
        let _guard = get_db_once().await;
        let storage = Storage::new(_guard.clone());

        for i in 0..100 {
            let key = pack(&("key", &i));
            storage
                .set(&key, format!("value{}", i).as_bytes())
                .await
                .expect("Unable to set key");
        }

        let start = pack(&("key", &0));
        let end = pack(&("key", &100));

        let result = storage
            .full_scan(&start, &end)
            .await
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result.len(), 100);
    }
}
