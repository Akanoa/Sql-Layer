use foundationdb::FdbBindingError;

pub type Result<T> = std::result::Result<T, SqlLayerError>;

#[derive(Debug, thiserror::Error)]
pub enum SqlLayerError {
    #[error("FoundationDB error : {0}")]
    Fdb(#[from] foundationdb::FdbBindingError),
    #[error("FoundationDB error : {0}")]
    FdbError(#[from] foundationdb::FdbError),
    #[error("Apache Avro error : {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("Missing column in primary key: {0}")]
    MissingColumn(String),
    #[error("Mismatched column type in primary key: {0} vs {1}")]
    MismatchedColumnType(String, String),
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("Index not found: {0}")]
    IndexNotFound(String),
}

impl From<SqlLayerError> for FdbBindingError {
    fn from(value: SqlLayerError) -> Self {
        FdbBindingError::CustomError(Box::new(value))
    }
}
