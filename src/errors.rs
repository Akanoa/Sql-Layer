pub type Result<T> = std::result::Result<T, SqlLayerError>;

#[derive(Debug, thiserror::Error)]
pub enum SqlLayerError {
    #[error("FoundationDB error : {0}")]
    Fdb(#[from] foundationdb::FdbBindingError),
    #[error("FoundationDB error : {0}")]
    FdbError(#[from] foundationdb::FdbError),
    #[error("Apache Avro error : {0}")]
    Avro(#[from] apache_avro::Error),
}
