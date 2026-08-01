use zip::result::ZipError;

#[derive(Debug)]
pub enum PageOperationError {
    NotInitialized,
    ZipError(ZipError),
    FileStorageError(String),
}

impl From<ZipError> for PageOperationError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}

impl From<crate::file_storage::FileStorageError> for PageOperationError {
    fn from(src: crate::file_storage::FileStorageError) -> Self {
        Self::FileStorageError(format!("{}", src))
    }
}
