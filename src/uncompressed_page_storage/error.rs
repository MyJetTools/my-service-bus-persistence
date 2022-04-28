#[derive(Clone, Debug)]
pub enum UncompressedStorageError {
    Corrupted,
    FileNotFound,
    OtherError(String),
}

impl From<std::io::Error> for UncompressedStorageError {
    fn from(src: std::io::Error) -> Self {
        if src.kind() == std::io::ErrorKind::NotFound {
            return UncompressedStorageError::FileNotFound;
        }

        Self::OtherError(format!("{:?}", src))
    }
}

impl From<prost::DecodeError> for UncompressedStorageError {
    fn from(src: prost::DecodeError) -> Self {
        Self::OtherError(format!("{:?}", src))
    }
}
