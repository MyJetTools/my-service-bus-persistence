use zip::result::ZipError;

#[derive(Debug)]
pub enum ReadCompressedPageError {
    ZipError(ZipError),
    DecodeError(prost::DecodeError),

    Other(String),
}

impl From<ZipError> for ReadCompressedPageError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}

impl From<prost::DecodeError> for ReadCompressedPageError {
    fn from(src: prost::DecodeError) -> Self {
        Self::DecodeError(src)
    }
}
