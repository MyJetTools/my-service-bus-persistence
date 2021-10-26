use my_azure_page_blob_append::PageBlobAppendError;

pub enum ReadingUncompressedMessagesError {
    PageBlobAppendError(PageBlobAppendError),
    CorruptedContent { reason: String, pos: usize },
}

impl From<PageBlobAppendError> for ReadingUncompressedMessagesError {
    fn from(src: PageBlobAppendError) -> Self {
        ReadingUncompressedMessagesError::PageBlobAppendError(src)
    }
}
