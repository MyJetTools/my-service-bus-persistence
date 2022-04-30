use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess, RandomAccessData};

use super::{
    load_trait::LoadFromStorage,
    toc::{MessageContentOffset, UncompressedFileToc, TOC_SIZE, TOC_SIZE_IN_PAGES},
    upload_payload::PayloadsToUploadContainer,
    UncompressedStorageError,
};

pub struct UncompressedPageStorage {
    page_blob: PageBlobRandomAccess,
}

impl UncompressedPageStorage {
    pub fn new(page_blob: PageBlobRandomAccess) -> Self {
        Self { page_blob }
    }
    pub async fn read_toc(&mut self) -> UncompressedFileToc {
        let size = self.page_blob.get_blob_size(Some(TOC_SIZE_IN_PAGES)).await;

        if size < TOC_SIZE {
            self.page_blob.resize_blob(TOC_SIZE_IN_PAGES).await;
        }

        let content = self
            .page_blob
            .load_pages(
                &PageBlobPageId::new(0),
                TOC_SIZE_IN_PAGES,
                Some(TOC_SIZE_IN_PAGES),
            )
            .await;

        UncompressedFileToc::new(content)
    }

    pub async fn read_content(&mut self, offset: &MessageContentOffset) -> RandomAccessData {
        self.page_blob
            .read_randomly(offset.offset, offset.size)
            .await
    }

    pub async fn append_payload(
        &mut self,
        payload_to_upload: PayloadsToUploadContainer,
    ) -> Result<(), UncompressedStorageError> {
        todo!("Implement")
    }
}
