use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess};

use super::{
    load_trait::LoadFromStorage,
    toc::{UncompressedFileToc, TOC_SIZE, TOC_SIZE_IN_PAGES},
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

    pub async fn append_payload(
        &mut self,
        payload_to_upload: PayloadsToUploadContainer,
    ) -> Result<(), UncompressedStorageError> {
        todo!("Implement")
    }
}

async fn load_messages_from_storage<TLoadFromStorage: LoadFromStorage>(
    as_file: &mut TLoadFromStorage,
    max_message_size: usize,
) -> Result<Vec<MessageProtobufModel>, UncompressedStorageError> {
    let mut result = Vec::new();

    let mut pos = 0;

    loop {
        let mut len = [0u8; 4];

        if !as_file.read(len.as_mut()).await? {
            as_file.reset_file(pos).await?;
        }

        let len = u32::from_le_bytes(len) as usize;

        if len == 0 {
            return Ok(result);
        }

        if len > max_message_size {
            as_file.reset_file(pos).await?;
            return Err(UncompressedStorageError::Corrupted);
        }

        let mut payload = vec![0u8; len];

        if !as_file.read(payload.as_mut()).await? {
            as_file.reset_file(pos).await?;
        }

        let model = prost::Message::decode(payload.as_slice())?;

        result.push(model);

        pos += (len as u64) + 4;
    }
}
