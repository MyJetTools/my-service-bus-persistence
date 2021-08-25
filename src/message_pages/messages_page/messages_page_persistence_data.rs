use std::sync::Arc;

use my_azure_storage_sdk::AzureStorageError;

use crate::{
    app::AppContext, azure_storage::messages_page_blob::MessagesPageBlob,
    message_pages::MessagePageId,
};

pub enum MessagesPageStorageType {
    UncompressedPage,
    CompressedPage,
}

pub struct MessagesPageStorage {
    pub blob: Option<MessagesPageBlob>,
    pub storage_type: Option<MessagesPageStorageType>,
}

impl MessagesPageStorage {
    pub fn new(
        blob: Option<MessagesPageBlob>,
        storage_type: Option<MessagesPageStorageType>,
    ) -> Self {
        Self { blob, storage_type }
    }

    pub async fn initialize_uncompressed_blob(
        &mut self,
        topic_id: String,
        page_id: MessagePageId,
        app: Arc<AppContext>,
    ) -> Result<(), AzureStorageError> {
        let mut page_blob = MessagesPageBlob::new(topic_id, page_id, app);

        page_blob.create_new().await?;

        self.blob = Some(page_blob);
        self.storage_type = Some(MessagesPageStorageType::UncompressedPage);

        Ok(())
    }
}
