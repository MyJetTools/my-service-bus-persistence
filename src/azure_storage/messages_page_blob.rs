use std::sync::Arc;

use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};
use my_azure_page_blob_append::PageBlobAppendCacheError;
use my_azure_storage_sdk::{AzureConnection, AzureStorageError};
use my_service_bus_shared::protobuf_models::MessageProtobufModel;

use crate::{
    app::{AppContext, AppError},
    azure_storage::consts::generage_blob_name,
    message_pages::MessagePageId,
    operations::MessagesStream,
};

pub struct MessagesPageBlob {
    pub topic_id: String,
    pub page_id: MessagePageId,
    pub messages_stream: MessagesStream,
    pub app: Arc<AppContext>,
    pub blob: MyAzurePageBlob,
}

impl MessagesPageBlob {
    pub fn new(topic_id: String, page_id: MessagePageId, app: Arc<AppContext>) -> Self {
        let connection =
            AzureConnection::from_conn_string(app.settings.messages_connection_string.as_str());
        let blob_name = generage_blob_name(&page_id);

        let blob = MyAzurePageBlob::new(connection, topic_id.clone(), blob_name);

        let messages_stream =
            MessagesStream::new(app.settings.load_blob_pages_size, 16384, 1024 * 1024 * 1024);
        Self {
            topic_id,
            page_id,
            app,
            messages_stream: messages_stream,
            blob,
        }
    }

    pub async fn load(&mut self) -> Result<Vec<MessageProtobufModel>, PageBlobAppendCacheError> {
        let mut result = Vec::new();

        while let Some(message) = self
            .messages_stream
            .get_next_message(&mut self.blob)
            .await?
        {
            result.push(message);
        }

        Ok(result)
    }

    pub async fn save_messages(
        &mut self,
        messages: &[MessageProtobufModel],
    ) -> Result<(), AppError> {
        self.messages_stream
            .append(&mut self.blob, &messages)
            .await?;
        Ok(())
    }

    pub async fn get_wirte_position(&self) -> usize {
        self.messages_stream.get_write_position().await
    }

    pub async fn create_new(&mut self) -> Result<(), AzureStorageError> {
        self.blob.create_if_not_exists(0).await?;
        self.blob.resize(0).await?;
        Ok(())
    }
}
